package agent

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/wereliang/aiagw/api/proto"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func setupInternalServer(t *testing.T) (
	pb.GatewayInternalClient,
	pb.AgentGateway_ConnectClient,
	*GRPCServer,
) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	pool := NewPool(rdb, testGatewayInstance, testHeartbeatTimeout)
	srv := NewGRPCServer(pool)
	internalSrv := NewInternalServer(srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAgentGatewayServer(grpcServer, srv)
	pb.RegisterGatewayInternalServer(grpcServer, internalSrv)

	go func() {
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
		}
	}()
	t.Cleanup(func() { grpcServer.Stop() })

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create gRPC client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	agentClient := pb.NewAgentGatewayClient(conn)
	ctx := context.Background()

	agentStream, err := agentClient.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect returned unexpected error: %v", err)
	}

	regMsg := &pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   "agent-internal-1",
				AgentType: "openai",
			},
		},
	}
	if err := agentStream.Send(regMsg); err != nil {
		t.Fatalf("Send register returned unexpected error: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	internalClient := pb.NewGatewayInternalClient(conn)

	return internalClient, agentStream, srv
}

func TestInternalForwardRequest(t *testing.T) {
	internalClient, agentStream, _ := setupInternalServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "x-agent-id", "agent-internal-1")

	req := &pb.AgentRequest{
		RequestId: "fwd-req-1",
		SessionId: "fwd-sess-1",
		Model:     "gpt-4",
		Messages: []*pb.ChatMessage{
			{Role: "user", Content: []byte("Hello from peer gateway")},
		},
	}

	stream, err := internalClient.ForwardRequest(ctx, req)
	if err != nil {
		t.Fatalf("ForwardRequest returned unexpected error: %v", err)
	}

	gwMsg, err := agentStream.Recv()
	if err != nil {
		t.Fatalf("agent Recv returned unexpected error: %v", err)
	}

	gotReq := gwMsg.GetRequest()
	if gotReq == nil {
		t.Fatal("expected GatewayMessage with Request payload, got nil")
	}
	if gotReq.GetRequestId() != "fwd-req-1" {
		t.Errorf("RequestId = %q, want %q", gotReq.GetRequestId(), "fwd-req-1")
	}

	chunkMsg := &pb.AgentMessage{
		Payload: &pb.AgentMessage_Response{
			Response: &pb.AgentResponse{
				RequestId: "fwd-req-1",
				SessionId: "fwd-sess-1",
				Content: &pb.AgentResponse_Chunk{
					Chunk: &pb.StreamChunk{Content: "Hello "},
				},
				Done: false,
			},
		},
	}
	if err := agentStream.Send(chunkMsg); err != nil {
		t.Fatalf("agent Send chunk returned unexpected error: %v", err)
	}

	doneMsg := &pb.AgentMessage{
		Payload: &pb.AgentMessage_Response{
			Response: &pb.AgentResponse{
				RequestId: "fwd-req-1",
				SessionId: "fwd-sess-1",
				Content: &pb.AgentResponse_Message{
					Message: &pb.ChatMessage{
						Role:    "assistant",
						Content: []byte("Hello from agent!"),
					},
				},
				Done: true,
			},
		},
	}
	if err := agentStream.Send(doneMsg); err != nil {
		t.Fatalf("agent Send done returned unexpected error: %v", err)
	}

	resp1, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv() #1 error: %v", err)
	}
	if resp1.GetChunk().GetContent() != "Hello " {
		t.Errorf("resp1 chunk content = %q, want %q", resp1.GetChunk().GetContent(), "Hello ")
	}

	resp2, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv() #2 error: %v", err)
	}
	if string(resp2.GetMessage().GetContent()) != "Hello from agent!" {
		t.Errorf("resp2 message content = %q, want %q", resp2.GetMessage().GetContent(), "Hello from agent!")
	}
	if !resp2.GetDone() {
		t.Error("resp2 Done = false, want true")
	}
}

func TestInternalForwardRequestMissingMetadata(t *testing.T) {
	internalClient, _, _ := setupInternalServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.AgentRequest{
		RequestId: "fwd-req-2",
		SessionId: "fwd-sess-2",
	}

	stream, err := internalClient.ForwardRequest(ctx, req)
	if err != nil {
		t.Fatalf("ForwardRequest returned unexpected error: %v", err)
	}

	_, recvErr := stream.Recv()
	if recvErr == nil {
		t.Fatal("expected error from Recv, got nil")
	}

	st, ok := status.FromError(recvErr)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", recvErr)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("status code = %v, want %v", st.Code(), codes.InvalidArgument)
	}
}

func TestInternalForwardRequestAgentNotFound(t *testing.T) {
	internalClient, _, _ := setupInternalServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "x-agent-id", "nonexistent-agent")

	req := &pb.AgentRequest{
		RequestId: "fwd-req-3",
		SessionId: "fwd-sess-3",
	}

	stream, err := internalClient.ForwardRequest(ctx, req)
	if err != nil {
		t.Fatalf("ForwardRequest returned unexpected error: %v", err)
	}

	_, recvErr := stream.Recv()
	if recvErr == nil {
		t.Fatal("expected error from Recv, got nil")
	}

	st, ok := status.FromError(recvErr)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", recvErr)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("status code = %v, want %v", st.Code(), codes.NotFound)
	}
}
