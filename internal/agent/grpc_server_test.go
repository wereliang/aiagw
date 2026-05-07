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
	"google.golang.org/grpc/credentials/insecure"
)

func setupGRPCServer(t *testing.T) (pb.AgentGatewayClient, *GRPCServer, *redis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	pool := NewPool(rdb, testGatewayInstance, testHeartbeatTimeout)
	srv := NewGRPCServer(pool)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAgentGatewayServer(grpcServer, srv)

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

	client := pb.NewAgentGatewayClient(conn)

	return client, srv, rdb
}

func TestAgentConnectAndRegister(t *testing.T) {
	client, srv, rdb := setupGRPCServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect returned unexpected error: %v", err)
	}

	regMsg := &pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   "agent-1",
				AgentType: "openai",
			},
		},
	}

	if err := stream.Send(regMsg); err != nil {
		t.Fatalf("Send register returned unexpected error: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	vals, err := rdb.HGetAll(ctx, "agent:openai:agent-1").Result()
	if err != nil {
		t.Fatalf("HGetAll returned unexpected error: %v", err)
	}
	if len(vals) == 0 {
		t.Fatal("agent hash not found in Redis after registration")
	}
	if vals["agent_type"] != "openai" {
		t.Errorf("agent_type in Redis = %q, want %q", vals["agent_type"], "openai")
	}
	if vals["status"] != StatusOnline {
		t.Errorf("status in Redis = %q, want %q", vals["status"], StatusOnline)
	}

	if !srv.HasLocalAgent("agent-1") {
		t.Error("HasLocalAgent returned false for connected agent")
	}

	if srv.HasLocalAgent("nonexistent") {
		t.Error("HasLocalAgent returned true for nonexistent agent")
	}
}

func TestSendRequestToLocalAgent(t *testing.T) {
	client, srv, _ := setupGRPCServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect returned unexpected error: %v", err)
	}

	regMsg := &pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   "agent-2",
				AgentType: "openai",
			},
		},
	}
	if err := stream.Send(regMsg); err != nil {
		t.Fatalf("Send register returned unexpected error: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	req := &pb.AgentRequest{
		RequestId: "req-1",
		SessionId: "sess-1",
		Model:     "gpt-4",
	}

	if err := srv.SendRequest("agent-2", req); err != nil {
		t.Fatalf("SendRequest returned unexpected error: %v", err)
	}

	gwMsg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv returned unexpected error: %v", err)
	}

	gotReq := gwMsg.GetRequest()
	if gotReq == nil {
		t.Fatal("expected GatewayMessage with Request payload, got nil")
	}
	if gotReq.GetRequestId() != "req-1" {
		t.Errorf("RequestId = %q, want %q", gotReq.GetRequestId(), "req-1")
	}
	if gotReq.GetModel() != "gpt-4" {
		t.Errorf("Model = %q, want %q", gotReq.GetModel(), "gpt-4")
	}
}

func TestSendRequestAgentNotFound(t *testing.T) {
	_, srv, _ := setupGRPCServer(t)

	req := &pb.AgentRequest{
		RequestId: "req-1",
		SessionId: "sess-1",
	}

	err := srv.SendRequest("nonexistent", req)
	if err != ErrAgentNotFound {
		t.Fatalf("expected ErrAgentNotFound, got %v", err)
	}
}

func TestResponseHandler(t *testing.T) {
	client, srv, _ := setupGRPCServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect returned unexpected error: %v", err)
	}

	regMsg := &pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   "agent-3",
				AgentType: "openai",
			},
		},
	}
	if err := stream.Send(regMsg); err != nil {
		t.Fatalf("Send register returned unexpected error: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	gotResponse := make(chan *pb.AgentResponse, 1)
	srv.RegisterResponseHandler("req-42", func(resp *pb.AgentResponse) {
		gotResponse <- resp
	})
	defer srv.UnregisterResponseHandler("req-42")

	respMsg := &pb.AgentMessage{
		Payload: &pb.AgentMessage_Response{
			Response: &pb.AgentResponse{
				RequestId: "req-42",
				SessionId: "sess-1",
				Content: &pb.AgentResponse_Message{
					Message: &pb.ChatMessage{
						Role:    "assistant",
						Content: []byte("Hello!"),
					},
				},
				Done: true,
			},
		},
	}
	if err := stream.Send(respMsg); err != nil {
		t.Fatalf("Send response returned unexpected error: %v", err)
	}

	select {
	case resp := <-gotResponse:
		if resp.GetRequestId() != "req-42" {
			t.Errorf("RequestId = %q, want %q", resp.GetRequestId(), "req-42")
		}
		if string(resp.GetMessage().GetContent()) != "Hello!" {
			t.Errorf("Content = %q, want %q", resp.GetMessage().GetContent(), "Hello!")
		}
		if !resp.GetDone() {
			t.Error("Done = false, want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for response handler to be called")
	}
}
