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
	"google.golang.org/grpc/metadata"
)

func TestForwarderRegisterAndLookup(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	f := NewForwarder(client, "gw-1", "localhost:9091", 60*time.Second)

	if err := f.RegisterInstance(ctx); err != nil {
		t.Fatalf("RegisterInstance() error: %v", err)
	}

	addr, err := f.LookupInstance(ctx, "gw-1")
	if err != nil {
		t.Fatalf("LookupInstance() error: %v", err)
	}
	if addr != "localhost:9091" {
		t.Errorf("addr = %q, want %q", addr, "localhost:9091")
	}
}

func TestForwarderLookupNotFound(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	f := NewForwarder(client, "gw-1", "localhost:9091", 60*time.Second)

	_, err := f.LookupInstance(ctx, "gw-nonexistent")
	if err != ErrGatewayNotFound {
		t.Errorf("LookupInstance() error = %v, want ErrGatewayNotFound", err)
	}
}

// mockInternalServer echoes the request back as a single AgentResponse and
// verifies x-agent-id metadata.
type mockInternalServer struct {
	pb.UnimplementedGatewayInternalServer
	receivedAgentID string
}

func (s *mockInternalServer) ForwardRequest(req *pb.AgentRequest, stream grpc.ServerStreamingServer[pb.AgentResponse]) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if ok {
		vals := md.Get("x-agent-id")
		if len(vals) > 0 {
			s.receivedAgentID = vals[0]
		}
	}

	return stream.Send(&pb.AgentResponse{
		RequestId: req.GetRequestId(),
		SessionId: req.GetSessionId(),
		Done:      true,
	})
}

func TestForwarderForwardToRemote(t *testing.T) {
	// Start a mock gRPC server.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := grpc.NewServer()
	mock := &mockInternalServer{}
	pb.RegisterGatewayInternalServer(srv, mock)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	// Create a Forwarder (Redis not needed for this test but required by
	// NewForwarder).
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	f := NewForwarder(client, "gw-test", "localhost:0", 60*time.Second)
	t.Cleanup(f.Close)

	ctx := context.Background()
	req := &pb.AgentRequest{
		RequestId: "req-42",
		SessionId: "sess-1",
	}

	respCh, err := f.ForwardToRemote(ctx, lis.Addr().String(), "agent-echo", req)
	if err != nil {
		t.Fatalf("ForwardToRemote() error: %v", err)
	}

	var responses []*pb.AgentResponse
	for resp := range respCh {
		responses = append(responses, resp)
	}

	if len(responses) != 1 {
		t.Fatalf("got %d responses, want 1", len(responses))
	}
	if responses[0].RequestId != "req-42" {
		t.Errorf("RequestId = %q, want %q", responses[0].RequestId, "req-42")
	}
	if !responses[0].Done {
		t.Error("Done = false, want true")
	}
	if mock.receivedAgentID != "agent-echo" {
		t.Errorf("receivedAgentID = %q, want %q", mock.receivedAgentID, "agent-echo")
	}
}
