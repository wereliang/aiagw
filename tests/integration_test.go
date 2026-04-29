package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	pb "github.com/wereliang/aiagw/api/proto"
	"github.com/wereliang/aiagw/internal/agent"
	"github.com/wereliang/aiagw/internal/handler"
	"github.com/wereliang/aiagw/internal/openai"
	"github.com/wereliang/aiagw/internal/router"
	"github.com/wereliang/aiagw/internal/server"
	"github.com/wereliang/aiagw/internal/session"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestEndToEnd_ChatCompletions(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer redisClient.Close()

	ctx := context.Background()
	_ = ctx

	gatewayInstance := "test-gw-1"
	validAPIKeys := map[string]bool{"e2e-key": true}

	sessionMgr := session.NewManager(redisClient, 30*time.Minute)
	pool := agent.NewPool(redisClient, gatewayInstance, 60*time.Second)
	grpcSrv := agent.NewGRPCServer(pool)
	rtr := router.New(pool)

	chatHandler := handler.NewChatHandler(sessionMgr, grpcSrv, rtr, pool, nil, gatewayInstance)
	modelsHandler := handler.NewModelsHandler(pool)
	sessionHandler := handler.NewSessionHandler(sessionMgr)

	logger := zap.NewNop()

	grpcLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen for gRPC: %v", err)
	}
	grpcAddr := grpcLis.Addr().String()

	grpcServer := grpc.NewServer()
	pb.RegisterAgentGatewayServer(grpcServer, grpcSrv)

	go func() {
		if err := grpcServer.Serve(grpcLis); err != nil {
			t.Logf("gRPC server stopped: %v", err)
		}
	}()
	defer grpcServer.GracefulStop()

	httpLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen for HTTP: %v", err)
	}
	httpAddr := httpLis.Addr().String()

	httpPort := httpLis.Addr().(*net.TCPAddr).Port
	httpSrv := server.NewHTTPServer(httpPort, logger, validAPIKeys, chatHandler, modelsHandler, sessionHandler)

	go func() {
		if err := http.Serve(httpLis, httpSrv.Engine()); err != nil && err != http.ErrServerClosed {
			t.Logf("HTTP server stopped: %v", err)
		}
	}()

	conn, err := grpc.NewClient(
		grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create gRPC client: %v", err)
	}
	defer conn.Close()

	agentClient := pb.NewAgentGatewayClient(conn)
	stream, err := agentClient.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect agent stream: %v", err)
	}

	if err := stream.Send(&pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   "echo-agent",
				AgentType: "echo",
			},
		},
	}); err != nil {
		t.Fatalf("failed to send register: %v", err)
	}

	agentDone := make(chan struct{})
	go func() {
		defer close(agentDone)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			req := msg.GetRequest()
			if req == nil {
				continue
			}
			lastMsg := req.Messages[len(req.Messages)-1]
			sessionID := req.SessionId
			if sessionID == "" {
				sessionID = "agent-session-" + req.RequestId
			}
			if sendErr := stream.Send(&pb.AgentMessage{
				Payload: &pb.AgentMessage_Response{
					Response: &pb.AgentResponse{
						RequestId: req.RequestId,
						SessionId: sessionID,
						Content: &pb.AgentResponse_Message{
							Message: &pb.ChatMessage{
								Role:    "assistant",
								Content: "echo: " + lastMsg.Content,
							},
						},
						Done: true,
					},
				},
			}); sendErr != nil {
				return
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)

	reqBody := openai.ChatCompletionRequest{
		Model: "echo",
		Messages: []openai.Message{
			{Role: "user", Content: "hello world"},
		},
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	httpReq, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("http://%s/v1/chat/completions", httpAddr),
		bytes.NewReader(bodyBytes),
	)
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer e2e-key")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d; body: %s", resp.StatusCode, string(respBody))
	}

	var chatResp openai.ChatCompletionResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		t.Fatalf("failed to unmarshal response: %v; body: %s", err, string(respBody))
	}

	if len(chatResp.Choices) == 0 {
		t.Fatal("expected at least one choice in response")
	}

	choice := chatResp.Choices[0]
	if choice.Message == nil {
		t.Fatal("expected message in first choice")
	}

	expectedContent := "echo: hello world"
	if choice.Message.Content != expectedContent {
		t.Errorf("expected content %q, got %q", expectedContent, choice.Message.Content)
	}

	sessionID := resp.Header.Get("X-Session-Id")
	if sessionID == "" {
		t.Error("expected X-Session-Id header to be set")
	}
	if !strings.HasPrefix(sessionID, "echo:agent-session-") {
		t.Errorf("expected composite session ID with echo: prefix, got %q", sessionID)
	}

	if chatResp.Model != "echo" {
		t.Errorf("expected model %q, got %q", "echo", chatResp.Model)
	}

	if err := stream.CloseSend(); err != nil {
		t.Logf("failed to close agent stream: %v", err)
	}
}
