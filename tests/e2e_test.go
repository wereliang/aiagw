package tests

import (
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

// ===== Auth =====

func TestE2E_Auth_MissingHeader(t *testing.T) {
	h := newTestHarness(t)

	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/v1/chat/completions", h.HTTPAddr), strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestE2E_Auth_InvalidKey(t *testing.T) {
	h := newTestHarness(t)

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "echo",
		Messages: []openai.Message{{Role: "user", Content: "hi"}},
		APIKey:   "bad-key",
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestE2E_Auth_ValidKey(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectEchoAgent(t, "auth-echo-1", "echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "echo",
		Messages: []openai.Message{{Role: "user", Content: "hi"}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusOK, body)
	}
}

// ===== Non-Stream Chat =====

func TestE2E_Chat_NonStream_Echo(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectEchoAgent(t, "echo-1", "echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "echo",
		Messages: []openai.Message{{Role: "user", Content: "hello world"}},
	})

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status = %d; body: %s", resp.StatusCode, body)
	}

	chatResp := readChatResponse(t, resp)
	if len(chatResp.Choices) == 0 {
		t.Fatal("expected at least one choice")
	}
	if chatResp.Choices[0].Message == nil {
		t.Fatal("expected message in choice")
	}
	if got := chatResp.Choices[0].Message.Content; got != "echo: hello world" {
		t.Errorf("content = %q, want %q", got, "echo: hello world")
	}
}

func TestE2E_Chat_NonStream_EmptyMessages(t *testing.T) {
	h := newTestHarness(t)

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "echo",
		Messages: []openai.Message{},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestE2E_Chat_NonStream_NoAgent(t *testing.T) {
	h := newTestHarness(t)

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "nonexistent",
		Messages: []openai.Message{{Role: "user", Content: "hello"}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestE2E_Chat_NonStream_ResponseFormat(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectEchoAgent(t, "fmt-echo-1", "echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "echo",
		Messages: []openai.Message{{Role: "user", Content: "test"}},
	})

	chatResp := readChatResponse(t, resp)

	if chatResp.Object != "chat.completion" {
		t.Errorf("object = %q, want %q", chatResp.Object, "chat.completion")
	}
	if chatResp.Model != "echo" {
		t.Errorf("model = %q, want %q", chatResp.Model, "echo")
	}
	if !strings.HasPrefix(chatResp.ID, "chatcmpl-") {
		t.Errorf("id = %q, want prefix %q", chatResp.ID, "chatcmpl-")
	}
	if len(chatResp.Choices) != 1 {
		t.Fatalf("choices count = %d, want 1", len(chatResp.Choices))
	}
	if chatResp.Choices[0].FinishReason == nil || *chatResp.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason = %v, want %q", chatResp.Choices[0].FinishReason, "stop")
	}
}

func TestE2E_Chat_NonStream_SessionCreated(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectEchoAgent(t, "sess-echo-1", "echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "echo",
		Messages: []openai.Message{{Role: "user", Content: "test"}},
	})
	defer resp.Body.Close()

	sessionID := resp.Header.Get("X-Session-Id")
	if sessionID == "" {
		t.Fatal("expected X-Session-Id header")
	}
	if !strings.HasPrefix(sessionID, "echo:") {
		t.Errorf("session ID = %q, want prefix %q", sessionID, "echo:")
	}
}

// ===== Stream Chat =====

func TestE2E_Chat_Stream_Basic(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectStreamAgent(t, "stream-1", "stream-echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "stream-echo",
		Messages: []openai.Message{{Role: "user", Content: "hello world"}},
		Stream:   true,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d; body: %s", resp.StatusCode, body)
	}

	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	reader := NewSSEReader(resp.Body)
	chunks, err := reader.CollectChunks()
	if err != nil {
		t.Fatalf("collect chunks: %v", err)
	}

	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 chunks (data + final), got %d", len(chunks))
	}
}

func TestE2E_Chat_Stream_ContentAccumulation(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectStreamAgent(t, "acc-stream-1", "stream-echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "stream-echo",
		Messages: []openai.Message{{Role: "user", Content: "foo bar baz"}},
		Stream:   true,
	})
	defer resp.Body.Close()

	reader := NewSSEReader(resp.Body)
	chunks, err := reader.CollectChunks()
	if err != nil {
		t.Fatalf("collect chunks: %v", err)
	}

	var accumulated strings.Builder
	for _, chunk := range chunks {
		if len(chunk.Choices) > 0 && chunk.Choices[0].Delta.Content != nil {
			accumulated.WriteString(*chunk.Choices[0].Delta.Content)
		}
	}

	got := strings.TrimSpace(accumulated.String())
	if got != "foo bar baz" {
		t.Errorf("accumulated content = %q, want %q", got, "foo bar baz")
	}
}

func TestE2E_Chat_Stream_ChunkFormat(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectStreamAgent(t, "cfmt-stream-1", "stream-echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "stream-echo",
		Messages: []openai.Message{{Role: "user", Content: "hello"}},
		Stream:   true,
	})
	defer resp.Body.Close()

	reader := NewSSEReader(resp.Body)
	chunks, err := reader.CollectChunks()
	if err != nil {
		t.Fatalf("collect chunks: %v", err)
	}

	for i, chunk := range chunks {
		if chunk.Object != "chat.completion.chunk" {
			t.Errorf("chunk[%d].object = %q, want %q", i, chunk.Object, "chat.completion.chunk")
		}
		if chunk.Model != "stream-echo" {
			t.Errorf("chunk[%d].model = %q, want %q", i, chunk.Model, "stream-echo")
		}
		if !strings.HasPrefix(chunk.ID, "chatcmpl-") {
			t.Errorf("chunk[%d].id = %q, want prefix %q", i, chunk.ID, "chatcmpl-")
		}
		if len(chunk.Choices) != 1 {
			t.Errorf("chunk[%d].choices count = %d, want 1", i, len(chunk.Choices))
		}
	}
}

func TestE2E_Chat_Stream_FinalChunk(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectStreamAgent(t, "fin-stream-1", "stream-echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "stream-echo",
		Messages: []openai.Message{{Role: "user", Content: "hello"}},
		Stream:   true,
	})
	defer resp.Body.Close()

	reader := NewSSEReader(resp.Body)
	chunks, err := reader.CollectChunks()
	if err != nil {
		t.Fatalf("collect chunks: %v", err)
	}

	last := chunks[len(chunks)-1]
	if len(last.Choices) == 0 {
		t.Fatal("last chunk has no choices")
	}
	if last.Choices[0].FinishReason == nil || *last.Choices[0].FinishReason != "stop" {
		t.Errorf("last chunk finish_reason = %v, want %q", last.Choices[0].FinishReason, "stop")
	}
}

func TestE2E_Chat_Stream_SessionCreated(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectStreamAgent(t, "ssess-stream-1", "stream-echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "stream-echo",
		Messages: []openai.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	})
	defer resp.Body.Close()

	// Drain the stream to ensure headers are written
	reader := NewSSEReader(resp.Body)
	reader.CollectChunks()

	sessionID := resp.Header.Get("X-Session-Id")
	if sessionID == "" {
		t.Fatal("expected X-Session-Id header")
	}
	if !strings.HasPrefix(sessionID, "stream-echo:") {
		t.Errorf("session ID = %q, want prefix %q", sessionID, "stream-echo:")
	}
}

// ===== Session Management =====

func TestE2E_Session_Reuse(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectEchoAgent(t, "reuse-echo-1", "echo")

	resp1 := h.DoChat(t, ChatRequestOpts{
		Model:    "echo",
		Messages: []openai.Message{{Role: "user", Content: "first"}},
	})
	chatResp1 := readChatResponse(t, resp1)
	sessionID := resp1.Header.Get("X-Session-Id")
	if sessionID == "" {
		t.Fatal("expected session ID from first request")
	}

	resp2 := h.DoChat(t, ChatRequestOpts{
		Model:     "echo",
		Messages:  []openai.Message{{Role: "user", Content: "second"}},
		SessionID: sessionID,
	})
	chatResp2 := readChatResponse(t, resp2)

	returnedSessionID := resp2.Header.Get("X-Session-Id")
	if returnedSessionID != sessionID {
		t.Errorf("second request returned session %q, want %q", returnedSessionID, sessionID)
	}

	if chatResp1.Choices[0].Message.Content != "echo: first" {
		t.Errorf("first response = %q", chatResp1.Choices[0].Message.Content)
	}
	if chatResp2.Choices[0].Message.Content != "echo: second" {
		t.Errorf("second response = %q", chatResp2.Choices[0].Message.Content)
	}
}

func TestE2E_Session_Delete(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectEchoAgent(t, "del-echo-1", "echo")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "echo",
		Messages: []openai.Message{{Role: "user", Content: "hi"}},
	})
	readChatResponse(t, resp)
	sessionID := resp.Header.Get("X-Session-Id")
	if sessionID == "" {
		t.Fatal("expected session ID")
	}

	delResp := h.DoDelete(t, "/v1/sessions/"+sessionID)
	defer delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(delResp.Body)
		t.Fatalf("delete status = %d; body: %s", delResp.StatusCode, body)
	}

	var delBody map[string]interface{}
	json.NewDecoder(delResp.Body).Decode(&delBody)
	if deleted, _ := delBody["deleted"].(bool); !deleted {
		t.Error("expected deleted=true")
	}
}

func TestE2E_Session_DeleteNotFound(t *testing.T) {
	h := newTestHarness(t)

	resp := h.DoDelete(t, "/v1/sessions/nonexistent:session-id")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestE2E_Session_CompositeFormat(t *testing.T) {
	h := newTestHarness(t)

	var receivedSessionID string
	h.ConnectMockAgent(t, "comp-echo-1", "mytype", func(req *pb.AgentRequest, stream pb.AgentGateway_ConnectClient) {
		receivedSessionID = req.SessionId
		stream.Send(&pb.AgentMessage{
			Payload: &pb.AgentMessage_Response{
				Response: &pb.AgentResponse{
					RequestId: req.RequestId,
					SessionId: "agent-sess-123",
					Content: &pb.AgentResponse_Message{
						Message: &pb.ChatMessage{Role: "assistant", Content: "ok"},
					},
					Done: true,
				},
			},
		})
	})

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "mytype",
		Messages: []openai.Message{{Role: "user", Content: "hi"}},
	})
	readChatResponse(t, resp)

	compositeID := resp.Header.Get("X-Session-Id")
	if compositeID == "" {
		t.Fatal("expected X-Session-Id")
	}

	if !strings.HasPrefix(compositeID, "mytype:") {
		t.Errorf("composite ID = %q, want prefix %q", compositeID, "mytype:")
	}

	// Agent should receive empty session ID for a new request (no existing session)
	if receivedSessionID != "" {
		t.Errorf("agent received sessionID = %q, want empty (new request)", receivedSessionID)
	}
}

// ===== Routing =====

func TestE2E_Routing_ByModel(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectEchoAgent(t, "type-a-1", "type-a")
	h.ConnectEchoAgent(t, "type-b-1", "type-b")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "type-a",
		Messages: []openai.Message{{Role: "user", Content: "hi"}},
	})
	chatResp := readChatResponse(t, resp)
	if chatResp.Model != "type-a" {
		t.Errorf("model = %q, want %q", chatResp.Model, "type-a")
	}

	resp2 := h.DoChat(t, ChatRequestOpts{
		Model:    "type-b",
		Messages: []openai.Message{{Role: "user", Content: "hi"}},
	})
	chatResp2 := readChatResponse(t, resp2)
	if chatResp2.Model != "type-b" {
		t.Errorf("model = %q, want %q", chatResp2.Model, "type-b")
	}
}

func TestE2E_Routing_AgentDisconnect(t *testing.T) {
	h := newTestHarness(t)
	ma := h.ConnectEchoAgent(t, "disc-echo-1", "disc-type")

	// First request succeeds
	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "disc-type",
		Messages: []openai.Message{{Role: "user", Content: "hi"}},
	})
	readChatResponse(t, resp)

	// Disconnect the agent
	ma.stream.CloseSend()
	ma.conn.Close()
	time.Sleep(200 * time.Millisecond)

	// Second request should fail (no agent available)
	resp2 := h.DoChat(t, ChatRequestOpts{
		Model:    "disc-type",
		Messages: []openai.Message{{Role: "user", Content: "hi"}},
	})
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusServiceUnavailable {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("status = %d, want %d; body: %s", resp2.StatusCode, http.StatusServiceUnavailable, body)
	}
}

// ===== Agent Lifecycle =====

func TestE2E_Agent_StreamingAgentNonStreamRequest(t *testing.T) {
	h := newTestHarness(t)
	h.ConnectStreamAgent(t, "hybrid-stream-1", "hybrid")

	resp := h.DoChat(t, ChatRequestOpts{
		Model:    "hybrid",
		Messages: []openai.Message{{Role: "user", Content: "foo bar baz"}},
		Stream:   false,
	})

	chatResp := readChatResponse(t, resp)
	if len(chatResp.Choices) == 0 || chatResp.Choices[0].Message == nil {
		t.Fatal("expected message in response")
	}

	got := chatResp.Choices[0].Message.Content
	// Stream agent sends chunks ("foo ", "bar ", "baz ") then a done message ("foo bar baz").
	// collectNonStreamResponse accumulates all of them.
	want := "foo bar baz foo bar baz"
	if got != want {
		t.Errorf("content = %q, want %q", got, want)
	}
}

// ===== Cross-Gateway Forwarding =====

func TestE2E_Forward_RemoteAgent(t *testing.T) {
	// Gateway A: has the agent connected
	mrA := miniredis.RunT(t)
	redisClientA := redis.NewClient(&redis.Options{Addr: mrA.Addr()})
	t.Cleanup(func() { redisClientA.Close() })

	poolA := agent.NewPool(redisClientA, "gw-a", 60*time.Second)
	grpcSrvA := agent.NewGRPCServer(poolA)
	sessionMgrA := session.NewManager(redisClientA, 30*time.Minute)

	grpcLisA, _ := net.Listen("tcp", "localhost:0")
	grpcServerA := grpc.NewServer()
	pb.RegisterAgentGatewayServer(grpcServerA, grpcSrvA)
	go grpcServerA.Serve(grpcLisA)
	t.Cleanup(func() { grpcServerA.GracefulStop() })

	internalLisA, _ := net.Listen("tcp", "localhost:0")
	internalAddrA := internalLisA.Addr().String()
	forwarderA := agent.NewForwarder(redisClientA, "gw-a", internalAddrA, 60*time.Second)
	forwarderA.RegisterInstance(context.Background())
	t.Cleanup(func() {
		forwarderA.UnregisterInstance(context.Background())
		forwarderA.Close()
	})

	internalServerA := agent.NewInternalServer(grpcSrvA)
	internalGRPCSrvA := grpc.NewServer()
	pb.RegisterGatewayInternalServer(internalGRPCSrvA, internalServerA)
	go internalGRPCSrvA.Serve(internalLisA)
	t.Cleanup(func() { internalGRPCSrvA.GracefulStop() })

	// Connect echo agent to GW-A
	connA, _ := grpc.NewClient(grpcLisA.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	t.Cleanup(func() { connA.Close() })
	agentClientA := pb.NewAgentGatewayClient(connA)
	streamA, _ := agentClientA.Connect(context.Background())
	streamA.Send(&pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{AgentId: "remote-echo-1", AgentType: "remote-echo"},
		},
	})
	go func() {
		for {
			msg, err := streamA.Recv()
			if err != nil {
				return
			}
			req := msg.GetRequest()
			if req == nil {
				continue
			}
			lastMsg := req.Messages[len(req.Messages)-1]
			streamA.Send(&pb.AgentMessage{
				Payload: &pb.AgentMessage_Response{
					Response: &pb.AgentResponse{
						RequestId: req.RequestId,
						SessionId: "remote-sess-" + req.RequestId,
						Content: &pb.AgentResponse_Message{
							Message: &pb.ChatMessage{Role: "assistant", Content: "remote: " + lastMsg.Content},
						},
						Done: true,
					},
				},
			})
		}
	}()
	t.Cleanup(func() { streamA.CloseSend() })

	time.Sleep(100 * time.Millisecond)

	// Gateway B: no agent connected, uses same Redis, routes to GW-A via forwarding
	poolB := agent.NewPool(redisClientA, "gw-b", 60*time.Second)
	grpcSrvB := agent.NewGRPCServer(poolB)
	sessionMgrB := session.NewManager(redisClientA, 30*time.Minute)

	forwarderB := agent.NewForwarder(redisClientA, "gw-b", "localhost:0", 60*time.Second)
	forwarderB.RegisterInstance(context.Background())
	t.Cleanup(func() {
		forwarderB.UnregisterInstance(context.Background())
		forwarderB.Close()
	})

	rtrB := router.New(poolB)
	validKeys := map[string]bool{"fwd-key": true}
	chatHandlerB := handler.NewChatHandler(sessionMgrB, grpcSrvB, rtrB, poolB, forwarderB, "gw-b")
	modelsHandlerB := handler.NewModelsHandler(poolB)
	sessionHandlerB := handler.NewSessionHandler(sessionMgrA)
	logger := zap.NewNop()

	httpLisB, _ := net.Listen("tcp", "localhost:0")
	httpPortB := httpLisB.Addr().(*net.TCPAddr).Port
	httpSrvB := server.NewHTTPServer(httpPortB, logger, validKeys, chatHandlerB, modelsHandlerB, sessionHandlerB)
	go http.Serve(httpLisB, httpSrvB.Engine())

	// Send request to GW-B; agent is on GW-A
	reqBody := openai.ChatCompletionRequest{
		Model:    "remote-echo",
		Messages: []openai.Message{{Role: "user", Content: "forwarded"}},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	httpReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/v1/chat/completions", httpLisB.Addr().String()), strings.NewReader(string(bodyBytes)))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer fwd-key")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		t.Fatalf("forward request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d; body: %s", resp.StatusCode, body)
	}

	var chatResp openai.ChatCompletionResponse
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &chatResp)

	if len(chatResp.Choices) == 0 || chatResp.Choices[0].Message == nil {
		t.Fatal("expected message in forwarded response")
	}
	if got := chatResp.Choices[0].Message.Content; got != "remote: forwarded" {
		t.Errorf("content = %q, want %q", got, "remote: forwarded")
	}
}

// ===== Health Check =====

func TestE2E_Health(t *testing.T) {
	h := newTestHarness(t)

	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/health", h.HTTPAddr), nil)
	resp, err := h.httpClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Errorf("status = %q, want %q", body["status"], "ok")
	}
}
