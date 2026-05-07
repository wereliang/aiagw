package tests

import (
	"bufio"
	"bytes"
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

const defaultTestAPIKey = "e2e-test-key"

type TestHarness struct {
	HTTPAddr     string
	GRPCAddr     string
	InternalAddr string
	RedisClient  *redis.Client
	SessionMgr   *session.Manager
	Pool         *agent.Pool
	Forwarder    *agent.Forwarder
	APIKey       string
	httpClient   *http.Client
}

func newTestHarness(t *testing.T) *TestHarness {
	t.Helper()

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { redisClient.Close() })

	gatewayInstance := "test-gw-e2e"
	sessionMgr := session.NewManager(redisClient, 30*time.Minute)
	pool := agent.NewPool(redisClient, gatewayInstance, 60*time.Second)
	grpcSrv := agent.NewGRPCServer(pool)
	rtr := router.New(pool)

	internalLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen internal gRPC: %v", err)
	}
	internalAddr := internalLis.Addr().String()

	forwarder := agent.NewForwarder(redisClient, gatewayInstance, internalAddr, 60*time.Second)
	if err := forwarder.RegisterInstance(t.Context()); err != nil {
		t.Fatalf("register forwarder: %v", err)
	}
	t.Cleanup(func() {
		forwarder.UnregisterInstance(t.Context())
		forwarder.Close()
	})

	validAPIKeys := map[string]bool{defaultTestAPIKey: true}
	logger := zap.NewNop()

	chatHandler := handler.NewChatHandler(sessionMgr, grpcSrv, rtr, pool, forwarder, gatewayInstance, logger)
	modelsHandler := handler.NewModelsHandler(pool)
	sessionHandler := handler.NewSessionHandler(sessionMgr)

	grpcLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen gRPC: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAgentGatewayServer(grpcServer, grpcSrv)
	go func() {
		if err := grpcServer.Serve(grpcLis); err != nil {
			t.Logf("gRPC server stopped: %v", err)
		}
	}()
	t.Cleanup(func() { grpcServer.GracefulStop() })

	internalServer := agent.NewInternalServer(grpcSrv)
	internalGRPCSrv := grpc.NewServer()
	pb.RegisterGatewayInternalServer(internalGRPCSrv, internalServer)
	go func() {
		if err := internalGRPCSrv.Serve(internalLis); err != nil {
			t.Logf("internal gRPC server stopped: %v", err)
		}
	}()
	t.Cleanup(func() { internalGRPCSrv.GracefulStop() })

	httpLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen HTTP: %v", err)
	}
	httpPort := httpLis.Addr().(*net.TCPAddr).Port
	httpSrv := server.NewHTTPServer(httpPort, logger, validAPIKeys, chatHandler, modelsHandler, sessionHandler)

	go func() {
		if err := http.Serve(httpLis, httpSrv.Engine()); err != nil && err != http.ErrServerClosed {
			t.Logf("HTTP server stopped: %v", err)
		}
	}()

	return &TestHarness{
		HTTPAddr:     httpLis.Addr().String(),
		GRPCAddr:     grpcLis.Addr().String(),
		InternalAddr: internalAddr,
		RedisClient:  redisClient,
		SessionMgr:   sessionMgr,
		Pool:         pool,
		Forwarder:    forwarder,
		APIKey:       defaultTestAPIKey,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
	}
}

type MockAgent struct {
	ID        string
	AgentType string
	stream    pb.AgentGateway_ConnectClient
	conn      *grpc.ClientConn
	done      chan struct{}
}

func (h *TestHarness) ConnectMockAgent(t *testing.T, agentID, agentType string, handler func(req *pb.AgentRequest, stream pb.AgentGateway_ConnectClient)) *MockAgent {
	t.Helper()

	conn, err := grpc.NewClient(h.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial gRPC: %v", err)
	}

	client := pb.NewAgentGatewayClient(conn)
	stream, err := client.Connect(t.Context())
	if err != nil {
		conn.Close()
		t.Fatalf("connect agent stream: %v", err)
	}

	if err := stream.Send(&pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   agentID,
				AgentType: agentType,
			},
		},
	}); err != nil {
		conn.Close()
		t.Fatalf("send register: %v", err)
	}

	ma := &MockAgent{
		ID:        agentID,
		AgentType: agentType,
		stream:    stream,
		conn:      conn,
		done:      make(chan struct{}),
	}

	go func() {
		defer close(ma.done)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			req := msg.GetRequest()
			if req == nil {
				continue
			}
			handler(req, stream)
		}
	}()

	t.Cleanup(func() {
		stream.CloseSend()
		conn.Close()
	})

	time.Sleep(100 * time.Millisecond)
	return ma
}

func (h *TestHarness) ConnectEchoAgent(t *testing.T, agentID, agentType string) *MockAgent {
	t.Helper()
	return h.ConnectMockAgent(t, agentID, agentType, func(req *pb.AgentRequest, stream pb.AgentGateway_ConnectClient) {
		lastMsg := req.Messages[len(req.Messages)-1]
		sessionID := req.SessionId
		if sessionID == "" {
			sessionID = "sess-" + req.RequestId
		}
		var plainText string
		json.Unmarshal(lastMsg.Content, &plainText)
		stream.Send(&pb.AgentMessage{
			Payload: &pb.AgentMessage_Response{
				Response: &pb.AgentResponse{
					RequestId: req.RequestId,
					SessionId: sessionID,
					Content: &pb.AgentResponse_Message{
						Message: &pb.ChatMessage{
							Role:    "assistant",
							Content: []byte("echo: " + plainText),
						},
					},
					Done: true,
				},
			},
		})
	})
}

func (h *TestHarness) ConnectStreamAgent(t *testing.T, agentID, agentType string) *MockAgent {
	t.Helper()
	return h.ConnectMockAgent(t, agentID, agentType, func(req *pb.AgentRequest, stream pb.AgentGateway_ConnectClient) {
		lastMsg := req.Messages[len(req.Messages)-1]
		sessionID := req.SessionId
		if sessionID == "" {
			sessionID = "sess-" + req.RequestId
		}
		var plainText string
		json.Unmarshal(lastMsg.Content, &plainText)
		words := strings.Fields(plainText)

		var accumulated strings.Builder
		for _, word := range words {
			accumulated.WriteString(word + " ")
			stream.Send(&pb.AgentMessage{
				Payload: &pb.AgentMessage_Response{
					Response: &pb.AgentResponse{
						RequestId: req.RequestId,
						SessionId: sessionID,
						Content: &pb.AgentResponse_Chunk{
							Chunk: &pb.StreamChunk{Content: accumulated.String()},
						},
						Done: false,
					},
				},
			})
		}

		stream.Send(&pb.AgentMessage{
			Payload: &pb.AgentMessage_Response{
				Response: &pb.AgentResponse{
					RequestId: req.RequestId,
					SessionId: sessionID,
					Content: &pb.AgentResponse_Message{
						Message: &pb.ChatMessage{
							Role:    "assistant",
							Content: []byte(strings.Join(words, " ")),
						},
					},
					Done: true,
				},
			},
		})
	})
}

type ChatRequestOpts struct {
	Model     string
	Messages  []openai.Message
	Stream    bool
	SessionID string
	APIKey    string
}

func (h *TestHarness) DoChat(t *testing.T, opts ChatRequestOpts) *http.Response {
	t.Helper()

	reqBody := openai.ChatCompletionRequest{
		Model:    opts.Model,
		Messages: opts.Messages,
		Stream:   opts.Stream,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/v1/chat/completions", h.HTTPAddr), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	apiKey := h.APIKey
	if opts.APIKey != "" {
		apiKey = opts.APIKey
	}
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	if opts.SessionID != "" {
		httpReq.Header.Set("X-Session-Id", opts.SessionID)
	}

	resp, err := h.httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	return resp
}

func (h *TestHarness) DoGet(t *testing.T, path string) *http.Response {
	t.Helper()

	httpReq, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s%s", h.HTTPAddr, path), nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+h.APIKey)

	resp, err := h.httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	return resp
}

func (h *TestHarness) DoDelete(t *testing.T, path string) *http.Response {
	t.Helper()

	httpReq, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s%s", h.HTTPAddr, path), nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+h.APIKey)

	resp, err := h.httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	return resp
}

func (h *TestHarness) DoRawGet(t *testing.T, path string, apiKey string) *http.Response {
	t.Helper()

	httpReq, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s%s", h.HTTPAddr, path), nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
	}

	resp, err := h.httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	return resp
}

type SSEReader struct {
	scanner *bufio.Scanner
}

func NewSSEReader(body io.Reader) *SSEReader {
	return &SSEReader{scanner: bufio.NewScanner(body)}
}

func (r *SSEReader) Next() (data string, done bool, err error) {
	for r.scanner.Scan() {
		line := r.scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimPrefix(line, "data: ")
			if payload == "[DONE]" {
				return "", true, nil
			}
			return payload, false, nil
		}
	}
	if err := r.scanner.Err(); err != nil {
		return "", false, err
	}
	return "", true, nil
}

func (r *SSEReader) CollectChunks() ([]openai.ChatCompletionChunk, error) {
	var chunks []openai.ChatCompletionChunk
	for {
		data, done, err := r.Next()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		var chunk openai.ChatCompletionChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return nil, fmt.Errorf("unmarshal chunk: %w (data: %s)", err, data)
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func readChatResponse(t *testing.T, resp *http.Response) openai.ChatCompletionResponse {
	t.Helper()
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	var chatResp openai.ChatCompletionResponse
	if err := json.Unmarshal(body, &chatResp); err != nil {
		t.Fatalf("unmarshal response: %v; body: %s", err, string(body))
	}
	return chatResp
}

func readErrorResponse(t *testing.T, resp *http.Response) map[string]interface{} {
	t.Helper()
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal error response: %v; body: %s", err, string(body))
	}
	return result
}
