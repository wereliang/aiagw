package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/wereliang/aiagw/internal/agent"
	"github.com/wereliang/aiagw/internal/middleware"
	"github.com/wereliang/aiagw/internal/openai"
	"github.com/wereliang/aiagw/internal/router"
	"github.com/wereliang/aiagw/internal/session"
	"github.com/wereliang/aiagw/pkg/errcode"
	"go.uber.org/zap"
)

const (
	testAPIKey    = "test-key"
	testAgentType = "support"
	testGateway   = "gw-test-1"
)

func setupTest(t *testing.T) (*gin.Engine, *miniredis.Miniredis, *agent.GRPCServer) {
	t.Helper()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	validKeys := map[string]bool{testAPIKey: true}
	sessionMgr := session.NewManager(client, 30*time.Minute)
	pool := agent.NewPool(client, testGateway, 60*time.Second)
	grpcServer := agent.NewGRPCServer(pool)
	rtr := router.New(pool)

	handler := NewChatHandler(sessionMgr, grpcServer, rtr, pool, nil, testGateway, zap.NewNop())

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(middleware.Auth(validKeys))
	engine.POST("/v1/chat/completions", handler.Handle)

	return engine, mr, grpcServer
}

func newChatRequest(t *testing.T, req openai.ChatCompletionRequest) *http.Request {
	t.Helper()

	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	httpReq.Header.Set("Authorization", "Bearer "+testAPIKey)
	httpReq.Header.Set("Content-Type", "application/json")
	return httpReq
}

func decodeErrorResponse(t *testing.T, w *httptest.ResponseRecorder) *errcode.ErrorResponse {
	t.Helper()

	var resp errcode.ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error in response, got nil")
	}
	return &resp
}

func TestChatMissingMessages(t *testing.T) {
	engine, _, _ := setupTest(t)

	req := newChatRequest(t, openai.ChatCompletionRequest{
		Model:    testAgentType,
		Messages: nil,
	})
	w := httptest.NewRecorder()

	engine.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusBadRequest, w.Body.String())
	}

	resp := decodeErrorResponse(t, w)
	if resp.Error.Type != "invalid_request_error" {
		t.Errorf("error type = %q, want %q", resp.Error.Type, "invalid_request_error")
	}
	if resp.Error.Message != "messages must not be empty" {
		t.Errorf("error message = %q, want %q", resp.Error.Message, "messages must not be empty")
	}
}

func TestChatNoAgentAvailable(t *testing.T) {
	engine, _, _ := setupTest(t)

	req := newChatRequest(t, openai.ChatCompletionRequest{
		Model: testAgentType,
		Messages: []openai.Message{
			{Role: "user", Content: json.RawMessage(`"hello"`)},
		},
	})
	w := httptest.NewRecorder()

	engine.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusServiceUnavailable, w.Body.String())
	}

	resp := decodeErrorResponse(t, w)
	if resp.Error.Type != "service_unavailable" {
		t.Errorf("error type = %q, want %q", resp.Error.Type, "service_unavailable")
	}
}
