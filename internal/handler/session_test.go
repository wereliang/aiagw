package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wereliang/aiagw/internal/middleware"
	"github.com/wereliang/aiagw/internal/session"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
)

func setupSessionTest(t *testing.T) (*gin.Engine, *session.Manager) {
	t.Helper()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	validKeys := map[string]bool{testAPIKey: true}
	sessionMgr := session.NewManager(client, 30*time.Minute)

	sessionHandler := NewSessionHandler(sessionMgr)

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(middleware.Auth(validKeys))
	engine.DELETE("/v1/sessions/:session_id", sessionHandler.Handle)

	return engine, sessionMgr
}

func TestDeleteSessionHandler(t *testing.T) {
	engine, sessionMgr := setupSessionTest(t)

	sess, err := sessionMgr.Create(context.Background(), "agent-1", "openai", testGateway)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/sessions/"+sess.ID, nil)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if deleted, ok := resp["deleted"].(bool); !ok || !deleted {
		t.Errorf("deleted = %v, want true", resp["deleted"])
	}
	if id, ok := resp["id"].(string); !ok || id != sess.ID {
		t.Errorf("id = %q, want %q", resp["id"], sess.ID)
	}

	_, err = sessionMgr.Get(context.Background(), sess.ID)
	if err == nil {
		t.Error("expected session to be deleted, but Get returned nil error")
	}
}

func TestDeleteSessionNotFound(t *testing.T) {
	engine, _ := setupSessionTest(t)

	req := httptest.NewRequest(http.MethodDelete, "/v1/sessions/nonexistent-session-id", nil)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusNotFound, w.Body.String())
	}

	resp := decodeErrorResponse(t, w)
	if resp.Error.Type != "not_found_error" {
		t.Errorf("error type = %q, want %q", resp.Error.Type, "not_found_error")
	}
}
