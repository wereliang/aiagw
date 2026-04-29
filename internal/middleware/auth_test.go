package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/wereliang/aiagw/pkg/errcode"
)

func setupRouter(validKeys map[string]bool) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Auth(validKeys))
	r.GET("/test", func(c *gin.Context) {
		apiKey := GetAPIKey(c)
		c.JSON(http.StatusOK, gin.H{"api_key": apiKey})
	})
	return r
}

func TestAuthValidKey(t *testing.T) {
	router := setupRouter(map[string]bool{"test-key": true})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["api_key"] != "test-key" {
		t.Errorf("api_key = %q, want %q", body["api_key"], "test-key")
	}
}

func TestAuthMissingHeader(t *testing.T) {
	router := setupRouter(map[string]bool{"test-key": true})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusUnauthorized, w.Body.String())
	}

	var resp errcode.ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error in response, got nil")
	}
	if resp.Error.Type != "authentication_error" {
		t.Errorf("error type = %q, want %q", resp.Error.Type, "authentication_error")
	}
}

func TestAuthInvalidKey(t *testing.T) {
	router := setupRouter(map[string]bool{"test-key": true})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer invalid-key")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusUnauthorized, w.Body.String())
	}

	var resp errcode.ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error in response, got nil")
	}
	if resp.Error.Message != "invalid API key" {
		t.Errorf("error message = %q, want %q", resp.Error.Message, "invalid API key")
	}
}

func TestAuthWrongFormat(t *testing.T) {
	router := setupRouter(map[string]bool{"test-key": true})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Basic abc123")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusUnauthorized, w.Body.String())
	}

	var resp errcode.ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error in response, got nil")
	}
	if resp.Error.Message != "Authorization header must use Bearer scheme" {
		t.Errorf("error message = %q, want %q", resp.Error.Message, "Authorization header must use Bearer scheme")
	}
}

func TestAuthEmptyKeysPassAll(t *testing.T) {
	router := setupRouter(map[string]bool{})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer any-key")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}
}
