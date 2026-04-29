package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wereliang/aiagw/internal/openai"
)

func TestModelsHandler(t *testing.T) {
	engine, _, _ := setupTest(t)

	modelsHandler := NewModelsHandler(nil)
	engine.GET("/v1/models", modelsHandler.Handle)

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var modelList openai.ModelList
	if err := json.NewDecoder(w.Body).Decode(&modelList); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if modelList.Object != "list" {
		t.Errorf("object = %q, want %q", modelList.Object, "list")
	}
}
