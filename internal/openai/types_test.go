package openai

import (
	"encoding/json"
	"testing"
)

func TestChatCompletionRequestUnmarshal(t *testing.T) {
	raw := `{
		"model": "customer-service",
		"messages": [
			{"role": "system", "content": "You are helpful."},
			{"role": "user", "content": "Hello"}
		],
		"stream": true,
		"temperature": 0.7
	}`
	var req ChatCompletionRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if req.Model != "customer-service" {
		t.Errorf("Model = %q, want %q", req.Model, "customer-service")
	}
	if len(req.Messages) != 2 {
		t.Fatalf("Messages len = %d, want 2", len(req.Messages))
	}
	if req.Messages[0].Role != "system" {
		t.Errorf("Messages[0].Role = %q, want %q", req.Messages[0].Role, "system")
	}
	if !req.Stream {
		t.Error("Stream = false, want true")
	}
	if req.Temperature == nil || *req.Temperature != 0.7 {
		t.Errorf("Temperature = %v, want 0.7", req.Temperature)
	}
}

func TestChatCompletionResponseMarshal(t *testing.T) {
	resp := ChatCompletionResponse{
		ID:     "chatcmpl-abc123",
		Object: "chat.completion",
		Model:  "customer-service",
		Choices: []Choice{
			{
				Index:        0,
				Message:      &Message{Role: "assistant", Content: "Hello!"},
				FinishReason: stringPtr("stop"),
			},
		},
		Usage: Usage{PromptTokens: 0, CompletionTokens: 0, TotalTokens: 0},
	}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded["id"] != "chatcmpl-abc123" {
		t.Errorf("id = %v, want chatcmpl-abc123", decoded["id"])
	}
	if decoded["object"] != "chat.completion" {
		t.Errorf("object = %v, want chat.completion", decoded["object"])
	}
}

func TestChatCompletionChunkMarshal(t *testing.T) {
	chunk := ChatCompletionChunk{
		ID:     "chatcmpl-abc123",
		Object: "chat.completion.chunk",
		Model:  "customer-service",
		Choices: []ChunkChoice{
			{
				Index: 0,
				Delta: Delta{Content: stringPtr("Hello")},
			},
		},
	}
	data, err := json.Marshal(chunk)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded["object"] != "chat.completion.chunk" {
		t.Errorf("object = %v, want chat.completion.chunk", decoded["object"])
	}
}

func TestModelListMarshal(t *testing.T) {
	list := ModelList{
		Object: "list",
		Data: []Model{
			{ID: "customer-service", Object: "model", OwnedBy: "tenant-a"},
		},
	}
	data, err := json.Marshal(list)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded["object"] != "list" {
		t.Errorf("object = %v, want list", decoded["object"])
	}
}

func stringPtr(s string) *string {
	return &s
}
