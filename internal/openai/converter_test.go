package openai

import (
	"encoding/json"
	"testing"

	pb "github.com/wereliang/aiagw/api/proto"
)

func float64Ptr(f float64) *float64 { return &f }
func intPtr(i int) *int             { return &i }

func TestToAgentRequest(t *testing.T) {
	temp := 0.7
	maxTok := 512
	topP := 0.9

	req := &ChatCompletionRequest{
		Model: "customer-service",
		Messages: []Message{
			{Role: "system", Content: json.RawMessage(`"You are helpful."`)},
			{Role: "user", Content: json.RawMessage(`"Hello"`)},
		},
		Temperature: &temp,
		MaxTokens:   &maxTok,
		TopP:        &topP,
	}

	got := ToAgentRequest("req-123", "sess-456", req)

	if got.GetRequestId() != "req-123" {
		t.Errorf("RequestId = %q, want %q", got.GetRequestId(), "req-123")
	}
	if got.GetSessionId() != "sess-456" {
		t.Errorf("SessionId = %q, want %q", got.GetSessionId(), "sess-456")
	}
	if got.GetModel() != "customer-service" {
		t.Errorf("Model = %q, want %q", got.GetModel(), "customer-service")
	}

	msgs := got.GetMessages()
	if len(msgs) != 2 {
		t.Fatalf("Messages len = %d, want 2", len(msgs))
	}
	if msgs[0].GetRole() != "system" || string(msgs[0].GetContent()) != `"You are helpful."` {
		t.Errorf("Messages[0] = {%q, %q}, want {system, You are helpful.}",
			msgs[0].GetRole(), msgs[0].GetContent())
	}
	if msgs[1].GetRole() != "user" || string(msgs[1].GetContent()) != `"Hello"` {
		t.Errorf("Messages[1] = {%q, %q}, want {user, Hello}",
			msgs[1].GetRole(), msgs[1].GetContent())
	}

	params := got.GetParameters()
	if params["temperature"] != "0.7" {
		t.Errorf("parameters[temperature] = %q, want %q", params["temperature"], "0.7")
	}
	if params["max_tokens"] != "512" {
		t.Errorf("parameters[max_tokens] = %q, want %q", params["max_tokens"], "512")
	}
	if params["top_p"] != "0.9" {
		t.Errorf("parameters[top_p] = %q, want %q", params["top_p"], "0.9")
	}
}

func TestToAgentRequestNoOptionalParams(t *testing.T) {
	req := &ChatCompletionRequest{
		Model: "test-model",
		Messages: []Message{
			{Role: "user", Content: json.RawMessage(`"Hi"`)},
		},
	}

	got := ToAgentRequest("r1", "s1", req)

	params := got.GetParameters()
	if len(params) != 0 {
		t.Errorf("expected empty parameters, got %v", params)
	}
}

func TestFromAgentResponseFull(t *testing.T) {
	resp := &pb.AgentResponse{
		RequestId: "req-abc",
		SessionId: "sess-def",
		Content: &pb.AgentResponse_Message{
			Message: &pb.ChatMessage{
				Role:    "assistant",
				Content: []byte("Hello! How can I help?"),
			},
		},
		Done: true,
	}

	got := FromAgentResponse(resp, "customer-service")

	if got.ID != "chatcmpl-req-abc" {
		t.Errorf("ID = %q, want %q", got.ID, "chatcmpl-req-abc")
	}
	if got.Object != "chat.completion" {
		t.Errorf("Object = %q, want %q", got.Object, "chat.completion")
	}
	if got.Model != "customer-service" {
		t.Errorf("Model = %q, want %q", got.Model, "customer-service")
	}
	if len(got.Choices) != 1 {
		t.Fatalf("Choices len = %d, want 1", len(got.Choices))
	}
	choice := got.Choices[0]
	if choice.Index != 0 {
		t.Errorf("Choice.Index = %d, want 0", choice.Index)
	}
	if choice.Message == nil {
		t.Fatal("Choice.Message is nil")
	}
	if choice.Message.Role != "assistant" {
		t.Errorf("Choice.Message.Role = %q, want %q", choice.Message.Role, "assistant")
	}
	if choice.Message.Content == nil || string(choice.Message.Content) != "Hello! How can I help?" {
		t.Errorf("Choice.Message.Content = %q, want %q", choice.Message.Content, "Hello! How can I help?")
	}
	if choice.FinishReason == nil || *choice.FinishReason != "stop" {
		t.Errorf("FinishReason = %v, want %q", choice.FinishReason, "stop")
	}
}

func TestFromAgentResponseChunk(t *testing.T) {
	resp := &pb.AgentResponse{
		RequestId: "req-abc",
		SessionId: "sess-def",
		Content: &pb.AgentResponse_Chunk{
			Chunk: &pb.StreamChunk{
				Content: "Hello",
			},
		},
		Done: false,
	}

	got := FromAgentResponseChunk(resp, "customer-service")

	if got.ID != "chatcmpl-req-abc" {
		t.Errorf("ID = %q, want %q", got.ID, "chatcmpl-req-abc")
	}
	if got.Object != "chat.completion.chunk" {
		t.Errorf("Object = %q, want %q", got.Object, "chat.completion.chunk")
	}
	if got.Model != "customer-service" {
		t.Errorf("Model = %q, want %q", got.Model, "customer-service")
	}
	if len(got.Choices) != 1 {
		t.Fatalf("Choices len = %d, want 1", len(got.Choices))
	}
	choice := got.Choices[0]
	if choice.Delta.Content == nil || *choice.Delta.Content != "Hello" {
		t.Errorf("Delta.Content = %v, want %q", choice.Delta.Content, "Hello")
	}
	if choice.FinishReason != nil {
		t.Errorf("FinishReason = %v, want nil", *choice.FinishReason)
	}
}

func TestFromAgentResponseChunkDone(t *testing.T) {
	resp := &pb.AgentResponse{
		RequestId: "req-abc",
		SessionId: "sess-def",
		Done:      true,
	}

	got := FromAgentResponseChunk(resp, "customer-service")

	if got.Object != "chat.completion.chunk" {
		t.Errorf("Object = %q, want %q", got.Object, "chat.completion.chunk")
	}
	if len(got.Choices) != 1 {
		t.Fatalf("Choices len = %d, want 1", len(got.Choices))
	}
	choice := got.Choices[0]
	if choice.Delta.Content != nil {
		t.Errorf("Delta.Content = %v, want nil (empty delta)", *choice.Delta.Content)
	}
	if choice.Delta.Role != nil {
		t.Errorf("Delta.Role = %v, want nil (empty delta)", *choice.Delta.Role)
	}
	if choice.FinishReason == nil || *choice.FinishReason != "stop" {
		t.Errorf("FinishReason = %v, want %q", choice.FinishReason, "stop")
	}
}
