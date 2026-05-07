package openai

import (
	"encoding/json"
	"fmt"
	"strconv"

	pb "github.com/wereliang/aiagw/api/proto"
)

// ToAgentRequest converts an OpenAI ChatCompletionRequest into a gRPC AgentRequest.
// requestID and sessionID are passed explicitly because they originate from the
// gateway layer, not from the OpenAI-compatible payload.
func ToAgentRequest(requestID, sessionID string, req *ChatCompletionRequest) *pb.AgentRequest {
	messages := make([]*pb.ChatMessage, len(req.Messages))
	for i, m := range req.Messages {
		messages[i] = &pb.ChatMessage{
			Role:    m.Role,
			Content: []byte(m.Content),
		}
	}

	params := make(map[string]string)
	if req.Temperature != nil {
		params["temperature"] = strconv.FormatFloat(*req.Temperature, 'f', -1, 64)
	}
	if req.MaxTokens != nil {
		params["max_tokens"] = strconv.Itoa(*req.MaxTokens)
	}
	if req.TopP != nil {
		params["top_p"] = strconv.FormatFloat(*req.TopP, 'f', -1, 64)
	}

	return &pb.AgentRequest{
		RequestId:  requestID,
		SessionId:  sessionID,
		Model:      req.Model,
		Messages:   messages,
		Parameters: params,
	}
}

// FromAgentResponse converts a gRPC AgentResponse into an OpenAI
// ChatCompletionResponse suitable for non-streaming replies.
func FromAgentResponse(resp *pb.AgentResponse, model string) *ChatCompletionResponse {
	finishReason := "stop"

	var msg *Message
	if m := resp.GetMessage(); m != nil {
		content := m.GetContent()
		if len(content) == 0 {
			content = []byte(`""`)
		}
		msg = &Message{
			Role:    m.GetRole(),
			Content: json.RawMessage(content),
		}
	}

	return &ChatCompletionResponse{
		ID:     fmt.Sprintf("chatcmpl-%s", resp.GetRequestId()),
		Object: "chat.completion",
		Model:  model,
		Choices: []Choice{
			{
				Index:        0,
				Message:      msg,
				FinishReason: &finishReason,
			},
		},
		Usage: Usage{},
	}
}

// MakeDeltaChunk creates an OpenAI ChatCompletionChunk with content and/or reasoning delta.
func MakeDeltaChunk(requestID, model string, content, reasoning string) *ChatCompletionChunk {
	delta := Delta{}
	if content != "" {
		delta.Content = &content
	}
	if reasoning != "" {
		delta.ReasoningContent = &reasoning
	}
	return &ChatCompletionChunk{
		ID:     fmt.Sprintf("chatcmpl-%s", requestID),
		Object: "chat.completion.chunk",
		Model:  model,
		Choices: []ChunkChoice{
			{
				Index: 0,
				Delta: delta,
			},
		},
	}
}
func FromAgentResponseChunk(resp *pb.AgentResponse, model string) *ChatCompletionChunk {
	chunk := &ChatCompletionChunk{
		ID:     fmt.Sprintf("chatcmpl-%s", resp.GetRequestId()),
		Object: "chat.completion.chunk",
		Model:  model,
	}

	if resp.GetDone() {
		finishReason := "stop"
		chunk.Choices = []ChunkChoice{
			{
				Index:        0,
				Delta:        Delta{},
				FinishReason: &finishReason,
			},
		}
		return chunk
	}

	delta := Delta{}
	if c := resp.GetChunk(); c != nil {
		if s := c.GetContent(); s != "" {
			delta.Content = &s
		}
		if r := c.GetReasoningContent(); r != "" {
			delta.ReasoningContent = &r
		}
	}

	chunk.Choices = []ChunkChoice{
		{
			Index: 0,
			Delta: delta,
		},
	}
	return chunk
}
