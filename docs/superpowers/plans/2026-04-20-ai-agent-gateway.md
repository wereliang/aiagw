# AI Agent Gateway 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个 Go 语言的 AI Agent 网关，对外提供 OpenAI 兼容 API，内部通过 gRPC 双向流路由请求到后端 AI Agent。

**Architecture:** 集中式网关架构，单进程内按职责分层：HTTP API → Auth → Router → Agent Pool。Redis 存储 session/agent/tenant 状态，支持多副本无状态部署。Agent 主动连接网关建立 gRPC 双向流。

**Tech Stack:** Go 1.22+, gin, gRPC, go-redis/v9, zap, protobuf, yaml.v3

---

## 文件结构

```
aiagw2/
├── cmd/gateway/main.go                  # 入口，初始化并启动所有服务
├── api/proto/agent.proto                # gRPC 协议定义（Agent ↔ Gateway）
├── api/proto/internal.proto             # gRPC 协议定义（Gateway ↔ Gateway）
├── internal/config/config.go            # 配置结构体与 YAML 加载
├── internal/store/redis.go              # Redis 客户端封装
├── internal/tenant/store.go             # 租户信息查询（Redis）
├── internal/session/manager.go          # Session CRUD（Redis）
├── internal/agent/pool.go               # Agent 连接池（内存 + Redis）
├── internal/agent/grpc_server.go        # gRPC 服务端，接受 Agent 注册
├── internal/agent/forwarder.go          # 网关间请求转发
├── internal/router/router.go            # tenant+model → Agent 路由选择
├── internal/openai/types.go             # OpenAI 请求/响应结构体
├── internal/openai/converter.go         # OpenAI ↔ gRPC 消息转换
├── internal/middleware/auth.go          # API Key 认证中间件
├── internal/middleware/logging.go       # 请求日志中间件
├── internal/handler/chat.go             # POST /v1/chat/completions
├── internal/handler/models.go           # GET /v1/models
├── internal/handler/session.go          # DELETE /v1/sessions/{id}
├── internal/server/http.go              # HTTP 服务器，注册路由
├── pkg/errcode/errors.go                # 统一错误码（OpenAI 格式）
├── configs/gateway.yaml                 # 配置文件模板
├── go.mod
└── go.sum
```

---

### Task 1: 项目脚手架与配置模块

**Files:**
- Create: `go.mod`
- Create: `configs/gateway.yaml`
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`

- [ ] **Step 1: 初始化 Go module**

```bash
cd /Users/hunter/xy/mywork/go/src/github.com/wereliang/aiagw2
go mod init github.com/wereliang/aiagw2
```

- [ ] **Step 2: 创建配置文件模板**

创建 `configs/gateway.yaml`：

```yaml
server:
  http_port: 8080
  grpc_port: 9090
  internal_grpc_port: 9091
  instance_id: ""

redis:
  addr: "localhost:6379"
  password: ""
  db: 0

session:
  ttl: 30m
  max_per_tenant: 1000

agent:
  heartbeat_interval: 30s
  heartbeat_timeout: 90s

log:
  level: info
  format: json
```

- [ ] **Step 3: 编写配置加载的测试**

创建 `internal/config/config_test.go`：

```go
package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	content := `
server:
  http_port: 8080
  grpc_port: 9090
  internal_grpc_port: 9091
  instance_id: "test-gw-1"
redis:
  addr: "localhost:6379"
  password: "secret"
  db: 1
session:
  ttl: 15m
  max_per_tenant: 500
agent:
  heartbeat_interval: 20s
  heartbeat_timeout: 60s
log:
  level: debug
  format: text
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Server.HTTPPort != 8080 {
		t.Errorf("HTTPPort = %d, want 8080", cfg.Server.HTTPPort)
	}
	if cfg.Server.GRPCPort != 9090 {
		t.Errorf("GRPCPort = %d, want 9090", cfg.Server.GRPCPort)
	}
	if cfg.Server.InternalGRPCPort != 9091 {
		t.Errorf("InternalGRPCPort = %d, want 9091", cfg.Server.InternalGRPCPort)
	}
	if cfg.Server.InstanceID != "test-gw-1" {
		t.Errorf("InstanceID = %q, want %q", cfg.Server.InstanceID, "test-gw-1")
	}
	if cfg.Redis.Addr != "localhost:6379" {
		t.Errorf("Redis.Addr = %q, want %q", cfg.Redis.Addr, "localhost:6379")
	}
	if cfg.Redis.Password != "secret" {
		t.Errorf("Redis.Password = %q, want %q", cfg.Redis.Password, "secret")
	}
	if cfg.Redis.DB != 1 {
		t.Errorf("Redis.DB = %d, want 1", cfg.Redis.DB)
	}
	if cfg.Session.TTL != 15*time.Minute {
		t.Errorf("Session.TTL = %v, want 15m", cfg.Session.TTL)
	}
	if cfg.Session.MaxPerTenant != 500 {
		t.Errorf("Session.MaxPerTenant = %d, want 500", cfg.Session.MaxPerTenant)
	}
	if cfg.Agent.HeartbeatInterval != 20*time.Second {
		t.Errorf("Agent.HeartbeatInterval = %v, want 20s", cfg.Agent.HeartbeatInterval)
	}
	if cfg.Agent.HeartbeatTimeout != 60*time.Second {
		t.Errorf("Agent.HeartbeatTimeout = %v, want 60s", cfg.Agent.HeartbeatTimeout)
	}
}

func TestLoadConfigAutoInstanceID(t *testing.T) {
	content := `
server:
  http_port: 8080
  grpc_port: 9090
  internal_grpc_port: 9091
redis:
  addr: "localhost:6379"
session:
  ttl: 30m
  max_per_tenant: 1000
agent:
  heartbeat_interval: 30s
  heartbeat_timeout: 90s
log:
  level: info
  format: json
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Server.InstanceID == "" {
		t.Error("InstanceID should be auto-generated when empty")
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path.yaml")
	if err == nil {
		t.Error("Load() should return error for nonexistent file")
	}
}
```

- [ ] **Step 4: 运行测试验证失败**

```bash
cd /Users/hunter/xy/mywork/go/src/github.com/wereliang/aiagw2
go test ./internal/config/ -v
```

预期：编译失败，`Load` 函数未定义。

- [ ] **Step 5: 实现配置模块**

创建 `internal/config/config.go`：

```go
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Redis   RedisConfig   `yaml:"redis"`
	Session SessionConfig `yaml:"session"`
	Agent   AgentConfig   `yaml:"agent"`
	Log     LogConfig     `yaml:"log"`
}

type ServerConfig struct {
	HTTPPort         int    `yaml:"http_port"`
	GRPCPort         int    `yaml:"grpc_port"`
	InternalGRPCPort int    `yaml:"internal_grpc_port"`
	InstanceID       string `yaml:"instance_id"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type SessionConfig struct {
	TTL          time.Duration `yaml:"ttl"`
	MaxPerTenant int           `yaml:"max_per_tenant"`
}

type AgentConfig struct {
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if cfg.Server.InstanceID == "" {
		cfg.Server.InstanceID = uuid.New().String()
	}

	return cfg, nil
}
```

- [ ] **Step 6: 安装依赖并运行测试**

```bash
cd /Users/hunter/xy/mywork/go/src/github.com/wereliang/aiagw2
go get github.com/google/uuid gopkg.in/yaml.v3
go test ./internal/config/ -v
```

预期：所有测试通过。

- [ ] **Step 7: 提交**

```bash
git add go.mod go.sum configs/ internal/config/
git commit -m "feat: add project scaffold and config module"
```

---

### Task 2: 统一错误码

**Files:**
- Create: `pkg/errcode/errors.go`
- Create: `pkg/errcode/errors_test.go`

- [ ] **Step 1: 编写错误码测试**

创建 `pkg/errcode/errors_test.go`：

```go
package errcode

import (
	"net/http"
	"testing"
)

func TestNewAPIError(t *testing.T) {
	err := ErrAuthentication("invalid api key")

	if err.HTTPStatus != http.StatusUnauthorized {
		t.Errorf("HTTPStatus = %d, want %d", err.HTTPStatus, http.StatusUnauthorized)
	}
	if err.Type != "authentication_error" {
		t.Errorf("Type = %q, want %q", err.Type, "authentication_error")
	}
	if err.Message != "invalid api key" {
		t.Errorf("Message = %q, want %q", err.Message, "invalid api key")
	}
	if err.Code != "invalid_api_key" {
		t.Errorf("Code = %q, want %q", err.Code, "invalid_api_key")
	}
}

func TestAllErrorTypes(t *testing.T) {
	tests := []struct {
		name       string
		fn         func(string) *APIError
		wantStatus int
		wantType   string
		wantCode   string
	}{
		{"authentication", ErrAuthentication, 401, "authentication_error", "invalid_api_key"},
		{"permission", ErrPermission, 403, "permission_error", "permission_denied"},
		{"not_found", ErrNotFound, 404, "not_found_error", "not_found"},
		{"invalid_request", ErrInvalidRequest, 400, "invalid_request_error", "invalid_request"},
		{"rate_limit", ErrRateLimit, 429, "rate_limit_error", "rate_limit_exceeded"},
		{"service_unavailable", ErrServiceUnavailable, 503, "service_unavailable", "agent_unavailable"},
		{"timeout", ErrTimeout, 504, "timeout_error", "request_timeout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn("test message")
			if err.HTTPStatus != tt.wantStatus {
				t.Errorf("HTTPStatus = %d, want %d", err.HTTPStatus, tt.wantStatus)
			}
			if err.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", err.Type, tt.wantType)
			}
			if err.Code != tt.wantCode {
				t.Errorf("Code = %q, want %q", err.Code, tt.wantCode)
			}
		})
	}
}

func TestAPIErrorImplementsError(t *testing.T) {
	err := ErrNotFound("model not found")
	var _ error = err

	want := "not_found_error: model not found"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./pkg/errcode/ -v
```

预期：编译失败。

- [ ] **Step 3: 实现错误码**

创建 `pkg/errcode/errors.go`：

```go
package errcode

import "fmt"

type APIError struct {
	HTTPStatus int    `json:"-"`
	Type       string `json:"type"`
	Message    string `json:"message"`
	Code       string `json:"code"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

type ErrorResponse struct {
	Error *APIError `json:"error"`
}

func ErrAuthentication(msg string) *APIError {
	return &APIError{HTTPStatus: 401, Type: "authentication_error", Message: msg, Code: "invalid_api_key"}
}

func ErrPermission(msg string) *APIError {
	return &APIError{HTTPStatus: 403, Type: "permission_error", Message: msg, Code: "permission_denied"}
}

func ErrNotFound(msg string) *APIError {
	return &APIError{HTTPStatus: 404, Type: "not_found_error", Message: msg, Code: "not_found"}
}

func ErrInvalidRequest(msg string) *APIError {
	return &APIError{HTTPStatus: 400, Type: "invalid_request_error", Message: msg, Code: "invalid_request"}
}

func ErrRateLimit(msg string) *APIError {
	return &APIError{HTTPStatus: 429, Type: "rate_limit_error", Message: msg, Code: "rate_limit_exceeded"}
}

func ErrServiceUnavailable(msg string) *APIError {
	return &APIError{HTTPStatus: 503, Type: "service_unavailable", Message: msg, Code: "agent_unavailable"}
}

func ErrTimeout(msg string) *APIError {
	return &APIError{HTTPStatus: 504, Type: "timeout_error", Message: msg, Code: "request_timeout"}
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./pkg/errcode/ -v
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add pkg/errcode/
git commit -m "feat: add unified error codes in OpenAI format"
```

---

### Task 3: OpenAI 类型定义

**Files:**
- Create: `internal/openai/types.go`
- Create: `internal/openai/types_test.go`

- [ ] **Step 1: 编写类型序列化测试**

创建 `internal/openai/types_test.go`：

```go
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
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./internal/openai/ -v
```

预期：编译失败。

- [ ] **Step 3: 实现 OpenAI 类型**

创建 `internal/openai/types.go`：

```go
package openai

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatCompletionRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Stream      bool      `json:"stream"`
	Temperature *float64  `json:"temperature,omitempty"`
	MaxTokens   *int      `json:"max_tokens,omitempty"`
	TopP        *float64  `json:"top_p,omitempty"`
}

type Choice struct {
	Index        int      `json:"index"`
	Message      *Message `json:"message,omitempty"`
	FinishReason *string  `json:"finish_reason,omitempty"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type ChatCompletionResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

type Delta struct {
	Role    *string `json:"role,omitempty"`
	Content *string `json:"content,omitempty"`
}

type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason,omitempty"`
}

type ChatCompletionChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
}

type Model struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
}

type ModelList struct {
	Object string  `json:"object"`
	Data   []Model `json:"data"`
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/openai/ -v
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/openai/
git commit -m "feat: add OpenAI request/response type definitions"
```

---

### Task 4: Proto 定义与代码生成

**Files:**
- Create: `api/proto/agent.proto`
- Create: `api/proto/internal.proto`
- Generate: `api/proto/*.pb.go`, `api/proto/*_grpc.pb.go`

- [ ] **Step 1: 安装 protoc 工具（如尚未安装）**

```bash
# 检查 protoc 是否已安装
which protoc && protoc --version

# 安装 Go 的 protoc 插件
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

- [ ] **Step 2: 编写 agent.proto**

创建 `api/proto/agent.proto`：

```protobuf
syntax = "proto3";

package agentgw;

option go_package = "github.com/wereliang/aiagw2/api/proto";

service AgentGateway {
  rpc Connect(stream AgentMessage) returns (stream GatewayMessage);
}

message AgentRegister {
  string agent_id = 1;
  string tenant_id = 2;
  string agent_type = 3;
  map<string, string> metadata = 4;
}

message AgentMessage {
  oneof payload {
    AgentRegister register = 1;
    AgentResponse response = 2;
    Heartbeat heartbeat = 3;
  }
}

message GatewayMessage {
  oneof payload {
    AgentRequest request = 1;
    Heartbeat heartbeat = 2;
  }
}

message AgentRequest {
  string request_id = 1;
  string session_id = 2;
  string model = 3;
  repeated ChatMessage messages = 4;
  map<string, string> parameters = 5;
}

message AgentResponse {
  string request_id = 1;
  string session_id = 2;
  oneof content {
    ChatMessage message = 3;
    StreamChunk chunk = 4;
  }
  bool done = 5;
}

message ChatMessage {
  string role = 1;
  string content = 2;
}

message StreamChunk {
  string content = 1;
}

message Heartbeat {
  int64 timestamp = 1;
}
```

- [ ] **Step 3: 编写 internal.proto**

创建 `api/proto/internal.proto`：

```protobuf
syntax = "proto3";

package agentgw;

option go_package = "github.com/wereliang/aiagw2/api/proto";

import "agent.proto";

service GatewayInternal {
  rpc ForwardRequest(AgentRequest) returns (stream AgentResponse);
}
```

- [ ] **Step 4: 生成 Go 代码**

```bash
cd /Users/hunter/xy/mywork/go/src/github.com/wereliang/aiagw2
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       -I api/proto \
       api/proto/agent.proto api/proto/internal.proto
```

- [ ] **Step 5: 安装 gRPC 依赖并验证编译**

```bash
go get google.golang.org/grpc
go get google.golang.org/protobuf
go build ./api/proto/...
```

预期：编译通过，无错误。

- [ ] **Step 6: 提交**

```bash
git add api/proto/
git commit -m "feat: add gRPC proto definitions and generated code"
```

---

### Task 5: Redis 存储层

**Files:**
- Create: `internal/store/redis.go`
- Create: `internal/store/redis_test.go`

- [ ] **Step 1: 编写 Redis 封装测试**

创建 `internal/store/redis_test.go`：

```go
package store

import (
	"testing"

	"github.com/wereliang/aiagw2/internal/config"
)

func TestNewRedisStore(t *testing.T) {
	cfg := config.RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}

	store := New(cfg)
	if store == nil {
		t.Fatal("New() returned nil")
	}
	if store.Client() == nil {
		t.Fatal("Client() returned nil")
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./internal/store/ -v
```

预期：编译失败。

- [ ] **Step 3: 实现 Redis 封装**

创建 `internal/store/redis.go`：

```go
package store

import (
	"github.com/redis/go-redis/v9"
	"github.com/wereliang/aiagw2/internal/config"
)

type RedisStore struct {
	client *redis.Client
}

func New(cfg config.RedisConfig) *RedisStore {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	return &RedisStore{client: client}
}

func (s *RedisStore) Client() *redis.Client {
	return s.client
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}
```

- [ ] **Step 4: 安装依赖并运行测试**

```bash
go get github.com/redis/go-redis/v9
go test ./internal/store/ -v
```

预期：测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/store/
git commit -m "feat: add Redis store wrapper"
```

---

### Task 6: 租户存储

**Files:**
- Create: `internal/tenant/store.go`
- Create: `internal/tenant/store_test.go`

- [ ] **Step 1: 编写租户存储测试**

创建 `internal/tenant/store_test.go`：

```go
package tenant

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestGetByAPIKey(t *testing.T) {
	client := setupTestRedis(t)
	ctx := context.Background()

	client.Set(ctx, "apikey:abc123hash", "tenant-1", 0)
	client.HSet(ctx, "tenant:tenant-1", map[string]any{
		"name":                "Test Tenant",
		"status":              "active",
		"allowed_agent_types": "customer-service,code-assistant",
		"max_sessions":        "1000",
		"max_agents":          "10",
	})

	store := NewStore(client)

	info, err := store.GetByAPIKey(ctx, "abc123hash")
	if err != nil {
		t.Fatalf("GetByAPIKey() error: %v", err)
	}
	if info.ID != "tenant-1" {
		t.Errorf("ID = %q, want %q", info.ID, "tenant-1")
	}
	if info.Name != "Test Tenant" {
		t.Errorf("Name = %q, want %q", info.Name, "Test Tenant")
	}
	if info.Status != "active" {
		t.Errorf("Status = %q, want %q", info.Status, "active")
	}
	if len(info.AllowedAgentTypes) != 2 {
		t.Fatalf("AllowedAgentTypes len = %d, want 2", len(info.AllowedAgentTypes))
	}
	if info.AllowedAgentTypes[0] != "customer-service" {
		t.Errorf("AllowedAgentTypes[0] = %q, want %q", info.AllowedAgentTypes[0], "customer-service")
	}
	if info.MaxSessions != 1000 {
		t.Errorf("MaxSessions = %d, want 1000", info.MaxSessions)
	}
}

func TestGetByAPIKeyNotFound(t *testing.T) {
	client := setupTestRedis(t)
	ctx := context.Background()

	store := NewStore(client)

	_, err := store.GetByAPIKey(ctx, "nonexistent")
	if err != ErrTenantNotFound {
		t.Errorf("GetByAPIKey() error = %v, want ErrTenantNotFound", err)
	}
}

func TestExists(t *testing.T) {
	client := setupTestRedis(t)
	ctx := context.Background()

	client.HSet(ctx, "tenant:tenant-1", "name", "Test")

	store := NewStore(client)

	if !store.Exists(ctx, "tenant-1") {
		t.Error("Exists() = false, want true")
	}
	if store.Exists(ctx, "tenant-999") {
		t.Error("Exists() = true, want false")
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go get github.com/alicebob/miniredis/v2
go test ./internal/tenant/ -v
```

预期：编译失败。

- [ ] **Step 3: 实现租户存储**

创建 `internal/tenant/store.go`：

```go
package tenant

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

var ErrTenantNotFound = errors.New("tenant not found")

type Info struct {
	ID                string
	Name              string
	Status            string
	AllowedAgentTypes []string
	MaxSessions       int
	MaxAgents         int
}

type Store struct {
	client *redis.Client
}

func NewStore(client *redis.Client) *Store {
	return &Store{client: client}
}

func (s *Store) GetByAPIKey(ctx context.Context, keyHash string) (*Info, error) {
	tenantID, err := s.client.Get(ctx, "apikey:"+keyHash).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrTenantNotFound
		}
		return nil, err
	}
	return s.GetByID(ctx, tenantID)
}

func (s *Store) GetByID(ctx context.Context, tenantID string) (*Info, error) {
	vals, err := s.client.HGetAll(ctx, "tenant:"+tenantID).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, ErrTenantNotFound
	}

	maxSessions, _ := strconv.Atoi(vals["max_sessions"])
	maxAgents, _ := strconv.Atoi(vals["max_agents"])

	var agentTypes []string
	if raw := vals["allowed_agent_types"]; raw != "" {
		agentTypes = strings.Split(raw, ",")
	}

	return &Info{
		ID:                tenantID,
		Name:              vals["name"],
		Status:            vals["status"],
		AllowedAgentTypes: agentTypes,
		MaxSessions:       maxSessions,
		MaxAgents:         maxAgents,
	}, nil
}

func (s *Store) Exists(ctx context.Context, tenantID string) bool {
	n, err := s.client.Exists(ctx, "tenant:"+tenantID).Result()
	return err == nil && n > 0
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/tenant/ -v
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/tenant/
git commit -m "feat: add tenant store with Redis backend"
```

---

### Task 7: Session 管理器

**Files:**
- Create: `internal/session/manager.go`
- Create: `internal/session/manager_test.go`

- [ ] **Step 1: 编写 Session 管理器测试**

创建 `internal/session/manager_test.go`：

```go
package session

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return client, mr
}

func TestCreateAndGet(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()
	mgr := NewManager(client, 30*time.Minute, 1000)

	info, err := mgr.Create(ctx, "tenant-1", "agent-1", "gw-instance-1")
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	if info.ID == "" {
		t.Error("Session ID should not be empty")
	}
	if info.TenantID != "tenant-1" {
		t.Errorf("TenantID = %q, want %q", info.TenantID, "tenant-1")
	}
	if info.AgentID != "agent-1" {
		t.Errorf("AgentID = %q, want %q", info.AgentID, "agent-1")
	}
	if info.GatewayInstance != "gw-instance-1" {
		t.Errorf("GatewayInstance = %q, want %q", info.GatewayInstance, "gw-instance-1")
	}

	got, err := mgr.Get(ctx, info.ID)
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got.AgentID != "agent-1" {
		t.Errorf("Get().AgentID = %q, want %q", got.AgentID, "agent-1")
	}
}

func TestGetNotFound(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()
	mgr := NewManager(client, 30*time.Minute, 1000)

	_, err := mgr.Get(ctx, "nonexistent")
	if err != ErrSessionNotFound {
		t.Errorf("Get() error = %v, want ErrSessionNotFound", err)
	}
}

func TestDelete(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()
	mgr := NewManager(client, 30*time.Minute, 1000)

	info, _ := mgr.Create(ctx, "tenant-1", "agent-1", "gw-1")

	if err := mgr.Delete(ctx, info.ID, "tenant-1"); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	_, err := mgr.Get(ctx, info.ID)
	if err != ErrSessionNotFound {
		t.Errorf("After delete, Get() error = %v, want ErrSessionNotFound", err)
	}
}

func TestTouch(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()
	mgr := NewManager(client, 30*time.Minute, 1000)

	info, _ := mgr.Create(ctx, "tenant-1", "agent-1", "gw-1")

	if err := mgr.Touch(ctx, info.ID); err != nil {
		t.Fatalf("Touch() error: %v", err)
	}

	got, _ := mgr.Get(ctx, info.ID)
	if got.LastActiveAt.IsZero() {
		t.Error("LastActiveAt should not be zero after Touch()")
	}
}

func TestDeleteByAgent(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()
	mgr := NewManager(client, 30*time.Minute, 1000)

	s1, _ := mgr.Create(ctx, "tenant-1", "agent-1", "gw-1")
	s2, _ := mgr.Create(ctx, "tenant-1", "agent-1", "gw-1")
	s3, _ := mgr.Create(ctx, "tenant-1", "agent-2", "gw-1")

	if err := mgr.DeleteByAgent(ctx, "agent-1", "tenant-1"); err != nil {
		t.Fatalf("DeleteByAgent() error: %v", err)
	}

	if _, err := mgr.Get(ctx, s1.ID); err != ErrSessionNotFound {
		t.Error("Session s1 should be deleted")
	}
	if _, err := mgr.Get(ctx, s2.ID); err != ErrSessionNotFound {
		t.Error("Session s2 should be deleted")
	}
	if _, err := mgr.Get(ctx, s3.ID); err != nil {
		t.Errorf("Session s3 should still exist, got error: %v", err)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./internal/session/ -v
```

预期：编译失败。

- [ ] **Step 3: 实现 Session 管理器**

创建 `internal/session/manager.go`：

```go
package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var ErrSessionNotFound = errors.New("session not found")

type Info struct {
	ID              string
	TenantID        string
	AgentID         string
	GatewayInstance string
	CreatedAt       time.Time
	LastActiveAt    time.Time
}

type Manager struct {
	client       *redis.Client
	ttl          time.Duration
	maxPerTenant int
}

func NewManager(client *redis.Client, ttl time.Duration, maxPerTenant int) *Manager {
	return &Manager{client: client, ttl: ttl, maxPerTenant: maxPerTenant}
}

func (m *Manager) Create(ctx context.Context, tenantID, agentID, gatewayInstance string) (*Info, error) {
	sessionID := uuid.New().String()
	now := time.Now()

	pipe := m.client.Pipeline()
	key := "session:" + sessionID
	pipe.HSet(ctx, key, map[string]any{
		"tenant_id":        tenantID,
		"agent_id":         agentID,
		"gateway_instance": gatewayInstance,
		"created_at":       now.Unix(),
		"last_active_at":   now.Unix(),
	})
	pipe.Expire(ctx, key, m.ttl)
	pipe.SAdd(ctx, "tenant_sessions:"+tenantID, sessionID)

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}

	return &Info{
		ID:              sessionID,
		TenantID:        tenantID,
		AgentID:         agentID,
		GatewayInstance: gatewayInstance,
		CreatedAt:       now,
		LastActiveAt:    now,
	}, nil
}

func (m *Manager) Get(ctx context.Context, sessionID string) (*Info, error) {
	vals, err := m.client.HGetAll(ctx, "session:"+sessionID).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, ErrSessionNotFound
	}

	createdAt, _ := parseUnix(vals["created_at"])
	lastActiveAt, _ := parseUnix(vals["last_active_at"])

	return &Info{
		ID:              sessionID,
		TenantID:        vals["tenant_id"],
		AgentID:         vals["agent_id"],
		GatewayInstance: vals["gateway_instance"],
		CreatedAt:       createdAt,
		LastActiveAt:    lastActiveAt,
	}, nil
}

func (m *Manager) Touch(ctx context.Context, sessionID string) error {
	key := "session:" + sessionID
	pipe := m.client.Pipeline()
	pipe.HSet(ctx, key, "last_active_at", time.Now().Unix())
	pipe.Expire(ctx, key, m.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (m *Manager) Delete(ctx context.Context, sessionID, tenantID string) error {
	pipe := m.client.Pipeline()
	pipe.Del(ctx, "session:"+sessionID)
	pipe.SRem(ctx, "tenant_sessions:"+tenantID, sessionID)
	_, err := pipe.Exec(ctx)
	return err
}

func (m *Manager) DeleteByAgent(ctx context.Context, agentID, tenantID string) error {
	sessionIDs, err := m.client.SMembers(ctx, "tenant_sessions:"+tenantID).Result()
	if err != nil {
		return err
	}

	for _, sid := range sessionIDs {
		aid, err := m.client.HGet(ctx, "session:"+sid, "agent_id").Result()
		if err != nil {
			continue
		}
		if aid == agentID {
			m.Delete(ctx, sid, tenantID)
		}
	}
	return nil
}

func (m *Manager) CountByTenant(ctx context.Context, tenantID string) (int64, error) {
	return m.client.SCard(ctx, "tenant_sessions:"+tenantID).Result()
}

func parseUnix(s string) (time.Time, error) {
	var ts int64
	fmt.Sscanf(s, "%d", &ts)
	if ts == 0 {
		return time.Time{}, fmt.Errorf("invalid timestamp: %s", s)
	}
	return time.Unix(ts, 0), nil
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/session/ -v
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/session/
git commit -m "feat: add session manager with Redis backend"
```

---

### Task 8: Agent 连接池

**Files:**
- Create: `internal/agent/pool.go`
- Create: `internal/agent/pool_test.go`

- [ ] **Step 1: 编写 Agent 连接池测试**

创建 `internal/agent/pool_test.go`：

```go
package agent

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestPoolRegisterAndGet(t *testing.T) {
	client := setupTestRedis(t)
	pool := NewPool(client, "gw-1", 90*time.Second)
	ctx := context.Background()

	info := &AgentInfo{
		ID:        "agent-1",
		TenantID:  "tenant-1",
		AgentType: "customer-service",
	}

	if err := pool.Register(ctx, info); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	got, err := pool.Get(ctx, "agent-1")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got.TenantID != "tenant-1" {
		t.Errorf("TenantID = %q, want %q", got.TenantID, "tenant-1")
	}
	if got.AgentType != "customer-service" {
		t.Errorf("AgentType = %q, want %q", got.AgentType, "customer-service")
	}
	if got.Status != StatusOnline {
		t.Errorf("Status = %q, want %q", got.Status, StatusOnline)
	}
	if got.GatewayInstance != "gw-1" {
		t.Errorf("GatewayInstance = %q, want %q", got.GatewayInstance, "gw-1")
	}
}

func TestPoolUnregister(t *testing.T) {
	client := setupTestRedis(t)
	pool := NewPool(client, "gw-1", 90*time.Second)
	ctx := context.Background()

	info := &AgentInfo{ID: "agent-1", TenantID: "tenant-1", AgentType: "support"}
	pool.Register(ctx, info)
	pool.Unregister(ctx, "agent-1", "tenant-1", "support")

	_, err := pool.Get(ctx, "agent-1")
	if err != ErrAgentNotFound {
		t.Errorf("Get() error = %v, want ErrAgentNotFound", err)
	}
}

func TestPoolListByTenantAndType(t *testing.T) {
	client := setupTestRedis(t)
	pool := NewPool(client, "gw-1", 90*time.Second)
	ctx := context.Background()

	pool.Register(ctx, &AgentInfo{ID: "a1", TenantID: "t1", AgentType: "support"})
	pool.Register(ctx, &AgentInfo{ID: "a2", TenantID: "t1", AgentType: "support"})
	pool.Register(ctx, &AgentInfo{ID: "a3", TenantID: "t1", AgentType: "coding"})
	pool.Register(ctx, &AgentInfo{ID: "a4", TenantID: "t2", AgentType: "support"})

	agents, err := pool.ListByTenantAndType(ctx, "t1", "support")
	if err != nil {
		t.Fatalf("ListByTenantAndType() error: %v", err)
	}
	if len(agents) != 2 {
		t.Errorf("len = %d, want 2", len(agents))
	}
}

func TestPoolUpdateHeartbeat(t *testing.T) {
	client := setupTestRedis(t)
	pool := NewPool(client, "gw-1", 90*time.Second)
	ctx := context.Background()

	pool.Register(ctx, &AgentInfo{ID: "agent-1", TenantID: "tenant-1", AgentType: "support"})

	if err := pool.UpdateHeartbeat(ctx, "agent-1"); err != nil {
		t.Fatalf("UpdateHeartbeat() error: %v", err)
	}
}

func TestPoolIncrDecrActiveSessions(t *testing.T) {
	client := setupTestRedis(t)
	pool := NewPool(client, "gw-1", 90*time.Second)
	ctx := context.Background()

	pool.Register(ctx, &AgentInfo{ID: "agent-1", TenantID: "tenant-1", AgentType: "support"})

	pool.IncrActiveSessions(ctx, "agent-1")
	pool.IncrActiveSessions(ctx, "agent-1")

	got, _ := pool.Get(ctx, "agent-1")
	if got.ActiveSessions != 2 {
		t.Errorf("ActiveSessions = %d, want 2", got.ActiveSessions)
	}

	pool.DecrActiveSessions(ctx, "agent-1")
	got, _ = pool.Get(ctx, "agent-1")
	if got.ActiveSessions != 1 {
		t.Errorf("ActiveSessions = %d, want 1", got.ActiveSessions)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./internal/agent/ -v
```

预期：编译失败。

- [ ] **Step 3: 实现 Agent 连接池**

创建 `internal/agent/pool.go`：

```go
package agent

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	StatusOnline  = "online"
	StatusOffline = "offline"
)

var ErrAgentNotFound = errors.New("agent not found")

type AgentInfo struct {
	ID              string
	TenantID        string
	AgentType       string
	GatewayInstance string
	Status          string
	LastHeartbeat   time.Time
	ConnectedAt     time.Time
	ActiveSessions  int
}

type Pool struct {
	client           *redis.Client
	gatewayInstance   string
	heartbeatTimeout time.Duration
}

func NewPool(client *redis.Client, gatewayInstance string, heartbeatTimeout time.Duration) *Pool {
	return &Pool{
		client:           client,
		gatewayInstance:   gatewayInstance,
		heartbeatTimeout: heartbeatTimeout,
	}
}

func (p *Pool) Register(ctx context.Context, info *AgentInfo) error {
	now := time.Now()
	key := "agent:" + info.ID

	pipe := p.client.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"tenant_id":        info.TenantID,
		"agent_type":       info.AgentType,
		"gateway_instance": p.gatewayInstance,
		"status":           StatusOnline,
		"last_heartbeat":   now.Unix(),
		"connected_at":     now.Unix(),
		"active_sessions":  0,
	})
	pipe.Expire(ctx, key, p.heartbeatTimeout*2)
	pipe.SAdd(ctx, fmt.Sprintf("tenant_agents:%s:%s", info.TenantID, info.AgentType), info.ID)

	_, err := pipe.Exec(ctx)
	return err
}

func (p *Pool) Unregister(ctx context.Context, agentID, tenantID, agentType string) error {
	pipe := p.client.Pipeline()
	pipe.Del(ctx, "agent:"+agentID)
	pipe.SRem(ctx, fmt.Sprintf("tenant_agents:%s:%s", tenantID, agentType), agentID)
	_, err := pipe.Exec(ctx)
	return err
}

func (p *Pool) Get(ctx context.Context, agentID string) (*AgentInfo, error) {
	vals, err := p.client.HGetAll(ctx, "agent:"+agentID).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, ErrAgentNotFound
	}

	lastHB, _ := parseUnix(vals["last_heartbeat"])
	connAt, _ := parseUnix(vals["connected_at"])
	activeSessions, _ := strconv.Atoi(vals["active_sessions"])

	return &AgentInfo{
		ID:              agentID,
		TenantID:        vals["tenant_id"],
		AgentType:       vals["agent_type"],
		GatewayInstance: vals["gateway_instance"],
		Status:          vals["status"],
		LastHeartbeat:   lastHB,
		ConnectedAt:     connAt,
		ActiveSessions:  activeSessions,
	}, nil
}

func (p *Pool) ListByTenantAndType(ctx context.Context, tenantID, agentType string) ([]*AgentInfo, error) {
	agentIDs, err := p.client.SMembers(ctx, fmt.Sprintf("tenant_agents:%s:%s", tenantID, agentType)).Result()
	if err != nil {
		return nil, err
	}

	var agents []*AgentInfo
	for _, id := range agentIDs {
		info, err := p.Get(ctx, id)
		if err != nil {
			continue
		}
		agents = append(agents, info)
	}
	return agents, nil
}

func (p *Pool) UpdateHeartbeat(ctx context.Context, agentID string) error {
	key := "agent:" + agentID
	pipe := p.client.Pipeline()
	pipe.HSet(ctx, key, "last_heartbeat", time.Now().Unix())
	pipe.Expire(ctx, key, p.heartbeatTimeout*2)
	_, err := pipe.Exec(ctx)
	return err
}

func (p *Pool) IncrActiveSessions(ctx context.Context, agentID string) error {
	return p.client.HIncrBy(ctx, "agent:"+agentID, "active_sessions", 1).Err()
}

func (p *Pool) DecrActiveSessions(ctx context.Context, agentID string) error {
	return p.client.HIncrBy(ctx, "agent:"+agentID, "active_sessions", -1).Err()
}

func parseUnix(s string) (time.Time, error) {
	var ts int64
	fmt.Sscanf(s, "%d", &ts)
	if ts == 0 {
		return time.Time{}, fmt.Errorf("invalid timestamp: %s", s)
	}
	return time.Unix(ts, 0), nil
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/agent/ -v
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/agent/pool.go internal/agent/pool_test.go
git commit -m "feat: add agent connection pool with Redis state"
```

---

### Task 9: Agent 路由器

**Files:**
- Create: `internal/router/router.go`
- Create: `internal/router/router_test.go`

- [ ] **Step 1: 编写路由器测试**

创建 `internal/router/router_test.go`：

```go
package router

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/wereliang/aiagw2/internal/agent"
	"github.com/wereliang/aiagw2/internal/tenant"
)

func setupTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func seedTenant(ctx context.Context, client *redis.Client, tenantID string, agentTypes []string) {
	client.HSet(ctx, "tenant:"+tenantID, map[string]any{
		"name":                "Test",
		"status":              "active",
		"allowed_agent_types": joinStrings(agentTypes),
		"max_sessions":        "1000",
		"max_agents":          "10",
	})
}

func joinStrings(ss []string) string {
	result := ""
	for i, s := range ss {
		if i > 0 {
			result += ","
		}
		result += s
	}
	return result
}

func TestRouteSelectsLeastLoaded(t *testing.T) {
	client := setupTestRedis(t)
	ctx := context.Background()

	seedTenant(ctx, client, "t1", []string{"support"})

	pool := agent.NewPool(client, "gw-1", 90*time.Second)
	pool.Register(ctx, &agent.AgentInfo{ID: "a1", TenantID: "t1", AgentType: "support"})
	pool.Register(ctx, &agent.AgentInfo{ID: "a2", TenantID: "t1", AgentType: "support"})

	pool.IncrActiveSessions(ctx, "a1")
	pool.IncrActiveSessions(ctx, "a1")
	pool.IncrActiveSessions(ctx, "a2")

	tenantStore := tenant.NewStore(client)
	r := New(pool, tenantStore)

	selected, err := r.Route(ctx, "t1", "support")
	if err != nil {
		t.Fatalf("Route() error: %v", err)
	}
	if selected.ID != "a2" {
		t.Errorf("Route() selected %q, want %q (least loaded)", selected.ID, "a2")
	}
}

func TestRouteNoAgentsAvailable(t *testing.T) {
	client := setupTestRedis(t)
	ctx := context.Background()

	seedTenant(ctx, client, "t1", []string{"support"})

	pool := agent.NewPool(client, "gw-1", 90*time.Second)
	tenantStore := tenant.NewStore(client)
	r := New(pool, tenantStore)

	_, err := r.Route(ctx, "t1", "support")
	if err != ErrNoAgentAvailable {
		t.Errorf("Route() error = %v, want ErrNoAgentAvailable", err)
	}
}

func TestRouteAgentTypeNotAllowed(t *testing.T) {
	client := setupTestRedis(t)
	ctx := context.Background()

	seedTenant(ctx, client, "t1", []string{"support"})

	pool := agent.NewPool(client, "gw-1", 90*time.Second)
	tenantStore := tenant.NewStore(client)
	r := New(pool, tenantStore)

	_, err := r.Route(ctx, "t1", "coding")
	if err != ErrAgentTypeNotAllowed {
		t.Errorf("Route() error = %v, want ErrAgentTypeNotAllowed", err)
	}
}

func TestRouteSkipsOfflineAgents(t *testing.T) {
	client := setupTestRedis(t)
	ctx := context.Background()

	seedTenant(ctx, client, "t1", []string{"support"})

	pool := agent.NewPool(client, "gw-1", 90*time.Second)
	pool.Register(ctx, &agent.AgentInfo{ID: "a1", TenantID: "t1", AgentType: "support"})
	pool.Register(ctx, &agent.AgentInfo{ID: "a2", TenantID: "t1", AgentType: "support"})

	client.HSet(ctx, "agent:a1", "status", agent.StatusOffline)

	tenantStore := tenant.NewStore(client)
	r := New(pool, tenantStore)

	selected, err := r.Route(ctx, "t1", "support")
	if err != nil {
		t.Fatalf("Route() error: %v", err)
	}
	if selected.ID != "a2" {
		t.Errorf("Route() selected %q, want %q (only online agent)", selected.ID, "a2")
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./internal/router/ -v
```

预期：编译失败。

- [ ] **Step 3: 实现路由器**

创建 `internal/router/router.go`：

```go
package router

import (
	"context"
	"errors"

	"github.com/wereliang/aiagw2/internal/agent"
	"github.com/wereliang/aiagw2/internal/tenant"
)

var (
	ErrNoAgentAvailable   = errors.New("no available agent")
	ErrAgentTypeNotAllowed = errors.New("agent type not allowed for tenant")
)

type Router struct {
	pool        *agent.Pool
	tenantStore *tenant.Store
}

func New(pool *agent.Pool, tenantStore *tenant.Store) *Router {
	return &Router{pool: pool, tenantStore: tenantStore}
}

func (r *Router) Route(ctx context.Context, tenantID, agentType string) (*agent.AgentInfo, error) {
	info, err := r.tenantStore.GetByID(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	if !isAllowed(info.AllowedAgentTypes, agentType) {
		return nil, ErrAgentTypeNotAllowed
	}

	agents, err := r.pool.ListByTenantAndType(ctx, tenantID, agentType)
	if err != nil {
		return nil, err
	}

	var best *agent.AgentInfo
	for _, a := range agents {
		if a.Status != agent.StatusOnline {
			continue
		}
		if best == nil || a.ActiveSessions < best.ActiveSessions {
			best = a
		}
	}

	if best == nil {
		return nil, ErrNoAgentAvailable
	}
	return best, nil
}

func isAllowed(allowed []string, agentType string) bool {
	for _, t := range allowed {
		if t == agentType {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/router/ -v
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/router/
git commit -m "feat: add agent router with least-connections load balancing"
```

---

### Task 10: OpenAI ↔ gRPC 转换器

**Files:**
- Create: `internal/openai/converter.go`
- Create: `internal/openai/converter_test.go`

- [ ] **Step 1: 编写转换器测试**

创建 `internal/openai/converter_test.go`：

```go
package openai

import (
	"testing"

	pb "github.com/wereliang/aiagw2/api/proto"
)

func TestToAgentRequest(t *testing.T) {
	req := &ChatCompletionRequest{
		Model: "customer-service",
		Messages: []Message{
			{Role: "system", Content: "You are helpful."},
			{Role: "user", Content: "Hello"},
		},
		Temperature: float64Ptr(0.7),
	}

	agentReq := ToAgentRequest("req-123", "sess-456", req)

	if agentReq.RequestId != "req-123" {
		t.Errorf("RequestId = %q, want %q", agentReq.RequestId, "req-123")
	}
	if agentReq.SessionId != "sess-456" {
		t.Errorf("SessionId = %q, want %q", agentReq.SessionId, "sess-456")
	}
	if agentReq.Model != "customer-service" {
		t.Errorf("Model = %q, want %q", agentReq.Model, "customer-service")
	}
	if len(agentReq.Messages) != 2 {
		t.Fatalf("Messages len = %d, want 2", len(agentReq.Messages))
	}
	if agentReq.Messages[0].Role != "system" {
		t.Errorf("Messages[0].Role = %q, want %q", agentReq.Messages[0].Role, "system")
	}
	if agentReq.Parameters["temperature"] != "0.7" {
		t.Errorf("Parameters[temperature] = %q, want %q", agentReq.Parameters["temperature"], "0.7")
	}
}

func TestFromAgentResponseFull(t *testing.T) {
	agentResp := &pb.AgentResponse{
		RequestId: "req-123",
		SessionId: "sess-456",
		Content:   &pb.AgentResponse_Message{Message: &pb.ChatMessage{Role: "assistant", Content: "Hi there!"}},
		Done:      true,
	}

	resp := FromAgentResponse(agentResp, "customer-service")

	if resp.ID != "chatcmpl-req-123" {
		t.Errorf("ID = %q, want %q", resp.ID, "chatcmpl-req-123")
	}
	if resp.Object != "chat.completion" {
		t.Errorf("Object = %q, want %q", resp.Object, "chat.completion")
	}
	if len(resp.Choices) != 1 {
		t.Fatalf("Choices len = %d, want 1", len(resp.Choices))
	}
	if resp.Choices[0].Message.Content != "Hi there!" {
		t.Errorf("Content = %q, want %q", resp.Choices[0].Message.Content, "Hi there!")
	}
	if *resp.Choices[0].FinishReason != "stop" {
		t.Errorf("FinishReason = %q, want %q", *resp.Choices[0].FinishReason, "stop")
	}
}

func TestFromAgentResponseChunk(t *testing.T) {
	agentResp := &pb.AgentResponse{
		RequestId: "req-123",
		Content:   &pb.AgentResponse_Chunk{Chunk: &pb.StreamChunk{Content: "Hello"}},
		Done:      false,
	}

	chunk := FromAgentResponseChunk(agentResp, "customer-service")

	if chunk.Object != "chat.completion.chunk" {
		t.Errorf("Object = %q, want %q", chunk.Object, "chat.completion.chunk")
	}
	if *chunk.Choices[0].Delta.Content != "Hello" {
		t.Errorf("Delta.Content = %q, want %q", *chunk.Choices[0].Delta.Content, "Hello")
	}
	if chunk.Choices[0].FinishReason != nil {
		t.Error("FinishReason should be nil for non-done chunk")
	}
}

func TestFromAgentResponseChunkDone(t *testing.T) {
	agentResp := &pb.AgentResponse{
		RequestId: "req-123",
		Done:      true,
	}

	chunk := FromAgentResponseChunk(agentResp, "customer-service")

	if *chunk.Choices[0].FinishReason != "stop" {
		t.Errorf("FinishReason = %v, want stop", chunk.Choices[0].FinishReason)
	}
}

func float64Ptr(f float64) *float64 {
	return &f
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./internal/openai/ -v -run TestTo
```

预期：编译失败。

- [ ] **Step 3: 实现转换器**

创建 `internal/openai/converter.go`：

```go
package openai

import (
	"fmt"

	pb "github.com/wereliang/aiagw2/api/proto"
)

func ToAgentRequest(requestID, sessionID string, req *ChatCompletionRequest) *pb.AgentRequest {
	messages := make([]*pb.ChatMessage, len(req.Messages))
	for i, m := range req.Messages {
		messages[i] = &pb.ChatMessage{Role: m.Role, Content: m.Content}
	}

	params := make(map[string]string)
	if req.Temperature != nil {
		params["temperature"] = fmt.Sprintf("%g", *req.Temperature)
	}
	if req.MaxTokens != nil {
		params["max_tokens"] = fmt.Sprintf("%d", *req.MaxTokens)
	}
	if req.TopP != nil {
		params["top_p"] = fmt.Sprintf("%g", *req.TopP)
	}

	return &pb.AgentRequest{
		RequestId:  requestID,
		SessionId:  sessionID,
		Model:      req.Model,
		Messages:   messages,
		Parameters: params,
	}
}

func FromAgentResponse(resp *pb.AgentResponse, model string) *ChatCompletionResponse {
	var msg *Message
	if m := resp.GetMessage(); m != nil {
		msg = &Message{Role: m.Role, Content: m.Content}
	}

	finishReason := "stop"
	return &ChatCompletionResponse{
		ID:     "chatcmpl-" + resp.RequestId,
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

func FromAgentResponseChunk(resp *pb.AgentResponse, model string) *ChatCompletionChunk {
	choice := ChunkChoice{Index: 0}

	if chunk := resp.GetChunk(); chunk != nil {
		choice.Delta = Delta{Content: &chunk.Content}
	}

	if resp.Done {
		stop := "stop"
		choice.FinishReason = &stop
		choice.Delta = Delta{}
	}

	return &ChatCompletionChunk{
		ID:      "chatcmpl-" + resp.RequestId,
		Object:  "chat.completion.chunk",
		Model:   model,
		Choices: []ChunkChoice{choice},
	}
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/openai/ -v
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/openai/converter.go internal/openai/converter_test.go
git commit -m "feat: add OpenAI to gRPC message converter"
```

---

### Task 11: Auth 中间件

**Files:**
- Create: `internal/middleware/auth.go`
- Create: `internal/middleware/auth_test.go`

- [ ] **Step 1: 编写 Auth 中间件测试**

创建 `internal/middleware/auth_test.go`：

```go
package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/wereliang/aiagw2/internal/tenant"
)

func setupTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func setupRouter(tenantStore *tenant.Store) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Auth(tenantStore))
	r.GET("/test", func(c *gin.Context) {
		tenantID := GetTenantID(c)
		c.JSON(200, gin.H{"tenant_id": tenantID})
	})
	return r
}

func TestAuthValidKey(t *testing.T) {
	client := setupTestRedis(t)
	ctx := context.Background()

	client.Set(ctx, "apikey:test-key-hash", "tenant-1", 0)
	client.HSet(ctx, "tenant:tenant-1", map[string]any{
		"name":                "Test",
		"status":              "active",
		"allowed_agent_types": "support",
		"max_sessions":        "1000",
		"max_agents":          "10",
	})

	store := tenant.NewStore(client)
	router := setupRouter(store)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer test-key-hash")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestAuthMissingHeader(t *testing.T) {
	client := setupTestRedis(t)
	store := tenant.NewStore(client)
	router := setupRouter(store)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestAuthInvalidKey(t *testing.T) {
	client := setupTestRedis(t)
	store := tenant.NewStore(client)
	router := setupRouter(store)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer invalid-key")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestAuthWrongFormat(t *testing.T) {
	client := setupTestRedis(t)
	store := tenant.NewStore(client)
	router := setupRouter(store)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Basic abc123")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go get github.com/gin-gonic/gin
go test ./internal/middleware/ -v
```

预期：编译失败。

- [ ] **Step 3: 实现 Auth 中间件**

创建 `internal/middleware/auth.go`：

```go
package middleware

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/wereliang/aiagw2/internal/tenant"
	"github.com/wereliang/aiagw2/pkg/errcode"
)

const tenantContextKey = "tenant_info"

func Auth(tenantStore *tenant.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			abortWithError(c, errcode.ErrAuthentication("missing Authorization header"))
			return
		}

		if !strings.HasPrefix(authHeader, "Bearer ") {
			abortWithError(c, errcode.ErrAuthentication("invalid Authorization format, expected Bearer token"))
			return
		}

		apiKey := strings.TrimPrefix(authHeader, "Bearer ")

		info, err := tenantStore.GetByAPIKey(c.Request.Context(), apiKey)
		if err != nil {
			abortWithError(c, errcode.ErrAuthentication("invalid API key"))
			return
		}

		c.Set(tenantContextKey, info)
		c.Next()
	}
}

func GetTenantID(c *gin.Context) string {
	info := GetTenantInfo(c)
	if info == nil {
		return ""
	}
	return info.ID
}

func GetTenantInfo(c *gin.Context) *tenant.Info {
	val, exists := c.Get(tenantContextKey)
	if !exists {
		return nil
	}
	info, ok := val.(*tenant.Info)
	if !ok {
		return nil
	}
	return info
}

func abortWithError(c *gin.Context, apiErr *errcode.APIError) {
	c.AbortWithStatusJSON(apiErr.HTTPStatus, errcode.ErrorResponse{Error: apiErr})
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/middleware/ -v
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/middleware/auth.go internal/middleware/auth_test.go
git commit -m "feat: add API Key auth middleware"
```

---

### Task 12: 日志中间件

**Files:**
- Create: `internal/middleware/logging.go`

- [ ] **Step 1: 实现日志中间件**

创建 `internal/middleware/logging.go`：

```go
package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func Logging(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		logger.Info("request",
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
			zap.String("tenant_id", GetTenantID(c)),
			zap.String("client_ip", c.ClientIP()),
		)
	}
}
```

- [ ] **Step 2: 安装 zap 依赖并验证编译**

```bash
go get go.uber.org/zap
go build ./internal/middleware/
```

预期：编译通过。

- [ ] **Step 3: 提交**

```bash
git add internal/middleware/logging.go
git commit -m "feat: add request logging middleware"
```

---

### Task 13: gRPC 服务端（Agent 注册与双向流）

**Files:**
- Create: `internal/agent/grpc_server.go`
- Create: `internal/agent/grpc_server_test.go`

- [ ] **Step 1: 编写 gRPC 服务端测试**

创建 `internal/agent/grpc_server_test.go`：

```go
package agent

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	pb "github.com/wereliang/aiagw2/api/proto"
	"github.com/wereliang/aiagw2/internal/tenant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupGRPCServer(t *testing.T) (pb.AgentGatewayClient, *GRPCServer, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	ctx := context.Background()
	client.HSet(ctx, "tenant:tenant-1", map[string]any{
		"name":                "Test",
		"status":              "active",
		"allowed_agent_types": "support",
		"max_sessions":        "1000",
		"max_agents":          "10",
	})

	pool := NewPool(client, "gw-test", 90*time.Second)
	tenantStore := tenant.NewStore(client)
	server := NewGRPCServer(pool, tenantStore)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAgentGatewayServer(grpcServer, server)

	go grpcServer.Serve(lis)
	t.Cleanup(func() { grpcServer.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewAgentGatewayClient(conn), server, client
}

func TestAgentConnectAndRegister(t *testing.T) {
	agentClient, server, client := setupGRPCServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := agentClient.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error: %v", err)
	}

	err = stream.Send(&pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   "agent-1",
				TenantId:  "tenant-1",
				AgentType: "support",
			},
		},
	})
	if err != nil {
		t.Fatalf("Send register error: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	info, err := client.HGetAll(ctx, "agent:agent-1").Result()
	if err != nil {
		t.Fatalf("Redis HGetAll error: %v", err)
	}
	if info["tenant_id"] != "tenant-1" {
		t.Errorf("tenant_id = %q, want %q", info["tenant_id"], "tenant-1")
	}
	if info["status"] != StatusOnline {
		t.Errorf("status = %q, want %q", info["status"], StatusOnline)
	}

	if !server.HasLocalAgent("agent-1") {
		t.Error("HasLocalAgent() = false, want true")
	}
}

func TestAgentConnectInvalidTenant(t *testing.T) {
	agentClient, _, _ := setupGRPCServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := agentClient.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error: %v", err)
	}

	err = stream.Send(&pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   "agent-bad",
				TenantId:  "nonexistent-tenant",
				AgentType: "support",
			},
		},
	})
	if err != nil {
		t.Fatalf("Send register error: %v", err)
	}

	_, err = stream.Recv()
	if err == nil {
		t.Error("Expected error for invalid tenant, got nil")
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./internal/agent/ -v -run TestAgent
```

预期：编译失败。

- [ ] **Step 3: 实现 gRPC 服务端**

创建 `internal/agent/grpc_server.go`：

```go
package agent

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/wereliang/aiagw2/api/proto"
	"github.com/wereliang/aiagw2/internal/tenant"
)

type agentConn struct {
	stream   pb.AgentGateway_ConnectServer
	info     *AgentInfo
	requests chan *pb.AgentRequest
	done     chan struct{}
}

type GRPCServer struct {
	pb.UnimplementedAgentGatewayServer
	pool        *Pool
	tenantStore *tenant.Store
	logger      *zap.Logger
	mu          sync.RWMutex
	localAgents map[string]*agentConn
}

func NewGRPCServer(pool *Pool, tenantStore *tenant.Store) *GRPCServer {
	logger, _ := zap.NewProduction()
	return &GRPCServer{
		pool:        pool,
		tenantStore: tenantStore,
		logger:      logger,
		localAgents: make(map[string]*agentConn),
	}
}

func (s *GRPCServer) Connect(stream pb.AgentGateway_ConnectServer) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}

	reg := msg.GetRegister()
	if reg == nil {
		return status.Error(codes.InvalidArgument, "first message must be register")
	}

	ctx := stream.Context()
	if !s.tenantStore.Exists(ctx, reg.TenantId) {
		return status.Error(codes.PermissionDenied, "invalid tenant_id")
	}

	info := &AgentInfo{
		ID:        reg.AgentId,
		TenantID:  reg.TenantId,
		AgentType: reg.AgentType,
	}

	if err := s.pool.Register(ctx, info); err != nil {
		return status.Errorf(codes.Internal, "register agent: %v", err)
	}

	conn := &agentConn{
		stream:   stream,
		info:     info,
		requests: make(chan *pb.AgentRequest, 64),
		done:     make(chan struct{}),
	}

	s.mu.Lock()
	s.localAgents[reg.AgentId] = conn
	s.mu.Unlock()

	s.logger.Info("agent registered",
		zap.String("agent_id", reg.AgentId),
		zap.String("tenant_id", reg.TenantId),
		zap.String("agent_type", reg.AgentType),
	)

	defer s.handleDisconnect(info)

	go s.sendLoop(conn)

	return s.recvLoop(conn)
}

func (s *GRPCServer) sendLoop(conn *agentConn) {
	for {
		select {
		case req := <-conn.requests:
			err := conn.stream.Send(&pb.GatewayMessage{
				Payload: &pb.GatewayMessage_Request{Request: req},
			})
			if err != nil {
				s.logger.Error("send to agent failed",
					zap.String("agent_id", conn.info.ID),
					zap.Error(err),
				)
				return
			}
		case <-conn.done:
			return
		}
	}
}

func (s *GRPCServer) recvLoop(conn *agentConn) error {
	for {
		msg, err := conn.stream.Recv()
		if err != nil {
			return err
		}

		switch p := msg.Payload.(type) {
		case *pb.AgentMessage_Heartbeat:
			s.pool.UpdateHeartbeat(context.Background(), conn.info.ID)
		case *pb.AgentMessage_Response:
			s.handleAgentResponse(conn.info.ID, p.Response)
		}
	}
}

func (s *GRPCServer) handleAgentResponse(agentID string, resp *pb.AgentResponse) {
	s.mu.RLock()
	handler := s.responseHandlers[resp.RequestId]
	s.mu.RUnlock()

	if handler != nil {
		handler(resp)
	}
}

func (s *GRPCServer) handleDisconnect(info *AgentInfo) {
	s.mu.Lock()
	if conn, ok := s.localAgents[info.ID]; ok {
		close(conn.done)
		delete(s.localAgents, info.ID)
	}
	s.mu.Unlock()

	ctx := context.Background()
	s.pool.Unregister(ctx, info.ID, info.TenantID, info.AgentType)

	s.logger.Info("agent disconnected",
		zap.String("agent_id", info.ID),
		zap.String("tenant_id", info.TenantID),
	)
}

func (s *GRPCServer) HasLocalAgent(agentID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.localAgents[agentID]
	return ok
}

func (s *GRPCServer) SendRequest(agentID string, req *pb.AgentRequest) error {
	s.mu.RLock()
	conn, ok := s.localAgents[agentID]
	s.mu.RUnlock()

	if !ok {
		return ErrAgentNotFound
	}

	select {
	case conn.requests <- req:
		return nil
	default:
		return status.Error(codes.ResourceExhausted, "agent request queue full")
	}
}

type ResponseHandler func(resp *pb.AgentResponse)

// responseHandlers 需要在 GRPCServer struct 中添加
// 这里通过方法注册/注销 response handler
func (s *GRPCServer) RegisterResponseHandler(requestID string, handler ResponseHandler) {
	s.mu.Lock()
	if s.responseHandlers == nil {
		s.responseHandlers = make(map[string]ResponseHandler)
	}
	s.responseHandlers[requestID] = handler
	s.mu.Unlock()
}

func (s *GRPCServer) UnregisterResponseHandler(requestID string) {
	s.mu.Lock()
	delete(s.responseHandlers, requestID)
	s.mu.Unlock()
}
```

注意：需要在 `GRPCServer` struct 中添加 `responseHandlers` 字段。更新 struct 定义：

```go
type GRPCServer struct {
	pb.UnimplementedAgentGatewayServer
	pool             *Pool
	tenantStore      *tenant.Store
	logger           *zap.Logger
	mu               sync.RWMutex
	localAgents      map[string]*agentConn
	responseHandlers map[string]ResponseHandler
}
```

并更新 `NewGRPCServer`：

```go
func NewGRPCServer(pool *Pool, tenantStore *tenant.Store) *GRPCServer {
	logger, _ := zap.NewProduction()
	return &GRPCServer{
		pool:             pool,
		tenantStore:      tenantStore,
		logger:           logger,
		localAgents:      make(map[string]*agentConn),
		responseHandlers: make(map[string]ResponseHandler),
	}
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/agent/ -v -run TestAgent
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/agent/grpc_server.go internal/agent/grpc_server_test.go
git commit -m "feat: add gRPC server for agent registration and bidirectional streaming"
```

---

### Task 14: HTTP Handler — Chat Completions

**Files:**
- Create: `internal/handler/chat.go`
- Create: `internal/handler/chat_test.go`

- [ ] **Step 1: 编写 Chat handler 测试**

创建 `internal/handler/chat_test.go`：

```go
package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/wereliang/aiagw2/internal/agent"
	"github.com/wereliang/aiagw2/internal/middleware"
	"github.com/wereliang/aiagw2/internal/openai"
	"github.com/wereliang/aiagw2/internal/router"
	"github.com/wereliang/aiagw2/internal/session"
	"github.com/wereliang/aiagw2/internal/tenant"
)

func setupTest(t *testing.T) (*gin.Engine, *redis.Client, *agent.GRPCServer) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	ctx := context.Background()
	client.Set(ctx, "apikey:test-key", "tenant-1", 0)
	client.HSet(ctx, "tenant:tenant-1", map[string]any{
		"name":                "Test Tenant",
		"status":              "active",
		"allowed_agent_types": "support",
		"max_sessions":        "1000",
		"max_agents":          "10",
	})

	tenantStore := tenant.NewStore(client)
	sessionMgr := session.NewManager(client, 30*time.Minute, 1000)
	pool := agent.NewPool(client, "gw-test", 90*time.Second)
	grpcServer := agent.NewGRPCServer(pool, tenantStore)
	rtr := router.New(pool, tenantStore)

	chatHandler := NewChatHandler(sessionMgr, grpcServer, rtr, pool, "gw-test")

	r := gin.New()
	r.Use(middleware.Auth(tenantStore))
	r.POST("/v1/chat/completions", chatHandler.Handle)

	return r, client, grpcServer
}

func TestChatNoAgentAvailable(t *testing.T) {
	r, _, _ := setupTest(t)

	body := openai.ChatCompletionRequest{
		Model:    "support",
		Messages: []openai.Message{{Role: "user", Content: "hello"}},
	}
	data, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestChatInvalidModel(t *testing.T) {
	r, _, _ := setupTest(t)

	body := openai.ChatCompletionRequest{
		Model:    "nonexistent-type",
		Messages: []openai.Message{{Role: "user", Content: "hello"}},
	}
	data, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", w.Code, http.StatusForbidden)
	}
}

func TestChatMissingMessages(t *testing.T) {
	r, _, _ := setupTest(t)

	body := `{"model": "support"}`

	req := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./internal/handler/ -v
```

预期：编译失败。

- [ ] **Step 3: 实现 Chat handler**

创建 `internal/handler/chat.go`：

```go
package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	pb "github.com/wereliang/aiagw2/api/proto"
	"github.com/wereliang/aiagw2/internal/agent"
	"github.com/wereliang/aiagw2/internal/middleware"
	"github.com/wereliang/aiagw2/internal/openai"
	"github.com/wereliang/aiagw2/internal/router"
	"github.com/wereliang/aiagw2/internal/session"
	"github.com/wereliang/aiagw2/pkg/errcode"
)

type ChatHandler struct {
	sessionMgr      *session.Manager
	grpcServer      *agent.GRPCServer
	router          *router.Router
	pool            *agent.Pool
	gatewayInstance string
}

func NewChatHandler(
	sessionMgr *session.Manager,
	grpcServer *agent.GRPCServer,
	rtr *router.Router,
	pool *agent.Pool,
	gatewayInstance string,
) *ChatHandler {
	return &ChatHandler{
		sessionMgr:      sessionMgr,
		grpcServer:      grpcServer,
		router:          rtr,
		pool:            pool,
		gatewayInstance: gatewayInstance,
	}
}

func (h *ChatHandler) Handle(c *gin.Context) {
	var req openai.ChatCompletionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, errcode.ErrInvalidRequest("invalid request body: "+err.Error()))
		return
	}

	if len(req.Messages) == 0 {
		respondError(c, errcode.ErrInvalidRequest("messages field is required and cannot be empty"))
		return
	}

	tenantID := middleware.GetTenantID(c)
	ctx := c.Request.Context()

	sessionID := c.GetHeader("X-Session-Id")
	var sess *session.Info
	var agentID string

	if sessionID != "" {
		var err error
		sess, err = h.sessionMgr.Get(ctx, sessionID)
		if err == nil {
			agentID = sess.AgentID
			h.sessionMgr.Touch(ctx, sessionID)
		}
	}

	if agentID == "" {
		selected, err := h.router.Route(ctx, tenantID, req.Model)
		if err != nil {
			if errors.Is(err, router.ErrAgentTypeNotAllowed) {
				respondError(c, errcode.ErrPermission(fmt.Sprintf("agent type %q not allowed for tenant", req.Model)))
				return
			}
			if errors.Is(err, router.ErrNoAgentAvailable) {
				respondError(c, errcode.ErrServiceUnavailable(fmt.Sprintf("no available agent for model %q", req.Model)))
				return
			}
			respondError(c, errcode.ErrServiceUnavailable("routing failed: "+err.Error()))
			return
		}

		agentID = selected.ID
		newSess, err := h.sessionMgr.Create(ctx, tenantID, agentID, h.gatewayInstance)
		if err != nil {
			respondError(c, errcode.ErrServiceUnavailable("create session failed"))
			return
		}
		sessionID = newSess.ID
		h.pool.IncrActiveSessions(ctx, agentID)
	}

	requestID := uuid.New().String()
	agentReq := openai.ToAgentRequest(requestID, sessionID, &req)

	c.Header("X-Session-Id", sessionID)

	if req.Stream {
		h.handleStream(c, agentID, requestID, agentReq, req.Model)
	} else {
		h.handleNonStream(c, agentID, requestID, agentReq, req.Model)
	}
}

func (h *ChatHandler) handleNonStream(c *gin.Context, agentID, requestID string, agentReq *pb.AgentRequest, model string) {
	responseCh := make(chan *pb.AgentResponse, 1)

	h.grpcServer.RegisterResponseHandler(requestID, func(resp *pb.AgentResponse) {
		responseCh <- resp
	})
	defer h.grpcServer.UnregisterResponseHandler(requestID)

	if err := h.grpcServer.SendRequest(agentID, agentReq); err != nil {
		respondError(c, errcode.ErrServiceUnavailable("failed to send request to agent"))
		return
	}

	select {
	case resp := <-responseCh:
		c.JSON(http.StatusOK, openai.FromAgentResponse(resp, model))
	case <-time.After(120 * time.Second):
		respondError(c, errcode.ErrTimeout("agent processing timeout"))
	case <-c.Request.Context().Done():
		return
	}
}

func (h *ChatHandler) handleStream(c *gin.Context, agentID, requestID string, agentReq *pb.AgentRequest, model string) {
	responseCh := make(chan *pb.AgentResponse, 64)

	h.grpcServer.RegisterResponseHandler(requestID, func(resp *pb.AgentResponse) {
		responseCh <- resp
	})
	defer h.grpcServer.UnregisterResponseHandler(requestID)

	if err := h.grpcServer.SendRequest(agentID, agentReq); err != nil {
		respondError(c, errcode.ErrServiceUnavailable("failed to send request to agent"))
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	c.Stream(func(w *gin.ResponseWriter) bool {
		select {
		case resp := <-responseCh:
			chunk := openai.FromAgentResponseChunk(resp, model)
			data, _ := json.Marshal(chunk)
			c.SSEvent("", string(data))

			if resp.Done {
				c.SSEvent("", "[DONE]")
				return false
			}
			return true
		case <-time.After(120 * time.Second):
			return false
		case <-c.Request.Context().Done():
			return false
		}
	})
}

func respondError(c *gin.Context, apiErr *errcode.APIError) {
	c.AbortWithStatusJSON(apiErr.HTTPStatus, errcode.ErrorResponse{Error: apiErr})
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/handler/ -v
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/handler/chat.go internal/handler/chat_test.go
git commit -m "feat: add chat completions handler with streaming support"
```

---

### Task 15: HTTP Handler — Models 与 Session

**Files:**
- Create: `internal/handler/models.go`
- Create: `internal/handler/session.go`
- Create: `internal/handler/models_test.go`
- Create: `internal/handler/session_test.go`

- [ ] **Step 1: 编写 Models handler 测试**

创建 `internal/handler/models_test.go`：

```go
package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wereliang/aiagw2/internal/openai"
)

func TestModelsHandler(t *testing.T) {
	r, _, _ := setupTest(t)

	modelsHandler := NewModelsHandler()
	r.GET("/v1/models", modelsHandler.Handle)

	req := httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var list openai.ModelList
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if list.Object != "list" {
		t.Errorf("Object = %q, want %q", list.Object, "list")
	}
	if len(list.Data) != 1 {
		t.Errorf("Data len = %d, want 1", len(list.Data))
	}
	if list.Data[0].ID != "support" {
		t.Errorf("Data[0].ID = %q, want %q", list.Data[0].ID, "support")
	}
}
```

- [ ] **Step 2: 编写 Session handler 测试**

创建 `internal/handler/session_test.go`：

```go
package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wereliang/aiagw2/internal/session"
)

func TestDeleteSessionHandler(t *testing.T) {
	r, client, _ := setupTest(t)

	sessionMgr := session.NewManager(client, 30*time.Minute, 1000)
	sessionHandler := NewSessionHandler(sessionMgr)
	r.DELETE("/v1/sessions/:session_id", sessionHandler.Handle)

	ctx := context.Background()
	sess, _ := sessionMgr.Create(ctx, "tenant-1", "agent-1", "gw-test")

	req := httptest.NewRequest("DELETE", "/v1/sessions/"+sess.ID, nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	_, err := sessionMgr.Get(ctx, sess.ID)
	if err != session.ErrSessionNotFound {
		t.Errorf("After delete, Get() error = %v, want ErrSessionNotFound", err)
	}
}

func TestDeleteSessionNotFound(t *testing.T) {
	r, client, _ := setupTest(t)

	sessionMgr := session.NewManager(client, 30*time.Minute, 1000)
	sessionHandler := NewSessionHandler(sessionMgr)
	r.DELETE("/v1/sessions/:session_id", sessionHandler.Handle)

	req := httptest.NewRequest("DELETE", "/v1/sessions/nonexistent", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}
```

- [ ] **Step 3: 运行测试验证失败**

```bash
go test ./internal/handler/ -v -run TestModels
go test ./internal/handler/ -v -run TestDelete
```

预期：编译失败。

- [ ] **Step 4: 实现 Models handler**

创建 `internal/handler/models.go`：

```go
package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/wereliang/aiagw2/internal/middleware"
	"github.com/wereliang/aiagw2/internal/openai"
)

type ModelsHandler struct{}

func NewModelsHandler() *ModelsHandler {
	return &ModelsHandler{}
}

func (h *ModelsHandler) Handle(c *gin.Context) {
	tenantInfo := middleware.GetTenantInfo(c)
	if tenantInfo == nil {
		respondError(c, errcode.ErrAuthentication("tenant info not found"))
		return
	}

	var models []openai.Model
	for _, agentType := range tenantInfo.AllowedAgentTypes {
		models = append(models, openai.Model{
			ID:      agentType,
			Object:  "model",
			OwnedBy: tenantInfo.ID,
		})
	}

	c.JSON(http.StatusOK, openai.ModelList{
		Object: "list",
		Data:   models,
	})
}
```

注意：需要在文件顶部 import 中加入 `"github.com/wereliang/aiagw2/pkg/errcode"`。

- [ ] **Step 5: 实现 Session handler**

创建 `internal/handler/session.go`：

```go
package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/wereliang/aiagw2/internal/middleware"
	"github.com/wereliang/aiagw2/internal/session"
	"github.com/wereliang/aiagw2/pkg/errcode"
)

type SessionHandler struct {
	sessionMgr *session.Manager
}

func NewSessionHandler(sessionMgr *session.Manager) *SessionHandler {
	return &SessionHandler{sessionMgr: sessionMgr}
}

func (h *SessionHandler) Handle(c *gin.Context) {
	sessionID := c.Param("session_id")
	tenantID := middleware.GetTenantID(c)

	sess, err := h.sessionMgr.Get(c.Request.Context(), sessionID)
	if err != nil {
		if errors.Is(err, session.ErrSessionNotFound) {
			respondError(c, errcode.ErrNotFound("session not found"))
			return
		}
		respondError(c, errcode.ErrServiceUnavailable("failed to get session"))
		return
	}

	if sess.TenantID != tenantID {
		respondError(c, errcode.ErrNotFound("session not found"))
		return
	}

	if err := h.sessionMgr.Delete(c.Request.Context(), sessionID, tenantID); err != nil {
		respondError(c, errcode.ErrServiceUnavailable("failed to delete session"))
		return
	}

	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": sessionID})
}
```

- [ ] **Step 6: 运行测试**

```bash
go test ./internal/handler/ -v
```

预期：所有测试通过。

- [ ] **Step 7: 提交**

```bash
git add internal/handler/models.go internal/handler/models_test.go
git add internal/handler/session.go internal/handler/session_test.go
git commit -m "feat: add models list and session delete handlers"
```

---

### Task 16: HTTP 服务器组装

**Files:**
- Create: `internal/server/http.go`

- [ ] **Step 1: 实现 HTTP 服务器**

创建 `internal/server/http.go`：

```go
package server

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/wereliang/aiagw2/internal/handler"
	"github.com/wereliang/aiagw2/internal/middleware"
	"github.com/wereliang/aiagw2/internal/tenant"
)

type HTTPServer struct {
	engine *gin.Engine
	port   int
}

func NewHTTPServer(
	port int,
	logger *zap.Logger,
	tenantStore *tenant.Store,
	chatHandler *handler.ChatHandler,
	modelsHandler *handler.ModelsHandler,
	sessionHandler *handler.SessionHandler,
) *HTTPServer {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(middleware.Logging(logger))

	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	v1 := engine.Group("/v1")
	v1.Use(middleware.Auth(tenantStore))
	{
		v1.POST("/chat/completions", chatHandler.Handle)
		v1.GET("/models", modelsHandler.Handle)
		v1.DELETE("/sessions/:session_id", sessionHandler.Handle)
	}

	return &HTTPServer{engine: engine, port: port}
}

func (s *HTTPServer) Run() error {
	return s.engine.Run(fmt.Sprintf(":%d", s.port))
}

func (s *HTTPServer) Engine() *gin.Engine {
	return s.engine
}
```

- [ ] **Step 2: 验证编译**

```bash
go build ./internal/server/
```

预期：编译通过。

- [ ] **Step 3: 提交**

```bash
git add internal/server/
git commit -m "feat: add HTTP server with route wiring"
```

---

### Task 17: 主入口

**Files:**
- Create: `cmd/gateway/main.go`

- [ ] **Step 1: 实现主入口**

创建 `cmd/gateway/main.go`：

```go
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "github.com/wereliang/aiagw2/api/proto"
	"github.com/wereliang/aiagw2/internal/agent"
	"github.com/wereliang/aiagw2/internal/config"
	"github.com/wereliang/aiagw2/internal/handler"
	"github.com/wereliang/aiagw2/internal/router"
	"github.com/wereliang/aiagw2/internal/server"
	"github.com/wereliang/aiagw2/internal/session"
	"github.com/wereliang/aiagw2/internal/store"
	"github.com/wereliang/aiagw2/internal/tenant"
)

func main() {
	configPath := flag.String("config", "configs/gateway.yaml", "config file path")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	var logger *zap.Logger
	if cfg.Log.Format == "json" {
		logger, _ = zap.NewProduction()
	} else {
		logger, _ = zap.NewDevelopment()
	}
	defer logger.Sync()

	redisStore := store.New(cfg.Redis)
	defer redisStore.Close()

	tenantStore := tenant.NewStore(redisStore.Client())
	sessionMgr := session.NewManager(redisStore.Client(), cfg.Session.TTL, cfg.Session.MaxPerTenant)
	agentPool := agent.NewPool(redisStore.Client(), cfg.Server.InstanceID, cfg.Agent.HeartbeatTimeout)
	grpcServer := agent.NewGRPCServer(agentPool, tenantStore)
	rtr := router.New(agentPool, tenantStore)

	chatHandler := handler.NewChatHandler(sessionMgr, grpcServer, rtr, agentPool, cfg.Server.InstanceID)
	modelsHandler := handler.NewModelsHandler()
	sessionHandler := handler.NewSessionHandler(sessionMgr)

	httpServer := server.NewHTTPServer(
		cfg.Server.HTTPPort, logger,
		tenantStore, chatHandler, modelsHandler, sessionHandler,
	)

	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.GRPCPort))
	if err != nil {
		log.Fatalf("listen gRPC: %v", err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterAgentGatewayServer(grpcSrv, grpcServer)

	go func() {
		logger.Info("gRPC server starting", zap.Int("port", cfg.Server.GRPCPort))
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Fatalf("gRPC serve: %v", err)
		}
	}()

	go func() {
		logger.Info("HTTP server starting", zap.Int("port", cfg.Server.HTTPPort))
		if err := httpServer.Run(); err != nil {
			log.Fatalf("HTTP serve: %v", err)
		}
	}()

	logger.Info("AI Agent Gateway started",
		zap.String("instance_id", cfg.Server.InstanceID),
		zap.Int("http_port", cfg.Server.HTTPPort),
		zap.Int("grpc_port", cfg.Server.GRPCPort),
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down...")
	grpcSrv.GracefulStop()
}
```

- [ ] **Step 2: 验证编译**

```bash
go build ./cmd/gateway/
```

预期：编译通过，生成 `gateway` 二进制文件。

- [ ] **Step 3: 提交**

```bash
git add cmd/gateway/
git commit -m "feat: add gateway main entry point"
```

---

### Task 18: 网关间请求转发

**Files:**
- Create: `internal/agent/forwarder.go`
- Create: `internal/agent/forwarder_test.go`

- [ ] **Step 1: 编写 forwarder 测试**

创建 `internal/agent/forwarder_test.go`：

```go
package agent

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestForwarderRegisterAndLookup(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	f := NewForwarder(client, "gw-1", "localhost:9091", 60*time.Second)

	if err := f.RegisterInstance(ctx); err != nil {
		t.Fatalf("RegisterInstance() error: %v", err)
	}

	addr, err := f.LookupInstance(ctx, "gw-1")
	if err != nil {
		t.Fatalf("LookupInstance() error: %v", err)
	}
	if addr != "localhost:9091" {
		t.Errorf("addr = %q, want %q", addr, "localhost:9091")
	}
}

func TestForwarderLookupNotFound(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	f := NewForwarder(client, "gw-1", "localhost:9091", 60*time.Second)

	_, err := f.LookupInstance(ctx, "gw-nonexistent")
	if err != ErrGatewayNotFound {
		t.Errorf("LookupInstance() error = %v, want ErrGatewayNotFound", err)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
go test ./internal/agent/ -v -run TestForwarder
```

预期：编译失败。

- [ ] **Step 3: 实现 forwarder**

创建 `internal/agent/forwarder.go`：

```go
package agent

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrGatewayNotFound = errors.New("gateway instance not found")

type Forwarder struct {
	client     *redis.Client
	instanceID string
	addr       string
	ttl        time.Duration
}

func NewForwarder(client *redis.Client, instanceID, addr string, ttl time.Duration) *Forwarder {
	return &Forwarder{
		client:     client,
		instanceID: instanceID,
		addr:       addr,
		ttl:        ttl,
	}
}

func (f *Forwarder) RegisterInstance(ctx context.Context) error {
	key := "gateway:" + f.instanceID
	pipe := f.client.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"addr":           f.addr,
		"started_at":     time.Now().Unix(),
		"last_keepalive": time.Now().Unix(),
	})
	pipe.Expire(ctx, key, f.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (f *Forwarder) RefreshKeepalive(ctx context.Context) error {
	key := "gateway:" + f.instanceID
	pipe := f.client.Pipeline()
	pipe.HSet(ctx, key, "last_keepalive", time.Now().Unix())
	pipe.Expire(ctx, key, f.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (f *Forwarder) LookupInstance(ctx context.Context, instanceID string) (string, error) {
	addr, err := f.client.HGet(ctx, "gateway:"+instanceID, "addr").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrGatewayNotFound
		}
		return "", err
	}
	return addr, nil
}

func (f *Forwarder) UnregisterInstance(ctx context.Context) error {
	return f.client.Del(ctx, "gateway:"+f.instanceID).Err()
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/agent/ -v -run TestForwarder
```

预期：所有测试通过。

- [ ] **Step 5: 提交**

```bash
git add internal/agent/forwarder.go internal/agent/forwarder_test.go
git commit -m "feat: add inter-gateway instance registry and lookup"
```

---

### Task 19: 集成测试

**Files:**
- Create: `tests/integration_test.go`

- [ ] **Step 1: 编写端到端集成测试**

创建 `tests/integration_test.go`：

```go
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/wereliang/aiagw2/api/proto"
	"github.com/wereliang/aiagw2/internal/agent"
	"github.com/wereliang/aiagw2/internal/handler"
	"github.com/wereliang/aiagw2/internal/openai"
	"github.com/wereliang/aiagw2/internal/router"
	"github.com/wereliang/aiagw2/internal/server"
	"github.com/wereliang/aiagw2/internal/session"
	"github.com/wereliang/aiagw2/internal/tenant"
)

func TestEndToEndNonStream(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	client.Set(ctx, "apikey:e2e-key", "tenant-e2e", 0)
	client.HSet(ctx, "tenant:tenant-e2e", map[string]any{
		"name":                "E2E Tenant",
		"status":              "active",
		"allowed_agent_types": "echo",
		"max_sessions":        "100",
		"max_agents":          "10",
	})

	tenantStore := tenant.NewStore(client)
	sessionMgr := session.NewManager(client, 30*time.Minute, 100)
	pool := agent.NewPool(client, "gw-e2e", 90*time.Second)
	grpcServer := agent.NewGRPCServer(pool, tenantStore)
	rtr := router.New(pool, tenantStore)

	grpcLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterAgentGatewayServer(grpcSrv, grpcServer)
	go grpcSrv.Serve(grpcLis)
	t.Cleanup(func() { grpcSrv.GracefulStop() })

	chatHandler := handler.NewChatHandler(sessionMgr, grpcServer, rtr, pool, "gw-e2e")
	modelsHandler := handler.NewModelsHandler()
	sessionHandler := handler.NewSessionHandler(sessionMgr)
	logger, _ := zap.NewDevelopment()
	httpServer := server.NewHTTPServer(0, logger, tenantStore, chatHandler, modelsHandler, sessionHandler)

	httpLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go http.Serve(httpLis, httpServer.Engine())
	t.Cleanup(func() { httpLis.Close() })

	// 启动模拟 Agent
	conn, err := grpc.NewClient(grpcLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	agentClient := pb.NewAgentGatewayClient(conn)
	stream, err := agentClient.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = stream.Send(&pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   "echo-agent",
				TenantId:  "tenant-e2e",
				AgentType: "echo",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Agent 后台接收请求并回复
	go func() {
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
			stream.Send(&pb.AgentMessage{
				Payload: &pb.AgentMessage_Response{
					Response: &pb.AgentResponse{
						RequestId: req.RequestId,
						SessionId: req.SessionId,
						Content: &pb.AgentResponse_Message{
							Message: &pb.ChatMessage{
								Role:    "assistant",
								Content: "echo: " + lastMsg.Content,
							},
						},
						Done: true,
					},
				},
			})
		}
	}()

	time.Sleep(300 * time.Millisecond)

	// 发送 HTTP 请求
	body := openai.ChatCompletionRequest{
		Model:    "echo",
		Messages: []openai.Message{{Role: "user", Content: "hello world"}},
		Stream:   false,
	}
	data, _ := json.Marshal(body)

	httpReq, _ := http.NewRequest("POST",
		"http://"+httpLis.Addr().String()+"/v1/chat/completions",
		bytes.NewReader(data))
	httpReq.Header.Set("Authorization", "Bearer e2e-key")
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var chatResp openai.ChatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		t.Fatalf("Decode error: %v", err)
	}

	if len(chatResp.Choices) == 0 {
		t.Fatal("No choices in response")
	}
	if chatResp.Choices[0].Message.Content != "echo: hello world" {
		t.Errorf("Content = %q, want %q", chatResp.Choices[0].Message.Content, "echo: hello world")
	}

	sessionID := resp.Header.Get("X-Session-Id")
	if sessionID == "" {
		t.Error("X-Session-Id header should not be empty")
	}
}
```

- [ ] **Step 2: 运行集成测试**

```bash
go test ./tests/ -v -timeout 30s
```

预期：测试通过，完整的请求→路由→Agent→响应链路工作正常。

- [ ] **Step 3: 提交**

```bash
git add tests/
git commit -m "test: add end-to-end integration test with mock echo agent"
```

---

### Task 20: 最终验证与清理

- [ ] **Step 1: 运行所有测试**

```bash
go test ./... -v -count=1
```

预期：所有测试通过。

- [ ] **Step 2: 检查编译**

```bash
go build ./...
go vet ./...
```

预期：无错误，无警告。

- [ ] **Step 3: 提交最终状态**

```bash
git add -A
git status
git commit -m "chore: final cleanup and verify all tests pass"
```
