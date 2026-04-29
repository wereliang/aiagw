# AI Agent Gateway

AI Agent Gateway 是一个轻量级的 AI Agent 网关服务，提供 OpenAI 兼容的 HTTP API，支持多 Agent 接入、会话管理和跨网关转发。

## 架构概览

```
┌─────────────┐     HTTP/SSE      ┌─────────────────┐      gRPC       ┌─────────────┐
│   Client    │ ◄───────────────► │  Agent Gateway  │ ◄─────────────► │   Agent 1   │
│  (OpenAI    │                   │                 │                 └─────────────┘
│   SDK)      │                   │  ┌───────────┐  │                 ┌─────────────┐
└─────────────┘                   │  │  Router   │  │ ◄─────────────► │   Agent 2   │
                                  │  ├───────────┤  │                 └─────────────┘
                                  │  │  Session  │  │                 ┌─────────────┐
                                  │  │  Manager  │  │ ◄─────────────► │   Agent N   │
                                  │  ├───────────┤  │                 └─────────────┘
                                  │  │   Redis   │  │
                                  │  └───────────┘  │
                                  └─────────────────┘
```

## 功能特性

- **OpenAI 兼容 API** - 支持 `/v1/chat/completions` 接口，可直接使用 OpenAI SDK 对接
- **流式/非流式响应** - 支持 SSE 流式输出和普通 JSON 响应
- **会话管理** - 基于 Redis 的会话持久化，支持多轮对话
- **Agent 路由** - 基于 Agent Type 的智能路由，支持负载均衡
- **多网关支持** - 支持多 Gateway 实例部署，自动跨网关请求转发
- **Agent SDK** - 提供 Go SDK，快速开发自定义 Agent

## 项目结构

```
aiagw/
├── api/proto/              # gRPC Proto 定义
│   ├── agent.proto         # Agent 通信协议
│   └── internal.proto      # 网关内部通信协议
├── cmd/gateway/            # Gateway 启动入口
├── configs/                # 配置文件
├── examples/               # 示例 Agent
│   ├── echo-agent/         # 简单回显 Agent
│   ├── stream-agent/       # 流式响应 Agent
│   └── claude-proxy-agent/ # Claude API 代理 Agent
├── internal/
│   ├── agent/              # Agent 连接池与 gRPC 服务
│   ├── config/             # 配置加载
│   ├── handler/            # HTTP 请求处理
│   ├── middleware/         # 中间件（认证、日志）
│   ├── openai/             # OpenAI 格式转换
│   ├── router/             # Agent 路由
│   ├── session/            # 会话管理
│   └── store/              # Redis 存储
├── pkg/
│   ├── agentsdk/           # Agent 开发 SDK
│   └── errcode/            # 错误码定义
└── tests/                  # 测试文件
```

## 快速开始

### 1. 启动 Redis

```bash
docker run -d --name redis -p 6379:6379 redis:latest
```

### 2. 启动 Gateway

```bash
# 编译
go build -o gateway ./cmd/gateway

# 启动
./gateway -config configs/gateway.yaml
```

### 3. 启动示例 Agent

```bash
# Echo Agent
go run examples/echo-agent/main.go

# 或 Stream Agent
go run examples/stream-agent/main.go
```

### 4. 测试请求

```bash
# 非流式请求
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer xiaoying" \
  -d '{
    "model": "echo",
    "messages": [{"role": "user", "content": "Hello, World!"}]
  }'

# 流式请求
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer xiaoying" \
  -d '{
    "model": "stream-echo",
    "stream": true,
    "messages": [{"role": "user", "content": "Hello, World!"}]
  }'
```

## 配置说明

```yaml
server:
  http_port: 8080          # HTTP API 端口
  grpc_port: 19090         # Agent gRPC 端口
  internal_grpc_port: 19091 # 网关内部通信端口
  instance_id: ""          # 网关实例 ID（为空则自动生成）

redis:
  addr: "localhost:6379"
  password: ""
  db: 0

session:
  ttl: 30m                 # 会话过期时间

agent:
  heartbeat_interval: 30s  # 心跳间隔
  heartbeat_timeout: 90s   # 心跳超时

auth:
  api_keys: ["xiaoying"]   # API 密钥列表

log:
  level: info
  format: json             # json 或 text
```

## Agent 开发

使用 `agentsdk` 可以快速开发自定义 Agent：

### 简单响应 Agent

```go
package main

import (
    "context"
    "github.com/wereliang/aiagw/pkg/agentsdk"
)

func main() {
    agent := agentsdk.New(
        "localhost:19090",  // Gateway gRPC 地址
        "my-agent-1",       // Agent ID
        "my-agent",         // Agent Type（用于路由）
        func(ctx context.Context, req *agentsdk.Request) agentsdk.Response {
            // 处理请求，返回响应
            return agentsdk.Response{
                Role:    "assistant",
                Content: "Hello from my agent!",
            }
        },
    )
    
    agent.Run(context.Background())
}
```

### 流式响应 Agent

```go
agent := agentsdk.New(
    "localhost:19090",
    "stream-agent-1",
    "stream-agent",
    nil,
    agentsdk.WithStreamHandler(
        func(ctx context.Context, req *agentsdk.Request, stream agentsdk.ResponseStream) {
            // 发送流式 chunk
            stream.SendChunk("Hello ")
            stream.SendChunk("World!")
            
            // 发送最终消息
            stream.SendMessage("assistant", "Hello World!")
        },
    ),
)
```

### SDK 配置选项

```go
agentsdk.New(addr, id, agentType, handler,
    agentsdk.WithHeartbeat(30*time.Second),      // 心跳间隔
    agentsdk.WithMetadata(map[string]string{}),  // 元数据
    agentsdk.WithReconnect(true),                // 启用自动重连
    agentsdk.WithReconnectDelay(3*time.Second),  // 重连延迟
    agentsdk.WithOnConnect(func() {}),           // 连接回调
    agentsdk.WithOnDisconnect(func(err error) {}), // 断开回调
)
```

## API 接口

### POST /v1/chat/completions

OpenAI 兼容的聊天补全接口。

**请求头：**
- `Authorization: Bearer <api_key>` - API 密钥认证
- `X-Session-Id` (可选) - 会话 ID，用于多轮对话

**请求体：**
```json
{
  "model": "agent-type",
  "messages": [
    {"role": "user", "content": "Hello"}
  ],
  "stream": false
}
```

**响应：**
```json
{
  "id": "chatcmpl-xxx",
  "object": "chat.completion",
  "model": "agent-type",
  "choices": [{
    "index": 0,
    "message": {
      "role": "assistant",
      "content": "Hello!"
    },
    "finish_reason": "stop"
  }]
}
```

### GET /v1/models

获取可用的 Agent 列表。

### GET /v1/sessions/:id

获取会话信息。

### DELETE /v1/sessions/:id

删除会话。

## 核心模块说明

| 模块 | 说明 |
|------|------|
| `agent/pool.go` | Agent 连接池，管理 Agent 注册与发现 |
| `agent/grpc_server.go` | Agent gRPC 服务，处理 Agent 连接与消息转发 |
| `agent/forwarder.go` | 跨网关请求转发，支持多网关部署 |
| `router/router.go` | Agent 路由，基于 Agent Type 选择目标 Agent |
| `session/manager.go` | 会话管理，支持会话创建、查询、续期 |
| `handler/chat.go` | Chat 请求处理，支持流式/非流式响应 |
| `openai/converter.go` | OpenAI 格式与内部格式转换 |

## 多网关部署

支持多个 Gateway 实例部署，通过 Redis 进行服务发现和请求转发：

```
                     ┌──────────────────┐
                     │      Redis       │
                     └────────┬─────────┘
                              │
           ┌──────────────────┼──────────────────┐
           │                  │                  │
    ┌──────┴──────┐    ┌──────┴──────┐    ┌──────┴──────┐
    │  Gateway 1  │    │  Gateway 2  │    │  Gateway 3  │
    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
           │                  │                  │
      ┌────┴────┐        ┌────┴────┐        ┌────┴────┐
      │ Agent A │        │ Agent B │        │ Agent C │
      └─────────┘        └─────────┘        └─────────┘
```

当请求到达 Gateway 1，但目标 Agent 连接在 Gateway 2 时，Gateway 1 会自动将请求转发到 Gateway 2。

## License

MIT License
