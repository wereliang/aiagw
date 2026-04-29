# AI Agent Gateway 设计文档

## 概述

基于 Go 语言实现的 AI Agent 网关，对外暴露 OpenAI 兼容的 API 接口，内部通过 gRPC 双向流将请求路由到后端 AI Agent。网关本身无状态（支持多副本部署），使用 Redis 作为共享存储，支持多租户和 API Key 认证。

用户像调用 LLM 一样与网关交互，网关根据租户和 model 字段的映射，将请求透明地路由到特定的 AI Agent。

## 架构

```
┌─────────────────────────────────────────────────────┐
│                    AI Agent Gateway                  │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────┐  │
│  │ HTTP API │  │  Auth    │  │   Router          │  │
│  │ (OpenAI) │─>│ Middleware│─>│ (tenant+model)    │  │
│  └──────────┘  └──────────┘  └────────┬──────────┘  │
│                                        │             │
│  ┌──────────────────┐  ┌──────────────▼──────────┐  │
│  │ Session Manager  │  │  Agent Connection Pool   │  │
│  │ (Redis-backed)   │◄─┤  (gRPC bidirectional)    │  │
│  └────────┬─────────┘  └──────────────┬──────────┘  │
│           │                            │             │
└───────────┼────────────────────────────┼─────────────┘
            │                            │
       ┌────▼────┐                  ┌────▼────┐
       │  Redis  │                  │ Agent 1 │
       │         │                  │ Agent 2 │
       └─────────┘                  │ Agent N │
                                    └─────────┘
```

### 模块职责

| 模块 | 职责 |
|------|------|
| **HTTP API** | 实现 OpenAI `/v1/chat/completions` 接口，支持普通和流式响应 |
| **Auth Middleware** | 从 Bearer Token 提取 API Key，验证并解析出 tenant_id 和权限 |
| **Router** | 根据 tenant_id + model 字段查找目标 Agent，检查 Agent 可用性 |
| **Session Manager** | 在 Redis 中维护 session_id → agent_instance 的映射，管理 session 生命周期 |
| **Agent Connection Pool** | 管理所有 Agent 的 gRPC 双向流连接，心跳检测，自动重连 |

### 请求处理流程

1. 用户发送 `POST /v1/chat/completions`（携带 Bearer Token + X-Session-Id header）
2. Auth Middleware 验证 API Key → 解析出 tenant_id
3. 检查 session_id 是否存在：
   - 存在 → 从 Redis 获取已绑定的 agent_instance，直接转发
   - 不存在 → Router 根据 tenant_id + model 查找可用 Agent
     → 创建新 session，绑定到选中的 agent_instance
     → 写入 Redis
4. 通过 gRPC 双向流将请求发送给 Agent
5. 接收 Agent 响应，转换为 OpenAI 格式返回用户（支持 SSE streaming）

## gRPC 协议与 Agent 注册

### Proto 定义

```protobuf
service AgentGateway {
  // Agent 启动时调用，注册自身并建立双向流
  rpc Connect(stream AgentMessage) returns (stream GatewayMessage);
}

message AgentRegister {
  string agent_id = 1;       // Agent 唯一标识
  string tenant_id = 2;      // 所属租户
  string agent_type = 3;     // Agent 类型，对应 model 字段映射
  map<string, string> metadata = 4;  // 扩展信息（版本、能力等）
}

message AgentMessage {
  oneof payload {
    AgentRegister register = 1;      // 注册消息（首条消息）
    AgentResponse response = 2;      // Agent 对请求的响应
    Heartbeat heartbeat = 3;         // 心跳
  }
}

message GatewayMessage {
  oneof payload {
    AgentRequest request = 1;        // 网关转发的用户请求
    Heartbeat heartbeat = 2;         // 心跳
  }
}

message AgentRequest {
  string request_id = 1;
  string session_id = 2;
  string model = 3;                  // 原始 model 字段
  repeated ChatMessage messages = 4; // OpenAI 格式的消息列表
  map<string, string> parameters = 5; // temperature 等参数
}

message AgentResponse {
  string request_id = 1;
  string session_id = 2;
  oneof content {
    ChatMessage message = 3;         // 完整响应
    StreamChunk chunk = 4;           // 流式响应片段
  }
  bool done = 5;                     // 流式场景标记结束
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

### Agent 生命周期

```
Agent 启动
    │
    ▼
连接网关 gRPC ──> 发送 Register 消息（agent_id, tenant_id, agent_type）
    │
    ▼
网关验证 tenant_id ──> 记录到 Redis（Agent 在线状态 + 所在网关实例）
    │
    ▼
进入就绪状态 ◄──── 双向流保持 ────► 定时心跳（30s）
    │
    ├── 收到请求 → 处理 → 发送响应
    ├── 心跳超时（90s） → 网关标记 Agent 离线，清理 session 绑定
    └── Agent 主动断开 → 网关标记离线，清理 session 绑定
```

### 多副本场景下的 Agent 连接路由

由于网关多副本部署，Agent 的 gRPC 连接只存在于某一个网关实例上。需要解决请求路由问题：

```
Redis 中存储：
  agent:{agent_id} → { tenant_id, agent_type, gateway_instance, status, last_heartbeat }
  session:{session_id} → { agent_id, gateway_instance, tenant_id, created_at }

请求到达任意网关实例后：
  1. 查 Redis 找到 session 绑定的 agent_id 和 gateway_instance
  2. 如果 agent 在本实例 → 直接转发
  3. 如果 agent 在其他实例 → 通过网关间 gRPC 内部通信转发
```

网关实例之间维护一个内部 gRPC 服务用于转发请求，每个实例启动时向 Redis 注册自己的地址。

```
# 网关实例注册（Hash，通过定期 keepalive 刷新 TTL）
gateway:{instance_id} → { addr: "10.0.1.5:9091", started_at, last_keepalive }
```

内部转发 proto 定义：

```protobuf
service GatewayInternal {
  rpc ForwardRequest(AgentRequest) returns (stream AgentResponse);
}
```

当 Agent 注册时携带的 tenant_id 无效（Redis 中不存在），网关会拒绝注册，关闭 gRPC 流并返回 PERMISSION_DENIED 错误码。

## Session 管理与多租户

### Redis Key 设计

```
# 租户信息（Hash）
tenant:{tenant_id} → {
  name, api_key_hash, status,
  allowed_agent_types: ["customer-service", "code-assistant", ...],
  max_sessions, max_agents
}

# API Key 反查租户（String）
apikey:{key_hash} → tenant_id

# Agent 实例（Hash，带 TTL 自动过期）
agent:{agent_id} → {
  tenant_id, agent_type, gateway_instance,
  status: "online|offline|busy",
  last_heartbeat, connected_at,
  active_sessions: 3
}

# 租户下的 Agent 索引（Set）
tenant_agents:{tenant_id}:{agent_type} → { agent_id_1, agent_id_2, ... }

# Session（Hash，带 TTL）
session:{session_id} → {
  tenant_id, agent_id, gateway_instance,
  created_at, last_active_at
}

# 租户下的活跃 Session 索引（Set）
tenant_sessions:{tenant_id} → { session_id_1, session_id_2, ... }
```

### Session 生命周期

**创建 Session：**
1. 用户首次请求（无 session_id 或 session_id 不存在）
2. 路由选择 Agent：从 `tenant_agents:{tenant_id}:{agent_type}` 中选择
   - 过滤 status=online 的 Agent
   - 按 active_sessions 数量做负载均衡（最少连接数）
3. 生成 session_id（UUID），写入 Redis
4. 响应 Header 中返回 X-Session-Id

**复用 Session：**
1. 用户带 session_id 请求
2. Redis 查到 agent_id + gateway_instance
3. 更新 last_active_at
4. 转发到对应 Agent

**销毁 Session：**
- TTL 过期自动清理（默认 30 分钟无活动）
- Agent 断开时，批量清理其所有 session
- 用户可主动关闭（DELETE /v1/sessions/{session_id}）

### 多租户隔离

- **数据隔离**：Redis Key 均含 tenant_id，查询时强制匹配
- **路由隔离**：Agent 注册时绑定 tenant_id，路由只在同租户 Agent 中选择
- **配额隔离**：每个租户独立的 max_sessions、max_agents 限制
- **认证隔离**：API Key 与 tenant_id 一一对应，无法跨租户访问

## OpenAI API 兼容层

### 支持的接口

```
POST /v1/chat/completions          # 核心接口，对话请求
GET  /v1/models                    # 返回当前租户可用的 Agent 列表（伪装为 model）
DELETE /v1/sessions/{session_id}   # 扩展接口，主动关闭 session
GET  /health                       # 网关健康检查
```

### 请求格式

标准 OpenAI 请求：
```json
{
  "model": "customer-service",
  "messages": [{"role": "user", "content": "你好"}],
  "stream": true,
  "temperature": 0.7
}
```
请求头：
- `Authorization: Bearer <api_key>`
- `X-Session-Id: <session_id>`（可选）

### 响应格式

**非流式响应：**
```json
{
  "id": "chatcmpl-{request_id}",
  "object": "chat.completion",
  "model": "customer-service",
  "choices": [{
    "index": 0,
    "message": {"role": "assistant", "content": "你好，有什么可以帮您？"},
    "finish_reason": "stop"
  }],
  "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
}
```
响应头：`X-Session-Id: <session_id>`

**流式响应（SSE）：**
```
data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","choices":[{"delta":{"content":"你"},"index":0}]}

data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","choices":[{"delta":{"content":"好"},"index":0}]}

data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","choices":[{"delta":{},"index":0,"finish_reason":"stop"}]}

data: [DONE]
```

### /v1/models 映射

将租户可用的 Agent 类型伪装为 OpenAI model 列表：
```json
{
  "object": "list",
  "data": [
    {"id": "customer-service", "object": "model", "owned_by": "tenant-a"},
    {"id": "code-assistant", "object": "model", "owned_by": "tenant-a"}
  ]
}
```

### 错误处理

所有错误统一使用 OpenAI 错误格式：

| 场景 | HTTP 状态码 | error.type |
|------|------------|------------|
| API Key 无效 | 401 | authentication_error |
| 租户无权访问该 Agent 类型 | 403 | permission_error |
| model 字段指定的 Agent 类型不存在 | 404 | not_found_error |
| 无可用的在线 Agent | 503 | service_unavailable |
| Agent 处理超时 | 504 | timeout_error |
| 租户 session 数达到上限 | 429 | rate_limit_error |
| 请求格式错误 | 400 | invalid_request_error |

错误响应格式：
```json
{
  "error": {
    "message": "No available agent for model 'customer-service'",
    "type": "service_unavailable",
    "code": "agent_unavailable"
  }
}
```

### 超时与重试策略

- **请求超时**：单次请求默认 120s，可通过租户配置调整
- **心跳超时**：Agent 心跳 30s 间隔，90s 无心跳标记离线
- **连接重试**：网关间内部转发失败时，检查 Agent 是否已迁移到其他实例
- **不重试用户请求**：Agent 处理失败直接返回错误，不做自动重试（Agent 有状态，重试可能产生副作用）

## 项目结构

```
aiagw2/
├── cmd/
│   └── gateway/
│       └── main.go              # 入口，初始化各模块并启动
├── api/
│   └── proto/
│       └── agent.proto          # gRPC 协议定义
├── internal/
│   ├── config/
│   │   └── config.go            # 配置加载（YAML）
│   ├── server/
│   │   └── http.go              # HTTP 服务器，注册路由
│   ├── handler/
│   │   ├── chat.go              # POST /v1/chat/completions
│   │   ├── models.go            # GET /v1/models
│   │   └── session.go           # DELETE /v1/sessions/{id}
│   ├── middleware/
│   │   ├── auth.go              # API Key 验证，注入 tenant context
│   │   └── logging.go           # 请求日志
│   ├── router/
│   │   └── router.go            # tenant + model → Agent 路由选择
│   ├── session/
│   │   └── manager.go           # Session CRUD，Redis 操作
│   ├── agent/
│   │   ├── pool.go              # Agent 连接池管理
│   │   ├── grpc_server.go       # gRPC 服务端，接受 Agent 连接
│   │   └── forwarder.go         # 网关间请求转发
│   ├── tenant/
│   │   └── store.go             # 租户信息查询
│   ├── openai/
│   │   ├── types.go             # OpenAI 请求/响应结构体
│   │   └── converter.go         # OpenAI ↔ AgentRequest 转换
│   └── store/
│       └── redis.go             # Redis 客户端封装
├── pkg/
│   └── errcode/
│       └── errors.go            # 统一错误码定义
├── configs/
│   └── gateway.yaml             # 配置文件模板
├── go.mod
└── go.sum
```

### 核心依赖

| 库 | 用途 |
|---|------|
| `google.golang.org/grpc` | gRPC 服务端 |
| `github.com/gin-gonic/gin` | HTTP 框架（轻量、性能好） |
| `github.com/redis/go-redis/v9` | Redis 客户端 |
| `github.com/google/uuid` | Session ID 生成 |
| `go.uber.org/zap` | 结构化日志 |
| `gopkg.in/yaml.v3` | 配置解析 |

### 配置文件

```yaml
server:
  http_port: 8080
  grpc_port: 9090              # Agent 连接端口
  internal_grpc_port: 9091     # 网关间转发端口
  instance_id: ""              # 留空则自动生成

redis:
  addr: "localhost:6379"
  password: ""
  db: 0

session:
  ttl: 30m                     # Session 空闲超期
  max_per_tenant: 1000

agent:
  heartbeat_interval: 30s
  heartbeat_timeout: 90s

log:
  level: info
  format: json
```

## 部署架构

```
              ┌─── LB (Nginx/云 LB) ───┐
              │        HTTP :8080       │
              ▼            ▼            ▼
         ┌─────────┐ ┌─────────┐ ┌─────────┐
         │ GW #1   │ │ GW #2   │ │ GW #3   │
         │ :9090   │ │ :9090   │ │ :9090   │  ← Agent gRPC 端口
         │ :9091   │─│ :9091   │─│ :9091   │  ← 网关间转发端口
         └────┬────┘ └────┬────┘ └────┬────┘
              │           │           │
              └─────── Redis ─────────┘
```

- **水平扩展**：加网关实例即可，无状态，Redis 共享
- **Agent 连接分布**：Agent 通过 LB 随机连接到某个网关实例
- **故障转移**：网关实例挂掉 → Agent 心跳超时 → 重连其他实例 → 重新注册
