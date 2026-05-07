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

var (
	ErrAgentNotFound      = errors.New("agent not found")
	ErrAgentAlreadyExists = errors.New("agent already exists")
)

type AgentInfo struct {
	ID              string
	AgentType       string
	GatewayInstance string
	Status          string
	LastHeartbeat   time.Time
	ConnectedAt     time.Time
	ActiveSessions  int
}

type Pool struct {
	client           *redis.Client
	gatewayInstance  string
	heartbeatTimeout time.Duration
}

func NewPool(client *redis.Client, gatewayInstance string, heartbeatTimeout time.Duration) *Pool {
	return &Pool{
		client:           client,
		gatewayInstance:  gatewayInstance,
		heartbeatTimeout: heartbeatTimeout,
	}
}

func agentKey(agentType, agentID string) string {
	return "agent:" + agentType + ":" + agentID
}

func agentTypeKey(agentType string) string {
	return "agents_by_type:" + agentType
}

func gatewayAgentsKey(gatewayInstance string) string {
	return "gateway_agents:" + gatewayInstance
}

func (p *Pool) Register(ctx context.Context, info *AgentInfo) error {
	key := agentKey(info.AgentType, info.ID)

	exists, err := p.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to check agent existence: %w", err)
	}
	if exists > 0 {
		return ErrAgentAlreadyExists
	}

	now := time.Now()
	ttl := p.heartbeatTimeout * 2

	fields := map[string]interface{}{
		"agent_type":       info.AgentType,
		"gateway_instance": info.GatewayInstance,
		"status":           info.Status,
		"last_heartbeat":   strconv.FormatInt(now.Unix(), 10),
		"connected_at":     strconv.FormatInt(now.Unix(), 10),
		"active_sessions":  "0",
	}

	gwAgentsKey := gatewayAgentsKey(info.GatewayInstance)

	pipe := p.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, ttl)
	pipe.SAdd(ctx, agentTypeKey(info.AgentType), info.ID)
	pipe.SAdd(ctx, gwAgentsKey, info.AgentType+":"+info.ID)
	pipe.Expire(ctx, gwAgentsKey, ttl)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to register agent %s: %w", info.ID, err)
	}

	return nil
}

func (p *Pool) ReRegister(ctx context.Context, info *AgentInfo) error {
	key := agentKey(info.AgentType, info.ID)

	now := time.Now()
	ttl := p.heartbeatTimeout * 2

	fields := map[string]interface{}{
		"agent_type":       info.AgentType,
		"gateway_instance": info.GatewayInstance,
		"status":           info.Status,
		"last_heartbeat":   strconv.FormatInt(now.Unix(), 10),
		"connected_at":     strconv.FormatInt(now.Unix(), 10),
		"active_sessions":  "0",
	}

	gwAgentsKey := gatewayAgentsKey(info.GatewayInstance)

	pipe := p.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, ttl)
	pipe.SAdd(ctx, agentTypeKey(info.AgentType), info.ID)
	pipe.SAdd(ctx, gwAgentsKey, info.AgentType+":"+info.ID)
	pipe.Expire(ctx, gwAgentsKey, ttl)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to re-register agent %s: %w", info.ID, err)
	}

	return nil
}

func (p *Pool) Unregister(ctx context.Context, agentID, agentType string) error {
	key := agentKey(agentType, agentID)

	vals, err := p.client.HGetAll(ctx, key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to get agent info: %w", err)
	}

	pipe := p.client.TxPipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, agentTypeKey(agentType), agentID)

	if gatewayInstance := vals["gateway_instance"]; gatewayInstance != "" {
		pipe.SRem(ctx, gatewayAgentsKey(gatewayInstance), agentType+":"+agentID)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to unregister agent %s: %w", agentID, err)
	}

	return nil
}

func (p *Pool) Get(ctx context.Context, agentType, agentID string) (*AgentInfo, error) {
	vals, err := p.client.HGetAll(ctx, agentKey(agentType, agentID)).Result()
	if errors.Is(err, redis.Nil) || len(vals) == 0 {
		return nil, ErrAgentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get agent %s: %w", agentID, err)
	}

	return parseAgentInfo(agentID, vals)
}

func (p *Pool) ListByType(ctx context.Context, agentType string) ([]*AgentInfo, error) {
	setKey := agentTypeKey(agentType)

	agentIDs, err := p.client.SMembers(ctx, setKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list agents for type %s: %w", agentType, err)
	}

	var agents []*AgentInfo
	for _, id := range agentIDs {
		info, err := p.Get(ctx, agentType, id)
		if errors.Is(err, ErrAgentNotFound) {
			p.client.SRem(ctx, setKey, id)
			continue
		}
		if err != nil {
			return nil, err
		}
		agents = append(agents, info)
	}

	return agents, nil
}

func (p *Pool) UpdateHeartbeat(ctx context.Context, agentType, agentID string) error {
	key := agentKey(agentType, agentID)

	vals, err := p.client.HGetAll(ctx, key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to get agent info: %w", err)
	}
	if len(vals) == 0 {
		return ErrAgentNotFound
	}

	now := strconv.FormatInt(time.Now().Unix(), 10)
	ttl := p.heartbeatTimeout * 2

	pipe := p.client.TxPipeline()
	pipe.HSet(ctx, key, "last_heartbeat", now)
	pipe.Expire(ctx, key, ttl)

	if gatewayInstance := vals["gateway_instance"]; gatewayInstance != "" {
		pipe.Expire(ctx, gatewayAgentsKey(gatewayInstance), ttl)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to update heartbeat for agent %s: %w", agentID, err)
	}

	return nil
}

func (p *Pool) IncrActiveSessions(ctx context.Context, agentType, agentID string) error {
	key := agentKey(agentType, agentID)

	exists, err := p.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to check agent existence: %w", err)
	}
	if exists == 0 {
		return ErrAgentNotFound
	}

	if err := p.client.HIncrBy(ctx, key, "active_sessions", 1).Err(); err != nil {
		return fmt.Errorf("failed to increment active sessions for agent %s: %w", agentID, err)
	}

	return nil
}

func (p *Pool) DecrActiveSessions(ctx context.Context, agentType, agentID string) error {
	key := agentKey(agentType, agentID)

	exists, err := p.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to check agent existence: %w", err)
	}
	if exists == 0 {
		return ErrAgentNotFound
	}

	if err := p.client.HIncrBy(ctx, key, "active_sessions", -1).Err(); err != nil {
		return fmt.Errorf("failed to decrement active sessions for agent %s: %w", agentID, err)
	}

	return nil
}

func (p *Pool) UnregisterByGateway(ctx context.Context, gatewayInstance string) (int, error) {
	setKey := gatewayAgentsKey(gatewayInstance)

	members, err := p.client.SMembers(ctx, setKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to list agents for gateway %s: %w", gatewayInstance, err)
	}

	var count int
	for _, member := range members {
		parts := splitAgentMember(member)
		if len(parts) != 2 {
			continue
		}
		agentType, agentID := parts[0], parts[1]

		pipe := p.client.TxPipeline()
		pipe.Del(ctx, agentKey(agentType, agentID))
		pipe.SRem(ctx, agentTypeKey(agentType), agentID)
		pipe.SRem(ctx, setKey, member)

		if _, err := pipe.Exec(ctx); err != nil {
			continue
		}
		count++
	}

	p.client.Del(ctx, setKey)

	return count, nil
}

func splitAgentMember(member string) []string {
	for i := 0; i < len(member); i++ {
		if member[i] == ':' {
			return []string{member[:i], member[i+1:]}
		}
	}
	return nil
}

func parseAgentInfo(agentID string, vals map[string]string) (*AgentInfo, error) {
	lastHeartbeatUnix, err := strconv.ParseInt(vals["last_heartbeat"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse last_heartbeat for agent %s: %w", agentID, err)
	}

	connectedAtUnix, err := strconv.ParseInt(vals["connected_at"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connected_at for agent %s: %w", agentID, err)
	}

	activeSessions, err := strconv.Atoi(vals["active_sessions"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse active_sessions for agent %s: %w", agentID, err)
	}

	return &AgentInfo{
		ID:              agentID,
		AgentType:       vals["agent_type"],
		GatewayInstance: vals["gateway_instance"],
		Status:          vals["status"],
		LastHeartbeat:   time.Unix(lastHeartbeatUnix, 0),
		ConnectedAt:     time.Unix(connectedAtUnix, 0),
		ActiveSessions:  activeSessions,
	}, nil
}
