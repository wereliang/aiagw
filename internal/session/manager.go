package session

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	ErrSessionNotFound    = errors.New("session not found")
	ErrInvalidSessionID   = errors.New("invalid composite session ID")
)

type Info struct {
	ID              string
	AgentID         string
	AgentType       string
	GatewayInstance string
	CreatedAt       time.Time
	LastActiveAt    time.Time
}

func ComposeSessionID(agentType, rawID string) string {
	return agentType + ":" + rawID
}

func ParseSessionID(compositeID string) (agentType, rawID string, err error) {
	idx := strings.IndexByte(compositeID, ':')
	if idx < 0 {
		return "", "", ErrInvalidSessionID
	}
	return compositeID[:idx], compositeID[idx+1:], nil
}

type Manager struct {
	client *redis.Client
	ttl    time.Duration
}

func NewManager(client *redis.Client, ttl time.Duration) *Manager {
	return &Manager{
		client: client,
		ttl:    ttl,
	}
}

func sessionKey(sessionID string) string {
	return "session:" + sessionID
}

func agentSessionsKey(agentID string) string {
	return "agent_sessions:" + agentID
}

func (m *Manager) Create(ctx context.Context, agentID, agentType, gatewayInstance string) (*Info, error) {
	return m.CreateWithID(ctx, uuid.New().String(), agentID, agentType, gatewayInstance)
}

func (m *Manager) CreateWithID(ctx context.Context, rawID, agentID, agentType, gatewayInstance string) (*Info, error) {
	compositeID := ComposeSessionID(agentType, rawID)
	now := time.Now()

	key := sessionKey(compositeID)
	fields := map[string]interface{}{
		"agent_id":         agentID,
		"agent_type":       agentType,
		"gateway_instance": gatewayInstance,
		"created_at":       strconv.FormatInt(now.Unix(), 10),
		"last_active_at":   strconv.FormatInt(now.Unix(), 10),
	}

	pipe := m.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, m.ttl)
	pipe.SAdd(ctx, agentSessionsKey(agentID), compositeID)

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	return &Info{
		ID:              compositeID,
		AgentID:         agentID,
		AgentType:       agentType,
		GatewayInstance: gatewayInstance,
		CreatedAt:       time.Unix(now.Unix(), 0),
		LastActiveAt:    time.Unix(now.Unix(), 0),
	}, nil
}

func (m *Manager) Get(ctx context.Context, sessionID string) (*Info, error) {
	vals, err := m.client.HGetAll(ctx, sessionKey(sessionID)).Result()
	if errors.Is(err, redis.Nil) || len(vals) == 0 {
		return nil, ErrSessionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get session %s: %w", sessionID, err)
	}

	return parseInfo(sessionID, vals)
}

func (m *Manager) Touch(ctx context.Context, sessionID string) error {
	key := sessionKey(sessionID)

	exists, err := m.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to check session existence: %w", err)
	}
	if exists == 0 {
		return ErrSessionNotFound
	}

	now := strconv.FormatInt(time.Now().Unix(), 10)

	pipe := m.client.TxPipeline()
	pipe.HSet(ctx, key, "last_active_at", now)
	pipe.Expire(ctx, key, m.ttl)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to touch session %s: %w", sessionID, err)
	}

	return nil
}

func (m *Manager) Delete(ctx context.Context, sessionID string) error {
	sess, err := m.Get(ctx, sessionID)
	if err != nil {
		return err
	}

	pipe := m.client.TxPipeline()
	pipe.Del(ctx, sessionKey(sessionID))
	pipe.SRem(ctx, agentSessionsKey(sess.AgentID), sessionID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to delete session %s: %w", sessionID, err)
	}

	return nil
}

func (m *Manager) DeleteByAgent(ctx context.Context, agentID string) error {
	setKey := agentSessionsKey(agentID)

	sessionIDs, err := m.client.SMembers(ctx, setKey).Result()
	if err != nil {
		return fmt.Errorf("failed to list sessions for agent %s: %w", agentID, err)
	}

	for _, sid := range sessionIDs {
		key := sessionKey(sid)
		exists, err := m.client.Exists(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("failed to check session %s: %w", sid, err)
		}
		if exists == 0 {
			m.client.SRem(ctx, setKey, sid)
			continue
		}

		pipe := m.client.TxPipeline()
		pipe.Del(ctx, key)
		pipe.SRem(ctx, setKey, sid)
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to delete session %s: %w", sid, err)
		}
	}

	return nil
}

func parseInfo(sessionID string, vals map[string]string) (*Info, error) {
	createdAtUnix, err := strconv.ParseInt(vals["created_at"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse created_at for session %s: %w", sessionID, err)
	}

	lastActiveAtUnix, err := strconv.ParseInt(vals["last_active_at"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse last_active_at for session %s: %w", sessionID, err)
	}

	return &Info{
		ID:              sessionID,
		AgentID:         vals["agent_id"],
		AgentType:       vals["agent_type"],
		GatewayInstance: vals["gateway_instance"],
		CreatedAt:       time.Unix(createdAtUnix, 0),
		LastActiveAt:    time.Unix(lastActiveAtUnix, 0),
	}, nil
}
