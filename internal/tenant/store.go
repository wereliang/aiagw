package tenant

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ErrTenantNotFound is returned when a tenant cannot be found by API key or ID.
var ErrTenantNotFound = errors.New("tenant not found")

// Info holds tenant metadata retrieved from Redis.
type Info struct {
	ID                string
	Name              string
	Status            string
	AllowedAgentTypes []string
	MaxSessions       int
	MaxAgents         int
}

// Store provides tenant lookup backed by Redis.
type Store struct {
	client *redis.Client
}

// NewStore creates a Store that reads tenant data from the given Redis client.
func NewStore(client *redis.Client) *Store {
	return &Store{client: client}
}

// GetByAPIKey resolves an API key hash to tenant info.
//
// Redis keys used:
//   - apikey:{keyHash} -> string containing tenant_id
//   - tenant:{tenant_id} -> hash with tenant fields
func (s *Store) GetByAPIKey(ctx context.Context, keyHash string) (*Info, error) {
	tenantID, err := s.client.Get(ctx, "apikey:"+keyHash).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get tenant id for api key: %w", err)
	}

	return s.GetByID(ctx, tenantID)
}

// GetByID retrieves tenant info by tenant ID from a Redis hash.
//
// Redis key: tenant:{tenantID}
// Hash fields: name, status, allowed_agent_types (comma-separated), max_sessions, max_agents
func (s *Store) GetByID(ctx context.Context, tenantID string) (*Info, error) {
	vals, err := s.client.HGetAll(ctx, "tenant:"+tenantID).Result()
	if errors.Is(err, redis.Nil) || len(vals) == 0 {
		return nil, ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get tenant %s: %w", tenantID, err)
	}

	info := &Info{
		ID:     tenantID,
		Name:   vals["name"],
		Status: vals["status"],
	}

	if raw := vals["allowed_agent_types"]; raw != "" {
		info.AllowedAgentTypes = strings.Split(raw, ",")
	}

	if raw := vals["max_sessions"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse max_sessions for tenant %s: %w", tenantID, err)
		}
		info.MaxSessions = n
	}

	if raw := vals["max_agents"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse max_agents for tenant %s: %w", tenantID, err)
		}
		info.MaxAgents = n
	}

	return info, nil
}

// Exists returns true if a tenant hash exists in Redis for the given ID.
func (s *Store) Exists(ctx context.Context, tenantID string) bool {
	n, err := s.client.Exists(ctx, "tenant:"+tenantID).Result()
	return err == nil && n > 0
}
