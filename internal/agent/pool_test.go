package agent

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const (
	testGatewayInstance  = "gw-node-1"
	testHeartbeatTimeout = 30 * time.Second
)

// newTestPool starts a miniredis server and returns a Pool plus the underlying
// miniredis instance so callers can inspect state.
func newTestPool(t *testing.T) (*Pool, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return NewPool(client, testGatewayInstance, testHeartbeatTimeout), mr
}

func TestPoolRegisterAndGet(t *testing.T) {
	pool, _ := newTestPool(t)
	ctx := context.Background()

	info := &AgentInfo{
		ID:              "agent-1",
		AgentType:       "openai",
		GatewayInstance: testGatewayInstance,
		Status:          StatusOnline,
	}

	if err := pool.Register(ctx, info); err != nil {
		t.Fatalf("Register returned unexpected error: %v", err)
	}

	got, err := pool.Get(ctx, "openai", "agent-1")
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}

	if got.ID != "agent-1" {
		t.Errorf("ID = %q, want %q", got.ID, "agent-1")
	}
	if got.AgentType != "openai" {
		t.Errorf("AgentType = %q, want %q", got.AgentType, "openai")
	}
	if got.GatewayInstance != testGatewayInstance {
		t.Errorf("GatewayInstance = %q, want %q", got.GatewayInstance, testGatewayInstance)
	}
	if got.Status != StatusOnline {
		t.Errorf("Status = %q, want %q", got.Status, StatusOnline)
	}
	if got.ActiveSessions != 0 {
		t.Errorf("ActiveSessions = %d, want 0", got.ActiveSessions)
	}
	if got.ConnectedAt.IsZero() {
		t.Error("ConnectedAt is zero")
	}
	if got.LastHeartbeat.IsZero() {
		t.Error("LastHeartbeat is zero")
	}
}

func TestPoolUnregister(t *testing.T) {
	pool, _ := newTestPool(t)
	ctx := context.Background()

	info := &AgentInfo{
		ID:              "agent-1",
		AgentType:       "openai",
		GatewayInstance: testGatewayInstance,
		Status:          StatusOnline,
	}

	if err := pool.Register(ctx, info); err != nil {
		t.Fatalf("Register returned unexpected error: %v", err)
	}

	if err := pool.Unregister(ctx, "agent-1", "openai"); err != nil {
		t.Fatalf("Unregister returned unexpected error: %v", err)
	}

	_, err := pool.Get(ctx, "openai", "agent-1")
	if err != ErrAgentNotFound {
		t.Fatalf("expected ErrAgentNotFound after unregister, got %v", err)
	}
}

func TestPoolListByType(t *testing.T) {
	pool, _ := newTestPool(t)
	ctx := context.Background()

	agents := []*AgentInfo{
		{ID: "a1", AgentType: "support", GatewayInstance: testGatewayInstance, Status: StatusOnline},
		{ID: "a2", AgentType: "support", GatewayInstance: testGatewayInstance, Status: StatusOnline},
		{ID: "a3", AgentType: "sales", GatewayInstance: testGatewayInstance, Status: StatusOnline},
	}

	for _, a := range agents {
		if err := pool.Register(ctx, a); err != nil {
			t.Fatalf("Register(%s) returned unexpected error: %v", a.ID, err)
		}
	}

	got, err := pool.ListByType(ctx, "support")
	if err != nil {
		t.Fatalf("ListByType returned unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListByType returned %d agents, want 2", len(got))
	}

	ids := make(map[string]bool)
	for _, a := range got {
		ids[a.ID] = true
	}
	if !ids["a1"] || !ids["a2"] {
		t.Errorf("expected agents a1 and a2, got %v", ids)
	}
}

func TestPoolUpdateHeartbeat(t *testing.T) {
	pool, _ := newTestPool(t)
	ctx := context.Background()

	info := &AgentInfo{
		ID:              "agent-1",
		AgentType:       "openai",
		GatewayInstance: testGatewayInstance,
		Status:          StatusOnline,
	}

	if err := pool.Register(ctx, info); err != nil {
		t.Fatalf("Register returned unexpected error: %v", err)
	}

	if err := pool.UpdateHeartbeat(ctx, "openai", "agent-1"); err != nil {
		t.Fatalf("UpdateHeartbeat returned unexpected error: %v", err)
	}

	// Verify agent is still retrievable after heartbeat update.
	got, err := pool.Get(ctx, "openai", "agent-1")
	if err != nil {
		t.Fatalf("Get after heartbeat returned unexpected error: %v", err)
	}
	if got.LastHeartbeat.IsZero() {
		t.Error("LastHeartbeat is zero after update")
	}
}

func TestPoolUpdateHeartbeatNotFound(t *testing.T) {
	pool, _ := newTestPool(t)
	ctx := context.Background()

	err := pool.UpdateHeartbeat(ctx, "openai", "nonexistent")
	if err != ErrAgentNotFound {
		t.Fatalf("expected ErrAgentNotFound, got %v", err)
	}
}

func TestPoolIncrDecrActiveSessions(t *testing.T) {
	pool, _ := newTestPool(t)
	ctx := context.Background()

	info := &AgentInfo{
		ID:              "agent-1",
		AgentType:       "openai",
		GatewayInstance: testGatewayInstance,
		Status:          StatusOnline,
	}

	if err := pool.Register(ctx, info); err != nil {
		t.Fatalf("Register returned unexpected error: %v", err)
	}

	// Increment twice.
	if err := pool.IncrActiveSessions(ctx, "openai", "agent-1"); err != nil {
		t.Fatalf("IncrActiveSessions (1st) returned unexpected error: %v", err)
	}
	if err := pool.IncrActiveSessions(ctx, "openai", "agent-1"); err != nil {
		t.Fatalf("IncrActiveSessions (2nd) returned unexpected error: %v", err)
	}

	got, err := pool.Get(ctx, "openai", "agent-1")
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}
	if got.ActiveSessions != 2 {
		t.Errorf("ActiveSessions after 2 increments = %d, want 2", got.ActiveSessions)
	}

	// Decrement once.
	if err := pool.DecrActiveSessions(ctx, "openai", "agent-1"); err != nil {
		t.Fatalf("DecrActiveSessions returned unexpected error: %v", err)
	}

	got, err = pool.Get(ctx, "openai", "agent-1")
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}
	if got.ActiveSessions != 1 {
		t.Errorf("ActiveSessions after decrement = %d, want 1", got.ActiveSessions)
	}
}

func TestPoolIncrActiveSessionsNotFound(t *testing.T) {
	pool, _ := newTestPool(t)
	ctx := context.Background()

	err := pool.IncrActiveSessions(ctx, "openai", "nonexistent")
	if err != ErrAgentNotFound {
		t.Fatalf("expected ErrAgentNotFound, got %v", err)
	}
}

func TestPoolDecrActiveSessionsNotFound(t *testing.T) {
	pool, _ := newTestPool(t)
	ctx := context.Background()

	err := pool.DecrActiveSessions(ctx, "openai", "nonexistent")
	if err != ErrAgentNotFound {
		t.Fatalf("expected ErrAgentNotFound, got %v", err)
	}
}
