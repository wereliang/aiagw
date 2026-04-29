package router

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/wereliang/aiagw/internal/agent"
)

const (
	testGatewayInstance  = "gw-node-1"
	testHeartbeatTimeout = 30 * time.Second
)

func newTestRouter(t *testing.T) (*Router, *agent.Pool) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	pool := agent.NewPool(client, testGatewayInstance, testHeartbeatTimeout)
	r := New(pool)

	return r, pool
}

func TestRouteSelectsLeastLoaded(t *testing.T) {
	r, pool := newTestRouter(t)
	ctx := context.Background()

	a1 := &agent.AgentInfo{
		ID: "a1", AgentType: "support",
		GatewayInstance: testGatewayInstance, Status: agent.StatusOnline,
	}
	a2 := &agent.AgentInfo{
		ID: "a2", AgentType: "support",
		GatewayInstance: testGatewayInstance, Status: agent.StatusOnline,
	}

	if err := pool.Register(ctx, a1); err != nil {
		t.Fatalf("Register(a1) returned unexpected error: %v", err)
	}
	if err := pool.Register(ctx, a2); err != nil {
		t.Fatalf("Register(a2) returned unexpected error: %v", err)
	}

	if err := pool.IncrActiveSessions(ctx, "support", "a1"); err != nil {
		t.Fatalf("IncrActiveSessions(a1, 1) returned unexpected error: %v", err)
	}
	if err := pool.IncrActiveSessions(ctx, "support", "a1"); err != nil {
		t.Fatalf("IncrActiveSessions(a1, 2) returned unexpected error: %v", err)
	}

	if err := pool.IncrActiveSessions(ctx, "support", "a2"); err != nil {
		t.Fatalf("IncrActiveSessions(a2, 1) returned unexpected error: %v", err)
	}

	got, err := r.Route(ctx, "support")
	if err != nil {
		t.Fatalf("Route returned unexpected error: %v", err)
	}
	if got.ID != "a2" {
		t.Errorf("Route selected agent %q, want %q", got.ID, "a2")
	}
}

func TestRouteNoAgentsAvailable(t *testing.T) {
	r, _ := newTestRouter(t)
	ctx := context.Background()

	_, err := r.Route(ctx, "support")
	if err != ErrNoAgentAvailable {
		t.Fatalf("expected ErrNoAgentAvailable, got %v", err)
	}
}

func TestRouteSkipsOfflineAgents(t *testing.T) {
	r, pool := newTestRouter(t)
	ctx := context.Background()

	a1 := &agent.AgentInfo{
		ID: "a1", AgentType: "support",
		GatewayInstance: testGatewayInstance, Status: agent.StatusOffline,
	}
	a2 := &agent.AgentInfo{
		ID: "a2", AgentType: "support",
		GatewayInstance: testGatewayInstance, Status: agent.StatusOnline,
	}

	if err := pool.Register(ctx, a1); err != nil {
		t.Fatalf("Register(a1) returned unexpected error: %v", err)
	}
	if err := pool.Register(ctx, a2); err != nil {
		t.Fatalf("Register(a2) returned unexpected error: %v", err)
	}

	got, err := r.Route(ctx, "support")
	if err != nil {
		t.Fatalf("Route returned unexpected error: %v", err)
	}
	if got.ID != "a2" {
		t.Errorf("Route selected agent %q, want %q", got.ID, "a2")
	}
}
