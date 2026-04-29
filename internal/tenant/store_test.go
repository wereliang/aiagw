package tenant

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestStore starts a miniredis server and returns a Store plus the
// underlying miniredis instance so callers can seed data.
func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return NewStore(client), mr
}

func TestGetByAPIKey(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()

	// Seed: apikey:hash123 -> tenant_1
	mr.Set("apikey:hash123", "tenant_1")

	// Seed: tenant:tenant_1 hash
	mr.HSet("tenant:tenant_1",
		"name", "Acme Corp",
		"status", "active",
		"allowed_agent_types", "openai,anthropic",
		"max_sessions", "100",
		"max_agents", "10",
	)

	info, err := store.GetByAPIKey(ctx, "hash123")
	if err != nil {
		t.Fatalf("GetByAPIKey returned unexpected error: %v", err)
	}

	if info.ID != "tenant_1" {
		t.Errorf("ID = %q, want %q", info.ID, "tenant_1")
	}
	if info.Name != "Acme Corp" {
		t.Errorf("Name = %q, want %q", info.Name, "Acme Corp")
	}
	if info.Status != "active" {
		t.Errorf("Status = %q, want %q", info.Status, "active")
	}
	if len(info.AllowedAgentTypes) != 2 ||
		info.AllowedAgentTypes[0] != "openai" ||
		info.AllowedAgentTypes[1] != "anthropic" {
		t.Errorf("AllowedAgentTypes = %v, want [openai anthropic]", info.AllowedAgentTypes)
	}
	if info.MaxSessions != 100 {
		t.Errorf("MaxSessions = %d, want 100", info.MaxSessions)
	}
	if info.MaxAgents != 10 {
		t.Errorf("MaxAgents = %d, want 10", info.MaxAgents)
	}
}

func TestGetByAPIKeyNotFound(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	_, err := store.GetByAPIKey(ctx, "nonexistent")
	if err != ErrTenantNotFound {
		t.Fatalf("expected ErrTenantNotFound, got %v", err)
	}
}

func TestGetByIDNotFound(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	_, err := store.GetByID(ctx, "no_such_tenant")
	if err != ErrTenantNotFound {
		t.Fatalf("expected ErrTenantNotFound, got %v", err)
	}
}

func TestExists(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()

	// Seed one tenant hash
	mr.HSet("tenant:tenant_1", "name", "Acme Corp")

	if !store.Exists(ctx, "tenant_1") {
		t.Error("Exists returned false for existing tenant")
	}
	if store.Exists(ctx, "nonexistent") {
		t.Error("Exists returned true for nonexistent tenant")
	}
}
