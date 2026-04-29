package session

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestManager(t *testing.T) (*Manager, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return NewManager(client, 30*time.Minute), mr
}

func TestCreateAndGet(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	info, err := mgr.Create(ctx, "agent_1", "openai", "gw-instance-1")
	if err != nil {
		t.Fatalf("Create returned unexpected error: %v", err)
	}

	if info.ID == "" {
		t.Fatal("expected non-empty session ID")
	}
	agentType, _, parseErr := ParseSessionID(info.ID)
	if parseErr != nil {
		t.Fatalf("ParseSessionID returned unexpected error: %v", parseErr)
	}
	if agentType != "openai" {
		t.Errorf("parsed agentType = %q, want %q", agentType, "openai")
	}
	if info.AgentID != "agent_1" {
		t.Errorf("AgentID = %q, want %q", info.AgentID, "agent_1")
	}
	if info.AgentType != "openai" {
		t.Errorf("AgentType = %q, want %q", info.AgentType, "openai")
	}
	if info.GatewayInstance != "gw-instance-1" {
		t.Errorf("GatewayInstance = %q, want %q", info.GatewayInstance, "gw-instance-1")
	}
	if info.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
	if info.LastActiveAt.IsZero() {
		t.Error("LastActiveAt should not be zero")
	}

	got, err := mgr.Get(ctx, info.ID)
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}

	if got.ID != info.ID {
		t.Errorf("Get ID = %q, want %q", got.ID, info.ID)
	}
	if got.AgentID != info.AgentID {
		t.Errorf("Get AgentID = %q, want %q", got.AgentID, info.AgentID)
	}
	if got.AgentType != info.AgentType {
		t.Errorf("Get AgentType = %q, want %q", got.AgentType, info.AgentType)
	}
	if got.GatewayInstance != info.GatewayInstance {
		t.Errorf("Get GatewayInstance = %q, want %q", got.GatewayInstance, info.GatewayInstance)
	}
	if !got.CreatedAt.Equal(info.CreatedAt) {
		t.Errorf("Get CreatedAt = %v, want %v", got.CreatedAt, info.CreatedAt)
	}
	if !got.LastActiveAt.Equal(info.LastActiveAt) {
		t.Errorf("Get LastActiveAt = %v, want %v", got.LastActiveAt, info.LastActiveAt)
	}
}

func TestGetNotFound(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	_, err := mgr.Get(ctx, "nonexistent-session-id")
	if err != ErrSessionNotFound {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	info, err := mgr.Create(ctx, "agent_1", "openai", "gw-1")
	if err != nil {
		t.Fatalf("Create returned unexpected error: %v", err)
	}

	if err := mgr.Delete(ctx, info.ID); err != nil {
		t.Fatalf("Delete returned unexpected error: %v", err)
	}

	_, err = mgr.Get(ctx, info.ID)
	if err != ErrSessionNotFound {
		t.Fatalf("expected ErrSessionNotFound after delete, got %v", err)
	}
}

func TestTouch(t *testing.T) {
	mgr, mr := newTestManager(t)
	ctx := context.Background()

	info, err := mgr.Create(ctx, "agent_1", "openai", "gw-1")
	if err != nil {
		t.Fatalf("Create returned unexpected error: %v", err)
	}

	originalLastActive := info.LastActiveAt

	mr.FastForward(5 * time.Second)
	time.Sleep(1100 * time.Millisecond)

	if err := mgr.Touch(ctx, info.ID); err != nil {
		t.Fatalf("Touch returned unexpected error: %v", err)
	}

	got, err := mgr.Get(ctx, info.ID)
	if err != nil {
		t.Fatalf("Get after Touch returned unexpected error: %v", err)
	}

	if !got.LastActiveAt.After(originalLastActive) && !got.LastActiveAt.Equal(originalLastActive) {
		t.Errorf("LastActiveAt after Touch (%v) should be >= original (%v)",
			got.LastActiveAt, originalLastActive)
	}
}

func TestTouchNotFound(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	err := mgr.Touch(ctx, "nonexistent-session-id")
	if err != ErrSessionNotFound {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

func TestDeleteByAgent(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	s1, err := mgr.Create(ctx, "agent-1", "openai", "gw-1")
	if err != nil {
		t.Fatalf("Create s1 returned unexpected error: %v", err)
	}

	s2, err := mgr.Create(ctx, "agent-1", "openai", "gw-1")
	if err != nil {
		t.Fatalf("Create s2 returned unexpected error: %v", err)
	}

	s3, err := mgr.Create(ctx, "agent-2", "claude", "gw-1")
	if err != nil {
		t.Fatalf("Create s3 returned unexpected error: %v", err)
	}

	if err := mgr.DeleteByAgent(ctx, "agent-1"); err != nil {
		t.Fatalf("DeleteByAgent returned unexpected error: %v", err)
	}

	if _, err := mgr.Get(ctx, s1.ID); err != ErrSessionNotFound {
		t.Errorf("expected s1 to be deleted, got err=%v", err)
	}
	if _, err := mgr.Get(ctx, s2.ID); err != ErrSessionNotFound {
		t.Errorf("expected s2 to be deleted, got err=%v", err)
	}

	got, err := mgr.Get(ctx, s3.ID)
	if err != nil {
		t.Fatalf("expected s3 to remain, got err=%v", err)
	}
	if got.AgentID != "agent-2" {
		t.Errorf("remaining session AgentID = %q, want %q", got.AgentID, "agent-2")
	}
}
