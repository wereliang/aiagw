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
agent:
  heartbeat_interval: 20s
  heartbeat_timeout: 60s
auth:
  api_keys:
    - "key1"
    - "key2"
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
	if len(cfg.Auth.APIKeys) != 2 {
		t.Errorf("Auth.APIKeys len = %d, want 2", len(cfg.Auth.APIKeys))
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
