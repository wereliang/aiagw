package store

import (
	"testing"

	"github.com/wereliang/aiagw/internal/config"
)

func TestNewRedisStore(t *testing.T) {
	cfg := config.RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}

	store := New(cfg)
	if store == nil {
		t.Fatal("New() returned nil")
	}
	if store.Client() == nil {
		t.Fatal("Client() returned nil")
	}
}
