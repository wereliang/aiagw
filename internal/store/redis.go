package store

import (
	"github.com/redis/go-redis/v9"
	"github.com/wereliang/aiagw/internal/config"
)

type RedisStore struct {
	client *redis.Client
}

func New(cfg config.RedisConfig) *RedisStore {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	return &RedisStore{client: client}
}

func (s *RedisStore) Client() *redis.Client {
	return s.client
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}
