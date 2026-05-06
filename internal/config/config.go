package config

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Redis   RedisConfig   `yaml:"redis"`
	Session SessionConfig `yaml:"session"`
	Agent   AgentConfig   `yaml:"agent"`
	Auth    AuthConfig    `yaml:"auth"`
	Log     LogConfig     `yaml:"log"`
}

type ServerConfig struct {
	HTTPPort         int    `yaml:"http_port"`
	GRPCPort         int    `yaml:"grpc_port"`
	InternalGRPCPort int    `yaml:"internal_grpc_port"`
	InstanceID       string `yaml:"instance_id"`
	AdvertiseAddr    string `yaml:"advertise_addr"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type SessionConfig struct {
	TTL time.Duration `yaml:"ttl"`
}

type AgentConfig struct {
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
}

type AuthConfig struct {
	APIKeys []string `yaml:"api_keys"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if v := os.Getenv("INSTANCE_ID"); v != "" {
		cfg.Server.InstanceID = v
	}
	if cfg.Server.InstanceID == "" {
		cfg.Server.InstanceID = uuid.New().String()
	}

	if v := os.Getenv("ADVERTISE_ADDR"); v != "" {
		cfg.Server.AdvertiseAddr = v
	}

	return cfg, nil
}
