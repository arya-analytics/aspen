package cluster

import (
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/pledge"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
)

type Config struct {
	// Storage
	Storage kv.KV
	// StorageKey
	StorageKey []byte
	// Pledge
	Pledge pledge.Config
	// Shutdown
	Shutdown shutdown.Shutdown
	// Logger
	Logger *zap.Logger
	// Gossip
	Gossip gossip.Config
}

func (cfg Config) Merge(override Config) Config {
	cfg.Pledge = cfg.Pledge.Merge(override.Pledge)
	return cfg
}

func DefaultConfig() Config {
	return Config{
		Pledge:     pledge.DefaultConfig(),
		StorageKey: []byte("aspen.cluster"),
	}
}
