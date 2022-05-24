package cluster

import (
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	pledge_ "github.com/arya-analytics/aspen/internal/pledge"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"time"
)

type Config struct {
	// Storage
	Storage kv.KV
	// StorageKey
	StorageKey []byte
	// StorageFlushInterval
	StorageFlushInterval time.Duration
	// Pledge
	Pledge pledge_.Config
	// Shutdown
	Shutdown shutdown.Shutdown
	// Logger
	Logger *zap.Logger
	// Gossip
	Gossip gossip.Config
}

func (cfg Config) Merge(override Config) Config {
	if cfg.Pledge.Logger == nil {
		cfg.Pledge.Logger = cfg.Logger
	}
	cfg.Pledge = cfg.Pledge.Merge(override.Pledge)
	if cfg.Gossip.Logger == nil {
		cfg.Gossip.Logger = cfg.Logger
	}
	return cfg
}

func DefaultConfig() Config {
	return Config{
		Pledge:     pledge_.DefaultConfig(),
		StorageKey: []byte("aspen.cluster"),
		Logger:     zap.NewNop(),
	}
}
