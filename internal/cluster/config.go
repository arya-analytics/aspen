package cluster

import (
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	pledge_ "github.com/arya-analytics/aspen/internal/cluster/pledge"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"time"
)

type Config struct {
	// Storage is a key-value storage backend for the cluster. Cluster will flush changes to its state to this backend
	// based on Config.StorageFlushInterval. Join will also attempt to load an existing cluster from this backend.
	// If Config.Storage is not provided, Cluster state will only be stored in memory.
	Storage kv.KV
	// StorageKey is the key used to store the cluster state in the backend.
	StorageKey []byte
	// StorageFlushInterval	is the interval at which the cluster state is flushed to the backend. If this is set to 0,
	// the cluster state will be flushed every time a change is made.
	StorageFlushInterval time.Duration
	// Pledge is the configuration for pledging to the cluster upon a Join call. See the pledge package for more details
	// on how to configure this.
	Pledge pledge_.Config
	// Shutdown is used to shut down cluster activities gracefully.
	Shutdown shutdown.Shutdown
	// Logger is the logger used by the cluster.
	Logger *zap.Logger
	// Gossip is the configuration for propagating Cluster state through gossip. See the gossip package for more details
	// on how to configure this.
	Gossip gossip.Config
}

func (cfg Config) Merge(def Config) Config {
	if cfg.Pledge.Logger == nil {
		cfg.Pledge.Logger = cfg.Logger
	}
	cfg.Pledge = cfg.Pledge.Merge(def.Pledge)
	if cfg.Gossip.Logger == nil {
		cfg.Gossip.Logger = cfg.Logger
	}
	cfg.Gossip = cfg.Gossip.Merge(def.Gossip)
	return cfg
}

func DefaultConfig() Config {
	return Config{
		Pledge:     pledge_.DefaultConfig(),
		StorageKey: []byte("aspen.cluster"),
		Logger:     zap.NewNop(),
		Gossip:     gossip.DefaultConfig(),
	}
}
