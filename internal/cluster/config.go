package cluster

import (
	"github.com/arya-analytics/aspen/internal/pledge"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/shutdown"
	"github.com/arya-analytics/x/transport"
	"time"
)

type Config struct {
	// Storage
	Storage kv.KV
	// StorageKey
	StorageKey []byte
	// Pledge
	Pledge pledge.Config
	// GossipInterval
	GossipInterval time.Duration
	// GossipTransport
	GossipTransport transport.Unary[Message, Message]
	// Shutdown
	Shutdown shutdown.Shutdown
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
