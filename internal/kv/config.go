package kv

import (
	"errors"
	"github.com/arya-analytics/aspen/internal/cluster"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"time"
)

type Config struct {
	Cluster             cluster.Cluster
	OperationsTransport OperationsTransport
	FeedbackTransport   FeedbackTransport
	LeaseTransport      LeaseTransport
	Shutdown            shutdown.Shutdown
	Logger              *zap.Logger
	RecoveryThreshold   int
	Engine              kv_.KV
	GossipInterval      time.Duration
}

func (cfg Config) Merge(def Config) Config {
	if cfg.Cluster == nil {
		cfg.Cluster = def.Cluster
	}
	if cfg.OperationsTransport == nil {
		cfg.OperationsTransport = def.OperationsTransport
	}
	if cfg.FeedbackTransport == nil {
		cfg.FeedbackTransport = def.FeedbackTransport
	}
	if cfg.LeaseTransport == nil {
		cfg.LeaseTransport = def.LeaseTransport
	}
	if cfg.Shutdown == nil {
		cfg.Shutdown = def.Shutdown
	}
	if cfg.Logger == nil {
		cfg.Logger = def.Logger
	}
	if cfg.RecoveryThreshold == 0 {
		cfg.RecoveryThreshold = def.RecoveryThreshold
	}
	if cfg.Engine == nil {
		cfg.Engine = def.Engine
	}
	if cfg.GossipInterval == 0 {
		cfg.GossipInterval = def.GossipInterval
	}
	return cfg
}

func (cfg Config) Validate() error {
	if cfg.Cluster == nil {
		return errors.New("cluster must be set")
	}
	if cfg.OperationsTransport == nil {
		return errors.New("operations transport must be set")
	}
	if cfg.FeedbackTransport == nil {
		return errors.New("feedback transport must be set")
	}
	if cfg.LeaseTransport == nil {
		return errors.New("lease transport must be set")
	}
	if cfg.Engine == nil {
		return errors.New("engine must be set")
	}
	return nil
}

func DefaultConfig() Config {
	return Config{GossipInterval: 1 * time.Second, RecoveryThreshold: 5}
}
