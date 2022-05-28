package kv

import (
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
	Interval            time.Duration
	Logger              *zap.Logger
	RecoveryThreshold   int
	Engine              kv_.KV
	GossipInterval      time.Duration
}

func (cfg Config) Merge(def Config) Config {
	return Config{}
}

func DefaultConfig() Config {
	return Config{}
}
