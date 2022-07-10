package kv

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/x/alamos"
	kvx "github.com/arya-analytics/x/kv"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"time"
)

// Config is the configuration for the aspen kv service. For default values, see DefaultConfig().
type Config struct {
	// Cluster is the cluster that the KV will use to communicate with other databases.
	// [Required]
	Cluster cluster.Cluster
	// OperationsTransport is used to send key-value operations between nodes.
	// [Required]
	OperationsTransport OperationsTransport
	// FeedbackTransport is used to send gossip feedback between nodes.
	// [Required]
	FeedbackTransport FeedbackTransport
	// LeaseTransport is used to send lease operations between nodes.
	// [Required]
	LeaseTransport LeaseTransport
	// Logger is the witness of it all.
	// [Not Required]
	Logger *zap.SugaredLogger
	// Engine is the underlying key-value engine that KV writes its operations to.
	//[Required]
	Engine kvx.KV
	// GossipInterval is how often a node initiates gossip with a peer.
	// [Not Required]
	GossipInterval time.Duration
	// Recovery threshold for the SIR gossip protocol i.e. how many times the node must send a redundant operation
	// for it to stop propagating it.
	//[Not Required]
	RecoveryThreshold int
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
		return errors.AssertionFailedf("[kv] - a valid cluster must be provided")
	}
	if cfg.OperationsTransport == nil {
		return errors.AssertionFailedf("[kv] - operations transport is required")
	}
	if cfg.FeedbackTransport == nil {
		return errors.AssertionFailedf("[kv]  - feedback transport is required")
	}
	if cfg.LeaseTransport == nil {
		return errors.AssertionFailedf("[kv] lease transport is required")
	}
	if cfg.Engine == nil {
		return errors.AssertionFailedf("[kv] - engine is required")
	}
	return nil
}

func (cfg Config) String() string { return cfg.Report().String() }

func (cfg Config) Report() alamos.Report {
	report := make(alamos.Report)
	report["recoveryThreshold"] = cfg.RecoveryThreshold
	report["gossipInterval"] = cfg.GossipInterval.String()
	report["operationsTransport"] = cfg.OperationsTransport.String()
	report["feedbackTransport"] = cfg.FeedbackTransport.String()
	report["leaseTransport"] = cfg.LeaseTransport.String()
	return report
}

func DefaultConfig() Config {
	return Config{
		GossipInterval:    1 * time.Second,
		RecoveryThreshold: 5,
	}
}
