package gossip

import (
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/shutdown"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"time"
)

// Config sets specific parameters for the gossip service. See DefaultConfig() for default values.
type Config struct {
	// Interval is the interval at which a node will gossip its state.
	// [Not Required]
	Interval time.Duration
	// Transport is the transport used to exchange gossip between nodes.
	// [Required]
	Transport Transport
	// Shutdown is used to gracefully stop gossip operations.
	// [Not Required]
	Shutdown shutdown.Shutdown
	// Logger is the witness of it all.
	// [Not Required]
	Logger *zap.SugaredLogger
	// Experiment is where the gossip services saves its metrics and reports.
	// [Not Required]
	Experiment alamos.Experiment
}

func (cfg Config) Merge(def Config) Config {
	if cfg.Shutdown == nil {
		cfg.Shutdown = def.Shutdown
	}
	if cfg.Interval <= 0 {
		cfg.Interval = def.Interval
	}
	if cfg.Logger == nil {
		cfg.Logger = def.Logger
	}
	if cfg.Transport == nil {
		cfg.Transport = def.Transport
	}
	return cfg
}

func (cfg Config) Validate() error {
	if cfg.Transport == nil {
		return errors.New("[gossip] - transport required")
	}
	return nil
}

// String returns a pretty printed string representation of the config.
func (cfg Config) String() string { return cfg.Report().String() }

// Report implements the alamos.Reporter interface.
func (cfg Config) Report() alamos.Report {
	report := make(alamos.Report)
	report["interval"] = cfg.Interval
	report["transport"] = cfg.Transport.String()
	return report
}

func DefaultConfig() Config {
	return Config{
		Interval: 1 * time.Second,
		Logger:   zap.NewNop().Sugar(),
		Shutdown: shutdown.New(),
	}
}
