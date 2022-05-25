package gossip

import (
	"github.com/arya-analytics/x/shutdown"
	"github.com/arya-analytics/x/transport"
	"go.uber.org/zap"
	"time"
)

type Transport = transport.Unary[Message, Message]

type Config struct {
	Interval  time.Duration
	Transport Transport
	Shutdown  shutdown.Shutdown
	Logger    *zap.Logger
}

func (cfg Config) Merge(def Config) Config {
	if cfg.Interval == 0 {
		cfg.Interval = def.Interval
	}
	if cfg.Logger == nil {
		cfg.Logger = def.Logger
	}
	return cfg
}

func DefaultConfig() Config {
	return Config{
		Interval: 1 * time.Second,
		Logger:   zap.NewNop(),
	}
}
