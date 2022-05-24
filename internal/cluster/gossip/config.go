package gossip

import (
	"github.com/arya-analytics/x/shutdown"
	"github.com/arya-analytics/x/transport"
	"go.uber.org/zap"
	"time"
)

type Config struct {
	Interval  time.Duration
	Transport transport.Unary[Message, Message]
	Shutdown  shutdown.Shutdown
	Logger    *zap.Logger
}

func (cfg Config) Merge(override Config) Config {
	if cfg.Interval == 0 {
		cfg.Interval = override.Interval
	}
	if cfg.Logger == nil {
		cfg.Logger = override.Logger
	}
	return cfg
}

func DefaultConfig() Config {
	return Config{
		Interval: 1 * time.Second,
		Logger:   zap.NewNop(),
	}
}
