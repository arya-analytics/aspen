package leasholder

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/kv/operation"
	"github.com/arya-analytics/x/kv"
)

type Config struct {
	Reader    kv.Reader
	Cluster   cluster.Cluster
	Transport Transport
	Executor  operation.Executor
}

func (cfg Config) Merge(def Config) Config {
	return cfg
}
