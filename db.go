package aspen

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/errutil"
)

type (
	Cluster = cluster.Cluster
	Node    = node.Node
	ID      = node.ID
	KV      = kv.KV
	Address = address.Address
	State   = node.State
)

const (
	Healthy = node.StateHealthy
	Left    = node.StateLeft
	Dead    = node.StateDead
	Suspect = node.StateSuspect
)

type DB interface {
	Cluster
	KV
}

type db struct {
	Cluster
	KV
	options *options
}

func (db *db) Close() error {
	c := errutil.NewCatchSimple(errutil.WithAggregation())
	c.Exec(db.options.shutdown.Shutdown)
	c.Exec(db.options.kv.Engine.Close)
	return c.Error()
}
