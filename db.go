package aspen

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/aspen/internal/node"
)

type (
	Cluster = cluster.Cluster
	Node    = node.Node
	ID      = node.ID
	KV      = kv.KV
)

type DB interface {
	Cluster
	KV
}

type db struct {
	Cluster
	KV
}