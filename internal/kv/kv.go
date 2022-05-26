package kv

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/kv/leasholder"
	"github.com/arya-analytics/aspen/internal/node"
	kv_ "github.com/arya-analytics/x/kv"
)

type (
	// Writer is a writable key-value store.
	Writer = leasholder.Writer
	// Reader is a readable key-value store.
	Reader = kv_.Reader
	// Closer is a key-value store that can be closed. Block until all pending
	// operations have persisted to disk.
	Closer = kv_.Closer
)

// KV is a readable and writable key-value store.
type KV interface {
	Writer
	Reader
	Closer
}

type kv struct {
	kv_.KV
	Config
}

func New(clust cluster.Cluster, kvEngine kv_.KV, cfg Config) KV {
	return &kv{KV: kvEngine, Config: cfg.Merge(DefaultConfig())}
}

func (k *kv) SetWithLease(key []byte, leaseholder node.ID, value []byte) error {
	return nil
}
