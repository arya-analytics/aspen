package kv

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/node"
	kv_ "github.com/arya-analytics/x/kv"
)

// Writer is a writable key-value store.
type Writer interface {
	// SetWithLease is similar to Set, but also takes an id for a leaseholder node.
	// If the leaseholder node is not the host, the request will be forwarded to the
	// leaseholder for execution. Only the leaseholder node will be able to perform
	// set and delete operations on the requested key.
	SetWithLease(key []byte, leaseholder node.ID, value []byte) error
	// Writer represents the same interface to a typical key-value store.
	// kv.Write.Set operations call SetWithLease internally and mark the leaseholder as
	// the host.
	kv_.Writer
}

type (
	//Reader is a readable key-value store.
	Reader = kv_.Reader
	// Closer is a key-value store that can be closed. Block until all pending
	// operations have persisted to disk.
	Closer = kv_.Closer
)

// KV is a readable and writable key-value store.
type KV interface {
	Writer
	Reader
	kv_.Closer
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
