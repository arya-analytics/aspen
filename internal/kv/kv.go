package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/kv"
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
	kv.Writer
}

//Reader is a readable key-value store.
type Reader interface{ kv.Reader }

type DB interface {
	Writer
	Reader
}
