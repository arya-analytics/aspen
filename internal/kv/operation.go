package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/filter"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/util/errutil"
	"github.com/arya-analytics/x/version"
	"io"
)

type Variant byte

const (
	Set Variant = iota
	Delete
)

type State byte

const (
	Infected State = iota
	Recovered
)

type Executor func(op Operation) error

type Operation struct {
	Key         []byte
	Value       []byte
	Variant     Variant
	Version     version.Counter
	Leaseholder node.ID
	State       State
}

// Load implements the kv.Loader interface.
func (o *Operation) Load(r io.Reader) error {
	c := errutil.NewCatchRead(r)
	c.Read(&o.Variant)
	c.Read(&o.Version)
	c.Read(&o.Leaseholder)
	return c.Error()
}

// Flush implements the kv.Flusher interface.
func (o Operation) Flush(w io.Writer) error {
	c := errutil.NewCatchWrite(w)
	c.Write(o.Variant)
	c.Write(o.Version)
	c.Write(o.Leaseholder)
	return c.Error()
}

const operationKey = "op"

func metadataKey(key []byte) (opKey []byte, err error) { return kv_.CompositeKey(operationKey, key) }

func loadMetadata(kve kv_.Reader, key []byte) (op Operation, err error) {
	opKey, err := metadataKey(key)
	if err != nil {
		return op, err
	}
	return op, kv_.Load(kve, opKey, &op)
}

type Operations []Operation

func (ops Operations) whereState(state State) Operations {
	return ops.where(func(op Operation) bool { return op.State == state })
}

func (ops Operations) where(cond func(Operation) bool) Operations { return filter.Slice(ops, cond) }

type batch struct {
	Sender     node.ID
	Operations Operations
	Errors     chan error
}

type operationMap map[string]Operation

func (m operationMap) Merge(operations Operations) {
	for _, op := range operations {
		m[string(op.Key)] = op
	}
}

func (m operationMap) Copy() operationMap {
	mCopy := make(operationMap, len(m))
	for k, v := range m {
		mCopy[k] = v
	}
	return mCopy
}

func (m operationMap) Operations() Operations {
	ops := make(Operations, 0, len(m))
	for _, op := range m {
		ops = append(ops, op)
	}
	return ops
}
