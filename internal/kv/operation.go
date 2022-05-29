package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/confluence"
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

type Operation struct {
	Key         []byte
	Value       []byte
	Variant     Variant
	Version     version.Counter
	Leaseholder node.ID
	state       State
}

func (o Operation) Digest() Digest {
	return Digest{
		Version:     o.Version,
		Leaseholder: o.Leaseholder,
	}
}

type Digest struct {
	Key         []byte
	Version     version.Counter
	Leaseholder node.ID
}

// Load implements the kv.Loader interface.
func (d *Digest) Load(r io.Reader) error {
	c := errutil.NewCatchRead(r)
	c.Read(&d.Version)
	c.Read(&d.Leaseholder)
	return c.Error()
}

// Flush implements the kv.Flusher interface.
func (d Digest) Flush(w io.Writer) error {
	c := errutil.NewCatchWrite(w)
	c.Write(d.Version)
	c.Write(d.Leaseholder)
	return c.Error()
}

func (d Digest) Operation() Operation {
	return Operation{
		Version:     d.Version,
		Leaseholder: d.Leaseholder,
	}
}

// Flush implements the kv.Flusher interface.
func (o Operation) Flush(w io.Writer) error {
	c := errutil.NewCatchWrite(w)
	c.Write(o.Variant)
	c.Write(o.Version)
	c.Write(o.Leaseholder)
	return c.Error()
}

const operationKey = "--op--"

func digestKey(key []byte) (opKey []byte, err error) { return kv_.CompositeKey(operationKey, key) }

type Operations []Operation

func (ops Operations) whereState(state State) Operations {
	return ops.where(func(op Operation) bool { return op.state == state })
}

func (ops Operations) where(cond func(Operation) bool) Operations { return filter.Slice(ops, cond) }

type batch struct {
	sender     node.ID
	operations Operations
	errors     chan error
}

type segment = confluence.Segment[batch]

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
