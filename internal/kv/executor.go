package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/confluence"
)

type executor struct {
	Config
	confluence.UnarySource[batch]
}

func newExecutor(cfg Config) *executor { return &executor{Config: cfg} }

func (e *executor) setWithLease(key []byte, leaseholder node.ID, value []byte) error {
	// We need to make copies of the key and value so that the caller can safely modify
	// them after the call returns.
	return e.exec(Operation{
		Key:         binary.MakeCopy(key),
		Value:       binary.MakeCopy(value),
		Leaseholder: leaseholder,
		Variant:     Set,
	})
}

func (e *executor) delete(key []byte) error { return e.exec(Operation{Key: key, Variant: Delete}) }

func (e *executor) exec(op Operation) error {
	errors := make(chan error, 1)
	e.Out.Inlet() <- batch{errors: errors, operations: Operations{op}}
	return <-errors
}
