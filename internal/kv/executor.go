package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/confluence"
)

type executor struct {
	Config
	confluence.UnarySource[batch]
}

func newExecutor(cfg Config) *executor { return &executor{Config: cfg} }

func (e *executor) Flow(ctx confluence.Context) {}

func (e *executor) setWithLease(key []byte, leaseholder node.ID, value []byte) error {
	return e.exec(Operation{Key: key, Value: value, Leaseholder: leaseholder, Variant: Set})
}

func (e *executor) delete(key []byte) error { return e.exec(Operation{Key: key, Variant: Delete}) }

func (e *executor) exec(op Operation) error {
	errors := make(chan error, 1)
	e.Out.Inlet() <- batch{errors: errors, operations: Operations{op}}
	return <-errors
}
