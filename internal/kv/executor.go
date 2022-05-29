package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/confluence"
)

type executor struct {
	Config
	confluence.CoreSource[batch]
}

func newExecutor(cfg Config) *executor { return &executor{Config: cfg} }

func (e *executor) Flow(ctx confluence.Context) {}

func (e *executor) setWithLease(key []byte, leaseholder node.ID, value []byte) error {
	return e.exec(Operation{Key: key, Value: value, Leaseholder: leaseholder, Variant: Set})
}

func (e *executor) delete(key []byte) error { return e.exec(Operation{Key: key, Variant: Delete}) }

func (e *executor) exec(op Operation) error {
	b := batch{errors: make(chan error, 1), operations: Operations{op}}
	for _, inlet := range e.Out {
		inlet.Inlet() <- b
	}
	return <-b.errors
}
