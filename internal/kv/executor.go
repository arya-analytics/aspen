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

func (e *executor) delete(key []byte) error {
	return e.exec(Operation{Key: key, Variant: Delete})
}

func (e *executor) exec(op Operation) error {
	batch := batch{Errors: make(chan error, 1), Operations: Operations{op}}
	for _, inlet := range e.Out {
		inlet.Inlet() <- batch
	}
	return <-batch.Errors
}
