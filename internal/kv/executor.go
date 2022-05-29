package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/confluence"
	"go.uber.org/zap"
)

type executor struct {
	Config
	confluence.CoreSource[batch]
}

func newExecutor(cfg Config) *executor { return &executor{Config: cfg} }

func (e *executor) Flow(ctx confluence.Context) {}

func (e *executor) setWithLease(key []byte, leaseholder node.ID, value []byte) error {
	e.Logger.Debug("executing set operation",
		zap.String("key", string(key)),
		zap.Uint32("leaseholder", uint32(leaseholder)),
	)
	return e.exec(Operation{Key: key, Value: value, Leaseholder: leaseholder, Variant: Set})
}

func (e *executor) delete(key []byte) error {
	e.Logger.Debug("executing delete operation",
		zap.String("key", string(key)),
	)
	return e.exec(Operation{Key: key, Variant: Delete})
}

func (e *executor) exec(op Operation) error {
	b := batch{errors: make(chan error, 1), operations: Operations{op}}
	for _, inlet := range e.Out {
		inlet.Inlet() <- b
	}
	return <-b.errors
}
