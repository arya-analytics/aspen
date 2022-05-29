package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/confluence"
)

type executor struct {
	Config
	confluence.CoreSource[Batch]
}

func newExecutor(cfg Config) *executor { return &executor{Config: cfg} }

func (e *executor) setWithLease(key []byte, leaseholder node.ID, value []byte) error {
	errC := make(chan error, 1)
	batch := Batch{Errors: errC, Operations: Operations{{
		Key:         key,
		Value:       value,
		Leaseholder: leaseholder,
		Variant:     Set,
	}}}
	for _, inlet := range e.Out {
		inlet.Inlet() <- batch
	}
	return <-errC
}
