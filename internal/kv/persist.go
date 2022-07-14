package kv

import (
	"github.com/arya-analytics/x/confluence"
	kvx "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/signal"
)

type persist struct {
	bw kvx.BatchWriter
	confluence.LinearTransform[BatchRequest, BatchRequest]
}

func newPersist(bw kvx.BatchWriter) segment {
	ps := &persist{bw: bw}
	ps.LinearTransform.ApplyTransform = ps.persist
	return ps
}

func (ps *persist) persist(ctx signal.Context, bd BatchRequest) (BatchRequest, bool, error) {
	err := bd.commitTo(ps.bw)
	return bd, err == nil, err
}
