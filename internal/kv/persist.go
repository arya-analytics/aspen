package kv

import (
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
)

type persist struct {
	Config
	confluence.Transform[Batch]
}

func newPersist(cfg Config) Segment {
	ps := &persist{Config: cfg}
	ps.Transform.Transform = ps.persist
	return ps
}

func (ps *persist) persist(batch Batch) Batch {
	var accepted Batch
	for _, op := range batch.Operations {
		var err error
		if op.Variant == Set {
			err = ps.Engine.Set(op.Key, op.Value)
		} else {
			err = ps.Engine.Delete(op.Key)
		}
		if err != nil {
			batch.Errors <- err
			continue
		}
		key, err := Key(op.Key)
		if err != nil {
			panic(err)
		}
		if err = kv_.Flush(ps.Engine, key, op); err != nil {
			batch.Errors <- err
			continue
		}
		accepted.Operations = append(accepted.Operations, op)
	}
	return batch
}
