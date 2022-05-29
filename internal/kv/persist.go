package kv

import (
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
)

type persist struct {
	Config
	confluence.Transform[batch]
}

func newPersist(cfg Config) segment {
	ps := &persist{Config: cfg}
	ps.Transform.Transform = ps.persist
	return ps
}

func (ps *persist) persist(ctx confluence.Context, b batch) (batch, bool) {
	var accepted batch
	for _, op := range b.Operations {
		var err error
		if op.Variant == Set {
			err = ps.Engine.Set(op.Key, op.Value)
		} else {
			err = ps.Engine.Delete(op.Key)
		}
		if err != nil {
			b.Errors <- err
			continue
		}
		key, err := metadataKey(op.Key)
		if err != nil {
			b.Errors <- err
			return b, false
		}
		if err = kv_.Flush(ps.Engine, key, op); err != nil {
			b.Errors <- err
			return b, false
		}
		accepted.Operations = append(accepted.Operations, op)
	}
	return accepted, true
}
