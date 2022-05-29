package kv

import (
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/util/errutil"
	"go.uber.org/zap"
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
	c := errutil.NewCatchSimple(errutil.WithHooks(errutil.NewPipeHook(b.errors)))
	ps.Logger.Debug("persisting batch", zap.Int("numOps", len(b.operations)))
	for _, op := range b.operations {
		if op.Variant == Set {
			c.Exec(func() error { return ps.Engine.Set(op.Key, op.Value) })
		} else {
			c.Exec(func() error { return ps.Engine.Delete(op.Key) })
		}
		c.Exec(func() error {
			key, err := digestKey(op.Key)
			if err != nil {
				return err
			}
			if err = kv_.Flush(ps.Engine, key, op.Digest()); err != nil {
				return err
			}
			accepted.operations = append(accepted.operations, op)
			return nil
		})
	}
	return accepted, true
}
