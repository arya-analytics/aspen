package kv

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/errutil"
	kvx "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/signal"
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

func (ps *persist) persist(ctx signal.Context, b batch) (batch, bool, error) {
	var accepted batch
	c := errutil.NewCatchSimple(errutil.WithHooks(errutil.NewPipeHook(b.errors)))
	ps.Logger.Debugw("persisting batch", "host", ps.Cluster.HostID(), "batch", len(b.operations))
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
			if err = kvx.Flush(ps.Engine, key, op.Digest()); err != nil {
				return err
			}
			accepted.operations = append(accepted.operations, op)
			return nil
		})
	}
	if b.errors != nil {
		b.errors <- c.Error()
	}
	return accepted, true, nil
}
