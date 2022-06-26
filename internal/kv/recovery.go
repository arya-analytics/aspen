package kv

import (
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/signal"
)

type recoveryTransform struct {
	Config
	confluence.Transform[batch]
	repetitions map[string]int
}

func newRecoveryTransform(cfg Config) segment {
	r := &recoveryTransform{Config: cfg, repetitions: make(map[string]int)}
	r.Transform.Transform = r.transform
	return r
}

func (r *recoveryTransform) transform(ctx signal.Context,
	batch batch) (oBatch batch, ok bool, err error) {
	for _, op := range batch.operations {
		key, err := kv_.CompositeKey(op.Key, op.Version)
		if err != nil {
			ctx.Transient() <- err
			return oBatch, false, nil
		}
		strKey := string(key)
		if r.repetitions[strKey] > r.RecoveryThreshold {
			r.Logger.Debugw("recovering op", "host", r.Cluster.HostID(), "key", strKey)
			op.state = recovered
			oBatch.operations = append(oBatch.operations, op)
			delete(r.repetitions, strKey)
		}
		r.repetitions[strKey]++
	}
	return oBatch, true, nil
}
