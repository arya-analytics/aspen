package kv

import (
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"go.uber.org/zap"
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

func (r *recoveryTransform) transform(ctx confluence.Context, batch batch) (oBatch batch, ok bool) {
	for _, op := range batch.operations {
		key, err := kv_.CompositeKey(op.Key, op.Version)
		if err != nil {
			ctx.ErrC <- err
		}
		strKey := string(key)
		if r.repetitions[strKey] > r.RecoveryThreshold {
			r.Logger.Debug("recovering op", zap.String("key", strKey))
			op.state = Recovered
			oBatch.operations = append(oBatch.operations, op)
			delete(r.repetitions, strKey)
		}
		r.repetitions[strKey]++
	}
	return oBatch, true
}
