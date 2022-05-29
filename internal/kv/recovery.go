package kv

import (
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
)

type recovery struct {
	Config
	confluence.Transform[batch]
	repetitions map[string]int
}

func (r *recovery) transform(ctx confluence.Context, batch batch) (oBatch batch, ok bool) {
	for _, op := range batch.operations {
		key, err := kv_.CompositeKey(op.Key, op.Version)
		if err != nil {
			panic(err)
		}
		strKey := string(key)
		if r.repetitions[strKey] > r.RecoveryThreshold {
			op.State = Recovered
			oBatch.operations = append(oBatch.operations, op)
			delete(r.repetitions, strKey)
		}
		r.repetitions[strKey]++
	}
	return oBatch, true
}

func newRecoveryTransform(cfg Config) segment {
	r := &recovery{Config: cfg, repetitions: make(map[string]int)}
	r.Transform.Transform = r.transform
	return r
}
