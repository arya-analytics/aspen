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

func (r *recovery) transform(batch batch) (oBatch batch) {
	for _, op := range batch.Operations {
		key, err := kv_.CompositeKey(op.Key, op.Version)
		if err != nil {
			panic(err)
		}
		strKey := string(key)
		if r.repetitions[strKey] > r.RecoveryThreshold {
			op.State = Recovered
			oBatch.Operations = append(oBatch.Operations, op)
			delete(r.repetitions, strKey)
		}
		r.repetitions[strKey]++
	}
	return oBatch
}

func newRecoveryTransform(cfg Config) segment {
	r := &recovery{Config: cfg, repetitions: make(map[string]int)}
	r.Transform.Transform = r.transform
}
