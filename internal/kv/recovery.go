package kv

import (
	"github.com/arya-analytics/x/confluence"
	kvx "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/signal"
)

type recoveryTransform struct {
	Config
	confluence.LinearTransform[BatchRequest, BatchRequest]
	repetitions map[string]int
}

func newRecoveryTransform(cfg Config) segment {
	r := &recoveryTransform{Config: cfg, repetitions: make(map[string]int)}
	r.LinearTransform.ApplyTransform = r.transform
	return r
}

func (r *recoveryTransform) transform(
	ctx signal.Context,
	in BatchRequest,
) (out BatchRequest, ok bool, err error) {
	for _, op := range in.Operations {
		key, err := kvx.CompositeKey(op.Key, op.Version)
		if err != nil {
			ctx.Transient() <- err
			return out, false, nil
		}
		strKey := string(key)
		if r.repetitions[strKey] > r.RecoveryThreshold {
			r.Logger.Debugw("recovering op", "host", r.Cluster.HostID(), "key", strKey)
			op.state = recovered
			out.Operations = append(out.Operations, op)
			delete(r.repetitions, strKey)
		}
		r.repetitions[strKey]++
	}
	return out, true, nil
}
