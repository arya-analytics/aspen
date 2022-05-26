package operation

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kv"
)

type persistSegment struct {
	kv kv.KV
	confluence.Transform[Operation]
}

func newPersistSegment(kv kv.KV) confluence.Segment[Operation] {
	ps := &persistSegment{kv: kv}
	ps.Transform.Transform = ps.persist
	return ps
}

func (ps *persistSegment) persist(op Operation) Operation {
	var err error
	if op.Variant == Set {
		err = ps.kv.Set(op.Key, op.Value)
	} else {
		err = ps.kv.Delete(op.Key)
	}
	if err != nil {
		panic(err)
	}
	key, err := Key(op.Key)
	if err != nil {
		panic(err)
	}
	if err := kv.Flush(ps.kv, key, op); err != nil {
		panic(err)
	}
	return op
}
