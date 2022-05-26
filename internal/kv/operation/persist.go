package operation

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kv"
)

type persistSegment struct {
	kv kv.KV
	confluence.CoreSink[Operation]
}

func newPersistSegment(kv kv.KV) confluence.Segment[Operation] {
	ps := &persistSegment{kv: kv}
	ps.CoreSink.Sink = ps.persist
	return ps
}

func (ps *persistSegment) persist(op Operation) error {
	if op.Error != nil {
		return op.Error
	}
	var err error
	if op.Variant == Set {
		err = ps.kv.Set(op.Key, op.Value)
	} else {
		err = ps.kv.Delete(op.Key)
	}
	return err
}
