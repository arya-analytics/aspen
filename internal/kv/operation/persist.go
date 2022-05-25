package operation

import (
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/pipe"
	"github.com/arya-analytics/x/shutdown"
)

type persistSegment struct {
	kv kv.KV
	sd shutdown.Shutdown
}

func (ps *persistSegment) Feed(in <-chan Operation) <-chan Operation {
	return pipe.Transform(in, ps.sd, ps.persist)

}

func (ps *persistSegment) persist(op Operation) Operation {
	if op.Error != nil {
		return op
	}
	var err error
	if op.Variant == Set {
		err = ps.kv.Set(op.Key, op.Value)
	} else {
		err = ps.kv.Delete(op.Key)
	}
	if err != nil {
		op.Error = err
	}
	return op
}
