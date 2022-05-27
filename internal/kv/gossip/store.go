package gossip

import (
	"github.com/arya-analytics/aspen/internal/kv/operation"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/filter"
	"github.com/arya-analytics/x/store"
)

type sinkStore struct {
	confluence.CoreSink[operation.Operation]
	store.Observable[Operations]
}

func _copy(ops Operations) Operations { return ops.Copy() }

func newStore() store.Store[Operations] {
	u := &sinkStore{Observable: store.ObservableWrap[Operations](store.New(_copy))}
	u.CoreSink.Sink = u.sink
	return u
}

func (u *sinkStore) sink(op operation.Operation) error {
	u.Observable.SetState(append(u.Observable.GetState(), Operation{Operation: op, State: StateInfected}))
	return nil
}

func (u *sinkStore) operations() Operations { return u.Observable.GetState() }

type Operations []Operation

func (ops Operations) WhereInfected() Operations { return ops.WhereState(StateInfected) }

func (ops Operations) WhereRecovered() Operations { return ops.WhereState(StateRecovered) }

func (ops Operations) WhereState(state State) Operations {
	return ops.Where(func(op Operation) bool { return op.State == state })
}

func (ops Operations) Where(cond func(Operation) bool) Operations { return filter.Slice(ops, cond) }

func (ops Operations) Copy() (oOps Operations) {
	for _, op := range ops {
		oOps = append(oOps, op)
	}
	return oOps
}
