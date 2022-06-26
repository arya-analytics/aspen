package kv

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/store"
)

type emitter struct {
	Config
	store.Observable[operationMap]
	confluence.Emitter[batch]
}

func newEmitter(cfg Config) *emitter {
	s := &emitter{
		Observable: store.ObservableWrap[operationMap](store.New(func(m operationMap) operationMap {
			return m.Copy()
		})),
		Config: cfg,
	}
	s.Emitter.Emit = s.Emit
	s.Emitter.Store = s.Store
	s.Emitter.Interval = cfg.GossipInterval
	return s
}

func (u *emitter) Store(_ signal.Context, batch batch) error {
	snap := u.Observable.CopyState()
	snap.Merge(batch.operations)
	u.Observable.SetState(snap)
	return nil
}

func (u *emitter) Emit(_ signal.Context) (batch, error) {
	b := batch{operations: u.Observable.ReadState().Operations().whereState(infected)}
	return b, nil
}
