package kv

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/store"
)

type emitter struct {
	Config
	store.Observable[operationMap]
	confluence.Emitter[batch]
}

func newEmitter(cfg Config) segment {
	s := &emitter{
		Observable: store.ObservableWrap[operationMap](store.New(func(m operationMap) operationMap { return m.Copy() })),
		Config:     cfg,
	}
	s.Emitter.Emit = s.Emit
	s.Emitter.Store = s.Store
	s.Emitter.Interval = cfg.GossipInterval
	return s
}

func (u *emitter) Store(_ confluence.Context, batch batch) {
	snap := u.Observable.GetState()
	snap.Merge(batch.Operations)
	u.Observable.SetState(snap)
}

func (u *emitter) Emit(_ confluence.Context) batch {
	return batch{Operations: u.Observable.GetState().Operations()}
}
