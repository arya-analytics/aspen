package kv

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/store"
)

type emitter struct {
	Config
	store.Observable[Map]
	confluence.Emitter[Batch]
}

func newEmitter(cfg Config) Segment {
	s := &emitter{
		Observable: store.ObservableWrap[Map](store.New(func(m Map) Map { return m.Copy() })),
		Config:     cfg,
	}
	s.Emitter.Emit = s.Emit
	s.Emitter.Store = s.Store
	s.Emitter.Interval = cfg.GossipInterval
	return s
}

func (u *emitter) Store(batch Batch) {
	snap := u.Observable.GetState()
	snap.Merge(batch.Operations)
	u.Observable.SetState(snap)
}

func (u *emitter) Emit() Batch { return Batch{Operations: u.Observable.GetState().Operations()} }
