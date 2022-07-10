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
	confluence.UnarySink[batch]
}

func (e *emitter) Flow(ctx signal.Context, opts ...confluence.Option) {
	e.Emitter.Flow(ctx, opts...)
	e.UnarySink.Flow(ctx, opts...)
}

func newEmitter(cfg Config) *emitter {
	s := &emitter{
		Observable: store.ObservableWrap[operationMap](store.New(func(m operationMap) operationMap {
			return m.Copy()
		})),
		Config: cfg,
	}
	s.Emitter.Emit = s.Emit
	s.UnarySink.Sink = s.Store
	s.Emitter.Interval = cfg.GossipInterval
	return s
}

func (e *emitter) Store(_ signal.Context, batch batch) error {
	snap := e.Observable.CopyState()
	snap.Merge(batch.operations)
	e.Observable.SetState(snap)
	return nil
}

func (e *emitter) Emit(_ signal.Context) (batch, error) {
	b := batch{operations: e.Observable.ReadState().Operations().whereState(infected)}
	return b, nil
}
