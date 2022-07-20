package kv

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	xstore "github.com/arya-analytics/x/store"
)

type storeState map[string]Operation

func (s storeState) Copy() storeState {
	mCopy := make(storeState, len(s))
	for k, v := range s {
		mCopy[k] = v
	}
	return mCopy
}

func (s storeState) toBatchRequest() BatchRequest {
	b := BatchRequest{Operations: make([]Operation, 0, len(s))}
	for _, op := range s {
		if op.state != infected {
			continue
		}
		// Since we're not writing to any underlying storage, any error
		// should panic.
		b.Operations = append(b.Operations, op)
	}
	return b
}

type store xstore.Observable[storeState]

func newStore() store {
	return xstore.ObservableWrap[storeState](xstore.New(func(
		m storeState) storeState {
		return m.Copy()
	}))
}

type storeEmitter struct {
	confluence.Emitter[BatchRequest]
	store store
}

func newStoreEmitter(s store, cfg Config) source {
	se := &storeEmitter{store: s}
	se.Interval = cfg.GossipInterval
	se.Emitter.Emit = se.Emit
	return se
}

func (e *storeEmitter) Emit(_ signal.Context) (BatchRequest, error) {
	return e.store.ReadState().toBatchRequest(), nil
}

type storeSink struct {
	confluence.UnarySink[BatchRequest]
	store store
}

func newStoreSink(s store) sink {
	ss := &storeSink{store: s}
	ss.UnarySink.Sink = ss.Store
	return ss
}

func (s *storeSink) Store(_ signal.Context, br BatchRequest) error {
	snap := s.store.CopyState()
	for _, op := range br.Operations {
		snap[string(op.Key)] = op
	}
	s.store.SetState(snap)
	return nil
}
