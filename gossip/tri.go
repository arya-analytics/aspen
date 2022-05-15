package gossip

import (
	"github.com/arya-analytics/aspen/internal/address"
	"time"
)

type AddressIterator interface {
	Next() address.Address
}

type TriMessenger[S, A, A2 Message] interface {
	EmitSync() S
	Ack(S) (A, error)
	Ack2(A) (A2, error)
	Close(A2) error
}

type TriConfig[S, A, A2 Message] struct {
	AddressIter AddressIterator
	Interval    time.Duration
	Messenger   TriMessenger[S, A, A2]
	Transport   TriTransport[S, A, A2]
}

type Tri[S, A, A2 Message] struct {
	AddrMap map[address.Address]int32
	TriConfig[S, A, A2]
}

func (t *Tri) bindTransport() {
	t.Transport.HandleSync(t.processSync)
	t.Transport.HandleAck(t.processAck)
	t.Transport.HandleAck2(t.processAck2)
}

func (t *Tri) Gossip() <-chan error {
	errChan := make(chan error)
	t.bindTransport()
	tick := time.NewTicker(t.Interval)
	for range tick.C {
		if err := t.GossipOnce(); err != nil {
			errChan <- err
		}
	}
	return errChan
}

func (t *Tri[S, A, A2]) GossipOnce() error {
	addr := t.AddressIter.Next()
	if addr == "" {
		return nil
	}
	pld := t.Messenger.EmitSync()
	return t.Transport.SendSync(MessageWrapper[S]{Payload: pld, Address: addr})
}

func (t *Tri[S, A, A2]) processSync(msg MessageWrapper[S]) error {
	pld, err := t.Messenger.Ack(msg)
	if err != nil {
		return err
	}
	return t.Transport.SendAck(MessageWrapper[A]{Payload: pld, Address: msg.Address})
}

func (t *Tri[S, A, A2]) processAck(msg MessageWrapper[A]) error {
	pld, err := t.Messenger.Ack2(msg)
	if err != nil {
		return err
	}
	return t.Transport.SendAck2(MessageWrapper[A2]{Payload: pld, Address: msg.Address})
}

func (t *Tri[S, A, A2]) processAck2(msg MessageWrapper[A2]) error {
	return t.Messenger.Close(msg)
}
