package gossip

import (
	"github.com/arya-analytics/aspen/internal/address"
	"github.com/arya-analytics/aspen/internal/version"
)

type Message[S comparable] interface {
	Source() S
	Version() version.Version
	Address() address.Address
}

type Middleware interface {
	Exec(Message) error
}

type MiddlewareChain []Middleware

func (m MiddlewareChain) Exec(msg Message) error {
	for _, mw := range m {
		if err := mw.Exec(msg); err != nil {
			return err
		}
	}
	return nil
}

type Base[M Message] struct {
	Middleware MiddlewareChain
	Transport  Transport[M]
}

func (b *Base[M]) Gossip(m Message) error {
	return b.Transport.Send(m)
}

func (b *Base[M]) Handle(handle func(m M) error) {
	b.Transport.Handle(func(m M) error {
		if err := b.Middleware.Exec(m); err != nil {
			return err
		}
		return handle(m.(M))
	})
}
