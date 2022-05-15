package node

import (
	"errors"
	"github.com/arya-analytics/aspen/internal/address"
	log "github.com/sirupsen/logrus"
)

type Router struct {
	Transports map[address.Address]*SyncTransport
}

type SyncTransport struct {
	router  *Router
	handler func(m message) error
}

func (t *SyncTransport) Send(m message) error {
	st, ok := t.router.Transports[m.to]
	if !ok {
		log.Fatal(m.to)
		return errors.New("no route")
	}
	return st.handler(m)
}

func (t *SyncTransport) Handle(h func(m message) error) {
	t.handler = h
}

func (r *Router) New(addr address.Address) Transport {
	t := &SyncTransport{router: r}
	r.Transports[addr] = t
	return t
}
