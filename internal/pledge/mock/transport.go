package mock

import (
	"context"
	"github.com/arya-analytics/aspen/internal/member"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"sync"
)

type Transport struct {
	Network        *Network
	RequestHandler func(ctx context.Context, req member.ProposalRequest) error
	PledgeHandler  func(ctx context.Context) (node.ID, error)
}

func (t *Transport) Request(ctx context.Context, addr address.Address, req member.ProposalRequest) error {
	t.Network.LogRequest(addr)
	return t.Network.Transports[addr].RequestHandler(ctx, req)
}

func (t *Transport) HandleRequest(handler func(ctx context.Context, req member.ProposalRequest) error) {
	t.RequestHandler = handler
}

func (t *Transport) Pledge(ctx context.Context, addr address.Address) (node.ID, error) {
	t.Network.LogRequest(addr)
	return t.Network.Transports[addr].PledgeHandler(ctx)
}

func (t *Transport) HandlePledge(handler func(ctx context.Context) (node.ID, error)) {
	t.PledgeHandler = handler
}

type Network struct {
	Transports map[address.Address]*Transport
	Requests   []address.Address
	mu         sync.Mutex
}

func (n *Network) NewTransport(addr address.Address) *Transport {
	t := &Transport{Network: n}
	n.Transports[addr] = t
	return t
}

func (n *Network) LogRequest(addr address.Address) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Requests = append(n.Requests, addr)
}
