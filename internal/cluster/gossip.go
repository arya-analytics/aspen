package cluster

import (
	"context"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/rand"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"time"
)

type Gossip struct {
	Config
	state *State
}

func NewGossip(state *State, cfg Config) *Gossip {
	g := &Gossip{Config: cfg, state: state}
	g.bindTransport()
	return g
}

func (g *Gossip) Gossip(ctx context.Context) <-chan error {
	t, errC := time.NewTicker(g.GossipInterval), make(chan error)
	g.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-t.C:
				if err := g.GossipOnce(ctx); err != nil {
					errC <- err
				}
			case <-sig:
				return nil
			}
		}
	})
	return errC
}

func (g *Gossip) GossipOnce(ctx context.Context) error {
	var (
		snap = g.state.snapshot()
		addr = rand.MapValue(snap.
			WhereState(node.StateHealthy).
			Where(func(id node.ID, _ node.Node) bool { return id != g.state.HostID }),
		).Address
		sync = Message{Digests: snap.Digests()}
	)
	g.Logger.Debug("initiating gossip",
		zap.Uint32("host", uint32(g.state.HostID)),
		zap.String("address", string(addr)),
	)
	g.state.host().Heartbeat.Increment()
	ack, err := g.send(ctx, addr, sync)
	if err != nil {
		return err
	}
	ack2, err := g.processAck(ack)
	if err != nil {
		return err
	}
	_, err = g.send(ctx, addr, ack2)
	return err
}

func (g *Gossip) send(ctx context.Context, addr address.Address, msg Message) (Message, error) {
	return g.GossipTransport.Send(ctx, addr, msg)
}

func (g *Gossip) bindTransport() { g.GossipTransport.Handle(g.processGossip) }

func (g *Gossip) processGossip(ctx context.Context, msg Message) (Message, error) {
	if msg.Nodes == nil {
		return g.processSync(msg)
	} else if msg.Digests == nil {
		return Message{}, g.processAck2(msg)
	}
	return g.processAck(msg)
}

func (g *Gossip) processSync(sync Message) (ack Message, err error) {
	digests, nodes := g.state.digest(sync.Digests)
	return Message{Digests: digests, Nodes: nodes}, nil
}

func (g *Gossip) processAck(ack Message) (ack2 Message, err error) {
	g.state.merge(ack.Nodes)
	_, resNodes := g.state.digest(ack.Digests)
	return Message{Nodes: resNodes}, nil
}

func (g *Gossip) processAck2(ack2 Message) (err error) {
	g.state.merge(ack2.Nodes)
	return nil
}

type Message struct {
	Digests node.Digests
	Nodes   node.Group
}
