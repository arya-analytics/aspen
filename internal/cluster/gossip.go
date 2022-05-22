package cluster

import (
	"context"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/rand"
	"github.com/arya-analytics/x/shutdown"
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
		addr = rand.MapValue(snap.WhereState(node.StateHealthy)).Address
		sync = Message{Digests: snap.Digests()}
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
	digests, nodes := processDigests(g.state, sync.Digests)
	return Message{Digests: digests, Nodes: nodes}, nil
}

func (g *Gossip) processAck(ack Message) (ack2 Message, err error) {
	processNodes(g.state, ack.Nodes)
	_, resNodes := processDigests(g.state, ack.Digests)
	return Message{Nodes: resNodes}, nil
}

func (g *Gossip) processAck2(ack2 Message) (err error) {
	processNodes(g.state, ack2.Nodes)
	return nil
}

type Message struct {
	Digests node.Digests
	Nodes   node.Group
}

func processNodes(state *State, other node.Group) (res node.Group) {
	for otherID, otherNode := range other {
		internalNode, ok := state.Nodes[otherID]
		// If:
		// 	1. otherNode's version is more recent than internal.
		// 	2. We don't have other.
		// Then:
		// 	Replace internal with otherNode.
		if !ok || otherNode.Heartbeat.OlderThan(*internalNode.Heartbeat) {
			state.setNode(otherNode)
			// If:
			//  We have a new version that other,
			// Then:
			//	send our version back to the client.
		} else if otherNode.Heartbeat.YoungerThan(*internalNode.Heartbeat) {
			res[otherID] = otherNode
		}
	}
	return res
}

func processDigests(state *State, digests node.Digests) (resDigests node.Digests, resNodes node.Group) {
	resNodes = make(node.Group)
	resDigests = make(node.Digests)
	for _, dig := range digests {
		internalNode, ok := state.Nodes[dig.ID]
		// If:
		// 	1. We have the internalNode and our version is more recent.
		// Then:
		// 	send it back to the client.
		if ok && internalNode.Heartbeat.OlderThan(*dig.Heartbeat) {
			resNodes[dig.ID] = internalNode
		}
		// If we don't have the internalNode, add it to the list of digests.
		if !ok {
			resDigests[dig.ID] = dig
		}
	}

	// If:
	// 1. we have a internalNode that the other internalNode doesn't know about.
	// Then:
	// 	send it back to the client.
	for id, internalNode := range state.Nodes {
		if _, ok := digests[id]; !ok {
			resNodes[id] = internalNode
		}
	}

	return resDigests, resNodes
}
