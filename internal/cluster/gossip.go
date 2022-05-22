package cluster

import (
	"context"
	. "github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/rand"
	"github.com/arya-analytics/x/shutdown"
	"time"
)

type Gossip struct {
	Config
	state *state
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
		addr = rand.MapValue(snap).Address
		sync = Message{Digests: snap.Digests()}
	)
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
	Digests Digests
	Nodes   Group
}

func processNodes(state *state, other Group) (res Group) {
	for otherID, otherNode := range other {
		internalNode, ok := state.nodes[otherID]
		// If:
		// 	1. otherNode's version is more recent than internal.
		// 	2. We don't have other.
		// Then:
		// 	Replace internal with otherNode.
		if !ok || otherNode.Heartbeat.OlderThan(internalNode.Heartbeat) {
			state.setNode(otherNode)
			// If:
			//  We have a new version that other,
			// Then:
			//	send our version back to the client.
		}
		if otherNode.Heartbeat.YoungerThan(internalNode.Heartbeat) {
			res[otherID] = otherNode
		}
	}
	return res
}

func processDigests(state *state, digests Digests) (resDigests Digests, resNodes Group) {
	resNodes = make(Group)
	for _, dig := range digests {
		node, ok := state.nodes[dig.ID]
		// If:
		// 	1. We have the node and our version is more recent.
		// Then:
		// 	send it back to the client.
		if ok && node.Heartbeat.OlderThan(dig.Heartbeat) {
			resNodes[dig.ID] = node
		}
		// If we don't have the node, add it to the list of digests.
		if !ok {
			resDigests[dig.ID] = dig
		}
	}

	// If:
	// 1. we have a node that the other node doesn't know about.
	// Then:
	// 	send it back to the client.
	for id, node := range state.nodes {
		if _, ok := digests[id]; !ok {
			resNodes[id] = node
		}
	}

	return resDigests, resNodes
}
