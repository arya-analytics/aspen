package node

import (
	"github.com/arya-analytics/aspen/internal/address"
	"github.com/arya-analytics/aspen/internal/version"
	"math/rand"
	"sync"
	"time"
)

type ID uint16

type Node struct {
	ID      ID
	Address address.Address
	Version version.Byte
	digest
}

type Nodes map[ID]Node

type digest struct {
	heartbeat Heartbeat
}

type digests map[ID]digest

type message struct {
	from    address.Address
	to      address.Address
	nodes   Nodes
	digests digests
}

func (m message) ack() bool {
	return !m.ack2() && !m.sync()
}

func (m message) ack2() bool {
	return len(m.digests) == 0
}

func (m message) sync() bool {
	return len(m.nodes) == 0
}

type Transport interface {
	Send(m message) error
	Handle(func(m message) error)
}

type Gossip struct {
	mu            sync.Mutex
	NodeID        ID
	PeerAddresses []address.Address
	Nodes         Nodes
	Transport     Transport
}

func (g *Gossip) randomAddr() address.Address {
	peers := g.peers()
	if len(peers) < len(g.PeerAddresses) {
		if len(g.PeerAddresses) == 0 {
			return ""
		}
		// return a random value from our peer address list
		return g.PeerAddresses[rand.Intn(len(g.PeerAddresses))]
	}
	if len(peers) == 0 {
		return ""
	}
	v := rand.Intn(len(peers))
	i := 0
	for _, n := range peers {
		i++
		if i == v {
			return n.Address
		}
	}
	return ""
}

func (g *Gossip) peers() Nodes {
	nodes := make(Nodes)
	for k, v := range g.Nodes {
		if v.ID != g.NodeID {
			nodes[k] = v
		}
	}
	return nodes
}

func (g *Gossip) Gossip() chan error {
	g.Transport.Handle(g.process)
	errChan := make(chan error)
	t := time.NewTicker(10 * time.Millisecond)
	go func() {
		for range t.C {
			if err := g.Emit(); err != nil {
				errChan <- err
			}
		}
	}()
	return errChan
}

func (g *Gossip) Emit() error {
	addr := g.randomAddr()
	if addr != "" {
		//log.Infof("Node %v gossiping with %v", g.NodeID, addr)
		m := message{
			to:      addr,
			from:    g.Nodes[g.NodeID].Address,
			digests: g.digests(),
		}
		return g.Transport.Send(m)
	}
	return nil
}

func (g *Gossip) process(m message) error {
	//g.mu.Lock()
	//defer g.mu.Unlock()
	//log.Infof("Node %v processing message from %s", g.NodeID, m.from)
	if m.sync() {
		return g.processSync(m)
	} else if m.ack() {
		return g.processAck(m)
	} else if m.ack2() {
		return g.processAck2(m)
	}
	panic("unreachable")
}

func (g *Gossip) processSync(m message) error {

	//log.Info(m.digests, m.nodes)

	ack := message{
		nodes:   make(Nodes),
		digests: make(digests),
		to:      m.from,
		from:    m.to,
	}

	// make the node map satisfy the message digests
	for k, dig := range m.digests {
		in, ok := g.Nodes[k]
		// If we don'Transports have the node or our node heartbeat is younger than the digest
		// heartbeat, add it to our digest list.
		if !ok || in.heartbeat.YoungerThan(dig.heartbeat) {
			ack.digests[k] = dig
			// Otherwise, add our node to the node list
		} else if !in.heartbeat.EqualTo(dig.heartbeat) {
			ack.nodes[k] = in
		}
	}

	// if the digest doesn'Transports have a node that we do, add it to the node list
	for k, in := range g.Nodes {
		if _, ok := m.digests[k]; !ok {
			ack.nodes[k] = in
		}
	}

	//log.Info(ack.nodes, ack.digests)

	return g.Transport.Send(ack)
}

func (g *Gossip) processAck(m message) error {

	//log.Warn(m.digests, m.nodes)

	ack2 := message{
		nodes: make(Nodes),
		from:  m.to,
		to:    m.from,
	}

	// ack message contains:
	// 1. A list of Nodes satisfying the digest list we sent
	// 2. A digest list of Nodes the peer node is requesting

	for k, rm := range m.nodes {
		in, ok := g.Nodes[k]
		// If we don't have the node, or our node heartbeat is younger than the remote node, add it to our node list
		if !ok || in.heartbeat.YoungerThan(rm.heartbeat) {
			g.Nodes[k] = rm
		}
	}

	for k, dig := range m.digests {
		in, ok := g.Nodes[k]
		// If we have the node and our heartbeat is OLDER than the remote node, add it to our sender list
		if ok && (dig.heartbeat.YoungerThan(in.heartbeat) || dig.heartbeat.EqualTo(in.heartbeat)) {
			ack2.nodes[k] = in
		}
	}

	for k, in := range g.Nodes {
		if _, ok := m.digests[k]; !ok {
			ack2.nodes[k] = in
		}
	}

	return g.Transport.Send(ack2)
}

func (g *Gossip) processAck2(m message) error {
	// ack2 message contains:
	// 1. A list od Nodes satisfying the digest list we sent

	//log.Error(m.nodes)

	for k, rn := range m.nodes {
		in, ok := g.Nodes[k]
		if !ok || in.heartbeat.YoungerThan(rn.heartbeat) {
			g.Nodes[k] = rn
		}
	}

	return nil
}

func (g *Gossip) digests() digests {
	d := make(digests)
	for _, n := range g.Nodes {
		d[n.ID] = digest{heartbeat: n.heartbeat}
	}
	return d
}
