package node

// |||||| TRANSPORT ||||||

type Transport interface {
	TransportClient
	TransportServer
}

type TransportClient interface {
	SendSync(msg SyncMessage) error
	SendAck(msg AckMessage) error
	SendAck2(msg Ack2Message) error
}

type TransportServer interface {
	HandleSync(func(SyncMessage) error)
	HandleAck(func(AckMessage) error)
	HandleAck2(func(Ack2Message) error)
}

// |||||| MESSAGES ||||||

type SyncMessage struct {
	Digests Digests
}

type AckMessage struct {
	Nodes   Nodes
	Digests Digests
}

type Ack2Message struct {
	Nodes Nodes
}

// |||||| GOSSIP ||||||

type Gossip struct {
	HostID    ID
	Transport Transport
	Nodes     Nodes
}

func (g *Gossip) Digests() Digests {
	digests := make(Digests, len(g.Nodes))
	for id, node := range g.Nodes {
		digests[id] = node.Digest()
	}
	return digests
}

func (g *Gossip) Host() Node {
	return g.Nodes[g.HostID]
}

func (g *Gossip) Gossip() error {
	g.Host().heartbeat.Increment()
	return g.Transport.SendSync(SyncMessage{Digests: g.Digests()})
}

func (g *Gossip) processSync(msg SyncMessage) error {
	nodes, digests := Merger{Nodes: g.Nodes}.Filter(msg.Digests)
	return g.Transport.SendAck(AckMessage{Nodes: nodes, Digests: digests})
}

func (g *Gossip) processAck(msg AckMessage) error {
	nodes := Merger{Nodes: g.Nodes}.Merge(msg.Nodes)
	return g.Transport.SendAck2(Ack2Message{Nodes: nodes})
}

func (g *Gossip) processAck2(msg Ack2Message) error {
	Merger{Nodes: g.Nodes}.Merge(msg.Nodes)
	return nil
}

func (g *Gossip) bindHandlers() {
	g.Transport.HandleSync(g.processSync)
	g.Transport.HandleAck(g.processAck)
	g.Transport.HandleAck2(g.processAck2)
}

func NewGossip(host Node, transport Transport) *Gossip {
	g := &Gossip{
		HostID:    host.ID,
		Transport: transport,
		Nodes:     Nodes{host.ID: host},
	}
	g.bindHandlers()
	return g
}
