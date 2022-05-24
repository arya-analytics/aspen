package gossip

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster/store"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/rand"
	"github.com/arya-analytics/x/shutdown"
	"github.com/arya-analytics/x/transport"
	"go.uber.org/zap"
	"time"
)

type Config struct {
	Interval  time.Duration
	Transport transport.Unary[Message, Message]
	Shutdown  shutdown.Shutdown
	Logger    *zap.Logger
}

type Gossip struct {
	Config
	store store.Store
}

func New(store store.Store, cfg Config) *Gossip {
	g := &Gossip{Config: cfg, store: store}
	g.Transport.Handle(g.processGossip)
	return g
}

func (g *Gossip) Gossip(ctx context.Context) <-chan error {
	t, errC := time.NewTicker(g.Interval), make(chan error)
	g.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case <-t.C:
				if err := g.GossipOnce(ctx); err != nil {
					errC <- err
				}
			}
		}
	})
	return errC
}

func (g *Gossip) GossipOnce(ctx context.Context) error {
	g.incrementHostHeartbeat()
	snap := g.store.GetState()
	peer := randomPeer(snap)
	if peer.Address == "" {
		g.Logger.Warn("no healthy nodes to gossip with")
		return nil
	}
	g.Logger.Debug("gossip",
		zap.Uint32("initiator", uint32(snap.HostID)),
		zap.Uint32("peer", uint32(peer.ID)),
		zap.Int("stateSize", len(snap.Nodes)),
	)
	return g.GossipOnceWith(ctx, peer.Address)

}

func (g *Gossip) GossipOnceWith(ctx context.Context, addr address.Address) error {
	sync := Message{Digests: g.store.GetState().Nodes.Digests()}
	ack, err := g.Transport.Send(ctx, addr, sync)
	if err != nil {
		return err
	}
	_, err = g.Transport.Send(ctx, addr, g.ack(ack))
	return err
}

func (g *Gossip) incrementHostHeartbeat() {
	host := g.store.GetHost()
	host.Heartbeat = host.Heartbeat.Increment()
	g.store.Set(host)
	g.Logger.Debug("host heartbeat",
		zap.Uint32("version", host.Heartbeat.Version),
		zap.Uint32("generation", host.Heartbeat.Generation),
	)
}

func (g *Gossip) processGossip(ctx context.Context, msg Message) (Message, error) {
	if ctx.Err() != nil {
		return Message{}, ctx.Err()
	}

	switch msg.variant() {
	case messageVariantSync:
		return g.sync(msg), nil
	case messageVariantAck2:
		g.ack2(msg)
		return Message{}, nil
	default:
		g.Logger.Panic("gossip received invalid message type")
		return Message{}, nil
	}
}

func (g *Gossip) sync(sync Message) (ack Message) {
	snap := g.store.GetState()
	ack = Message{Nodes: make(node.Group), Digests: make(node.Digests)}
	for _, dig := range sync.Digests {
		n, ok := snap.Nodes[dig.ID]

		// If we have a more recent version of the node,
		// return it to the initiator.
		if ok && n.Heartbeat.OlderThan(dig.Heartbeat) {
			ack.Nodes[dig.ID] = n
		}

		// If we don't have the node or our version is out of date,
		// add it to our digests.
		if !ok || n.Heartbeat.YoungerThan(dig.Heartbeat) {
			ack.Digests[dig.ID] = node.Digest{ID: dig.ID, Heartbeat: n.Heartbeat}
		}
	}

	for _, n := range snap.Nodes {

		// If we have a node that the initiator doesn't have,
		// send it to them.
		if _, ok := sync.Digests[n.ID]; !ok {
			ack.Nodes[n.ID] = n
		}
	}

	return ack
}

func (g *Gossip) ack(ack Message) (ack2 Message) {
	// Take a snapshot before we merge the peer's nodes.
	snap := g.store.GetState()
	g.store.Merge(ack.Nodes)
	ack2 = Message{Nodes: make(node.Group)}
	for _, dig := range ack.Digests {
		// If we have the node, and our version is newer, return it to the
		// peer.
		if n, ok := snap.Nodes[dig.ID]; !ok || n.Heartbeat.OlderThan(dig.Heartbeat) {
			ack2.Nodes[dig.ID] = n
		}
	}
	return ack2
}

func (g *Gossip) ack2(ack2 Message) {
	g.store.Merge(ack2.Nodes)
}

func randomPeer(snap store.State) node.Node {
	return rand.MapValue(snap.Nodes.WhereState(node.StateHealthy).WhereNot(snap.HostID))
}
