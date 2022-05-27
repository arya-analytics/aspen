package gossip

import (
	"bytes"
	"context"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/shutdown"
	"github.com/arya-analytics/x/store"
	"go.uber.org/zap"
)

type Gossip struct {
	Config
	cluster cluster.Cluster
	store   store.Reader[Operations]
	confluence.Writer[Operation]
	provider *feedBackProvider
}

func (g *Gossip) Gossip(ctx context.Context) <-chan error {
	errC := make(chan error)
	g.Shutdown.GoTick(
		g.Interval,
		func() error { return g.GossipOnce(ctx) },
		shutdown.WithContext(ctx),
		shutdown.WithErrPipe(errC),
	)
	return errC
}

func (g *Gossip) GossipOnce(ctx context.Context) error {
	snap := g.cluster.GetState()
	peer := gossip.RandomPeer(snap)
	if peer.Address == "" {
		g.Logger.Warn("no healthy nodes to gossip with")
	}
	g.Logger.Debug("gossip",
		zap.Uint32("initiator", uint32(snap.HostID)),
		zap.Uint32("peer", uint32(peer.ID)),
	)
	return g.GossipOnceWith(ctx, peer.Address)

}

func (g *Gossip) GossipOnceWith(ctx context.Context, addr address.Address) error {
	sync := Message{Operations: g.store.GetState().WhereState(StateInfected)}
	ack, err := g.Transport.Send(ctx, addr, sync)
	if err != nil {
		return err
	}
	_, err = g.Transport.Send(ctx, addr, g.ack(ack))
	return err
}

func (g *Gossip) process(ctx context.Context, msg Message) (Message, error) {
	if ctx.Err() != nil {
		return Message{}, ctx.Err()
	}
	switch msg.variant() {
	case messageVariantSync:
		return g.sync(msg), nil
	case messageVariantAck2:
		g.ack2(msg)
		return Message{}, nil
	}
	panic("invalid message type")
}

func (g *Gossip) sync(sync Message) (ack Message) {
	hostOperations := g.store.GetState()
	for _, op := range sync.Operations {
		fb, op, ok := g.provider.provide(op)
		g.Values <- op
		if ok {
			ack.Feedback = append(ack.Feedback, fb)
		}
	}
	ack.Operations = hostOperations.WhereState(StateInfected)
	return ack
}

func (g *Gossip) ack(ack Message) (ack2 Message) {
	hostOperations := g.store.GetState()
	for _, op := range ack.Operations {
		g.Writer.Values <- op
		ack.Feedback = provideFeedback(hostOperations, op)
	}
	return ack2
}

func (g *Gossip) ack2(ack2 Message) {
}

func provideFeedback(hostState Operations, remoteOp Operation) (feedback []Feedback) {
	for _, iOp := range hostState {
		if bytes.Equal(iOp.Key, remoteOp.Key) && !remoteOp.Version.OlderThan(iOp.Version) {
			feedback = append(feedback, Feedback{Key: remoteOp.Key, Version: remoteOp.Version})
		}
	}
	return feedback
}
