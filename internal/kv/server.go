package kv

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/store"
	"github.com/arya-analytics/x/transport"
	"github.com/arya-analytics/x/version"
	"go.uber.org/zap"
	"go/types"
)

type (
	OperationsTransport = transport.Unary[OperationsMessage, OperationsMessage]
	FeedbackTransport   = transport.Unary[FeedbackMessage, types.Nil]
	LeaseTransport      = transport.Unary[LeaseMessage, types.Nil]
)

type Feedback struct {
	Key     []byte
	Version version.Counter
}

type operationSender struct {
	Config
	confluence.Transform[Batch]
}

func newOperationSender(cfg Config) Segment {
	os := &operationSender{Config: cfg}
	os.Transform = os.transform

}

func (g *operationSender) transform(ctx confluence.Context, batch Batch) Batch {
	snap := g.Cluster.GetState()
	peer := gossip.RandomPeer(snap)
	if peer.Address == "" {
		g.Logger.Warn("no healthy nodes to gossip with")
	}
	g.Logger.Debug("gossip",
		zap.Uint32("initiator", uint32(snap.HostID)),
		zap.Uint32("peer", uint32(peer.ID)),
	)
	sync := OperationsMessage{Operations: batch.Operations, Sender: snap.HostID}
	ack, err := g.OperationsTransport.Send(ctx.Ctx, peer.Address, sync)
	if err != nil {
		ctx.ErrC <- err
	}
	return Batch{Operations: ack.Operations, Sender: ack.Sender}
}

type OperationReceiver struct {
	Config
	Cluster cluster.Cluster
	store.Store[Map]
	confluence.CoreSource[Batch]
}

func newOperationReceiver(cfg Config) Segment { return &OperationReceiver{Config: cfg} }

func (g *OperationReceiver) Flow(ctx confluence.Context) { g.OperationsTransport.Handle(g.handle) }

func (g *OperationReceiver) handle(ctx context.Context, message OperationsMessage) (OperationsMessage, error) {
	batch := Batch{Operations: message.Operations, Sender: message.Sender}
	for _, inlet := range g.Out {
		inlet.Inlet() <- batch
	}
	return OperationsMessage{Operations: g.Store.GetState().Operations(), Sender: g.Cluster.Host().ID}, nil
}

type FeedbackSender struct {
	Config
	Cluster cluster.Cluster
	confluence.CoreSink[Batch]
}

func newFeedbackSender(cfg Config) Segment {
	fs := &FeedbackSender{Config: cfg}
	fs.Sink = fs.sink
	return fs
}

func (f *FeedbackSender) sink(ctx confluence.Context, batch Batch) {
	msg := FeedbackMessage{}
	for _, op := range batch.Operations {
		msg.Feedback = append(msg.Feedback, Feedback{Key: op.Key, Version: op.Version})
	}
	sender, _ := f.Cluster.Member(batch.Sender)
	if _, err := f.FeedbackTransport.Send(ctx.Ctx, sender.Address, msg); err != nil {
		ctx.ErrC <- err
	}
}

type FeedbackReceiver struct {
	Config
	confluence.CoreSource[Batch]
}

func newFeedbackReceiver(cfg Config) Segment { return &FeedbackReceiver{Config: cfg} }

func (f *FeedbackReceiver) Flow(ctx confluence.Context) { f.FeedbackTransport.Handle(f.handle) }

func (f *FeedbackReceiver) handle(ctx context.Context, message FeedbackMessage) (types.Nil, error) {
	op := Batch{Sender: message.Sender}
	for _, feedback := range message.Feedback {
		op.Operations = append(op.Operations, Operation{Key: feedback.Key, Version: feedback.Version})
	}
	for _, inlet := range f.Out {
		inlet.Inlet() <- op
	}
	return types.Nil{}, nil
}
