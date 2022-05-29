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

type operationReceiver struct {
	Config
	Cluster cluster.Cluster
	store.Store[Map]
	confluence.CoreSource[Batch]
}

func newOperationReceiver(cfg Config) Segment { return &operationReceiver{Config: cfg} }

func (g *operationReceiver) Flow(ctx confluence.Context) { g.OperationsTransport.Handle(g.handle) }

func (g *operationReceiver) handle(ctx context.Context, message OperationsMessage) (OperationsMessage, error) {
	batch := Batch{Operations: message.Operations, Sender: message.Sender}
	for _, inlet := range g.Out {
		inlet.Inlet() <- batch
	}
	return OperationsMessage{Operations: g.Store.GetState().Operations(), Sender: g.Cluster.Host().ID}, nil
}

type feedbackSender struct {
	Config
	Cluster cluster.Cluster
	confluence.CoreSink[Batch]
}

func newFeedbackSender(cfg Config) Segment {
	fs := &feedbackSender{Config: cfg}
	fs.Sink = fs.sink
	return fs
}

func (f *feedbackSender) sink(ctx confluence.Context, batch Batch) {
	msg := FeedbackMessage{}
	for _, op := range batch.Operations {
		msg.Feedback = append(msg.Feedback, Feedback{Key: op.Key, Version: op.Version})
	}
	sender, _ := f.Cluster.Member(batch.Sender)
	if _, err := f.FeedbackTransport.Send(ctx.Ctx, sender.Address, msg); err != nil {
		ctx.ErrC <- err
	}
}

type feedbackRecevier struct {
	Config
	confluence.CoreSource[Batch]
}

func newFeedbackReceiver(cfg Config) Segment { return &feedbackRecevier{Config: cfg} }

func (f *feedbackRecevier) Flow(ctx confluence.Context) { f.FeedbackTransport.Handle(f.handle) }

func (f *feedbackRecevier) handle(ctx context.Context, message FeedbackMessage) (types.Nil, error) {
	op := Batch{Sender: message.Sender}
	for _, feedback := range message.Feedback {
		op.Operations = append(op.Operations, Operation{Key: feedback.Key, Version: feedback.Version})
	}
	for _, inlet := range f.Out {
		inlet.Inlet() <- op
	}
	return types.Nil{}, nil
}
