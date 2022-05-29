package kv

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/node"
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

type OperationsMessage struct {
	Sender     node.ID
	Operations Operations
}

type FeedbackMessage struct {
	Sender   node.ID
	Feedback []Feedback
}

type operationSender struct {
	Config
	confluence.Transform[batch]
}

func newOperationSender(cfg Config) segment {
	os := &operationSender{Config: cfg}
	os.Transform.Transform = os.transform
	return os
}

func (g *operationSender) transform(ctx confluence.Context, batch batch) (batch, bool) {
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
	return batch{Operations: ack.Operations, Sender: ack.Sender}, true
}

type operationReceiver struct {
	Config
	Cluster cluster.Cluster
	store.Store[operationMap]
	confluence.CoreSource[batch]
}

func newOperationReceiver(cfg Config) segment { return &operationReceiver{Config: cfg} }

func (g *operationReceiver) Flow(ctx confluence.Context) { g.OperationsTransport.Handle(g.handle) }

func (g *operationReceiver) handle(ctx context.Context, message OperationsMessage) (OperationsMessage, error) {
	batch := batch{Operations: message.Operations, Sender: message.Sender}
	for _, inlet := range g.Out {
		inlet.Inlet() <- batch
	}
	return OperationsMessage{Operations: g.Store.GetState().Operations(), Sender: g.Cluster.Host().ID}, nil
}

type feedbackSender struct {
	Config
	Cluster cluster.Cluster
	confluence.CoreSink[batch]
}

func newFeedbackSender(cfg Config) segment {
	fs := &feedbackSender{Config: cfg}
	fs.Sink = fs.sink
	return fs
}

func (f *feedbackSender) sink(ctx confluence.Context, batch batch) {
	msg := FeedbackMessage{}
	for _, op := range batch.Operations {
		msg.Feedback = append(msg.Feedback, Feedback{Key: op.Key, Version: op.Version})
	}
	sender, _ := f.Cluster.Node(batch.Sender)
	if _, err := f.FeedbackTransport.Send(ctx.Ctx, sender.Address, msg); err != nil {
		ctx.ErrC <- err
	}
}

type feedbackReceiver struct {
	Config
	confluence.CoreSource[batch]
}

func newFeedbackReceiver(cfg Config) segment { return &feedbackReceiver{Config: cfg} }

func (f *feedbackReceiver) Flow(ctx confluence.Context) { f.FeedbackTransport.Handle(f.handle) }

func (f *feedbackReceiver) handle(ctx context.Context, message FeedbackMessage) (types.Nil, error) {
	op := batch{Sender: message.Sender}
	for _, feedback := range message.Feedback {
		op.Operations = append(op.Operations, Operation{Key: feedback.Key, Version: feedback.Version})
	}
	for _, inlet := range f.Out {
		inlet.Inlet() <- op
	}
	return types.Nil{}, nil
}
