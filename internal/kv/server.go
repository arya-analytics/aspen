package kv

import (
	"context"
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

func (g *operationSender) transform(ctx confluence.Context, b batch) (batch, bool) {
	snap := g.Cluster.GetState()
	peer := gossip.RandomPeer(snap)
	if peer.Address == "" {
		g.Logger.Warn("no healthy nodes to gossip with")
		return b, false
	}
	g.Logger.Debug("operationSender",
		zap.Uint32("initiator", uint32(snap.HostID)),
		zap.Uint32("peer", uint32(peer.ID)),
		zap.Int("numOps", len(b.operations)),
	)
	sync := OperationsMessage{Operations: b.operations, Sender: snap.HostID}
	ack, err := g.OperationsTransport.Send(ctx.Ctx, peer.Address, sync)
	if err != nil {
		ctx.ErrC <- err
	}
	g.Logger.Debug("operationSender ack", zap.Int("numOps", len(ack.Operations)), zap.Error(err))
	return batch{operations: ack.Operations, sender: ack.Sender}, true
}

type operationReceiver struct {
	Config
	store.Store[operationMap]
	confluence.CoreSource[batch]
}

func newOperationReceiver(cfg Config, store store.Store[operationMap]) segment {
	return &operationReceiver{Config: cfg, Store: store}
}

func (g *operationReceiver) Flow(ctx confluence.Context) { g.OperationsTransport.Handle(g.handle) }

func (g *operationReceiver) handle(ctx context.Context, message OperationsMessage) (OperationsMessage, error) {
	b := batch{operations: message.Operations, sender: message.Sender}
	hostID := g.Cluster.Host().ID
	g.Logger.Info("operationReceiver",
		zap.Uint32("receivedFrom", uint32(message.Sender)),
		zap.Uint32("host", uint32(hostID)),
	)
	for _, inlet := range g.Out {
		inlet.Inlet() <- b
	}
	return OperationsMessage{Operations: g.Store.GetState().Operations(), Sender: hostID}, nil
}

type feedbackSender struct {
	Config
	confluence.CoreSink[batch]
}

func newFeedbackSender(cfg Config) segment {
	fs := &feedbackSender{Config: cfg}
	fs.Sink = fs.sink
	return fs
}

func (f *feedbackSender) sink(ctx confluence.Context, b batch) {
	msg := FeedbackMessage{}
	for _, op := range b.operations {
		msg.Feedback = append(msg.Feedback, Feedback{Key: op.Key, Version: op.Version})
	}
	sender, _ := f.Cluster.Node(b.sender)
	f.Logger.Debug("feedbackSender",
		zap.Uint32("host", uint32(f.Cluster.Host().ID)),
		zap.Uint32("sendingTo", uint32(b.sender)),
		zap.Int("numFeedback", len(msg.Feedback)),
	)
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
	b := batch{sender: message.Sender}
	f.Logger.Debug("feedbackReceiver",
		zap.Uint32("host", uint32(f.Cluster.Host().ID)),
		zap.Uint32("receivedFrom", uint32(message.Sender)),
		zap.Int("numFeedback", len(message.Feedback)),
	)
	for _, feedback := range message.Feedback {
		b.operations = append(b.operations, Operation{Key: feedback.Key, Version: feedback.Version})
	}
	for _, inlet := range f.Out {
		inlet.Inlet() <- b
	}
	return types.Nil{}, nil
}
