package kv

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/store"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"go/types"
)

// |||||| OPERATION ||||||

type OperationMessage struct {
	Sender     node.ID
	Operations Operations
}

func (o OperationMessage) toBatch() batch { return batch{sender: o.Sender, operations: o.Operations} }

type OperationsTransport = transport.Unary[OperationMessage, OperationMessage]

// |||| SENDER ||||

type operationSender struct {
	Config
	confluence.Transform[batch]
}

func newOperationSender(cfg Config) segment {
	os := &operationSender{Config: cfg}
	os.Transform.Transform = os.send
	return os
}

func (g *operationSender) send(ctx signal.Context, b batch) (batch, bool, error) {
	// If we have no operations to propagate, it's best to avoid the network chatter.
	if len(b.operations) == 0 {
		return batch{}, false, nil
	}

	hostID := g.Cluster.HostID()
	peer := gossip.RandomPeer(g.Cluster.Nodes(), hostID)
	if peer.Address == "" {
		g.Logger.Warnw("no healthy nodes to gossip with", "host", hostID)
		return batch{}, false, nil
	}

	g.Logger.Debugw("gossiping operations",
		"host", hostID,
		"peer", peer.ID,
		"count", len(b.operations),
	)

	sync := OperationMessage{Operations: b.operations, Sender: hostID}
	ack, err := g.OperationsTransport.Send(context.TODO(), peer.Address, sync)
	if err != nil {
		ctx.Transient() <- errors.Wrap(err, "[kv] - failed to gossip operations")
	}

	// If we have no operations to persist, avoid the pipeline overhead.
	if len(ack.Operations) == 0 {
		return b, false, nil
	}

	return ack.toBatch(), true, nil
}

// |||| RECEIVER ||||

type operationReceiver struct {
	Config
	store.Store[operationMap]
	confluence.UnarySource[batch]
}

func newOperationReceiver(cfg Config, store store.Store[operationMap]) segment {
	or := &operationReceiver{Config: cfg, Store: store}
	or.OperationsTransport.Handle(or.handle)
	return or
}

func (g *operationReceiver) handle(ctx context.Context, message OperationMessage) (OperationMessage, error) {
	b := message.toBatch()
	hostID := g.Cluster.HostID()
	g.Logger.Debug("received gossip", zap.Stringer("peer", message.Sender), zap.Stringer("host", hostID))
	g.Out.Inlet() <- b
	return OperationMessage{Operations: g.Store.ReadState().Operations(), Sender: hostID}, nil
}

// |||||| FEEDBACK ||||||

type FeedbackMessage struct {
	Sender  node.ID
	Digests Digests
}

func (f FeedbackMessage) toBatch() batch {
	return batch{sender: f.Sender, operations: f.Digests.Operations()}
}

type FeedbackTransport = transport.Unary[FeedbackMessage, types.Nil]

// |||| SENDER ||||

type feedbackSender struct {
	Config
	confluence.CoreSink[batch]
}

func newFeedbackSender(cfg Config) segment {
	fs := &feedbackSender{Config: cfg}
	fs.Sink = fs.send
	return fs
}

func (f *feedbackSender) send(ctx signal.Context, b batch) error {
	msg := FeedbackMessage{Sender: f.Cluster.Host().ID, Digests: b.operations.digests()}
	sender, _ := f.Cluster.Node(b.sender)
	f.Logger.Debugw("gossiping feedback",
		"host", f.Cluster.HostID(),
		"peer", b.sender,
		"count", len(msg.Digests),
	)
	if _, err := f.FeedbackTransport.Send(context.TODO(), sender.Address, msg); err != nil {
		ctx.Transient() <- errors.Wrap(err, "[kv] - failed to gossip feedback")
	}
	return nil
}

// |||| RECEIVER ||||

type feedbackReceiver struct {
	Config
	confluence.UnarySource[batch]
}

func newFeedbackReceiver(cfg Config) segment {
	fr := &feedbackReceiver{Config: cfg}
	fr.FeedbackTransport.Handle(fr.handle)
	return fr
}

func (f *feedbackReceiver) handle(ctx context.Context, message FeedbackMessage) (types.Nil, error) {
	f.Logger.Debugw("received feedback", "peer", message.Sender, "host", f.Cluster.HostID())
	f.Out.Inlet() <- message.toBatch()
	return types.Nil{}, nil
}
