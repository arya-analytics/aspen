package kv

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"go/types"
)

// |||||| OPERATION ||||||

type BatchTransport = transport.Unary[BatchRequest, BatchRequest]

// |||| SENDER ||||

type operationSender struct {
	Config
	confluence.LinearTransform[BatchRequest, BatchRequest]
}

func newOperationSender(cfg Config) segment {
	os := &operationSender{Config: cfg}
	os.TransformFunc.ApplyTransform = os.send
	return os
}

func (g *operationSender) send(ctx signal.Context, sync BatchRequest) (BatchRequest, bool, error) {
	// If we have no Operations to propagate, it's best to avoid the network chatter.
	if sync.empty() {
		return sync, false, nil
	}
	hostID := g.Cluster.HostID()
	peer := gossip.RandomPeer(g.Cluster.Nodes(), hostID)
	if peer.Address == "" {
		g.Logger.Warnw("no healthy nodes to gossip with", "host", hostID)
		return sync, false, nil
	}
	g.Logger.Debugw("gossiping Operations",
		"host", hostID,
		"peer", peer.ID,
		"size", sync.size(),
	)
	sync.Sender = hostID
	ack, err := g.OperationsTransport.Send(ctx, peer.Address, sync)
	if err != nil {
		ctx.Transient() <- errors.Wrap(err, "[kv] - failed to gossip Operations")
	}
	// If we have no Operations to apply, avoid the pipeline overhead.
	return ack, !ack.empty(), nil
}

// |||| RECEIVER ||||

type operationReceiver struct {
	Config
	store store
	confluence.AbstractUnarySource[BatchRequest]
	confluence.EmptyFlow
}

func newOperationReceiver(cfg Config, s store) source {
	or := &operationReceiver{Config: cfg, store: s}
	or.OperationsTransport.Handle(or.handle)
	return or
}

func (g *operationReceiver) handle(ctx context.Context, req BatchRequest) (BatchRequest, error) {
	hostID := g.Cluster.HostID()
	g.Logger.Debug("received gossip", zap.Stringer("peer", req.Sender), zap.Stringer("host", hostID))
	select {
	case <-ctx.Done():
		return BatchRequest{}, ctx.Err()
	case g.Out.Inlet() <- req:
	}
	br := g.store.ReadState().toBatchRequest()
	br.Sender = g.Cluster.HostID()
	return br, nil
}

// |||||| FEEDBACK ||||||

type FeedbackMessage struct {
	Sender  node.ID
	Digests Digests
}

type FeedbackTransport = transport.Unary[FeedbackMessage, types.Nil]

// |||| SENDER ||||

type feedbackSender struct {
	Config
	confluence.UnarySink[BatchRequest]
}

func newFeedbackSender(cfg Config) sink {
	fs := &feedbackSender{Config: cfg}
	fs.Sink = fs.send
	return fs
}

func (f *feedbackSender) send(ctx signal.Context, bd BatchRequest) error {
	msg := FeedbackMessage{Sender: f.Cluster.Host().ID, Digests: bd.digests()}
	sender, _ := f.Cluster.Node(bd.Sender)
	f.Logger.Debugw("gossiping feedback",
		"host", f.Cluster.HostID(),
		"peer", bd.Sender,
		"size", len(msg.Digests),
	)
	if _, err := f.FeedbackTransport.Send(context.TODO(), sender.Address, msg); err != nil {
		ctx.Transient() <- errors.Wrap(err, "[kv] - failed to gossip feedback")
	}
	return nil
}

// |||| RECEIVER ||||

type feedbackReceiver struct {
	Config
	confluence.AbstractUnarySource[BatchRequest]
	confluence.EmptyFlow
}

func newFeedbackReceiver(cfg Config) source {
	fr := &feedbackReceiver{Config: cfg}
	fr.FeedbackTransport.Handle(fr.handle)
	return fr
}

func (f *feedbackReceiver) handle(ctx context.Context, message FeedbackMessage) (types.Nil, error) {
	f.Logger.Debugw("received feedback", "peer", message.Sender, "host", f.Cluster.HostID())
	f.Out.Inlet() <- message.Digests.toRequest()
	return types.Nil{}, nil
}
