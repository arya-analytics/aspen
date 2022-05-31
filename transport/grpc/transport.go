package aspen

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/aspen/internal/node"
	aspenv1 "github.com/arya-analytics/aspen/transport/grpc/gen/proto/go/v1"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/version"
	"go/types"
	"google.golang.org/protobuf/types/known/emptypb"
)

type clusterPledge struct {
	aspenv1.UnimplementedPledgeServiceServer
	aspenv1.PledgeServiceClient
	handle func(ctx context.Context, id node.ID) (node.ID, error)
}

func (p *clusterPledge) Send(ctx context.Context, id node.ID) (node.ID, error) {
	res, err := p.PledgeServiceClient.Pledge(ctx, &aspenv1.ClusterPledge{NodeId: uint32(id)})
	return node.ID(res.NodeId), err
}

func (p *clusterPledge) Pledge(ctx context.Context, req *aspenv1.ClusterPledge) (*aspenv1.ClusterPledge, error) {
	id, err := p.handle(ctx, node.ID(req.NodeId))
	return &aspenv1.ClusterPledge{NodeId: uint32(id)}, err
}

func (p *clusterPledge) Handle(handle func(ctx context.Context, id node.ID) (node.ID, error)) {
	p.handle = handle
}

type clusterGossip struct {
	aspenv1.UnimplementedClusterGossipServiceServer
	aspenv1.ClusterGossipServiceClient
	handle func(ctx context.Context, m gossip.Message) (gossip.Message, error)
}

func (c *clusterGossip) Send(ctx context.Context, m gossip.Message) (gossip.Message, error) {
	res, err := c.ClusterGossipServiceClient.Gossip(ctx, c.convertRPC(m))
	return c.convertTransport(res), err
}

func (c *clusterGossip) Handle(handle func(ctx context.Context, m gossip.Message) (gossip.Message, error)) {
	c.handle = handle
}

func (c *clusterGossip) Gossip(ctx context.Context, req *aspenv1.ClusterGossip) (*aspenv1.ClusterGossip, error) {
	tRes, err := c.handle(ctx, c.convertTransport(req))
	return c.convertRPC(tRes), err
}

func (c *clusterGossip) convertRPC(msg gossip.Message) *aspenv1.ClusterGossip {
	tMsg := &aspenv1.ClusterGossip{}
	for _, d := range msg.Digests {
		tMsg.Digests[uint32(d.ID)] = &aspenv1.NodeDigest{
			Id:        uint32(d.ID),
			Heartbeat: &aspenv1.Heartbeat{Version: d.Heartbeat.Version, Generation: d.Heartbeat.Generation},
		}
	}
	for _, n := range msg.Nodes {
		tMsg.Nodes[uint32(n.ID)] = &aspenv1.Node{
			Id:        uint32(n.ID),
			Address:   string(n.Address),
			State:     uint32(n.State),
			Heartbeat: &aspenv1.Heartbeat{Version: n.Heartbeat.Version, Generation: n.Heartbeat.Generation},
		}
	}
	return tMsg
}

func (c *clusterGossip) convertTransport(msg *aspenv1.ClusterGossip) (tMsg gossip.Message) {
	for _, d := range msg.Digests {
		tMsg.Digests[node.ID(d.Id)] = node.Digest{
			ID:        node.ID(d.Id),
			Heartbeat: version.Heartbeat{Version: d.Heartbeat.Version, Generation: d.Heartbeat.Generation},
		}
	}
	for _, n := range msg.Nodes {
		tMsg.Nodes[node.ID(n.Id)] = node.Node{
			ID:        node.ID(n.Id),
			Address:   address.Address(n.Address),
			State:     node.State(n.State),
			Heartbeat: version.Heartbeat{Version: n.Heartbeat.Version, Generation: n.Heartbeat.Generation},
		}
	}
	return tMsg
}

type operations struct {
	aspenv1.UnimplementedOperationServiceServer
	aspenv1.OperationServiceClient
	handle func(ctx context.Context, msg kv.OperationMessage) (kv.OperationMessage, error)
}

func (o *operations) Send(ctx context.Context, msg kv.OperationMessage) (kv.OperationMessage, error) {
	res, err := o.OperationServiceClient.Operation(ctx, o.convertRPC(msg))
	return o.convertTransport(res), err
}

func (o *operations) Handle(handle func(context.Context, kv.OperationMessage) (kv.OperationMessage, error)) {
	o.handle = handle
}

func (o *operations) convertRPC(msg kv.OperationMessage) *aspenv1.OperationMessage {
	tMsg := &aspenv1.OperationMessage{
		Sender: uint32(msg.Sender),
	}
	for _, o := range msg.Operations {
		tMsg.Operations = append(tMsg.Operations, convertOperationRPC(o))
	}
	return tMsg
}

func (o *operations) convertTransport(msg *aspenv1.OperationMessage) (tMsg kv.OperationMessage) {
	tMsg.Sender = node.ID(msg.Sender)
	for _, o := range msg.Operations {
		tMsg.Operations = append(tMsg.Operations, convertOperationTransport(o))
	}
	return tMsg
}

func convertOperationRPC(msg kv.Operation) (tMsg *aspenv1.Operation) {
	return &aspenv1.Operation{
		Key:         msg.Key,
		Value:       msg.Value,
		Variant:     uint32(msg.Variant),
		Leaseholder: uint32(msg.Leaseholder),
		Version:     int64(msg.Version),
	}
}

func convertOperationTransport(msg *aspenv1.Operation) (tMsg kv.Operation) {
	return kv.Operation{
		Key:         msg.Key,
		Value:       msg.Value,
		Variant:     kv.Variant(msg.Variant),
		Leaseholder: node.ID(msg.Leaseholder),
		Version:     version.Counter(msg.Version),
	}
}

type lease struct {
	aspenv1.UnimplementedLeaseServiceServer
	aspenv1.LeaseServiceClient
	handle func(ctx context.Context, msg kv.LeaseMessage) (types.Nil, error)
}

func (l *lease) Send(ctx context.Context, msg kv.LeaseMessage) (types.Nil, error) {
	_, err := l.LeaseServiceClient.Forward(ctx, l.convertRPC(msg))
	return types.Nil{}, err
}

func (l *lease) Handle(handle func(ctx context.Context, msg kv.LeaseMessage) (types.Nil, error)) {
	l.handle = handle
}

func (l *lease) Forward(ctx context.Context, msg *aspenv1.LeaseMessage) (*emptypb.Empty, error) {
	_, err := l.handle(ctx, l.convertTransport(msg))
	return &emptypb.Empty{}, err
}

func (l *lease) convertRPC(msg kv.LeaseMessage) *aspenv1.LeaseMessage {
	return &aspenv1.LeaseMessage{Operation: convertOperationRPC(msg.Operation)}
}

func (l *lease) convertTransport(msg *aspenv1.LeaseMessage) (tMsg kv.LeaseMessage) {
	tMsg.Operation = convertOperationTransport(msg.Operation)
	return tMsg
}

type feedback struct {
	aspenv1.UnimplementedFeedbackServiceServer
	aspenv1.FeedbackServiceClient
	handle func(ctx context.Context, msg kv.FeedbackMessage) (types.Nil, error)
}

func (f *feedback) Send(ctx context.Context, msg kv.FeedbackMessage) (types.Nil, error) {
	_, err := f.FeedbackServiceClient.Feedback(ctx, f.convertRPC(msg))
	return types.Nil{}, err
}

func (f *feedback) Handle(handle func(ctx context.Context, msg kv.FeedbackMessage) (types.Nil, error)) {
	f.handle = handle
}

func (f *feedback) Feedback(ctx context.Context, msg *aspenv1.FeedbackMessage) (*emptypb.Empty, error) {
	_, err := f.handle(ctx, f.convertTransport(msg))
	return &emptypb.Empty{}, err
}

func (f *feedback) convertRPC(msg kv.FeedbackMessage) *aspenv1.FeedbackMessage {
	tMsg := &aspenv1.FeedbackMessage{Sender: uint32(msg.Sender)}
	for _, f := range msg.Digests {
		tMsg.Digests = append(tMsg.Digests, &aspenv1.OperationDigest{
			Key:         f.Key,
			Version:     int64(f.Version),
			Leaseholder: uint32(f.Leaseholder),
		})
	}
	return tMsg
}

func (f *feedback) convertTransport(msg *aspenv1.FeedbackMessage) (tMsg kv.FeedbackMessage) {
	tMsg.Sender = node.ID(msg.Sender)
	for _, f := range msg.Digests {
		tMsg.Digests = append(tMsg.Digests, kv.Digest{
			Key:         f.Key,
			Version:     version.Counter(f.Version),
			Leaseholder: node.ID(f.Leaseholder),
		})
	}
	return tMsg
}
