package grpc

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/cluster/pledge"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/aspen/internal/node"
	aspenv1 "github.com/arya-analytics/aspen/transport/grpc/gen/proto/go/v1"
	"github.com/arya-analytics/x/address"
	grpcx "github.com/arya-analytics/x/grpc"
	"github.com/arya-analytics/x/shutdown"
	"github.com/arya-analytics/x/version"
	"github.com/cockroachdb/errors"
	"go/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
)

// |||||| CORE ||||||

type core struct {
	*grpcx.Pool
}

func (c core) String() string { return "grpc" }

// |||||| PLEDGE ||||||

type pledgeTransport struct {
	aspenv1.UnimplementedClusterGossipServiceServer
	core
	handle func(ctx context.Context, req node.ID) (node.ID, error)
}

func (p *pledgeTransport) Send(ctx context.Context, addr address.Address, req node.ID) (node.ID, error) {
	c, err := p.Pool.Acquire(addr)
	if err != nil {
		return 0, err
	}
	defer c.Release()
	res, err := aspenv1.NewPledgeServiceClient(c).Pledge(ctx, p.translateBackward(req))
	if err != nil {
		return 0, err
	}
	return p.translateForward(res), nil
}

func (p *pledgeTransport) Handle(handle func(context.Context, node.ID) (node.ID, error)) {
	p.handle = handle
}

func (p *pledgeTransport) Pledge(ctx context.Context, req *aspenv1.ClusterPledge) (*aspenv1.ClusterPledge, error) {
	if p.handle == nil {
		return &aspenv1.ClusterPledge{}, errors.New("unavailable")
	}
	id, err := p.handle(ctx, p.translateForward(req))
	return p.translateBackward(id), err
}

func (p *pledgeTransport) translateForward(msg *aspenv1.ClusterPledge) node.ID {
	if msg == nil {
		return 0
	}
	return node.ID(msg.NodeId)
}

func (p *pledgeTransport) translateBackward(id node.ID) *aspenv1.ClusterPledge {
	return &aspenv1.ClusterPledge{NodeId: uint32(id)}
}

func (p *pledgeTransport) String() string { return "grpc" }

// |||||| CLUSTER ||||||

// clusterGossip implements the grpc.AbstractTranslatedTransport and the grpc.Translator interfaces.
type clusterGossip struct {
	aspenv1.UnimplementedClusterGossipServiceServer
	core
	handle func(ctx context.Context, req gossip.Message) (gossip.Message, error)
}

func (c *clusterGossip) Send(ctx context.Context, addr address.Address, req gossip.Message) (gossip.Message, error) {
	conn, err := c.Pool.Acquire(addr)
	if err != nil {
		return gossip.Message{}, err
	}
	defer conn.Release()
	tReq := c.translateBackward(req)
	res, err := aspenv1.NewClusterGossipServiceClient(conn).Gossip(ctx, tReq)
	tRes := c.translateForward(res)
	return tRes, err
}

func (c *clusterGossip) Handle(handle func(context.Context, gossip.Message) (gossip.Message, error)) {
	c.handle = handle
}

func (c *clusterGossip) Gossip(ctx context.Context, req *aspenv1.ClusterGossip) (*aspenv1.ClusterGossip, error) {
	tReq := c.translateForward(req)
	tRes, err := c.handle(ctx, tReq)
	res := c.translateBackward(tRes)
	return res, err

}

func (c *clusterGossip) translateForward(msg *aspenv1.ClusterGossip) (tMsg gossip.Message) {
	if msg == nil {
		return tMsg
	}
	if len(msg.Digests) > 0 {
		tMsg.Digests = make(map[node.ID]node.Digest)
	}
	if len(msg.Nodes) > 0 {
		tMsg.Nodes = make(map[node.ID]node.Node)
	}
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

func (c *clusterGossip) translateBackward(msg gossip.Message) *aspenv1.ClusterGossip {
	tMsg := &aspenv1.ClusterGossip{Digests: make(map[uint32]*aspenv1.NodeDigest), Nodes: make(map[uint32]*aspenv1.Node)}
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

func (c *clusterGossip) String() string { return "grpc" }

// |||||| OPERATIONS ||||||

type operations struct {
	aspenv1.UnimplementedOperationServiceServer
	core
	handle func(ctx context.Context, req kv.OperationMessage) (kv.OperationMessage, error)
}

func (o *operations) Send(ctx context.Context, addr address.Address, req kv.OperationMessage) (kv.OperationMessage, error) {
	c, err := o.Pool.Acquire(addr)
	if err != nil {
		return kv.OperationMessage{}, err
	}
	defer c.Release()
	res, err := aspenv1.NewOperationServiceClient(c).Operation(ctx, o.translateBackward(req))
	return o.translateForward(res), err
}

func (o *operations) Handle(handle func(context.Context, kv.OperationMessage) (kv.OperationMessage, error)) {
	o.handle = handle
}

func (o *operations) Operation(ctx context.Context, req *aspenv1.OperationMessage) (*aspenv1.OperationMessage, error) {
	msg, err := o.handle(ctx, o.translateForward(req))
	return o.translateBackward(msg), err
}

func (o *operations) translateBackward(msg kv.OperationMessage) *aspenv1.OperationMessage {
	tMsg := &aspenv1.OperationMessage{Sender: uint32(msg.Sender)}
	for _, o := range msg.Operations {
		tMsg.Operations = append(tMsg.Operations, convertOperationRPC(o))
	}
	return tMsg
}

func (o *operations) translateForward(msg *aspenv1.OperationMessage) (tMsg kv.OperationMessage) {
	if msg == nil {
		return tMsg
	}
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
	handle func(ctx context.Context, msg kv.LeaseMessage) (types.Nil, error)
	core
}

func (l *lease) Send(ctx context.Context, addr address.Address, msg kv.LeaseMessage) (types.Nil, error) {
	conn, err := l.Pool.Acquire(addr)
	if err != nil {
		return types.Nil{}, err
	}
	_, err = aspenv1.NewLeaseServiceClient(conn).Forward(ctx, l.translateBackward(msg))
	return types.Nil{}, err
}

func (l *lease) Handle(handle func(ctx context.Context, msg kv.LeaseMessage) (types.Nil, error)) {
	l.handle = handle
}

func (l *lease) Forward(ctx context.Context, msg *aspenv1.LeaseMessage) (*emptypb.Empty, error) {
	_, err := l.handle(ctx, l.translateForward(msg))
	return &emptypb.Empty{}, err
}

func (l *lease) translateBackward(msg kv.LeaseMessage) *aspenv1.LeaseMessage {
	return &aspenv1.LeaseMessage{Operation: convertOperationRPC(msg.Operation)}
}

func (l *lease) translateForward(msg *aspenv1.LeaseMessage) (tMsg kv.LeaseMessage) {
	if msg == nil {
		return tMsg
	}
	tMsg.Operation = convertOperationTransport(msg.Operation)
	return tMsg
}

type feedback struct {
	aspenv1.UnimplementedFeedbackServiceServer
	aspenv1.FeedbackServiceClient
	handle func(ctx context.Context, msg kv.FeedbackMessage) (types.Nil, error)
	core
}

func (f *feedback) Send(ctx context.Context, addr address.Address, msg kv.FeedbackMessage) (types.Nil, error) {
	conn, err := f.Pool.Acquire(addr)
	if err != nil {
		return types.Nil{}, err
	}
	_, err = aspenv1.NewFeedbackServiceClient(conn).Feedback(ctx, f.translateBackward(msg))
	return types.Nil{}, err
}

func (f *feedback) Handle(handle func(ctx context.Context, msg kv.FeedbackMessage) (types.Nil, error)) {
	f.handle = handle
}

func (f *feedback) Feedback(ctx context.Context, msg *aspenv1.FeedbackMessage) (*emptypb.Empty, error) {
	_, err := f.handle(ctx, f.translateForward(msg))
	return &emptypb.Empty{}, err
}

func (f *feedback) translateBackward(msg kv.FeedbackMessage) *aspenv1.FeedbackMessage {
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

func (f *feedback) translateForward(msg *aspenv1.FeedbackMessage) (tMsg kv.FeedbackMessage) {
	if msg == nil {
		return tMsg
	}
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

func New() *transport {
	pool := grpcx.NewPool(grpc.WithInsecure())
	return &transport{
		pool:          pool,
		pledge:        &pledgeTransport{core: core{Pool: pool}},
		lease:         &lease{core: core{Pool: pool}},
		feedback:      &feedback{core: core{Pool: pool}},
		operations:    &operations{core: core{Pool: pool}},
		clusterGossip: &clusterGossip{core: core{Pool: pool}},
	}
}

// transport implements the aspen.Transport interface.
type transport struct {
	pool          *grpcx.Pool
	pledge        *pledgeTransport
	clusterGossip *clusterGossip
	operations    *operations
	lease         *lease
	feedback      *feedback
}

func (t *transport) Pledge() pledge.Transport { return t.pledge }

func (t *transport) Cluster() gossip.Transport { return t.clusterGossip }

func (t *transport) Operations() kv.OperationsTransport { return t.operations }

func (t *transport) Lease() kv.LeaseTransport { return t.lease }

func (t *transport) Feedback() kv.FeedbackTransport { return t.feedback }

func (t *transport) Configure(addr address.Address, sd shutdown.Shutdown) error {
	server := grpc.NewServer()
	aspenv1.RegisterPledgeServiceServer(server, t.pledge)
	aspenv1.RegisterLeaseServiceServer(server, t.lease)
	aspenv1.RegisterFeedbackServiceServer(server, t.feedback)
	aspenv1.RegisterOperationServiceServer(server, t.operations)
	aspenv1.RegisterClusterGossipServiceServer(server, t.clusterGossip)
	lis, err := net.Listen("tcp", addr.PortString())
	if err != nil {
		return err
	}
	sd.Go(func(sig chan shutdown.Signal) (err error) {
		go func() {
			err = server.Serve(lis)
		}()
		if err != nil {
			return err
		}
		defer server.Stop()
		<-sig
		return nil
	})
	return nil
}
