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
	"github.com/arya-analytics/x/signal"
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

// clusterGossip implements the grpc.AbstractTranslatedTransport and the grpc.CoreTranslator interfaces.
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

type baseBatchRequest struct {
	core
}

type operations struct {
	aspenv1.UnimplementedOperationServiceServer
	core
	handle func(ctx context.Context, req kv.BatchRequest) (kv.BatchRequest, error)
}

func (o *operations) Send(ctx context.Context, addr address.Address,
	req kv.BatchRequest) (kv.BatchRequest, error) {
	c, err := o.Pool.Acquire(addr)
	if err != nil {
		return kv.BatchRequest{}, err
	}
	defer c.Release()
	res, err := aspenv1.NewOperationServiceClient(c).Operation(ctx, convertBatchRequestBackward(req))
	return convertBatchRequestForward(res), err
}

func (o *operations) Handle(handle func(context.Context, kv.BatchRequest) (kv.BatchRequest, error)) {
	o.handle = handle
}

func (o *operations) Operation(ctx context.Context, req *aspenv1.BatchRequest) (*aspenv1.BatchRequest, error) {
	msg, err := o.handle(ctx, convertBatchRequestForward(req))
	return convertBatchRequestBackward(msg), err
}

func convertBatchRequestBackward(msg kv.BatchRequest) *aspenv1.BatchRequest {
	tMsg := &aspenv1.BatchRequest{Sender: uint32(msg.Sender)}
	for _, o := range msg.Operations {
		tMsg.Operations = append(tMsg.Operations, convertOperationRPC(o))
	}
	return tMsg
}

func convertBatchRequestForward(msg *aspenv1.BatchRequest) (tMsg kv.BatchRequest) {
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
	handle func(ctx context.Context, msg kv.BatchRequest) (types.Nil, error)
	core
}

func (l *lease) Send(ctx context.Context, addr address.Address, msg kv.BatchRequest) (types.Nil, error) {
	conn, err := l.Pool.Acquire(addr)
	if err != nil {
		return types.Nil{}, err
	}
	_, err = aspenv1.NewLeaseServiceClient(conn).Forward(ctx, convertBatchRequestBackward(msg))
	return types.Nil{}, err
}

func (l *lease) Handle(handle func(ctx context.Context, msg kv.BatchRequest) (types.Nil, error)) {
	l.handle = handle
}

func (l *lease) Forward(ctx context.Context, msg *aspenv1.BatchRequest) (*emptypb.Empty, error) {
	_, err := l.handle(ctx, convertBatchRequestForward(msg))
	return &emptypb.Empty{}, err
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
	return NewWithPoolAndServer(
		grpcx.NewPool(grpc.WithInsecure()),
		grpc.NewServer(),
	)
}

func NewWithPoolAndServer(
	pool *grpcx.Pool,
	server *grpc.Server,
) *transport {
	t := &transport{
		server:        server,
		pool:          pool,
		pledge:        &pledgeTransport{core: core{Pool: pool}},
		lease:         &lease{core: core{Pool: pool}},
		feedback:      &feedback{core: core{Pool: pool}},
		operations:    &operations{core: core{Pool: pool}},
		clusterGossip: &clusterGossip{core: core{Pool: pool}},
	}
	aspenv1.RegisterPledgeServiceServer(t.server, t.pledge)
	aspenv1.RegisterLeaseServiceServer(t.server, t.lease)
	aspenv1.RegisterFeedbackServiceServer(t.server, t.feedback)
	aspenv1.RegisterOperationServiceServer(t.server, t.operations)
	aspenv1.RegisterClusterGossipServiceServer(t.server, t.clusterGossip)
	return t
}

// transport implements the aspen.Transport interface.
type transport struct {
	server        *grpc.Server
	pool          *grpcx.Pool
	pledge        *pledgeTransport
	clusterGossip *clusterGossip
	operations    *operations
	lease         *lease
	feedback      *feedback
}

func (t *transport) Pledge() pledge.Transport { return t.pledge }

func (t *transport) Cluster() gossip.Transport { return t.clusterGossip }

func (t *transport) Operations() kv.BatchTransport { return t.operations }

func (t *transport) Lease() kv.LeaseTransport { return t.lease }

func (t *transport) Feedback() kv.FeedbackTransport { return t.feedback }

func (t *transport) Configure(ctx signal.Context, addr address.Address) error {
	lis, err := net.Listen("tcp", addr.PortString())
	if err != nil {
		return err
	}
	ctx.Go(func(ctx signal.Context) (err error) {
		go func() {
			err = t.server.Serve(lis)
		}()
		if err != nil {
			return err
		}
		defer t.server.Stop()
		<-ctx.Done()
		return ctx.Err()
	})
	return nil
}
