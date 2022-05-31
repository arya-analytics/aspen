// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package aspenv1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ClusterGossipServiceClient is the client API for ClusterGossipService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ClusterGossipServiceClient interface {
	Gossip(ctx context.Context, in *ClusterGossip, opts ...grpc.CallOption) (*ClusterGossip, error)
}

type clusterGossipServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewClusterGossipServiceClient(cc grpc.ClientConnInterface) ClusterGossipServiceClient {
	return &clusterGossipServiceClient{cc}
}

func (c *clusterGossipServiceClient) Gossip(ctx context.Context, in *ClusterGossip, opts ...grpc.CallOption) (*ClusterGossip, error) {
	out := new(ClusterGossip)
	err := c.cc.Invoke(ctx, "/aspen.v1.ClusterGossipService/Gossip", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClusterGossipServiceServer is the server API for ClusterGossipService service.
// All implementations should embed UnimplementedClusterGossipServiceServer
// for forward compatibility
type ClusterGossipServiceServer interface {
	Gossip(context.Context, *ClusterGossip) (*ClusterGossip, error)
}

// UnimplementedClusterGossipServiceServer should be embedded to have forward compatible implementations.
type UnimplementedClusterGossipServiceServer struct {
}

func (UnimplementedClusterGossipServiceServer) Gossip(context.Context, *ClusterGossip) (*ClusterGossip, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Gossip not implemented")
}

// UnsafeClusterGossipServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ClusterGossipServiceServer will
// result in compilation errors.
type UnsafeClusterGossipServiceServer interface {
	mustEmbedUnimplementedClusterGossipServiceServer()
}

func RegisterClusterGossipServiceServer(s grpc.ServiceRegistrar, srv ClusterGossipServiceServer) {
	s.RegisterService(&ClusterGossipService_ServiceDesc, srv)
}

func _ClusterGossipService_Gossip_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClusterGossip)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterGossipServiceServer).Gossip(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/aspen.v1.ClusterGossipService/Gossip",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterGossipServiceServer).Gossip(ctx, req.(*ClusterGossip))
	}
	return interceptor(ctx, in, info, handler)
}

// ClusterGossipService_ServiceDesc is the grpc.ServiceDesc for ClusterGossipService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ClusterGossipService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "aspen.v1.ClusterGossipService",
	HandlerType: (*ClusterGossipServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Gossip",
			Handler:    _ClusterGossipService_Gossip_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "v1/cluster.proto",
}

// PledgeServiceClient is the client API for PledgeService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type PledgeServiceClient interface {
	Pledge(ctx context.Context, in *ClusterPledge, opts ...grpc.CallOption) (*ClusterPledge, error)
}

type pledgeServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewPledgeServiceClient(cc grpc.ClientConnInterface) PledgeServiceClient {
	return &pledgeServiceClient{cc}
}

func (c *pledgeServiceClient) Pledge(ctx context.Context, in *ClusterPledge, opts ...grpc.CallOption) (*ClusterPledge, error) {
	out := new(ClusterPledge)
	err := c.cc.Invoke(ctx, "/aspen.v1.PledgeService/Pledge", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PledgeServiceServer is the server API for PledgeService service.
// All implementations should embed UnimplementedPledgeServiceServer
// for forward compatibility
type PledgeServiceServer interface {
	Pledge(context.Context, *ClusterPledge) (*ClusterPledge, error)
}

// UnimplementedPledgeServiceServer should be embedded to have forward compatible implementations.
type UnimplementedPledgeServiceServer struct {
}

func (UnimplementedPledgeServiceServer) Pledge(context.Context, *ClusterPledge) (*ClusterPledge, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Pledge not implemented")
}

// UnsafePledgeServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to PledgeServiceServer will
// result in compilation errors.
type UnsafePledgeServiceServer interface {
	mustEmbedUnimplementedPledgeServiceServer()
}

func RegisterPledgeServiceServer(s grpc.ServiceRegistrar, srv PledgeServiceServer) {
	s.RegisterService(&PledgeService_ServiceDesc, srv)
}

func _PledgeService_Pledge_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClusterPledge)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PledgeServiceServer).Pledge(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/aspen.v1.PledgeService/Pledge",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PledgeServiceServer).Pledge(ctx, req.(*ClusterPledge))
	}
	return interceptor(ctx, in, info, handler)
}

// PledgeService_ServiceDesc is the grpc.ServiceDesc for PledgeService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var PledgeService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "aspen.v1.PledgeService",
	HandlerType: (*PledgeServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Pledge",
			Handler:    _PledgeService_Pledge_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "v1/cluster.proto",
}
