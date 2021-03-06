package kvmock

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/clustermock"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/kv/memkv"
	"github.com/arya-analytics/x/signal"
	tmock "github.com/arya-analytics/x/transport/mock"
	"go/types"
)

type Builder struct {
	clustermock.Builder
	BaseCfg     kv.Config
	OpNet       *tmock.Network[kv.BatchRequest, kv.BatchRequest]
	FeedbackNet *tmock.Network[kv.FeedbackMessage, types.Nil]
	LeaseNet    *tmock.Network[kv.BatchRequest, types.Nil]
	KVs         map[node.ID]kv.DB
}

func NewBuilder(baseKVCfg kv.Config, baseClusterCfg cluster.Config) *Builder {
	return &Builder{
		BaseCfg:     baseKVCfg,
		Builder:     *clustermock.NewBuilder(baseClusterCfg),
		OpNet:       tmock.NewNetwork[kv.BatchRequest, kv.BatchRequest](),
		FeedbackNet: tmock.NewNetwork[kv.FeedbackMessage, types.Nil](),
		LeaseNet:    tmock.NewNetwork[kv.BatchRequest, types.Nil](),
		KVs:         make(map[node.ID]kv.DB),
	}
}

func (b *Builder) New(ctx signal.Context, kvCfg kv.Config, clusterCfg cluster.Config) (kv.DB,
	error) {
	clust, err := b.Builder.New(ctx, clusterCfg)
	if err != nil {
		return nil, err
	}
	kvCfg = kvCfg.Merge(b.BaseCfg)
	if kvCfg.Engine == nil {
		kvCfg.Engine = memkv.New()
	}
	kvCfg.Cluster = clust
	addr := clust.Host().Address
	kvCfg.OperationsTransport = b.OpNet.RouteUnary(addr)
	kvCfg.FeedbackTransport = b.FeedbackNet.RouteUnary(addr)
	kvCfg.LeaseTransport = b.LeaseNet.RouteUnary(addr)
	kve, err := kv.Open(ctx, kvCfg)
	if err != nil {
		return nil, err
	}
	b.KVs[clust.Host().ID] = kve
	return kve, nil
}
