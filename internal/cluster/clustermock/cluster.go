package clustermock

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	tmock "github.com/arya-analytics/x/transport/mock"
)

type Builder struct {
	BaseCfg     cluster.Config
	GossipNet   *tmock.Network[gossip.Message, gossip.Message]
	PledgeNet   *tmock.Network[node.ID, node.ID]
	ClusterAPIs map[node.ID]cluster.Cluster
}

func NewBuilder(baseCfg cluster.Config) *Builder {
	return &Builder{
		BaseCfg:     baseCfg.Merge(cluster.DefaultConfig()),
		GossipNet:   tmock.NewNetwork[gossip.Message, gossip.Message](),
		PledgeNet:   tmock.NewNetwork[node.ID, node.ID](),
		ClusterAPIs: make(map[node.ID]cluster.Cluster),
	}
}

func (b *Builder) New(cfg cluster.Config) (cluster.Cluster, error) {
	gossipTransport := b.GossipNet.RouteUnary("")
	pledgeTransport := b.PledgeNet.RouteUnary(gossipTransport.Address)
	cfg.Gossip.Transport = gossipTransport
	cfg.Pledge.Transport = pledgeTransport
	cfg = cfg.Merge(b.BaseCfg)
	clust, err := cluster.Join(context.Background(), gossipTransport.Address, b.MemberAddresses(), cfg)
	b.ClusterAPIs[clust.Host().ID] = clust
	return clust, err
}

func (b *Builder) MemberAddresses() (memberAddresses []address.Address) {
	for _, api := range b.ClusterAPIs {
		memberAddresses = append(memberAddresses, api.Host().Address)
	}
	return memberAddresses
}
