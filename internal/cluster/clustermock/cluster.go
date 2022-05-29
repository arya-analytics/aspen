package clustermock

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/cluster/pledge"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/kv/kvmock"
	"github.com/arya-analytics/x/rand"
	tmock "github.com/arya-analytics/x/transport/mock"
	"time"
)

func Provision(n int, cfg cluster.Config) ([]cluster.Cluster, error) {
	var (
		ctx       = context.Background()
		gossipNet = tmock.NewNetwork[gossip.Message, gossip.Message]()
		pledgeNet = tmock.NewNetwork[node.ID, node.ID]()
	)
	var clusters []cluster.Cluster
	for i := 0; i < n; i++ {
		gossipTransport := gossipNet.Route("")
		pledgeTransport := pledgeNet.Route(gossipTransport.Address)
		var peerAddresses []address.Address
		for _, peer := range clusters {
			peerAddresses = append(peerAddresses, peer.Host().Address)
		}
		cCfg := cluster.Config{
			Pledge:  pledge.Config{Transport: pledgeTransport},
			Gossip:  gossip.Config{Transport: gossipTransport},
			Storage: kvmock.New(),
		}.Merge(cfg)
		time.Sleep(200 * time.Millisecond)
		clust, err := cluster.Join(ctx, gossipTransport.Address, rand.SubSlice(peerAddresses, 5), cCfg)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, clust)
	}
	return clusters, nil
}
