package cluster

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/cluster/store"
	"github.com/arya-analytics/aspen/internal/node"
	pledge_ "github.com/arya-analytics/aspen/internal/pledge"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/iter"
	"github.com/arya-analytics/x/kv"
	"go.uber.org/zap"
)

type Cluster interface {
	// Snapshot returns a copy of the current cluster state. This snapshot is safe
	// to modify, but is not guaranteed to remain up to date.
	Snapshot() node.Group
	// Host returns the host Node.
	Host() node.Node
}

func Join(ctx context.Context, addr address.Address, peers []address.Address, cfg Config) (Cluster, error) {
	cfg = cfg.Merge(DefaultConfig())

	// Attempt to open the cluster store from KV.
	s, err := openStore(cfg)
	if err != nil && err != kv.ErrNotFound {
		return nil, err
	}

	c := &cluster{Store: s, Config: cfg}

	// If our store is empty or invalid, we need to boostrap the cluster.
	if !s.Valid() {
		id, err := pledge(ctx, peers, c)
		if err != nil {
			return nil, err
		}
		c.Store.SetHost(node.Node{ID: id, Address: addr})
		// Gossip initial cluster state so we can contact it for
		// information on other nodes instead of peers.
		gossipInitialState(ctx, c.Store, c.Config, peers)
	}

	gossip.New(s, c.Config.Gossip).Gossip(ctx)

	return c, nil
}

type cluster struct {
	Config
	store.Store
}

func (c *cluster) Snapshot() node.Group {
	return c.Store.GetState().Nodes
}

func openStore(openStore Config) (store.Store, error) {
	s := store.New()
	return s, kv.Load(openStore.Storage, openStore.StorageKey, s)
}

func pledge(ctx context.Context, peers []address.Address, c *cluster) (node.ID, error) {
	candidates := func() node.Group { return c.Store.GetState().Nodes }
	return pledge_.Pledge(ctx, peers, candidates, c.Config.Pledge)
}

func gossipInitialState(
	ctx context.Context,
	s store.Store,
	cfg Config,
	peers []address.Address,
) {
	g := gossip.New(s, cfg.Gossip)
	nextAddr := iter.InfiniteSlice(peers)
	for peerAddr := nextAddr(); peerAddr != ""; peerAddr = nextAddr() {
		if err := g.GossipOnceWith(ctx, peerAddr); err != nil {
			cfg.Logger.Error("failed to gossip with peer", zap.String("peer", string(peerAddr)), zap.Error(err))
		}
		if len(s.GetState().Nodes) > 1 {
			break
		}
	}
}
