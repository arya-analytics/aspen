// Package cluster provides an interface for joining a cluster of nodes and exchanging state through an SI gossip model.
// Nodes can join the cluster without needing to know all members. Cluster will automatically manage the membership of
// new nodes by assigning them unique IDs and keeping them in sync with their peers. ToAddr Join a cluster, simply use
// cluster.Join.
package cluster

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	pledge_ "github.com/arya-analytics/aspen/internal/cluster/pledge"
	"github.com/arya-analytics/aspen/internal/cluster/store"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/iter"
	"github.com/arya-analytics/x/kv"
	xstore "github.com/arya-analytics/x/store"
	"go.uber.org/zap"
)

type State = store.State

// Cluster represents a group of nodes that can exchange their state with each other.
type Cluster interface {
	// Host returns the host Node (i.e. the node that Host is called on).
	Host() node.Node
	// Members returns a node.Group of all nodes in the cluster.
	Members() node.Group
	// Member returns the member Node with the given ID.
	Member(id node.ID) (node.Node, bool)
	// Reader returns a copy of the current cluster state. This snapshot is safe
	// to modify, but is not guaranteed to remain up to date.
	xstore.Reader[State]
}

// Join joins the host node to the cluster and begins gossiping its state. The node will spread addr as its listening
// address. A set of peer addresses (other nodes in the cluster) must be provided when joining an existing cluster
// for the first time. If restarting a node that is already a member of a cluster, the peer addresses can be left empty;
// Join will attempt to load the existing cluster state from storage (see Config.Storage and Config.StorageKey).
// If provisioning a new cluster, ensure that all storage for previous clusters is removed and provide no peers.
func Join(ctx context.Context, addr address.Address, peers []address.Address, cfg Config) (Cluster, error) {
	cfg = cfg.Merge(DefaultConfig())

	// Attempt to open the cluster store from kv.
	s, err := openStore(cfg)
	if err != nil && err != kv.ErrNotFound {
		return nil, err
	}

	c := &cluster{Store: s, Config: cfg}

	// If our store is empty or invalid, we need to boostrap the cluster.
	if !s.Valid() && len(peers) != 0 {
		cfg.Logger.Info("no existing cluster found in storage. pledging to cluster instead.")
		id, err := pledge(ctx, peers, c)
		if err != nil {
			return nil, err
		}
		c.Store.SetHost(node.Node{ID: id, Address: addr})
		// Gossip initial cluster state, so we can contact it for
		// information on other nodes instead of peers.
		cfg.Logger.Info("gossiping initial state through peer addresses.")
		gossipInitialState(ctx, c.Store, c.Config, peers)
	} else if !s.Valid() && len(peers) == 0 {
		c.Store.SetHost(node.Node{ID: 1, Address: addr})
		pledge_.Arbitrate(c.Members, c.Pledge)
		cfg.Logger.Info("no peers provided, bootstrapping new cluster")
	} else {
		pledge_.Arbitrate(c.Members, c.Pledge)
		cfg.Logger.Info("existing cluster found in storage. restarting activities.")
	}

	gossip.New(s, c.Config.Gossip).Gossip(ctx)

	return c, nil
}

type cluster struct {
	Config
	store.Store
}

func (c *cluster) GetState() State { return c.Store.GetState() }

func (c *cluster) Host() node.Node { return c.Store.GetHost() }

func (c *cluster) Members() node.Group { return c.GetState().Nodes }

func (c *cluster) Member(id node.ID) (node.Node, bool) { return c.Store.Get(id) }

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
