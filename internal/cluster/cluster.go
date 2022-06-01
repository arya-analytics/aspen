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
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/iter"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/observe"
	"github.com/arya-analytics/x/shutdown"
	xstore "github.com/arya-analytics/x/store"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
)

type State = store.State

var (
	ErrNodeNotFound = errors.New("node not found")
)

// Cluster represents a group of nodes that can exchange their state with each other.
type Cluster interface {
	// Host returns the host Node (i.e. the node that Host is called on).
	Host() node.Node
	// HostID returns the ID of the host node.
	HostID() node.ID
	// Nodes returns a node.Group of all nodes in the cluster.
	Nodes() node.Group
	// Node returns the member Node with the given ID.
	Node(id node.ID) (node.Node, error)
	// Resolve resolves the address of a node with the given ID.
	Resolve(id node.ID) (address.Address, error)
	// Config returns the configuration parameters used by the cluster.
	Config() Config
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	alamos.AttachReporter(cfg.Experiment, "cluster", cfg)

	// Attempt to open the cluster store from kv. It's ok if we don't find it.
	s, err := openStore(cfg)
	if err != nil && err != kv.ErrNotFound {
		return nil, err
	}

	c := &cluster{Store: s, cfg: cfg}

	// If our store is empty or invalid, we need to boostrap the cluster.
	if !s.Valid() && len(peers) != 0 {
		cfg.Logger.Info("no existing cluster found in storage. pledging to cluster instead.")
		id, err := pledge(ctx, peers, c)
		if err != nil {
			return nil, err
		}
		c.Store.SetHost(node.Node{ID: id, Address: addr})
		// operationSender initial cluster state, so we can contact it for
		// information on other nodes instead of peers.
		cfg.Logger.Info("gossiping initial state through peer addresses.")
		if err = gossipInitialState(ctx, c.Store, c.cfg, peers); err != nil {
			return c, err
		}
	} else if !s.Valid() && len(peers) == 0 {
		c.Store.SetHost(node.Node{ID: 1, Address: addr})
		cfg.Logger.Info("no peers provided, bootstrapping new cluster")
		if err := pledge_.Arbitrate(c.Nodes, c.cfg.Pledge); err != nil {
			return c, err
		}
	} else {
		cfg.Logger.Info("existing cluster found in storage. restarting activities.")
		if err := pledge_.Arbitrate(c.Nodes, c.cfg.Pledge); err != nil {
			return nil, err
		}
	}

	g, err := gossip.New(s, c.cfg.Gossip)
	if err != nil {
		return nil, err
	}
	g.Gossip(ctx)

	flushStore(cfg, s)

	return c, nil
}

type cluster struct {
	cfg Config
	store.Store
}

func (c *cluster) GetState() State { return c.Store.CopyState() }

func (c *cluster) Host() node.Node { return c.Store.GetHost() }

func (c *cluster) HostID() node.ID { return c.Store.ReadState().HostID }

func (c *cluster) Nodes() node.Group { return c.GetState().Nodes }

func (c *cluster) Node(id node.ID) (node.Node, error) {
	n, ok := c.Store.Get(id)
	if !ok {
		return n, ErrNodeNotFound
	}
	return n, nil
}

func (c *cluster) Resolve(id node.ID) (address.Address, error) {
	n, err := c.Node(id)
	return n.Address, err
}

func (c *cluster) Config() Config { return c.cfg }

func openStore(cfg Config) (store.Store, error) {
	s := store.New()
	if cfg.Storage == nil {
		return s, nil
	}
	return s, kv.Load(cfg.Storage, cfg.StorageKey, s)
}

func pledge(ctx context.Context, peers []address.Address, c *cluster) (node.ID, error) {
	candidates := func() node.Group { return c.Store.CopyState().Nodes }
	return pledge_.Pledge(ctx, peers, candidates, c.cfg.Pledge)
}

func gossipInitialState(
	ctx context.Context,
	s store.Store,
	cfg Config,
	peers []address.Address,
) error {
	g, err := gossip.New(s, cfg.Gossip)
	if err != nil {
		return err
	}
	nextAddr := iter.InfiniteSlice(peers)
	for peerAddr := nextAddr(); peerAddr != ""; peerAddr = nextAddr() {
		if err := g.GossipOnceWith(ctx, peerAddr); err != nil {
			cfg.Logger.Error("failed to gossip with peer", zap.String("peer", string(peerAddr)), zap.Error(err))
		}
		if len(s.CopyState().Nodes) > 1 {
			break
		}
	}
	return nil
}

func flushStore(cfg Config, s store.Store) {
	errC := make(chan error, 5)
	if cfg.Storage != nil {
		flusher := &observe.FlushSubscriber[State]{
			Key:         cfg.StorageKey,
			MinInterval: cfg.StorageFlushInterval,
			Store:       cfg.Storage,
			ErrC:        errC,
		}
		s.OnChange(flusher.Flush)
		cfg.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case err := <-errC:
					cfg.Logger.Error("failed to flush state", zap.Error(err))
				}
			}
		})
	}
}
