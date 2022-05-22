package cluster

import (
	"context"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/aspen/internal/pledge"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/kv"
	"io"
)

type Cluster interface {
	Storage
	Snapshot
}

type Snapshot interface {
	Snapshot() node.Group
}

type Storage interface {
	// Flush implements kv.Flush.
	Flush(writer io.Writer) error
	// Load implements kv.Load.
	Load(reader io.Reader) error
}

func Join(ctx context.Context, addr address.Address, peers []address.Address, cfg Config) (Cluster, error) {
	cfg = cfg.Merge(DefaultConfig())

	c, err := tryLoadFromStorage(cfg)
	if err == nil {
		return c, nil
	}

	id, err := pledge.Pledge(ctx, peers, c.Snapshot, cfg.Pledge)
	if err != nil {
		return nil, err
	}

	c.state.setNode(node.Node{ID: id, Address: addr})

	g := &Gossip{state: c.state, Config: cfg}

	g.Gossip(ctx)

	return c, nil
}

type cluster struct {
	Config
	state *state
}

func (c *cluster) Load(reader io.Reader) error {
	return nil
}

func (c *cluster) Flush(write io.Writer) error {
	return nil
}

func (c *cluster) Snapshot() node.Group {
	return node.Group{}
}

func tryLoadFromStorage(cfg Config) (*cluster, error) {
	c := &cluster{Config: cfg}
	if cfg.Storage == nil {
		return nil, kv.ErrNotFound
	}
	return c, kv.Load(cfg.Storage, cfg.StorageKey, c)
}
