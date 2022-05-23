package store

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/store"
)

type Store interface {
	store.Observable[State]
	Set(node.Node)
	Get(node.ID) (node.Node, bool)
	Host() node.Node
	Merge(group node.Group)
}

func _copy(s State) State { return State{Nodes: s.Nodes.Copy(), HostID: s.HostID} }

func New() Store { return &core{Observable: store.NewObservable(_copy)} }

type State struct {
	Nodes  node.Group
	HostID node.ID
}

type core struct {
	store.Observable[State]
}

func (c *core) Get(id node.ID) (node.Node, bool) {
	n, ok := c.GetState().Nodes[id]
	return n, ok
}

func (c *core) Host() node.Node {
	snap := c.GetState()
	if n, ok := snap.Nodes[snap.HostID]; ok {
		return n
	}
	panic("no host")
}

func (c *core) Set(n node.Node) {
	snap := c.GetState()
	snap.Nodes[n.ID] = n
	c.SetState(snap)
}

func (c *core) Merge(other node.Group) {
	snap := c.GetState()
	for _, n := range other {
		in, ok := snap.Nodes[n.ID]
		if !ok || n.Heartbeat.OlderThan(in.Heartbeat) {
			snap.Nodes[n.ID] = n
		}
	}
	c.SetState(snap)
}
