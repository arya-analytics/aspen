package store

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/store"
	"github.com/arya-analytics/x/util/errutil"
	"io"
)

type Store interface {
	store.Observable[State]
	kv.FlushLoader
	Set(node.Node)
	Get(node.ID) (node.Node, bool)
	Merge(group node.Group)
	Valid() bool
	Host() node.Node
	SetHost(node.Node)
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
	n, ok := c.Observable.GetState().Nodes[id]
	return n, ok
}

func (c *core) Host() node.Node {
	snap := c.Observable.GetState()
	if n, ok := snap.Nodes[snap.HostID]; ok {
		return n
	}
	panic("no host")
}

func (c *core) SetHost(n node.Node) {
	snap := c.Observable.GetState()
	snap.Nodes[n.ID] = n
	snap.HostID = n.ID
	c.Observable.SetState(snap)
}

func (c *core) Set(n node.Node) {
	snap := c.Observable.GetState()
	snap.Nodes[n.ID] = n
	c.Observable.SetState(snap)
}

func (c *core) Merge(other node.Group) {
	snap := c.Observable.GetState()
	for _, n := range other {
		in, ok := snap.Nodes[n.ID]
		if !ok || n.Heartbeat.OlderThan(in.Heartbeat) {
			snap.Nodes[n.ID] = n
		}
	}
	c.Observable.SetState(snap)
}

func (c *core) Valid() bool {
	return c.Observable.GetState().HostID != node.ID(0)
}

func (c *core) Load(r io.Reader) error {
	var snap State
	catch := errutil.NewCatchRead(r)
	catch.Read(&snap)
	c.Observable.SetState(snap)
	return catch.Error()
}

func (c *core) Flush(w io.Writer) error {
	catch := errutil.NewCatchWrite(w)
	catch.Write(c.Observable.GetState())
	return catch.Error()
}
