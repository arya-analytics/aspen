// Package store exposes a simple copy-on-read store for managing cluster state.
// ToAddr create a new store, call store.New().
package store

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/store"
	"github.com/arya-analytics/x/util/errutil"
	"io"
)

// Store is an interface representing a copy-on-read store for managing cluster state.
type Store interface {
	// Observable allows the caller to react to state changes. This state is not diffed i.e.
	// any call that modifies the state, even if no actual change occurs, will get sent to the
	// Observable.
	store.Observable[State]
	// FlushLoader allows the caller to flush and load State from a persistent store.
	kv.FlushLoader
	// Set sets a node in state.
	Set(node.Node)
	// Get returns a node from state. Returns false if the node is not found.
	Get(node.ID) (node.Node, bool)
	// Merge merges a node.Group into Stac.Nodes by selecting nodes from group with heartbeats
	// that are either not in State or are older than in State.
	Merge(group node.Group)
	// Valid returns true if the store is valid i.e. State.HostID has been set.
	Valid() bool
	// GetHost returns the host node of the store.
	GetHost() node.Node
	// SetHost sets the host for the store.
	SetHost(node.Node)
}

func _copy(s State) State { return State{Nodes: s.Nodes.Copy(), HostID: s.HostID} }

// New opens a new empty, invalid Store.
func New() Store {
	c := &core{Observable: store.ObservableWrap[State](store.New(_copy))}
	c.Observable.SetState(State{Nodes: make(node.Group)})
	return c
}

// State is the current state of the cluster as viewed from the host.
type State struct {
	Nodes  node.Group
	HostID node.ID
}

type core struct{ store.Observable[State] }

// Get implements Store.
func (c *core) Get(id node.ID) (node.Node, bool) {
	n, ok := c.Observable.GetState().Nodes[id]
	return n, ok
}

// GetHost implements Store.
func (c *core) GetHost() node.Node {
	snap := c.Observable.GetState()
	if n, ok := snap.Nodes[snap.HostID]; ok {
		return n
	}
	panic("no host")
}

// SetHost implements Store.
func (c *core) SetHost(n node.Node) {
	snap := c.Observable.GetState()
	snap.Nodes[n.ID] = n
	snap.HostID = n.ID
	c.Observable.SetState(snap)
}

// Set implements Store.
func (c *core) Set(n node.Node) {
	snap := c.Observable.GetState()
	snap.Nodes[n.ID] = n
	c.Observable.SetState(snap)
}

// Merge implements Store.
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

// Valid implements Store.
func (c *core) Valid() bool { return c.Observable.GetState().HostID != node.ID(0) }

// Load implements kv.FlushLoader.
func (c *core) Load(r io.Reader) error {
	var snap State
	catch := errutil.NewCatchRead(r)
	catch.Read(&snap)
	c.Observable.SetState(snap)
	return catch.Error()
}

// Flush implements kv.FlushLoader.
func (c *core) Flush(w io.Writer) error {
	catch := errutil.NewCatchWrite(w)
	catch.Write(c.Observable.GetState())
	return catch.Error()
}
