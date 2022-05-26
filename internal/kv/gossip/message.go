package gossip

import (
	"github.com/arya-analytics/aspen/internal/kv/operation"
	"github.com/arya-analytics/x/transport"
	"github.com/arya-analytics/x/version"
)

type Feedback struct {
	Key     []byte
	Version version.Counter
}

type State byte

const (
	// StateInfected means that a node should actively gossip the update
	// to other nodes in the cluster.
	StateInfected State = iota
	// StateRecovered means that a node should no longer gossip the update.
	StateRecovered
)

type Operation struct {
	operation.Operation
	State State
}

type Transport = transport.Unary[Operation, Feedback]
