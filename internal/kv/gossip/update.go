package gossip

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/version"
)

type UpdateState byte

const (
	// UpdateStateInfected means that a node should actively gossip the update
	// to other nodes in the cluster.
	UpdateStateInfected UpdateState = iota
	// UpdateStateRecovered means that a node should no longer gossip the update.
	UpdateStateRecovered
)

// OperationVariant represents a distributed key-value operation.
type OperationVariant byte

const (
	OperationSet OperationVariant = iota
	OperationDelete
)

type Operation struct {
	Key         []byte
	Value       []byte
	Leaseholder node.ID
	State       UpdateState
	Version     version.Counter
	Variant     OperationVariant
}
