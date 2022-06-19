package mock

import (
	"github.com/arya-analytics/aspen"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/cluster/pledge"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/shutdown"
	tmock "github.com/arya-analytics/x/transport/mock"
	"go/types"
)

type Network struct {
	pledge     *tmock.Network[node.ID, node.ID]
	cluster    *tmock.Network[gossip.Message, gossip.Message]
	operations *tmock.Network[kv.OperationMessage, kv.OperationMessage]
	lease      *tmock.Network[kv.LeaseMessage, types.Nil]
	feedback   *tmock.Network[kv.FeedbackMessage, types.Nil]
}

func NewNetwork() *Network {
	return &Network{
		pledge:     tmock.NewNetwork[node.ID, node.ID](),
		cluster:    tmock.NewNetwork[gossip.Message, gossip.Message](),
		operations: tmock.NewNetwork[kv.OperationMessage, kv.OperationMessage](),
		lease:      tmock.NewNetwork[kv.LeaseMessage, types.Nil](),
		feedback:   tmock.NewNetwork[kv.FeedbackMessage, types.Nil](),
	}
}

func (n *Network) NewTransport() aspen.Transport { return &transport{net: n} }

// transport is an in-memory, synchronous implementation of aspen.Transport.
type transport struct {
	net        *Network
	pledge     *tmock.Unary[node.ID, node.ID]
	cluster    *tmock.Unary[gossip.Message, gossip.Message]
	operations *tmock.Unary[kv.OperationMessage, kv.OperationMessage]
	lease      *tmock.Unary[kv.LeaseMessage, types.Nil]
	feedback   *tmock.Unary[kv.FeedbackMessage, types.Nil]
}

// Configure implements aspen.Transport.
func (t *transport) Configure(addr address.Address, sd shutdown.Shutdown) error {
	t.pledge = t.net.pledge.Route(addr)
	t.cluster = t.net.cluster.Route(addr)
	t.operations = t.net.operations.Route(addr)
	t.lease = t.net.lease.Route(addr)
	t.feedback = t.net.feedback.Route(addr)
	return nil
}

// Pledge implements aspen.Transport.
func (t *transport) Pledge() pledge.Transport { return t.pledge }

// Cluster implements aspen.Transport.
func (t *transport) Cluster() gossip.Transport { return t.cluster }

// Operations implements aspen.Transport.
func (t *transport) Operations() kv.OperationsTransport { return t.operations }

// Lease implements aspen.Transport.
func (t *transport) Lease() kv.LeaseTransport { return t.lease }

// Feedback implements aspen.Transport.
func (t *transport) Feedback() kv.FeedbackTransport { return t.feedback }
