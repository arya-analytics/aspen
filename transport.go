package aspen

import (
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/cluster/pledge"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/shutdown"
)

type Transport interface {
	Configure(addr address.Address, sd shutdown.Shutdown) error
	Pledge() pledge.Transport
	Cluster() gossip.Transport
	Operations() kv.OperationsTransport
	Lease() kv.LeaseTransport
	Feedback() kv.FeedbackTransport
}
