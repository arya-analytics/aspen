package aspen

import (
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/cluster/pledge"
	"github.com/arya-analytics/aspen/internal/kv"
)

type Transport struct {
	Pledge     pledge.Transport
	Cluster    gossip.Transport
	Operations kv.OperationsTransport
	Lease      kv.LeaseTransport
	Feedback   kv.FeedbackTransport
}
