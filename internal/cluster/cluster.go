package cluster

import "github.com/arya-analytics/x/address"

func Join(peerAddresses []address.Address) Cluster {

}

func FromSnapshot() Cluster {}

type Cluster interface {
	Snapshot() Nodes
	Host() Node
}
