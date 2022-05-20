package node

import "github.com/arya-analytics/x/address"

type Cluster interface {
	Initialize() error
	Resolve(id ID) (address.Address, bool)
	Retrieve(id ID) (Node, bool)
	Members() Nodes
	Join([]address.Address) (ID, error)
}

type defaultCluster struct {
	members Nodes
}
