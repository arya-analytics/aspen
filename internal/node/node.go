package node

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/filter"
)

type ID uint32

func (id ID) Greater(o ID) bool {
	return id > o
}

func (id ID) Less(o ID) bool {
	return id < o
}

func (id ID) Equal(o ID) bool {
	return id == o
}

type Node struct {
	ID ID

	Address address.Address

	State State
}

type State byte

const (
	StateHealthy State = iota
	StateSuspect
	StateDead
	StateLeft
)

type Group map[ID]Node

func (n Group) WhereState(state State) Group {
	return n.Where(func(_ ID, n Node) bool { return n.State == state })
}

func (n Group) Where(cond func(ID, Node) bool) Group {
	return filter.Map(n, cond)
}

func (n Group) Addresses() (addresses []address.Address) {
	for _, v := range n {
		addresses = append(addresses, v.Address)
	}
	return addresses
}
