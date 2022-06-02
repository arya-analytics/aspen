package node

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/filter"
)

type Group map[ID]Node

func (n Group) WhereState(state State) Group {
	return n.Where(func(_ ID, n Node) bool { return n.State == state })
}
func (n Group) WhereNot(ids ...ID) Group {
	return n.Where(func(id ID, _ Node) bool { return !filter.ElementOf(ids, id) })
}

func (n Group) WhereActive() Group {
	return n.Where(func(_ ID, n Node) bool { return n.State != StateLeft })
}

func (n Group) Where(cond func(ID, Node) bool) Group { return filter.Map(n, cond) }

func (n Group) Addresses() (addresses []address.Address) {
	for _, v := range n {
		addresses = append(addresses, v.Address)
	}
	return addresses
}

func (n Group) Digests() Digests {
	dig := make(Digests, len(n))
	for id, node := range n {
		dig[id] = node.Digest()
	}
	return dig
}

func (n Group) Copy() Group { return filter.Map(n, func(_ ID, _ Node) bool { return true }) }
