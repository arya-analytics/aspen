package node

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/version"
	"strconv"
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

func (id ID) String() string { return "Node " + strconv.Itoa(int(id)) }

type Node struct {
	ID ID

	Address address.Address

	State State

	Heartbeat version.Heartbeat
}

func (n Node) Digest() Digest { return Digest{ID: n.ID, Heartbeat: n.Heartbeat} }

type State uint32

const (
	StateHealthy State = iota
	StateSuspect
	StateDead
	StateLeft
)

type Digest struct {
	ID        ID
	Heartbeat version.Heartbeat
}

type Digests map[ID]Digest
