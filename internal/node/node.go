package node

import (
	"encoding/gob"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/version"
	"io"
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

func (n Node) Flush(w io.Writer) error {
	g := gob.NewEncoder(w)
	return g.Encode(n)
}

func (n *Node) Load(r io.Reader) error { return gob.NewDecoder(r).Decode(&n) }

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
