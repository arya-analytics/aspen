package node

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/version"
)

type Cluster interface {
	Initialize() error
	Resolve(id ID) (address.Address, bool)
	Retrieve(id ID) (Node, bool)
	Members() Nodes
	Join([]address.Address) (ID, error)
}

type Node struct {
	ID ID

	Address address.Address

	Cluster Cluster

	heartbeat *version.Heartbeat
}

func (n Node) Digest() Digest {
	return Digest{Heartbeat: n.heartbeat}
}

type Digest struct {
	Heartbeat *version.Heartbeat
}

type Nodes map[ID]Node

type Digests map[ID]Digest

type Merger struct {
	Nodes Nodes
}

// Merge merges two maps of Nodes according to the following algorithm:
//
// 1. Replaces a node with otherNode if otherNode's heartbeat
//  is older.
// 2. If other has not seen node, or otherNode's heartbeat is younger,
//	adds node to an output map of Nodes.
//
func (m Merger) Merge(other Nodes) Nodes {
	oNodes := make(Nodes)
	for id, node := range m.Nodes {
		otherNode, ok := other[id]
		if !ok || otherNode.heartbeat.YoungerThan(*node.heartbeat) {
			oNodes[id] = node
			continue
		}
		if otherNode.heartbeat.OlderThan(*node.heartbeat) {
			m.Nodes[id] = otherNode
		}
	}
	return oNodes
}

// Filter returns a map of Nodes and digests according to the following algorithm:
//
// 1. If digests does not contain node OR digest heartbeat is younger than node,
//     adds node to output map of Nodes.
//
// 2. If digest heartbeat is older than node, adds digest to output map of Digests.
//
// 3. If digests contains a node not in Merger.Nodes, adds the digest to the output map of Digests.
func (m Merger) Filter(digests Digests) (Nodes, Digests) {
	filteredNodes, filteredDigests := make(Nodes), make(Digests)
	for id, node := range m.Nodes {
		digest, ok := digests[id]
		if !ok || digest.Heartbeat.YoungerThan(*node.heartbeat) {
			filteredNodes[id] = node
		}
		if digest.Heartbeat.OlderThan(*node.heartbeat) {
			filteredDigests[id] = digest
		}
	}
	for id, digest := range digests {
		if _, ok := m.Nodes[id]; !ok {
			filteredDigests[id] = digest
		}
	}
	return filteredNodes, filteredDigests
}
