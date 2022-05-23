package cluster

import (
	"github.com/arya-analytics/aspen/internal/node"
	"sync"
)

type State struct {
	MU     sync.RWMutex
	Nodes  node.Group
	HostID node.ID
}

func (s *State) setNode(n node.Node) {
	s.MU.Lock()
	defer s.MU.Unlock()
	s.Nodes[n.ID] = n
}

func (s *State) snapshot() node.Group {
	s.MU.RLock()
	defer s.MU.RUnlock()
	return s.Nodes.Copy()
}

func (s *State) host() node.Node {
	s.MU.RLock()
	defer s.MU.RUnlock()
	return s.Nodes[s.HostID]
}

func (s *State) merge(other node.Group) (res node.Group) {
	s.MU.Lock()
	defer s.MU.Unlock()
	for otherID, otherNode := range other {
		internalNode, ok := s.Nodes[otherID]
		// If otherNode's version is more recent than internal, or we don't have it, replace
		// internalNode with otherNode.
		if !ok || otherNode.Heartbeat.OlderThan(*internalNode.Heartbeat) {
			s.Nodes[otherID] = otherNode
			// If we have a new version that
			//  We have a new version that other,
			// Then:
			//	send our version back to the client.
		} else if otherNode.Heartbeat.YoungerThan(*internalNode.Heartbeat) {
			// If we have a newer version than the client, send our version back.
			res[otherID] = internalNode
		}
	}
	return res
}

func (s *State) digest(digests node.Digests) (node.Digests, node.Group) {
	s.MU.Lock()
	s.MU.Unlock()
	resNodes, resDigests := make(node.Group), make(node.Digests)
	for _, dig := range digests {
		internalNode, ok := s.Nodes[dig.ID]
		// If we have the internalNode and our version is more recent, add it to our response.
		if ok && internalNode.Heartbeat.OlderThan(*dig.Heartbeat) {
			resNodes[dig.ID] = internalNode
		}
		// If we don't have the internalNode, add our requested digests.
		if !ok {
			resDigests[dig.ID] = dig
		}
	}
	// If we have a node that the client doesn't know about, send it back to them.
	for id, internalNode := range s.Nodes {
		if _, ok := digests[id]; !ok {
			resNodes[id] = internalNode
		}
	}
	return resDigests, resNodes
}
