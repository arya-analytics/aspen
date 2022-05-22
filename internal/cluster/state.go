package cluster

import (
	"github.com/arya-analytics/aspen/internal/node"
	"sync"
)

type State struct {
	mu     sync.RWMutex
	Nodes  node.Group
	HostID node.ID
}

func (s *State) setNode(n node.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Nodes[n.ID] = n
}

func (s *State) snapshot() node.Group {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Nodes.Copy()
}

func (s *State) host() node.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Nodes[s.HostID]
}
