package cluster

import (
	. "github.com/arya-analytics/aspen/internal/node"
	"sync"
)

type state struct {
	mu    sync.RWMutex
	nodes Group
}

func (s *state) setNode(n Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[n.ID] = n
}

func (s *state) snapshot() Group {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodes.Copy()
}
