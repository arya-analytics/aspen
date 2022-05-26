package operation

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/version"
	"sync"
)

type versionSegment struct {
	kv    kv.KV
	state struct {
		mu       sync.RWMutex
		versions map[node.ID]version.Counter
	}
	confluence.Filter[Operation]
}

func newVersionSegment(kv kv.KV) confluence.Segment[Operation] {
	s := &versionSegment{kv: kv}
	s.state.versions = make(map[node.ID]version.Counter)
	s.Filter.Filter = s.filter
	return s
}

func (vc *versionSegment) filter(op Operation) bool {
	if vc.olderVersion(op) {
		vc.setVersion(op.Leaseholder, op.Version)
		return true
	}
	return false
}

func (vc *versionSegment) olderVersion(op Operation) bool {
	if op.Error != nil {
		return true
	}
	vc.state.mu.RLock()
	defer vc.state.mu.RUnlock()
	ver, ok := vc.state.versions[op.Leaseholder]
	if !ok {
		var err error
		ver, err = vc.getFromKV(op.Key)
		if err == kv.ErrNotFound {
			return true
		}
		if err != nil {
			op.Error = err
			return false
		}
	}
	return ver.OlderThan(op.Version)
}

func (vc *versionSegment) setVersion(leaseholder node.ID, version version.Counter) {
	vc.state.mu.Lock()
	defer vc.state.mu.Unlock()
	vc.state.versions[leaseholder] = version
}

func (vc *versionSegment) getFromKV(key []byte) (version.Counter, error) {
	key, err := Key(key)
	if err != nil {
		return 0, err
	}
	op := &Operation{}
	return op.Version, kv.Load(vc.kv, key, op)
}
