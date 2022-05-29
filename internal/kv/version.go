package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/version"
	"sync"
)

type versionFilter struct {
	Config
	state struct {
		mu       sync.RWMutex
		versions map[node.ID]version.Counter
	}
	acceptedTo address.Address
	rejectedTo address.Address
	confluence.BatchSwitch[Batch]
}

func newVersionFilter(cfg Config, acceptedTo address.Address, rejectedTo address.Address) Segment {
	s := &versionFilter{Config: cfg}
	s.state.versions = make(map[node.ID]version.Counter)
	s.BatchSwitch.Switch = s._switch
	return s
}

func (vc *versionFilter) _switch(batch Batch) map[address.Address]Batch {
	var rejected, accepted Batch
	for _, op := range batch.Operations {
		if vc.olderVersion(op) {
			vc.setVersion(op.Leaseholder, op.Version)
			accepted.Operations = append(accepted.Operations, op)
		}
		rejected.Operations = append(rejected.Operations, op)
	}
	return map[address.Address]Batch{vc.acceptedTo: accepted, vc.rejectedTo: rejected}
}

func (vc *versionFilter) olderVersion(op Operation) bool {
	vc.state.mu.RLock()
	defer vc.state.mu.RUnlock()
	ver, ok := vc.state.versions[op.Leaseholder]
	if !ok {
		var err error
		ver, err = vc.getFromKV(op.Key)
		if err != nil {
			return err == kv_.ErrNotFound
		}
	}
	return ver.OlderThan(op.Version)
}

func (vc *versionFilter) setVersion(leaseholder node.ID, version version.Counter) {
	vc.state.mu.Lock()
	defer vc.state.mu.Unlock()
	vc.state.versions[leaseholder] = version
}

func (vc *versionFilter) getFromKV(key []byte) (version.Counter, error) {
	key, err := Key(key)
	if err != nil {
		return 0, err
	}
	op := &Operation{}
	return op.Version, kv_.Load(vc.Engine, key, op)
}
