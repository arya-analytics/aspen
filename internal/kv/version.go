package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/version"
	"go.uber.org/zap"
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
	confluence.BatchSwitch[batch]
}

func newVersionFilter(cfg Config, acceptedTo address.Address, rejectedTo address.Address) segment {
	s := &versionFilter{Config: cfg, acceptedTo: acceptedTo, rejectedTo: rejectedTo}
	s.state.versions = make(map[node.ID]version.Counter)
	s.BatchSwitch.Switch = s._switch
	return s
}

func (vc *versionFilter) _switch(ctx confluence.Context, b batch) map[address.Address]batch {
	vc.Logger.Debug("versionFilter")
	var rejected, accepted batch
	rejected.errors = b.errors
	rejected.sender = b.sender
	accepted.errors = b.errors
	rejected.sender = b.sender
	for _, op := range b.operations {
		if vc.olderVersion(op) {
			vc.setVersion(op.Leaseholder, op.Version)
			accepted.operations = append(accepted.operations, op)
		} else {
			rejected.operations = append(rejected.operations, op)
		}
	}
	vc.Logger.Debug("versionFilter",
		zap.Int("accepted", len(accepted.operations)),
		zap.Int("rejected", len(rejected.operations)),
		zap.String("acceptedTo", string(vc.acceptedTo)),
		zap.String("rejectedTo", string(vc.rejectedTo)),
	)
	resMap := map[address.Address]batch{}
	if len(accepted.operations) > 0 {
		resMap[vc.acceptedTo] = accepted
	}
	if len(rejected.operations) > 0 {
		resMap[vc.rejectedTo] = rejected
	}
	return resMap
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
	key, err := metadataKey(key)
	if err != nil {
		return 0, err
	}
	op := &Operation{}
	return op.Version, kv_.Load(vc.Engine, key, op)
}

const versionCounterKey = "ver"

type versionAssigner struct {
	Config
	counter *kv_.PersistedCounter
	confluence.Transform[batch]
}

func newVersionAssigner(cfg Config) (segment, error) {
	c, err := kv_.NewPersistedCounter(cfg.Engine, []byte(versionCounterKey))
	v := &versionAssigner{Config: cfg, counter: c}
	v.Transform.Transform = v.transform
	return v, err
}

func (va *versionAssigner) transform(ctx confluence.Context, b batch) (batch, bool) {
	va.Logger.Info("versionAssigner")
	latestVer := va.counter.Value()
	if _, err := va.counter.Increment(int64(len(b.operations))); err != nil {
		b.errors <- err
		close(b.errors)
		return batch{}, false
	}
	for i, op := range b.operations {
		op.Version = version.Counter(latestVer + int64(i))
		va.Logger.Debug("versionAssigner",
			zap.String("key", string(op.Key)),
			zap.Int64("version", int64(op.Version)),
		)
	}
	return b, true
}
