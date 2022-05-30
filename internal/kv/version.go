package kv

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/version"
	"go.uber.org/zap"
)

// |||||| FILTER ||||||

type versionFilter struct {
	Config
	memKV      kv_.KV
	acceptedTo address.Address
	rejectedTo address.Address
	confluence.BatchSwitch[batch]
}

func newVersionFilter(cfg Config, acceptedTo address.Address, rejectedTo address.Address) segment {
	s := &versionFilter{Config: cfg, acceptedTo: acceptedTo, rejectedTo: rejectedTo, memKV: cfg.Engine}
	s.BatchSwitch.Switch = s._switch
	return s
}

func (vc *versionFilter) _switch(ctx confluence.Context, b batch) map[address.Address]batch {
	var (
		rejected = batch{errors: b.errors, sender: b.sender}
		accepted = batch{errors: b.errors, sender: b.sender}
	)
	for _, op := range b.operations {
		if vc.filter(op) {
			if err := vc.set(op); err != nil {
				ctx.ErrC <- err
			}
			accepted.operations = append(accepted.operations, op)
		} else {
			rejected.operations = append(rejected.operations, op)
		}
	}
	resMap := map[address.Address]batch{}
	if len(accepted.operations) > 0 {
		resMap[vc.acceptedTo] = accepted
	}
	if len(rejected.operations) > 0 {
		resMap[vc.rejectedTo] = rejected
	}
	vc.Logger.Debug("version filter",
		zap.Stringer("host", vc.Cluster.HostID()),
		zap.Int("accepted", len(accepted.operations)),
		zap.Int("rejected", len(rejected.operations)),
	)
	return resMap
}

func (vc *versionFilter) set(op Operation) error {
	return kv_.Flush(vc.memKV, op.Key, op.Digest())
}

func (vc *versionFilter) filter(op Operation) bool {
	dig, err := getDigestFromKV(vc.memKV, op.Key)
	if err != nil {
		dig, err = getDigestFromKV(vc.Engine, op.Key)
		if err != nil {
			return err == kv_.ErrNotFound
		}
	}
	if op.Version.YoungerThan(dig.Version) {
		return false
	}
	if op.Version.EqualTo(dig.Version) {
		return op.Leaseholder > dig.Leaseholder
	}
	return true
}

func getDigestFromKV(kve kv_.KV, key []byte) (Digest, error) {
	dig := &Digest{}
	key, err := digestKey(key)
	if err != nil {
		return *dig, err
	}
	return *dig, kv_.Load(kve, key, dig)
}

// |||||| ASSIGNER ||||||

const versionCounterKey = "ver"

type versionAssigner struct {
	Config
	counter *kv_.PersistedCounter
	confluence.Transform[batch]
}

func newVersionAssigner(cfg Config) (segment, error) {
	c, err := kv_.NewPersistedCounter(cfg.Engine, []byte(versionCounterKey))
	v := &versionAssigner{Config: cfg, counter: c}
	v.Transform.Transform = v.assign
	return v, err
}

func (va *versionAssigner) assign(ctx confluence.Context, b batch) (batch, bool) {
	latestVer := va.counter.Value()
	if _, err := va.counter.Increment(int64(len(b.operations))); err != nil {
		va.Logger.Error("failed to assign version", zap.Error(err))
		b.errors <- err
		ctx.ErrC <- err
		return batch{}, false
	}
	for i := range b.operations {
		b.operations[i].Version = version.Counter(latestVer + int64(i) + 1)
	}
	return b, true
}
