package kv

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kvx "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/version"
)

// |||||| FILTER ||||||

type versionFilter struct {
	Config
	memKV      kvx.KV
	acceptedTo address.Address
	rejectedTo address.Address
	confluence.BatchSwitch[batch, batch]
}

func newVersionFilter(cfg Config, acceptedTo address.Address, rejectedTo address.Address) segment {
	s := &versionFilter{Config: cfg, acceptedTo: acceptedTo, rejectedTo: rejectedTo, memKV: cfg.Engine}
	s.BatchSwitch.ApplySwitch = s._switch
	return s
}

func (vc *versionFilter) _switch(
	ctx signal.Context,
	b batch,
	o map[address.Address]batch,
) error {
	var (
		rejected = batch{errors: b.errors, sender: b.sender}
		accepted = batch{errors: b.errors, sender: b.sender}
	)
	for _, op := range b.operations {
		if vc.filter(op) {
			if err := vc.set(op); err != nil {
				ctx.Transient() <- err
			}
			accepted.operations = append(accepted.operations, op)
		} else {
			rejected.operations = append(rejected.operations, op)
		}
	}
	if len(accepted.operations) > 0 {
		o[vc.acceptedTo] = accepted
	}
	if len(rejected.operations) > 0 {
		o[vc.rejectedTo] = rejected
	}
	vc.Logger.Debugw("version filter",
		"host", vc.Cluster.HostID(),
		"accepted", len(accepted.operations),
		"rejected", len(rejected.operations),
	)
	return nil
}

func (vc *versionFilter) set(op Operation) error {
	return kvx.Flush(vc.memKV, op.Key, op.Digest())
}

func (vc *versionFilter) filter(op Operation) bool {
	dig, err := getDigestFromKV(vc.memKV, op.Key)
	if err != nil {
		dig, err = getDigestFromKV(vc.Engine, op.Key)
		if err != nil {
			return err == kvx.ErrNotFound
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

func getDigestFromKV(kve kvx.KV, key []byte) (Digest, error) {
	dig := &Digest{}
	key, err := digestKey(key)
	if err != nil {
		return *dig, err
	}
	return *dig, kvx.Load(kve, key, dig)
}

// |||||| ASSIGNER ||||||

const versionCounterKey = "ver"

type versionAssigner struct {
	Config
	counter *kvx.PersistedCounter
	confluence.LinearTransform[batch, batch]
}

func newVersionAssigner(cfg Config) (segment, error) {
	c, err := kvx.NewPersistedCounter(cfg.Engine, []byte(versionCounterKey))
	v := &versionAssigner{Config: cfg, counter: c}
	v.ApplyTransform = v.assign
	return v, err
}

func (va *versionAssigner) assign(ctx signal.Context, b batch) (batch, bool, error) {
	latestVer := va.counter.Value()
	if _, err := va.counter.Increment(int64(len(b.operations))); err != nil {
		va.Logger.Errorw("failed to assign version", "err", err)
		b.errors <- err
		return batch{}, false, nil
	}
	for i := range b.operations {
		b.operations[i].Version = version.Counter(latestVer + int64(i) + 1)
	}
	return b, true, nil
}
