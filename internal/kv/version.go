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
	memKV      kvx.DB
	acceptedTo address.Address
	rejectedTo address.Address
	confluence.BatchSwitch[BatchRequest, BatchRequest]
}

func newVersionFilter(cfg Config, acceptedTo address.Address, rejectedTo address.Address) segment {
	s := &versionFilter{Config: cfg, acceptedTo: acceptedTo, rejectedTo: rejectedTo, memKV: cfg.Engine}
	s.BatchSwitch.ApplySwitch = s._switch
	return s
}

func (vc *versionFilter) _switch(
	ctx signal.Context,
	b BatchRequest,
	o map[address.Address]BatchRequest,
) error {
	var (
		rejected = BatchRequest{Sender: b.Sender, done: b.done}
		accepted = BatchRequest{Sender: b.Sender, done: b.done}
	)
	for _, op := range b.Operations {
		if vc.filter(op) {
			if err := vc.set(op); err != nil {
				ctx.Transient() <- err
			}
			accepted.Operations = append(accepted.Operations, op)
		} else {
			rejected.Operations = append(rejected.Operations, op)
		}
	}
	if len(accepted.Operations) > 0 {
		o[vc.acceptedTo] = accepted
	}
	if len(rejected.Operations) > 0 {
		o[vc.rejectedTo] = rejected
	}
	vc.Logger.Debugw("version filter",
		"host", vc.Cluster.HostID(),
		"accepted", len(accepted.Operations),
		"rejected", len(rejected.Operations),
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
			return err == kvx.NotFound
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

func getDigestFromKV(kve kvx.DB, key []byte) (Digest, error) {
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
	confluence.LinearTransform[BatchRequest, BatchRequest]
}

func newVersionAssigner(cfg Config) (segment, error) {
	c, err := kvx.NewPersistedCounter(cfg.Engine, []byte(versionCounterKey))
	v := &versionAssigner{Config: cfg, counter: c}
	v.LinearTransform.ApplyTransform = v.assign
	return v, err
}

func (va *versionAssigner) assign(ctx signal.Context, br BatchRequest) (BatchRequest, bool, error) {
	latestVer := va.counter.Value()
	if _, err := va.counter.Increment(int64(br.size())); err != nil {
		va.Logger.Errorw("failed to assign version", "err", err)
		return BatchRequest{}, false, nil
	}
	for i := range br.Operations {
		br.Operations[i].Version = version.Counter(latestVer + int64(i) + 1)
	}
	return br, true, nil
}
