package aspen

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/kv/pebblekv"
	"github.com/cockroachdb/pebble"
)

func Join(dirname string, addr address.Address, peers []address.Address, opts ...Option) (DB, error) {
	o := newOptions(dirname, addr, peers, opts...)

	if err := openKV(o); err != nil {
		return nil, err
	}

	if err := validateOptions(o); err != nil {
		return nil, err
	}

	o.logger.Debug("configuration")
	o.logger.Debug(o.String())

	if err := o.transport.Configure(o.addr, o.shutdown); err != nil {
		return nil, err
	}

	clust, err := cluster.Join(o.ctx, o.addr, o.peerAddresses, o.cluster)
	if err != nil {
		return nil, err
	}

	o.kv.Cluster = clust

	kve, err := kv.Open(o.kv)
	if err != nil {
		return nil, err
	}

	return &db{
		Cluster: clust,
		KV:      kve,
	}, nil
}

func openKV(opts *options) error {
	if opts.kv.Engine == nil {
		pebbleDB, err := pebble.Open(opts.dirname, &pebble.Options{})
		opts.kv.Engine = pebblekv.Wrap(pebbleDB)
		return err
	}
	return nil
}
