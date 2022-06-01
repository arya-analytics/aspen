package aspen

import (
	"context"
	"errors"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/shutdown"
	"github.com/cockroachdb/pebble/vfs"
)

type Option func(*options)

type options struct {
	// ctx is the global context for the DB.
	ctx context.Context
	// dirname is the directory where aspen will store its data.
	// this option is ignored if a custom kv.Config.Engine is set.
	dirname string
	// addr sets the address for the host node.
	addr address.Address
	// peerAddresses sets the addresses for the peers of the host node.
	peerAddresses []address.Address
	// shutdown is used to safely shutdown aspen operations.
	shutdown shutdown.Shutdown
	// cluster gives the configuration for gossiping cluster state.
	cluster cluster.Config
	// kv gives the configuration for KV options.
	kv kv.Config
	// fs sets the filesystem to be used for storing data. This option is ignored
	// if a custom kv.Config.Engine is set.
	fs vfs.FS
	// bootstrap is a boolean used to indicate whether to bootstrap a new cluster.
	bootstrap bool
	// transport is the default transport package for the messages that aspen exchanges.
	// this setting overrides all other transport settings in sub-configs.
	transport Transport
}

func newOptions(dirname string, addr address.Address, peers []address.Address, opts ...Option) *options {
	o := &options{}
	o.dirname = dirname
	o.addr = addr
	o.peerAddresses = peers
	o.shutdown = shutdown.New()
	for _, opt := range opts {
		opt(o)
	}
	mergeDefaultOptions(o)
	return o
}

func validateOptions(o *options) error {
	if !o.bootstrap && len(o.peerAddresses) == 0 {
		return errors.New("peer addresses must be provided when not boostrapping a cluster")
	}

}

func mergeDefaultOptions(o *options) {
	def := defaultOptions()

	// |||| DIRNAME ||||

	if o.dirname == "" {
		o.dirname = def.dirname
	}

	// |||| KV ||||

	o.kv = o.kv.Merge(def.kv)

	// |||| CLUSTER ||||

	o.cluster = o.cluster.Merge(def.cluster)
	if o.cluster.Storage == nil {
		o.cluster.Storage = o.kv.Engine
	}

	// |||| SHUTDOWN ||||

	o.cluster.Shutdown = o.shutdown
	o.kv.Shutdown = o.shutdown

	// |||| TRANSPORT ||||

	o.cluster.Gossip.Transport = o.transport.Cluster()
	o.cluster.Pledge.Transport = o.transport.Pledge()
	o.kv.OperationsTransport = o.transport.Operations()
	o.kv.LeaseTransport = o.transport.Lease()
	o.kv.FeedbackTransport = o.transport.Feedback()

}

func defaultOptions() *options {
	return &options{
		dirname: "",
		cluster: cluster.DefaultConfig(),
		kv:      kv.DefaultConfig(),
	}
}

func Bootstrap() Option { return func(o *options) { o.bootstrap = true } }
