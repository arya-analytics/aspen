package aspen

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/aspen/transport/grpc"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/shutdown"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"go.uber.org/zap"
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
	// logger is the witness of it all.
	logger *zap.SugaredLogger
	// experiment is the experiment that aspen attaches its metrics to.
	experiment alamos.Experiment
}

func (o *options) String() string { return o.Report().String() }

func (o *options) Report() alamos.Report {
	return alamos.Report{
		"dirname":   o.dirname,
		"addr":      o.addr,
		"peers":     o.peerAddresses,
		"bootstrap": o.bootstrap,
	}
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
	alamos.AttachReporter(o.experiment, "aspen", alamos.Debug, o)
	return o
}

func validateOptions(o *options) error {
	if !o.bootstrap && len(o.peerAddresses) == 0 {
		return errors.New("peer addresses must be provided when not boostrapping a cluster")
	}
	return nil
}

func mergeDefaultOptions(o *options) {
	def := defaultOptions()

	// |||| CONTEXT ||||

	if o.ctx == nil {
		o.ctx = def.ctx
	}

	// |||| DIRNAME ||||

	if o.dirname == "" {
		o.dirname = def.dirname
	}

	// |||| KV ||||

	o.kv = o.kv.Merge(def.kv)

	// |||| CLUSTER ||||

	o.cluster.Experiment = o.experiment

	// |||| SHUTDOWN ||||

	o.cluster.Shutdown = o.shutdown
	o.kv.Shutdown = o.shutdown

	// |||| TRANSPORT ||||

	if o.transport == nil {
		o.transport = def.transport
	}
	o.cluster.Gossip.Transport = o.transport.Cluster()
	o.cluster.Pledge.Transport = o.transport.Pledge()
	o.kv.OperationsTransport = o.transport.Operations()
	o.kv.LeaseTransport = o.transport.Lease()
	o.kv.FeedbackTransport = o.transport.Feedback()

	// |||| LOGGER ||||

	if o.logger == nil {
		o.logger = def.logger
	}
	o.cluster.Logger = o.logger.Named("cluster")
	o.kv.Logger = o.logger.Named("kv")

}

func defaultOptions() *options {
	logger, _ := zap.NewProduction()
	return &options{
		ctx:       context.Background(),
		dirname:   "",
		cluster:   cluster.DefaultConfig(),
		kv:        kv.DefaultConfig(),
		transport: grpc.New(),
		logger:    logger.Sugar(),
	}
}

func Bootstrap() Option { return func(o *options) { o.bootstrap = true } }

func WithLogger(logger *zap.SugaredLogger) Option { return func(o *options) { o.logger = logger } }

func WithExperiment(experiment alamos.Experiment) Option {
	return func(o *options) { o.experiment = experiment }
}
