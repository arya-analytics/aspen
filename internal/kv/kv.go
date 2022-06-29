package kv

import (
	"fmt"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kvx "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
)

// Writer is a writable key-value store.
type Writer interface {
	// SetWithLease is similar to Set, but also takes an id for a leaseholder node.
	// If the leaseholder node is not the host, the request will be forwarded to the
	// leaseholder for execution. Only the leaseholder node will be able to perform
	// set and delete operations on the requested key. It is safe to modify the contents
	// of key and value after SetWithLease returns.
	SetWithLease(key []byte, leaseholder node.ID, value []byte) error
	// Writer represents the same interface to a typical key-value store.
	// kv.Write.Set operations call SetWithLease internally and mark the leaseholder as
	// the host.
	kvx.Writer
}

type (
	// Reader is a readable key-value store.
	Reader = kvx.Reader
)

// KV is a readable and writable key-value store.
type KV interface {
	Writer
	Reader
	// Stringer returns a description of the KV store.
	fmt.Stringer
}

type kv struct {
	kvx.KV
	Config
	exec *executor
}

// SetWithLease implements KV.
func (k *kv) SetWithLease(key []byte, leaseholder node.ID, value []byte) error {
	return k.exec.setWithLease(key, leaseholder, value)
}

// Set implements KV.
func (k *kv) Set(key []byte, value []byte, opts ...interface{}) error {
	lease := DefaultLeaseholder
	if len(opts) == 1 {
		l, ok := opts[0].(node.ID)
		if !ok {
			return errors.New("[aspen] - leaseholder option must be of type node.ID")
		}
		lease = l
	}
	return k.SetWithLease(key, lease, value)
}

// Delete implements KV.
func (k *kv) Delete(key []byte) error { return k.exec.delete(key) }

// String implements KV.
func (k *kv) String() string {
	return fmt.Sprintf("aspen.kv{} backed by %s", k.Config.Engine)
}

const (
	versionFilterAddr     = "versionFilter"
	versionAssignerAddr   = "versionAssigner"
	persistAddr           = "persist"
	emitterAddr           = "emitter"
	operationSenderAddr   = "opSender"
	operationReceiverAddr = "opReceiver"
	feedbackSenderAddr    = "feedbackSender"
	feedbackReceiverAddr  = "feedbackReceiver"
	recoveryTransformAddr = "recoveryTransform"
	leaseSenderAddr       = "leaseSender"
	leaseReceiverAddr     = "leaseReceiver"
	leaseProxyAddr        = "leaseProxy"
	leaseAssignerAddr     = "leaseAssigner"
	executorAddr          = "executor"
)

func Open(ctx signal.Context, cfg Config) (KV, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfg = cfg.Merge(DefaultConfig())

	va, err := newVersionAssigner(cfg)
	if err != nil {
		return nil, err
	}

	exec := newExecutor(cfg)

	emitterStore := newEmitter(cfg)

	pipeline := confluence.NewPipeline[batch]()
	pipeline.Segment(executorAddr, exec)
	pipeline.Segment(leaseReceiverAddr, newLeaseReceiver(cfg))
	pipeline.Segment(leaseAssignerAddr, newLeaseAssigner(cfg))
	pipeline.Segment(leaseProxyAddr, newLeaseProxy(cfg, versionAssignerAddr, leaseSenderAddr))
	pipeline.Segment(operationReceiverAddr, newOperationReceiver(cfg, emitterStore))
	pipeline.Segment(versionFilterAddr, newVersionFilter(cfg, persistAddr, feedbackSenderAddr))
	pipeline.Segment(versionAssignerAddr, va)
	pipeline.Segment(leaseSenderAddr, newLeaseSender(cfg))
	pipeline.Segment(persistAddr, newPersist(cfg))
	pipeline.Segment(emitterAddr, emitterStore)
	pipeline.Segment(operationSenderAddr, newOperationSender(cfg))
	pipeline.Segment(feedbackSenderAddr, newFeedbackSender(cfg))
	pipeline.Segment(feedbackReceiverAddr, newFeedbackReceiver(cfg))
	pipeline.Segment(recoveryTransformAddr, newRecoveryTransform(cfg))

	builder := pipeline.NewRouteBuilder()

	builder.Route(confluence.UnaryRouter[batch]{
		SourceTarget: executorAddr,
		SinkTarget:   leaseAssignerAddr,
		Capacity:     1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		SourceTargets: []address.Address{leaseAssignerAddr, leaseReceiverAddr},
		SinkTargets:   []address.Address{leaseProxyAddr},
		Stitch:        confluence.StitchUnary,
		Capacity:      1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		SourceTargets: []address.Address{leaseProxyAddr},
		SinkTargets:   []address.Address{versionAssignerAddr, leaseSenderAddr},
		Stitch:        confluence.StitchWeave,
		Capacity:      1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		SourceTargets: []address.Address{versionAssignerAddr, operationReceiverAddr, operationSenderAddr},
		SinkTargets:   []address.Address{versionFilterAddr},
		Stitch:        confluence.StitchUnary,
		Capacity:      1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		SourceTargets: []address.Address{versionFilterAddr},
		SinkTargets:   []address.Address{feedbackSenderAddr, persistAddr},
		Stitch:        confluence.StitchWeave,
		Capacity:      1,
	})

	builder.Route(confluence.UnaryRouter[batch]{
		SourceTarget: feedbackReceiverAddr,
		SinkTarget:   recoveryTransformAddr,
		Capacity:     1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		SourceTargets: []address.Address{persistAddr, recoveryTransformAddr},
		SinkTargets:   []address.Address{emitterAddr},
		Stitch:        confluence.StitchUnary,
		Capacity:      1,
	})

	builder.Route(confluence.UnaryRouter[batch]{
		SourceTarget: emitterAddr,
		SinkTarget:   operationSenderAddr,
		Capacity:     1,
	})

	builder.PanicIfErr()

	pipeline.Flow(ctx)

	return &kv{
		Config: cfg,
		KV:     cfg.Engine,
		exec:   exec,
	}, nil
}
