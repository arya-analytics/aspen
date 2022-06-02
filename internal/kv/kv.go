package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/shutdown"
)

// Writer is a writable key-value store.
type Writer interface {
	// SetWithLease is similar to Set, but also takes an id for a leaseholder node.
	// If the leaseholder node is not the host, the request will be forwarded to the
	// leaseholder for execution. Only the leaseholder node will be able to perform
	// set and delete operations on the requested key.
	SetWithLease(key []byte, leaseholder node.ID, value []byte) error
	// Writer represents the same interface to a typical key-value store.
	// kv.Write.Set operations call SetWithLease internally and mark the leaseholder as
	// the host.
	kv_.Writer
}

type (
	// Reader is a readable key-value store.
	Reader = kv_.Reader
	// Closer is a key-value store that can be closed. Block until all pending
	// operations have persisted to disk.
	Closer = kv_.Closer
)

// KV is a readable and writable key-value store.
type KV interface {
	Writer
	Reader
	Closer
}

type kv struct {
	kv_.KV
	Config
	exec *executor
}

// SetWithLease implements KV.
func (k *kv) SetWithLease(key []byte, leaseholder node.ID, value []byte) error {
	return k.exec.setWithLease(key, leaseholder, value)
}

// Set implements KV.
func (k *kv) Set(key []byte, value []byte) error {
	return k.SetWithLease(key, DefaultLeaseholder, value)
}

// Delete implements KV.
func (k *kv) Delete(key []byte) error { return k.exec.delete(key) }

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

func Open(cfg Config) (KV, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfg = cfg.Merge(DefaultConfig())

	va, err := newVersionAssigner(cfg)
	if err != nil {
		return nil, err
	}

	exec := newExecutor(cfg)

	ctx := confluence.DefaultContext()
	ctx.Shutdown = cfg.Shutdown
	ctx.ErrC = make(chan error, 10)

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
		FromAddr: executorAddr,
		ToAddr:   leaseAssignerAddr,
		Capacity: 1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		FromAddresses: []address.Address{leaseAssignerAddr, leaseReceiverAddr},
		ToAddresses:   []address.Address{leaseProxyAddr},
		Stitch:        confluence.StitchLinear,
		Capacity:      1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		FromAddresses: []address.Address{leaseProxyAddr},
		ToAddresses:   []address.Address{versionAssignerAddr, leaseSenderAddr},
		Stitch:        confluence.StitchWeave,
		Capacity:      1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		FromAddresses: []address.Address{versionAssignerAddr, operationReceiverAddr, operationSenderAddr},
		ToAddresses:   []address.Address{versionFilterAddr},
		Stitch:        confluence.StitchLinear,
		Capacity:      1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		FromAddresses: []address.Address{versionFilterAddr},
		ToAddresses:   []address.Address{feedbackSenderAddr, persistAddr},
		Stitch:        confluence.StitchWeave,
		Capacity:      1,
	})

	builder.Route(confluence.UnaryRouter[batch]{
		FromAddr: feedbackReceiverAddr,
		ToAddr:   recoveryTransformAddr,
		Capacity: 1,
	})

	builder.Route(confluence.MultiRouter[batch]{
		FromAddresses: []address.Address{persistAddr, recoveryTransformAddr},
		ToAddresses:   []address.Address{emitterAddr},
		Stitch:        confluence.StitchLinear,
		Capacity:      1,
	})

	builder.Route(confluence.UnaryRouter[batch]{
		FromAddr: emitterAddr,
		ToAddr:   operationSenderAddr,
		Capacity: 1,
	})

	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case err = <-ctx.ErrC:
				cfg.Logger.Errorw("kv pipeline error", "err", err)
			}
		}
	})

	pipeline.Flow(ctx)

	return &kv{Config: cfg, KV: cfg.Engine, exec: exec}, builder.Error()
}
