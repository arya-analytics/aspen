package kv

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
)

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

func New(clust cluster.Cluster, kvEngine kv_.KV, cfg Config) KV {
	return &kv{KV: kvEngine, Config: cfg.Merge(DefaultConfig())}
}

func (k *kv) SetWithLease(key []byte, leaseholder node.ID, value []byte) error {
	return k.exec.setWithLease(key, leaseholder, value)
}

func (k *kv) Set(key []byte, value []byte) error {
	return k.exec.setWithLease(key, DefaultLeaseholder, value)
}

func Open(cfg Config) (KV, error) {
	ctx := confluence.DefaultContext()
	pipeline := confluence.NewPipeline[Batch]()

	pipeline.Segment(versionFilterAddr, newVersionFilter(cfg, persistAddr, feedbackSenderAddr))
	pipeline.Segment(persistAddr, newPersist(cfg))
	pipeline.Segment(operationSenderAddr, newOperationSender(cfg))
	pipeline.Segment(operationReceiverAddr, newOperationReceiver(cfg))
	pipeline.Segment(feedbackSenderAddr, newFeedbackSender(cfg))
	pipeline.Segment(feedbackReceiverAddr, newFeedbackReceiver(cfg))
	pipeline.Segment(recoveryTransformAddr, newRecoveryTransform(cfg))
	pipeline.Segment(leaseSenderAddr, newLeaseSender(cfg))
	pipeline.Segment(leaseReceiverAddr, newLeaseReceiver(cfg))
	pipeline.Segment(leaseProxyAddr, newLeaseProxy(cfg, persistAddr, leaseSenderAddr))

	builder := pipeline.NewRouteBuilder()
	builder.Route(confluence.MultiRouter[Batch]{
		FromAddresses: []address.Address{operationReceiverAddr},
		ToAddresses:   []address.Address{versionFilterAddr},
		Stitch:        confluence.StitchLinear,
	})
	builder.Route(confluence.MultiRouter[Batch]{
		FromAddresses: []address.Address{versionFilterAddr},
		ToAddresses:   []address.Address{persistAddr, feedbackSenderAddr},
		Stitch:        confluence.StitchWeave,
	})
	builder.Route(confluence.UnaryRouter[Batch]{
		FromAddr: feedbackReceiverAddr,
		ToAddr:   recoveryTransformAddr,
	})
	builder.Route(confluence.UnaryRouter[Batch]{
		FromAddr: emitterAddr,
		ToAddr:   operationSenderAddr,
	})

	pipeline.Flow(ctx)

	kve := &kv{Config: cfg, KV: cfg.Engine}

	return kve, builder.Error()
}
