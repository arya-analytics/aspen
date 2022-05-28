package kv

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/kv/kvmock"
	"time"
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
}

func New(clust cluster.Cluster, kvEngine kv_.KV, cfg Config) KV {
	return &kv{KV: kvEngine, Config: cfg.Merge(DefaultConfig())}
}

func (k *kv) SetWithLease(key []byte, leaseholder node.ID, value []byte) error {
	return nil
}

func wire() {
	kv := kvmock.New()
	ver := newVersionFilter(kv)
	persist := newPersist(kv)
	emitter := newEmitter(1 * time.Second)

	opSender := &operationSender{}
	opReceiver := &OperationReceiver{}

	fbSender := &FeedbackSender{}
	fbReceiver := &FeedbackReceiver{}

	recovery := &recovery{}

	pipe := confluence.NewPipeline[Batch]()

	pipe.Segment("version", ver)
	pipe.Segment("persist", persist)
	pipe.Segment("emitter", emitter)
	pipe.Segment("opSender", opSender)
	pipe.Segment("opReceiver", opReceiver)
	pipe.Segment("feedbackSenderAddr", fbSender)
	pipe.Segment("fbReceiver", fbReceiver)
	pipe.Segment("recovery", recovery)

	pipe.Route(confluence.MultiRouter[Batch]{
		FromAddresses: []address.Address{"opReceiver"},
		ToAddresses:   []address.Address{"version"},
		Stitch:        confluence.StitchLinear,
	})

	pipe.Route(confluence.MultiRouter[Batch]{
		FromAddresses: []address.Address{"version"},
		ToAddresses:   []address.Address{"persist"},
		Stitch:        confluence.StitchWeave,
	})

	pipe.Route(confluence.MultiRouter[Batch]{
		FromAddresses: []address.Address{"persist", "recovery"},
		ToAddresses:   []address.Address{"emitter"},
		Stitch:        confluence.StitchLinear,
	})

	pipe.Route(confluence.UnaryRouter[Batch]{
		FromAddr: "emitter",
		ToAddr:   "opSender",
	})

	pipe.Route(confluence.UnaryRouter[Batch]{
		FromAddr: "fbReceiver",
		ToAddr:   "recovery",
	})

}
