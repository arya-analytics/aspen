package kv

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
)

const (
	versionFilterAddr     = "versionFilter"
	persistAddr           = "persist"
	emitterAddr           = "emitter"
	operationSenderAddr   = "opSender"
	opReceiverAddr        = "opReceiver"
	feedbackSenderAddr    = "feedbackSenderAddr"
	feedbackReceiverAddr  = "feedbackReceiverAddr"
	recoveryTransformAddr = "recoveryTransform"
)

type Segment = confluence.Segment[Batch]

func newPipe(cfg Config) (confluence.Segment[Batch], error) {
	pipeline := confluence.NewPipeline[Batch]()
	pipeline.Segment(versionFilterAddr, newVersionFilter(cfg))
	pipeline.Segment(persistAddr, newPersist(cfg))
	pipeline.Segment(emitterAddr, newEmitter(cfg))
	pipeline.Segment(operationSenderAddr, newOperationSender(cfg))
	pipeline.Segment(opReceiverAddr, newOperationReceiver(cfg))
	pipeline.Segment(feedbackSenderAddr, newFeedbackSender(cfg))
	pipeline.Segment(feedbackReceiverAddr, newFeedbackReceiver(cfg))
	pipeline.Segment(recoveryTransformAddr, newRecoveryTransform(cfg))
	c := confluence.NewRouteCatcher(pipeline)
	c.Route(confluence.MultiRouter[Batch]{
		FromAddresses: []address.Address{opReceiverAddr},
		ToAddresses:   []address.Address{versionFilterAddr},
		Stitch:        confluence.StitchLinear,
	})
	c.Route(confluence.MultiRouter[Batch]{
		FromAddresses: []address.Address{versionFilterAddr},
		ToAddresses:   []address.Address{persistAddr, feedbackSenderAddr},
		Stitch:        confluence.StitchWeave,
	})
	c.Route(confluence.UnaryRouter[Batch]{
		FromAddr: feedbackReceiverAddr,
		ToAddr:   recoveryTransformAddr,
	})
	c.Route(confluence.MultiRouter[Batch]{
		FromAddresses: []address.Address{persistAddr, recoveryTransformAddr},
		ToAddresses:   []address.Address{emitterAddr},
	})
	c.Route(confluence.UnaryRouter[Batch]{
		FromAddr: emitterAddr,
		ToAddr:   operationSenderAddr,
	})

	return pipeline, c.Error()
}
