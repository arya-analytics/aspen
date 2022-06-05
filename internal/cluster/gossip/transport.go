package gossip

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/transport"
)

type Transport = transport.Unary[Message, Message]

type Message struct {
	Digests node.Digests
	Nodes   node.Group
}

func (msg Message) variant() messageVariant {
	if msg.Nodes == nil && msg.Digests != nil {
		return messageVariantSync
	}
	if msg.Digests == nil && msg.Nodes != nil {
		return messageVariantAck2
	}
	return messageVariantInvalid
}

type messageVariant byte

const (
	messageVariantSync = iota
	messageVariantAck2
	messageVariantInvalid
)
