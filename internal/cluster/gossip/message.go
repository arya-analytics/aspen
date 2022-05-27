package gossip

import "github.com/arya-analytics/aspen/internal/node"

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
	panic("invalid message")
}

type messageVariant byte

const (
	messageVariantSync = iota
	messageVariantAck2
)
