package gossip

import (
	"github.com/arya-analytics/aspen/internal/kv/operation"
	"github.com/arya-analytics/x/transport"
	"github.com/arya-analytics/x/version"
)

type Feedback struct {
	Key     []byte
	Version version.Counter
}

type State byte

const (
	// StateInfected means that a node should actively gossip the update
	// to other nodes in the cluster.
	StateInfected State = iota
	// StateRecovered means that a node should no longer gossip the update.
	StateRecovered
)

type Operation = operation.Operation

type Message struct {
	Feedback   []Feedback
	Operations Operations
}

func (msg Message) variant() messageVariant {
	if msg.Feedback == nil && msg.Operations != nil {
		return messageVariantSync
	}
	if msg.Feedback != nil && msg.Operations == nil {
		return messageVariantAck2
	}
	panic("invalid message")
}

type Transport = transport.Unary[Message, Message]

type messageVariant byte

const (
	messageVariantSync messageVariant = iota
	messageVariantAck2
)
