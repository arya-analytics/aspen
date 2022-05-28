package kv

import (
	"github.com/arya-analytics/aspen/internal/node"
)

type OperationsMessage struct {
	Sender     node.ID
	Operations Operations
}

type FeedbackMessage struct {
	Sender   node.ID
	Feedback []Feedback
}

type LeaseMessage struct {
	Operation Operation
}
