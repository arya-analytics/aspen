package gossip

import "github.com/arya-analytics/x/version"

type Feedback struct {
	Key     []byte
	Version version.Counter
}

type AckMessage struct {
	Updates UpdateP
}

type Updates