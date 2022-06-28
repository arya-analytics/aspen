package mock

import (
	"github.com/arya-analytics/aspen"
	"time"
)

func NewMemBuilder(defaultOpts ...aspen.Option) *Builder {
	propConfig := aspen.PropagationConfig{
		PledgeRetryInterval:   10 * time.Millisecond,
		PledgeRetryScale:      1,
		ClusterGossipInterval: 50 * time.Millisecond,
		KVGossipInterval:      50 * time.Millisecond,
	}
	return &Builder{
		DefaultOptions: append([]aspen.Option{
			aspen.WithTransport(NewNetwork().NewTransport()),
			aspen.MemBacked(),
			aspen.WithPropagationConfig(propConfig),
		}, defaultOpts...),
		Nodes: make(map[aspen.NodeID]NodeInfo),
	}
}
