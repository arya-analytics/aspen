package operation

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kv"
)

func NewSegment(kv kv.KV) confluence.Segment[Operation] {
	pipe := confluence.NewPipeline[Operation]()
	_ = pipe.RouteInletTo("version")
	pipe.Segment("version", newVersionSegment(kv))
	pipe.Segment("persist", newPersistSegment(kv))
	_ = pipe.Route(confluence.UnaryRouter[Operation]{FromAddr: "version", ToAddr: "persist", Capacity: 1})
	_ = pipe.RouteOutletFrom("persist")
	return pipe
}
