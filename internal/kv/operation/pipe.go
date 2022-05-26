package operation

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kv"
)

func NewSegment(kv kv.KV) confluence.Segment[Operation] {
	comp := &confluence.Composite[Operation]{}
	_ = comp.RouteInletTo("version")
	comp.SetSegment("version", newVersionSegment(kv))
	_ = comp.Route("version", "persist", 1)
	comp.SetSegment("persist", newPersistSegment(kv))
	_ = comp.RouteOutletFrom("persist")
	return comp
}
