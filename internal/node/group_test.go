package node_test

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/version"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Group", func() {
	Describe("Merge", func() {
		It("Should merge two groups to return the most up to date values", func() {
			gOne := node.Group{
				1: {ID: 1, Heartbeat: version.Heartbeat{Generation: 1, Version: 1}},
				2: {ID: 2, Heartbeat: version.Heartbeat{Generation: 2, Version: 6}},
				3: {ID: 3, Heartbeat: version.Heartbeat{Generation: 1, Version: 3}},
				5: {ID: 5, Heartbeat: version.Heartbeat{Generation: 1, Version: 17}},
				6: {ID: 6, Heartbeat: version.Heartbeat{Generation: 1, Version: 1}},
			}
			gTwo := node.Group{
				1: {ID: 1, Heartbeat: version.Heartbeat{Generation: 1, Version: 12}},
				2: {ID: 2, Heartbeat: version.Heartbeat{Generation: 2, Version: 6}},
				3: {ID: 3, Heartbeat: version.Heartbeat{Generation: 2, Version: 1}},
				4: {ID: 4, Heartbeat: version.Heartbeat{Generation: 1, Version: 1}},
				5: {ID: 5, Heartbeat: version.Heartbeat{Generation: 1, Version: 4}},
			}
			merged := node.Merge(gTwo, gOne)
			reverseMerge := node.Merge(gOne, gTwo)
			Expect(merged).To(Equal(node.Group{
				1: {ID: 1, Heartbeat: version.Heartbeat{Generation: 1, Version: 12}},
				2: {ID: 2, Heartbeat: version.Heartbeat{Generation: 2, Version: 6}},
				3: {ID: 3, Heartbeat: version.Heartbeat{Generation: 2, Version: 1}},
				4: {ID: 4, Heartbeat: version.Heartbeat{Generation: 1, Version: 1}},
				5: {ID: 5, Heartbeat: version.Heartbeat{Generation: 1, Version: 17}},
				6: {ID: 6, Heartbeat: version.Heartbeat{Generation: 1, Version: 1}},
			}))
			Expect(reverseMerge).To(Equal(merged))
		})
	})

})
