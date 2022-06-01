package node_test

import (
	"bytes"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/version"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Node", func() {
	Describe("Flush", func() {
		It("Should correctly flush the node to binary", func() {
			n := node.Node{
				ID:      1,
				Address: address.Address("localhost:1"),
				State:   node.StateHealthy,
				Heartbeat: version.Heartbeat{
					Generation: 1,
					Version:    1,
				},
			}
			b := bytes.NewBuffer([]byte{})
			err := n.Flush(b)
			Expect(err).ToNot(HaveOccurred())
			nn := &node.Node{}
			err = nn.Load(b)
			Expect(err).ToNot(HaveOccurred())
			Expect(nn.ID).To(Equal(node.ID(1)))
		})
	})

})
