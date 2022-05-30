package clustermock_test

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/clustermock"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/node"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Clustermock", func() {
	Describe("Builder", func() {
		It("Should provision a set of cluster APIs correctly", func() {
			cfg := cluster.Config{Gossip: gossip.Config{Interval: 50 * time.Millisecond}}
			builder := clustermock.NewBuilder(cfg)
			c1, err := builder.New(cfg)
			Expect(err).ToNot(HaveOccurred())
			Expect(c1.HostID()).To(Equal(node.ID(1)))
			c2, err := builder.New(cfg)
			Expect(err).ToNot(HaveOccurred())
			Expect(c2.HostID()).To(Equal(node.ID(2)))
			Expect(c2.Nodes()).To(HaveLen(2))
		})
	})
})
