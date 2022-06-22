package cluster_test

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/clustermock"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/x/address"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Cluster", func() {
	var builder *clustermock.Builder
	BeforeEach(func() {
		builder = clustermock.NewBuilder(cluster.Config{
			Gossip: gossip.Config{Interval: 5 * time.Millisecond},
		})
	})

	Describe("Node", func() {

		It("Should return a node by its ID", func() {
			c1, err := builder.New(cluster.Config{})
			Expect(err).ToNot(HaveOccurred())
			c2, err := builder.New(cluster.Config{})
			Expect(err).ToNot(HaveOccurred())
			time.Sleep(10 * time.Millisecond)
			Expect(c2.Node(c1.HostID())).To(Equal(c1.Host()))
			Expect(c1.Node(c2.HostID())).To(Equal(c2.Host()))
		})

	})

	Describe("Resolve", func() {

		It("Should resolve the address of a node by its ID", func() {
			c1, err := builder.New(cluster.Config{})
			Expect(err).ToNot(HaveOccurred())
			c2, err := builder.New(cluster.Config{})
			Expect(err).ToNot(HaveOccurred())
			time.Sleep(10 * time.Millisecond)
			Expect(c2.Resolve(c1.HostID())).To(Equal(address.Address("localhost:0")))
			Expect(c1.Resolve(c2.HostID())).To(Equal(address.Address("localhost:1")))
		})

	})

	Describe("Config", func() {

		It("Should return the cluster configuration", func() {
			c1, err := builder.New(cluster.Config{StorageKey: []byte("crazy")})
			Expect(err).ToNot(HaveOccurred())
			Expect(c1.Config().StorageKey).To(Equal([]byte("crazy")))
		})

	})

})
