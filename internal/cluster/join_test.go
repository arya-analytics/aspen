package cluster_test

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/cluster/pledge"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	kvc "github.com/arya-analytics/x/kv"
	kvmock "github.com/arya-analytics/x/kv/kvmock"
	"github.com/arya-analytics/x/shutdown"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"time"
)

var _ = Describe("Join", func() {
	var (
		gossipNet *tmock.Network[gossip.Message, gossip.Message]
		pledgeNet *tmock.Network[node.ID, node.ID]
		logger    *zap.Logger
		kv        kvc.KV
	)
	BeforeEach(func() {
		gossipNet = tmock.NewNetwork[gossip.Message, gossip.Message]()
		pledgeNet = tmock.NewNetwork[node.ID, node.ID]()
		logger = zap.NewNop()
		kv = kvmock.New()
	})
	It("Should correctly join the cluster", func() {
		By("Initializing the cluster correctly")
		gossipT1 := gossipNet.Route("")
		pledgeT1 := pledgeNet.Route(gossipT1.Address)
		sd := shutdown.New()
		clusterOne, err := cluster.Join(
			ctx,
			gossipT1.Address,
			[]address.Address{},
			cluster.Config{
				Logger: logger,
				Pledge: pledge.Config{
					Logger:    logger,
					Transport: pledgeT1,
				},
				Gossip: gossip.Config{
					Logger:    logger,
					Transport: gossipT1,
					Interval:  100 * time.Millisecond,
					Shutdown:  sd,
				},
				Storage: kv,
			},
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(clusterOne.Host().ID).To(Equal(node.ID(1)))

		By("Pledging a new node to the cluster")
		gossipT2 := gossipNet.Route("")
		pledgeT2 := pledgeNet.Route(gossipT2.Address)
		clusterTwo, err := cluster.Join(
			ctx,
			gossipT2.Address,
			[]address.Address{gossipT1.Address},
			cluster.Config{
				Logger: logger,
				Pledge: pledge.Config{
					Logger:    logger,
					Transport: pledgeT2,
				},
				Gossip: gossip.Config{
					Logger:    logger,
					Transport: gossipT2,
					Interval:  100 * time.Millisecond,
					Shutdown:  sd,
				},
				Storage: kv,
			},
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(clusterTwo.Host().ID).To(Equal(node.ID(2)))

		By("Converging cluster state through gossip")
		time.Sleep(300 * time.Millisecond)
		Expect(sd.Shutdown()).To(Succeed())
		Expect(clusterOne.Members()).To(HaveLen(2))
		Expect(clusterTwo.Members()).To(HaveLen(2))
	})
})
