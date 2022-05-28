package gossip_test

import (
	"github.com/arya-analytics/aspen/internal/cluster/store"
	"github.com/arya-analytics/aspen/internal/node"
	shut "github.com/arya-analytics/x/shutdown"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"time"

	"github.com/arya-analytics/aspen/internal/cluster/gossip"
)

var _ = Describe("OperationSender", func() {
	var (
		net    *tmock.Network[gossip.Message, gossip.Message]
		logger *zap.Logger
	)
	BeforeEach(func() {
		net = tmock.NewNetwork[gossip.Message, gossip.Message]()
		logger = zap.NewNop()
	})
	Describe("Two Node", func() {
		var (
			t1, t2, t3      *tmock.Unary[gossip.Message, gossip.Message]
			nodes, nodesTwo node.Group
			sOne            store.Store
			g1              *gossip.Gossip
			sd              shut.Shutdown
		)
		BeforeEach(func() {
			t1, t2, t3 = net.Route(""), net.Route(""), net.Route("")
			nodes = node.Group{1: {ID: 1, Address: t1.Address}, 2: {ID: 2, Address: t2.Address}}
			sOne = store.New()
			sOne.SetState(store.State{Nodes: nodes, HostID: 1})
			nodesTwo = nodes.Copy()
			nodesTwo[3] = node.Node{ID: 3, Address: t3.Address, State: node.StateDead}
			sTwo := store.New()
			sTwo.SetState(store.State{Nodes: nodesTwo, HostID: 2})
			sd = shut.New()
			g1 = gossip.New(sOne, gossip.Config{Transport: t1, Logger: logger, Shutdown: sd, Interval: 5 * time.Millisecond})
			gossip.New(sTwo, gossip.Config{Transport: t2, Logger: logger, Shutdown: sd, Interval: 5 * time.Millisecond})
		})
		It("Should converge after a single exchange", func() {
			Expect(g1.GossipOnce(ctx)).To(Succeed())
			Expect(sOne.GetState().Nodes).To(HaveLen(3))
			Expect(sOne.GetState().Nodes[1].Heartbeat.Version).To(Equal(uint32(1)))
			Expect(sOne.GetState().Nodes[3].State).To(Equal(node.StateDead))
			Expect(sOne.GetState().Nodes[2].Heartbeat.Version).To(Equal(uint32(0)))
		})
		It("Should gossip at the correct interval", func() {
			g1.Gossip(ctx)
			time.Sleep(12 * time.Millisecond)
			Expect(sd.Shutdown()).To(Succeed())
			Expect(sOne.GetState().Nodes).To(HaveLen(3))
			Expect(sOne.GetState().Nodes[1].Heartbeat.Version).To(Equal(uint32(2)))
			Expect(sOne.GetState().Nodes[3].State).To(Equal(node.StateDead))
			Expect(sOne.GetState().Nodes[2].Heartbeat.Version).To(Equal(uint32(0)))
		})
	})
})
