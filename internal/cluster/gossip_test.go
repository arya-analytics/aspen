package cluster_test

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/shutdown"
	tmock "github.com/arya-analytics/x/transport/mock"
	"github.com/arya-analytics/x/version"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"time"
)

var _ = Describe("Gossip", func() {
	var (
		net    *tmock.Network[cluster.Message, cluster.Message]
		logger *zap.Logger
	)
	BeforeEach(func() {
		net = tmock.NewNetwork[cluster.Message, cluster.Message]()
		logger, _ = zap.NewDevelopment()
	})
	Describe("Two Node", func() {
		It("Should gossip correctly", func() {
			t1, t2, t3 := net.Route(""), net.Route(""), net.Route("")
			logger = zap.NewNop()
			nodes := node.Group{
				1: {ID: 1, Address: t1.Address, Heartbeat: &version.Heartbeat{}},
				2: {ID: 2, Address: t2.Address, Heartbeat: &version.Heartbeat{}},
			}
			stateOne := &cluster.State{Nodes: nodes, HostID: 1}
			nodesTwo := nodes.Copy()
			nodesTwo[3] = node.Node{ID: 3, Address: t3.Address, State: node.StateDead, Heartbeat: &version.Heartbeat{}}
			stateTwo := &cluster.State{Nodes: nodesTwo, HostID: 2}
			shut := shutdown.New()
			g1 := cluster.NewGossip(stateOne, cluster.Config{GossipTransport: t1, GossipInterval: 10 * time.Microsecond, Shutdown: shut, Logger: logger})
			g2 := cluster.NewGossip(stateTwo, cluster.Config{GossipTransport: t2, GossipInterval: 10 * time.Microsecond, Shutdown: shut, Logger: logger})
			ctx := context.Background()
			Expect(g1.GossipOnce(ctx)).To(Succeed())
			Expect(g2.GossipOnce(ctx)).To(Succeed())
			Expect(stateOne.Nodes).To(HaveLen(3))
			Expect(stateOne.Nodes[1].Heartbeat.Version).To(Equal(uint32(1)))
			Expect(stateOne.Nodes[3].State).To(Equal(node.StateDead))
			Expect(stateOne.Nodes[2].Heartbeat.Version).To(Equal(uint32(1)))
		})
	})
})
