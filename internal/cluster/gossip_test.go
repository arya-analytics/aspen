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
	log "github.com/sirupsen/logrus"
	"time"
)

var _ = Describe("Gossip", func() {
	var (
		net *tmock.Network[cluster.Message, cluster.Message]
	)
	BeforeEach(func() {
		net = tmock.NewNetwork[cluster.Message, cluster.Message]()
	})
	It("Should gossip correctly", func() {
		log.Info("Starting")
		t1 := net.Route("")
		t2 := net.Route("")
		t3 := net.Route("")
		nodes := node.Group{
			1: {ID: 1, Address: t1.Address, State: node.StateHealthy, Heartbeat: &version.Heartbeat{}},
			2: {ID: 2, Address: t2.Address, State: node.StateHealthy, Heartbeat: &version.Heartbeat{}},
		}
		stateOne := &cluster.State{Nodes: nodes, HostID: 1}
		nodesTwo := nodes.Copy()
		nodesTwo[3] =
			node.Node{
				ID: 3, Address: t3.Address, State: node.StateDead, Heartbeat: &version.Heartbeat{},
			}
		stateTwo := &cluster.State{Nodes: nodesTwo, HostID: 2}
		shut := shutdown.New()
		g1 := cluster.NewGossip(stateOne, cluster.Config{GossipTransport: t1, GossipInterval: 5 * time.Millisecond, Shutdown: shut})
		g2 := cluster.NewGossip(stateTwo, cluster.Config{GossipTransport: t2, GossipInterval: 5 * time.Millisecond, Shutdown: shut})
		ctx := context.Background()
		log.Info(stateOne)
		g1.Gossip(ctx)
		g2.Gossip(ctx)

		time.Sleep(100 * time.Millisecond)

		log.Info(stateOne)
		Expect(shut.Shutdown()).To(Succeed())
	})
})
