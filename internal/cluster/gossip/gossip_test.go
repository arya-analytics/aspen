package gossip_test

import (
	"context"
	"github.com/arya-analytics/aspen/internal/cluster/store"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/signal"
	tmock "github.com/arya-analytics/x/transport/mock"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"time"

	"github.com/arya-analytics/aspen/internal/cluster/gossip"
)

var _ = Describe("OperationSender", func() {
	var (
		net    *tmock.Network[gossip.Message, gossip.Message]
		logger *zap.SugaredLogger
	)
	BeforeEach(func() {
		net = tmock.NewNetwork[gossip.Message, gossip.Message]()
		logger = zap.NewNop().Sugar()
	})
	Describe("Two Node", func() {
		var (
			t1, t2, t3      *tmock.Unary[gossip.Message, gossip.Message]
			nodes, nodesTwo node.Group
			sOne            store.Store
			g1              *gossip.Gossip
			gossipCtx       signal.Context
			shutdown        context.CancelFunc
		)
		BeforeEach(func() {
			t1, t2, t3 = net.RouteUnary(""), net.RouteUnary(""), net.RouteUnary("")
			nodes = node.Group{1: {ID: 1, Address: t1.Address}, 2: {ID: 2, Address: t2.Address}}
			sOne = store.New()
			sOne.SetState(store.State{Nodes: nodes, HostID: 1})
			nodesTwo = nodes.Copy()
			nodesTwo[3] = node.Node{ID: 3, Address: t3.Address, State: node.StateDead}
			sTwo := store.New()
			sTwo.SetState(store.State{Nodes: nodesTwo, HostID: 2})
			gossipCtx, shutdown = signal.WithCancel(ctx)
			signal.LogTransient(gossipCtx, logger)
			var err error
			g1, err = gossip.New(sOne, gossip.Config{Transport: t1, Logger: logger, Interval: 5 * time.Millisecond})
			Expect(err).ToNot(HaveOccurred())
			_, err = gossip.New(sTwo, gossip.Config{Transport: t2, Logger: logger, Interval: 5 * time.Millisecond})
			Expect(err).ToNot(HaveOccurred())
		})
		It("Should converge after a single exchange", func() {
			Expect(g1.GossipOnce(gossipCtx)).To(Succeed())
			Expect(sOne.CopyState().Nodes).To(HaveLen(3))
			Expect(sOne.CopyState().Nodes[1].Heartbeat.Version).To(Equal(uint32(1)))
			Expect(sOne.CopyState().Nodes[3].State).To(Equal(node.StateDead))
			Expect(sOne.CopyState().Nodes[2].Heartbeat.Version).To(Equal(uint32(0)))
			shutdown()
			Expect(errors.Is(gossipCtx.WaitOnAll(), context.Canceled)).To(BeTrue())
		})
		It("Should gossip at the correct interval", func() {
			g1.GoGossip(gossipCtx)
			time.Sleep(12 * time.Millisecond)
			shutdown()
			Expect(errors.Is(gossipCtx.WaitOnAll(), context.Canceled)).To(BeTrue())
			Expect(sOne.CopyState().Nodes).To(HaveLen(3))
			Expect(sOne.CopyState().Nodes[1].Heartbeat.Version).To(Equal(uint32(2)))
			Expect(sOne.CopyState().Nodes[3].State).To(Equal(node.StateDead))
			Expect(sOne.CopyState().Nodes[2].Heartbeat.Version).To(Equal(uint32(0)))
		})
		It("Should return an error when an invalid message is received", func() {
			_, err := t1.Send(context.Background(), t2.Address, gossip.Message{})
			Expect(err).To(HaveOccurred())
		})
	})
})
