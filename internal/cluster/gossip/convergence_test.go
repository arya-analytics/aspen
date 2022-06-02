package gossip_test

import (
	"context"
	"fmt"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/cluster/store"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/rand"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"sync"
)

type convergenceVars struct {
	nodeCount            int
	convergenceThreshold int
	initialViewCount     int
}

var progressiveConvergence = []convergenceVars{
	{
		nodeCount:            4,
		initialViewCount:     2,
		convergenceThreshold: 10,
	},
	{
		nodeCount:            10,
		initialViewCount:     2,
		convergenceThreshold: 10,
	},
	{
		nodeCount:            30,
		initialViewCount:     2,
		convergenceThreshold: 10,
	},
	{
		nodeCount:            100,
		initialViewCount:     5,
		convergenceThreshold: 10,
	},
}

var _ = Describe("Convergence", func() {
	var (
		net    *tmock.Network[gossip.Message, gossip.Message]
		logger *zap.SugaredLogger
	)
	BeforeEach(func() {
		net = tmock.NewNetwork[gossip.Message, gossip.Message]()
		logger = zap.NewNop().Sugar()
	})
	p := alamos.NewParametrize(alamos.IterVars(progressiveConvergence))
	p.Template(func(i int, values convergenceVars) {
		It(fmt.Sprintf("Should converge store across %v nodes in %v cycles",
			values.nodeCount,
			values.convergenceThreshold,
		), func() {
			group := make(node.Group)
			configs := make(map[node.ID]gossip.Config)
			for i := 1; i <= values.nodeCount; i++ {
				t := net.Route("")
				n := node.Node{ID: node.ID(i), Address: t.Address}
				group[n.ID] = n
				configs[n.ID] = gossip.Config{Transport: t, Logger: logger}
			}
			var (
				gossips []*gossip.Gossip
				stores  []store.Store
			)
			for _, n := range group {
				subNodes := rand.SubMap(group.WhereNot(n.ID), values.initialViewCount)
				subNodes[n.ID] = n
				s := store.New()
				s.SetState(store.State{Nodes: subNodes, HostID: n.ID})
				g, err := gossip.New(s, configs[n.ID])
				Expect(err).ToNot(HaveOccurred())
				gossips = append(gossips, g)
				stores = append(stores, s)
			}
			ctx := context.Background()
			for i := 0; i < values.convergenceThreshold; i++ {
				wg := sync.WaitGroup{}
				for _, g := range gossips {
					wg.Add(1)
					go func(g *gossip.Gossip) {
						defer wg.Done()
						Expect(g.GossipOnce(ctx)).To(Succeed())
					}(g)
				}
				wg.Wait()
			}
			for _, s := range stores {
				Expect(s.CopyState().Nodes).To(HaveLen(values.nodeCount))
			}
		})
	})
	p.Construct()
})
