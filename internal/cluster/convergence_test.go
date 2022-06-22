package cluster_test

import (
	"fmt"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/cluster/pledge"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/kv/memkv"
	"github.com/arya-analytics/x/rand"
	"github.com/arya-analytics/x/shutdown"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"time"
)

type newConvergenceVars struct {
	clusterSize          int
	convergenceThreshold time.Duration
	gossipInterval       time.Duration
	peerAddrCount        int
}

var progressiveNewConvergence = []newConvergenceVars{
	{
		clusterSize:          4,
		convergenceThreshold: time.Second * 1,
		gossipInterval:       time.Millisecond * 10,
		peerAddrCount:        1,
	},
	{
		clusterSize:          10,
		convergenceThreshold: time.Second * 3,
		gossipInterval:       time.Millisecond * 10,
		peerAddrCount:        3,
	},
}

var _ = Describe("Convergence", Serial, Ordered, func() {
	var (
		gossipNet *tmock.Network[gossip.Message, gossip.Message]
		pledgeNet *tmock.Network[node.ID, node.ID]
		logger    *zap.SugaredLogger
		sd        shutdown.Shutdown
		exp       alamos.Experiment
	)

	BeforeAll(func() {
		exp = alamos.New("convergence_test")
	})

	BeforeEach(func() {
		sd = shutdown.New()
		gossipNet = tmock.NewNetwork[gossip.Message, gossip.Message]()
		pledgeNet = tmock.NewNetwork[node.ID, node.ID]()
		log := zap.NewNop()
		logger = log.Sugar()
	})

	Context("Serial Pledge", func() {

		p := alamos.NewParametrize(alamos.IterVars(progressiveNewConvergence))
		p.Template(func(i int, values newConvergenceVars) {

			It(fmt.Sprintf("Should converge a cluster size of %v in %v "+
				"at an interval of %v seconds and a peer address count of %v",
				values.clusterSize, values.convergenceThreshold,
				values.gossipInterval, values.peerAddrCount), func() {
				var (
					clusters  []cluster.Cluster
					addresses []address.Address
				)
				subExp := alamos.Sub(exp, fmt.Sprintf("convergence_test_%v", i))
				for i := 0; i < values.clusterSize; i++ {
					gossipT := gossipNet.RouteUnary("")
					pledgeT := pledgeNet.RouteUnary(gossipT.Address)
					cluster, err := cluster.Join(
						ctx,
						gossipT.Address,
						rand.SubSlice[address.Address](addresses, values.peerAddrCount),
						cluster.Config{
							Logger:     logger,
							Pledge:     pledge.Config{Transport: pledgeT, RetryInterval: values.gossipInterval, RetryScale: 1},
							Gossip:     gossip.Config{Transport: gossipT, Interval: values.gossipInterval, Shutdown: sd},
							Storage:    memkv.Open(),
							Experiment: alamos.Sub(subExp, fmt.Sprintf("cluster_%v", i)),
						},
					)
					Expect(err).ToNot(HaveOccurred())
					addresses = append(addresses, gossipT.Address)
					clusters = append(clusters, cluster)
				}
				Expect(sd.ShutdownAfter(values.convergenceThreshold)).To(Succeed())
				for _, cluster_ := range clusters {
					Expect(cluster_.Nodes()).To(HaveLen(values.clusterSize))
				}
			})

		})

		p.Construct()
	})
})
