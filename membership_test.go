package aspen_test

import (
	"github.com/arya-analytics/aspen"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/filter"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"sync"
)

var _ = Describe("Membership", Serial, Ordered, func() {
	var (
		logger *zap.SugaredLogger
	)
	BeforeEach(func() {
		log := zap.NewNop()
		logger = log.Sugar()
	})
	Describe("Bootstrap Cluster", func() {
		It("Should correctly bootstrap a cluster", func() {
			db, err := aspen.Open(
				"",
				"localhost:22546",
				[]aspen.Address{},
				aspen.Bootstrap(),
				aspen.WithLogger(logger),
				aspen.MemBacked(),
			)

			By("Opening without error")
			Expect(err).ToNot(HaveOccurred())

			By("Assigning a valid ID of 1")
			Expect(db.HostID()).To(Equal(node.ID(1)))

			By("Adding itself to the node list.")
			Expect(db.Nodes()).To(HaveLen(1))

			By("By setting its state to healthy")
			Expect(db.Host().State).To(Equal(aspen.Healthy))

			Expect(db.Close()).To(Succeed())
		})
		It("Should correctly bootstrap a cluster with peers provided", func() {
			db, err := aspen.Open(
				"",
				"localhost:22546",
				[]aspen.Address{"localhost:22547"},
				aspen.WithLogger(logger),
				aspen.MemBacked(),
				aspen.Bootstrap(),
			)
			defer func() { Expect(db.Close()).To(Succeed()) }()

			By("Opening without error")
			Expect(err).ToNot(HaveOccurred())

			By("Assigning a valid ID of 1")
			Expect(db.HostID()).To(Equal(node.ID(1)))
		})
		It("Should correctly join a node that is already looking for peers", func() {
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				db, err := aspen.Open(
					"",
					"localhost:22546",
					[]aspen.Address{"localhost:22547"},
					aspen.WithLogger(logger),
					aspen.MemBacked(),
				)
				defer func() { Expect(db.Close()).To(Succeed()) }()

				By("Joining the second node to the cluster without error")
				Expect(err).ToNot(HaveOccurred())

				By("Assigning a unique ID of 2")
				Expect(db.HostID()).To(Equal(node.ID(2)))
			}()
			db, err := aspen.Open(
				"",
				"localhost:22547",
				[]aspen.Address{},
				aspen.WithLogger(logger),
				aspen.MemBacked(),
				aspen.Bootstrap(),
			)
			defer func() { Expect(db.Close()).To(Succeed()) }()

			By("Joining the first node to the cluster without error")
			Expect(err).ToNot(HaveOccurred())

			By("Assigning a unique ID of 1")
			Expect(db.HostID()).To(Equal(node.ID(1)))
			wg.Wait()
		})
	})
	Describe("Concurrent Pledges", func() {
		It("Should correctly join many nodes to the cluster concurrently", func() {
			numNodes := 10
			wg := sync.WaitGroup{}
			wg.Add(numNodes)
			var (
				addresses = address.NewLocalFactory(22546).NextN(numNodes)
				ids       = make([]node.ID, numNodes)
				dbs       = make([]aspen.DB, numNodes)
			)
			for i := 0; i < numNodes; i++ {
				go func(i int) {
					defer GinkgoRecover()
					defer wg.Done()
					opts := []aspen.Option{aspen.WithLogger(logger), aspen.MemBacked()}
					if i == 0 {
						opts = append(opts, aspen.Bootstrap())
					}
					db, err := aspen.Open("", addresses[i], addresses, opts...)
					ids[i] = db.HostID()
					dbs[i] = db
					By("Joining the node to the cluster without error")
					Expect(err).ToNot(HaveOccurred())
				}(i)
			}
			wg.Wait()
			Expect(len(filter.Duplicates(ids))).To(Equal(len(ids)))
			for _, db := range dbs {
				Expect(db.Close()).To(Succeed())
			}
		})
	})
})
