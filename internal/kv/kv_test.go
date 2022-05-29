package kv_test

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/clustermock"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/x/kv/kvmock"
	"github.com/arya-analytics/x/shutdown"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"go/types"
	"time"
)

var _ = Describe("KV", func() {
	var (
		opNet       *tmock.Network[kv.OperationsMessage, kv.OperationsMessage]
		feedbackNet *tmock.Network[kv.FeedbackMessage, types.Nil]
		leaseNet    *tmock.Network[kv.LeaseMessage, types.Nil]
		clusterSize int
		clusterAPIs []cluster.Cluster
		kvAPIs      []kv.KV
		logger      *zap.Logger
		sd          shutdown.Shutdown
	)
	BeforeEach(func() {
		opNet = tmock.NewNetwork[kv.OperationsMessage, kv.OperationsMessage]()
		feedbackNet = tmock.NewNetwork[kv.FeedbackMessage, types.Nil]()
		leaseNet = tmock.NewNetwork[kv.LeaseMessage, types.Nil]()
		sd = shutdown.New()
		clusterSize = 2
		logger, _ = zap.NewDevelopment()
		var err error
		clusterAPIs, err = clustermock.Provision(clusterSize, cluster.Config{Logger: logger, Shutdown: sd})
		for _, api := range clusterAPIs {
			kvEngine := kvmock.New()
			opT := opNet.Route("")
			feedbackT := feedbackNet.Route(opT.Address)
			leaseT := leaseNet.Route(opT.Address)
			kv, err := kv.Open(kv.Config{
				Cluster:             api,
				OperationsTransport: opT,
				FeedbackTransport:   feedbackT,
				LeaseTransport:      leaseT,
				Logger:              logger,
				Shutdown:            sd,
				RecoveryThreshold:   5,
				Engine:              kvEngine,
				GossipInterval:      100 * time.Millisecond,
			})
			kvAPIs = append(kvAPIs, kv)
			Expect(err).To(BeNil())
		}

		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		Expect(sd.Shutdown()).To(Succeed())
	})
	Context("Host as Leaseholder", func() {
		Describe("Set", func() {
			It("Should propagate a set operation", func() {
				kv1 := kvAPIs[0]
				//kv2 := kvAPIs[1]
				Expect(kv1.Set([]byte("hello"), []byte("world"))).To(Succeed())
				time.Sleep(500 * time.Second)
				//v, err := kv2.Get([]byte("hello"))
				//Expect(err).To(Succeed())
				//Expect(v).To(Equal([]byte("world")))
			})
		})
	})
})
