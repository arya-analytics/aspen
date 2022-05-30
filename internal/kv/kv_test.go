package kv_test

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/clustermock"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/x/filter"
	"github.com/arya-analytics/x/kv/kvmock"
	"github.com/arya-analytics/x/shutdown"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go/types"
	"time"
)

var _ = Describe("KV", func() {
	var (
		opNet       *tmock.Network[kv.OperationMessage, kv.OperationMessage]
		feedbackNet *tmock.Network[kv.FeedbackMessage, types.Nil]
		leaseNet    *tmock.Network[kv.LeaseMessage, types.Nil]
		clusterSize int
		clusterAPIs []cluster.Cluster
		kvAPIs      []kv.KV
		logger      *zap.Logger
		sd          shutdown.Shutdown
	)
	BeforeEach(func() {
		opNet = tmock.NewNetwork[kv.OperationMessage, kv.OperationMessage]()
		feedbackNet = tmock.NewNetwork[kv.FeedbackMessage, types.Nil]()
		leaseNet = tmock.NewNetwork[kv.LeaseMessage, types.Nil]()
		sd = shutdown.New()
		clusterSize = 20
		logger = zap.NewNop()
		var err error
		clusterAPIs, err = clustermock.Provision(clusterSize, cluster.Config{Logger: zap.NewNop(), Shutdown: sd})
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
				RecoveryThreshold:   12,
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
				go func() {
					Expect(kv1.Set([]byte("hello"), []byte("world"))).To(Succeed())
				}()
				go func() {
					t := time.NewTicker(100 * time.Millisecond)
					t0 := time.Now()
					var excluded []int
					for range t.C {
						allDone := true
						for i, api := range kvAPIs {
							if !filter.ElementOf(excluded, i) {
								_, err := api.Get([]byte("hello"))
								if err == nil {
									log.Error(clusterAPIs[i].Host().ID, time.Since(t0))
									excluded = append(excluded, i)
								} else {
									allDone = false
								}
							}
						}
						if allDone {
							log.Fatal("AllDone")
						}
					}
				}()
				time.Sleep(10 * time.Second)
				go func() {
					Expect(kv1.Set([]byte("hello2"), []byte("world"))).To(Succeed())
				}()
				time.Sleep(100 * time.Second)

				//v, err := kv2.Get([]byte("hello"))
				//Expect(err).To(Succeed())
				//Expect(v).To(EqualTo([]byte("world")))
			})
		})
	})
})
