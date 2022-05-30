package kv_test

import (
	"errors"
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/gossip"
	"github.com/arya-analytics/aspen/internal/kv"
	"github.com/arya-analytics/aspen/internal/kv/kvmock"
	"github.com/arya-analytics/x/shutdown"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"time"
)

var _ = Describe("KV", func() {
	var (
		logger  *zap.Logger
		builder *kvmock.Builder
		sd      shutdown.Shutdown
	)
	BeforeEach(func() {
		sd = shutdown.New()
		logger = zap.NewNop()
		builder = kvmock.NewBuilder(
			kv.Config{
				Logger:            logger,
				RecoveryThreshold: 12,
				GossipInterval:    100 * time.Millisecond,
				Shutdown:          sd,
			},
			cluster.Config{
				Shutdown: sd,
				Gossip: gossip.Config{
					Interval: 50 * time.Millisecond,
				},
			},
		)
	})
	AfterEach(func() {
		Expect(sd.Shutdown()).To(Succeed())
	})
	Describe("Open", func() {
		It("Should open a new KV store without error", func() {
			kv, err := builder.New(kv.Config{}, cluster.Config{})
			Expect(err).ToNot(HaveOccurred())
			Expect(kv).ToNot(BeNil())
		})
	})
	Describe("Set", func() {
		Describe("Local Leaseholder", func() {
			It("Should persist the operation to storage", func() {
				kv, err := builder.New(kv.Config{}, cluster.Config{})
				Expect(err).ToNot(HaveOccurred())
				Expect(kv).ToNot(BeNil())
				Expect(kv.Set([]byte("key"), []byte("value"))).To(Succeed())
				v, err := kv.Get([]byte("key"))
				Expect(err).ToNot(HaveOccurred())
				Expect(v).To(Equal([]byte("value")))
			})
			It("Should propagate the operation to other members of the cluster", func() {
				kv1, err := builder.New(kv.Config{}, cluster.Config{})
				Expect(err).ToNot(HaveOccurred())
				kv2, err := builder.New(kv.Config{}, cluster.Config{})
				Expect(err).ToNot(HaveOccurred())
				Expect(kv1.Set([]byte("key"), []byte("value"))).To(Succeed())
				time.Sleep(200 * time.Millisecond)
				v, err := kv2.Get([]byte("key"))
				Expect(err).ToNot(HaveOccurred())
				Expect(v).To(Equal([]byte("value")))
			})
			It("Should forward an update to the leaseholder", func() {
				kv1, err := builder.New(kv.Config{}, cluster.Config{})
				Expect(err).ToNot(HaveOccurred())
				kv2, err := builder.New(kv.Config{}, cluster.Config{})
				Expect(err).ToNot(HaveOccurred())
				Expect(kv1.Set([]byte("key"), []byte("value"))).To(Succeed())
				time.Sleep(200 * time.Millisecond)
				v, err := kv2.Get([]byte("key"))
				Expect(err).ToNot(HaveOccurred())
				Expect(v).To(Equal([]byte("value")))
				Expect(kv2.Set([]byte("key"), []byte("value2"))).To(Succeed())
				time.Sleep(200 * time.Millisecond)
				v, err = kv1.Get([]byte("key"))
				Expect(err).ToNot(HaveOccurred())
				Expect(v).To(Equal([]byte("value2")))
				v, err = kv2.Get([]byte("key"))
				Expect(err).ToNot(HaveOccurred())
				Expect(v).To(Equal([]byte("value2")))
			})
			It("Should return an error when attempting to transfer the lease", func() {
				kv1, err := builder.New(kv.Config{}, cluster.Config{})
				Expect(err).ToNot(HaveOccurred())
				_, err = builder.New(kv.Config{}, cluster.Config{})
				Expect(err).ToNot(HaveOccurred())
				Expect(kv1.Set([]byte("key"), []byte("value"))).To(Succeed())
				err = kv1.SetWithLease([]byte("key"), 2, []byte("value2"))
				Expect(err).To(HaveOccurred())
				Expect(errors.Is(err, kv.ErrLeaseNotTransferable)).To(BeTrue())
			})
		})
		Describe("Remote Leaseholder", func() {
			It("Should persist the operation to storage", func() {
				kv1, err := builder.New(kv.Config{}, cluster.Config{})
				Expect(err).ToNot(HaveOccurred())
				kv2, err := builder.New(kv.Config{}, cluster.Config{})
				Expect(err).ToNot(HaveOccurred())
				// Give the cluster time to propagate state.
				time.Sleep(50 * time.Millisecond)
				Expect(kv1.SetWithLease([]byte("key"), 2, []byte("value"))).To(Succeed())
				time.Sleep(200 * time.Millisecond)
				v, err := kv2.Get([]byte("key"))
				Expect(err).ToNot(HaveOccurred())
				Expect(v).To(Equal([]byte("value")))
			})

		})
	})
})
