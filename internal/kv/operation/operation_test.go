package operation_test

import (
	"github.com/arya-analytics/aspen/internal/kv/operation"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kv"
	mockkv "github.com/arya-analytics/x/kv/mock"
	"github.com/arya-analytics/x/version"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Operation", func() {
	var (
		kve     kv.KV
		segment confluence.Segment[operation.Operation]
		inlet   confluence.Stream[operation.Operation]
		outlet  confluence.Stream[operation.Operation]
	)
	BeforeEach(func() {
		kve = mockkv.New()
		segment = operation.NewSegment(kve)
		inlet = confluence.NewStream[operation.Operation](3)
		outlet = confluence.NewStream[operation.Operation](3)
		segment.InFrom(inlet)
		segment.OutTo(outlet)
	})
	Describe("Pipe", func() {
		Describe("Version filter", func() {
			It("Should filter out old operations", func() {
				ctx := confluence.DefaultContext()
				segment.Flow(ctx)
				inlet.Inlet() <- operation.Operation{
					Key:         []byte("key"),
					Value:       []byte("value"),
					Version:     version.Counter(2),
					Variant:     operation.Set,
					Leaseholder: 1,
				}
				inlet.Inlet() <- operation.Operation{
					Key:         []byte("key"),
					Value:       []byte("newvalue"),
					Version:     version.Counter(1),
					Variant:     operation.Set,
					Leaseholder: 1,
				}
				op := <-outlet.Outlet()
				Expect(op.Value).To(Equal([]byte("value")))
				Expect(ctx.Shutdown.ShutdownAfter(1 * time.Millisecond)).To(Succeed())
				value, err := kve.Get([]byte("key"))
				Expect(err).To(BeNil())
				Expect(value).To(Equal([]byte("value")))
			})
		})
	})
	Describe("Persisting operation metadata", func() {
		It("Should persist metadata correctly", func() {
			ctx := confluence.DefaultContext()
			segment.Flow(ctx)
			inlet.Inlet() <- operation.Operation{
				Key:         []byte("key"),
				Value:       []byte("value"),
				Version:     version.Counter(1),
				Variant:     operation.Set,
				Leaseholder: 1,
			}
			inlet.Inlet() <- operation.Operation{
				Key:     []byte("key"),
				Variant: operation.Delete,
				Version: version.Counter(2),
			}
			op := <-outlet.Outlet()
			Expect(op.Value).To(Equal([]byte("value")))
			Expect(ctx.Shutdown.ShutdownAfter(1 * time.Millisecond)).To(Succeed())
			key, err := operation.Key([]byte("key"))
			Expect(err).To(BeNil())
			oOp := operation.Operation{}
			Expect(kv.Load(kve, key, &oOp)).To(Succeed())
			Expect(oOp.Version).To(Equal(version.Counter(2)))
		})
	})
})
