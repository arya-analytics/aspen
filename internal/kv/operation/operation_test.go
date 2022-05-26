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
		kv kv.KV
	)
	BeforeEach(func() {
		kv = mockkv.New()
	})
	Describe("Pipe", func() {
		Describe("Version filter", func() {
			It("Should filter out old operations", func() {
				segment := operation.NewSegment(kv)
				inlet, outlet := confluence.NewStream[operation.Operation](3),
					confluence.NewStream[operation.Operation](3)
				segment.InFrom(inlet)
				segment.OutTo(outlet)
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
				value, err := kv.Get([]byte("key"))
				Expect(err).To(BeNil())
				Expect(value).To(Equal([]byte("value")))
			})
		})
	})
})
