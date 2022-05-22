package pledge_test

import (
	"context"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/aspen/internal/pledge"
	"github.com/arya-analytics/x/address"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Member", func() {
	Describe("Pledge", func() {
		Context("No Nodes Responding", func() {
			It("Should submit round robin propose requests at scaled intervals", func() {
				var (
					addresses     []address.Address
					numTransports = 4
					network       = tmock.NewNetwork[node.ID, node.ID]()
					handler       = func(ctx context.Context, id node.ID) (node.ID, error) {
						time.Sleep(2 * time.Millisecond)
						return 0, ctx.Err()
					}
				)
				t1 := network.Route("")
				for i := 0; i < numTransports; i++ {
					t := network.Route("")
					t.Handle(handler)
					addresses = append(addresses, t.Address)
				}
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(15 * time.Millisecond)
					cancel()
				}()
				id, err := pledge.Pledge(
					ctx,
					addresses,
					func() (g node.Group) { return g },
					pledge.Config{RequestTimeout: 1 * time.Millisecond, Transport: t1},
				)
				Expect(err).To(Equal(context.Canceled))
				Expect(id).To(Equal(node.ID(0)))
				for i, entry := range network.Entries {
					Expect(entry.Address).To(Equal(addresses[i%4]))
				}
			})
		})
	})
})
