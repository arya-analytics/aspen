package pledge_test

import (
	"context"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/aspen/internal/pledge"
	"github.com/arya-analytics/x/address"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"time"
)

var _ = Describe("Member", func() {
	Describe("Pledge", func() {
		Context("No Nodes Responding", func() {
			It("Should submit round robin propose requests at scaled intervals", func() {
				var (
					addresses     []address.Address
					numTransports = 4
					net           = tmock.NewNetwork[node.ID, node.ID]()
					handler       = func(ctx context.Context, id node.ID) (node.ID, error) {
						time.Sleep(2 * time.Millisecond)
						return 0, ctx.Err()
					}
				)
				t1 := net.Route("")
				for i := 0; i < numTransports; i++ {
					t := net.Route("")
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
				for i, entry := range net.Entries {
					Expect(entry.Address).To(Equal(addresses[i%4]))
				}
			})
		})
	})
	Describe("Responsible", func() {
		It("Should contact a quorum of nodes", func() {
			nodes := make(node.Group)
			candidates := func() node.Group { return nodes }
			net := tmock.NewNetwork[node.ID, node.ID]()
			t1 := net.Route("")
			logger, err := zap.NewDevelopment()
			for i := 0; i < 10; i++ {
				t := net.Route("")
				cfg := pledge.Config{Transport: t, Logger: logger}
				pledge.Arbitrate(candidates, cfg)
				id := node.ID(i)
				nodes[id] = node.Node{ID: node.ID(i), Address: t.Address, State: node.StateHealthy}
			}
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			Expect(err).To(BeNil())
			id, err := pledge.Pledge(
				ctx,
				nodes.Addresses(),
				candidates,
				pledge.Config{
					Transport: t1,
					Logger:    logger,
				},
			)
			Expect(err).To(BeNil())
			Expect(id).To(Equal(node.ID(10)))

		})
	})
})
