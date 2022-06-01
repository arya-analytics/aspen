package cluster_test

import (
	"github.com/arya-analytics/aspen/internal/cluster"
	"github.com/arya-analytics/aspen/internal/cluster/clustermock"
	"github.com/arya-analytics/x/kv/kvmock"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("Storage", func() {
	var (
		builder *clustermock.Builder
	)
	BeforeEach(func() {
		builder = clustermock.NewBuilder(cluster.Config{})
	})
	Context("One node has persisted state", func() {
		It("Should assign new IDs to nodes without state", func() {
			pkv := kvmock.New()
			cluster, err := builder.New(cluster.Config{
				Storage:              pkv,
				StorageFlushInterval: cluster.FlushOnEvery,
			})

		})
	})

})
