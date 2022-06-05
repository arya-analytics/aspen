package aspen_test

import (
	"fmt"
	"github.com/arya-analytics/aspen"
	"github.com/arya-analytics/aspen/mock"
	"github.com/arya-analytics/x/alamos"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

type kvSetVars struct {
	numNodes    int
	numOps      int
	numRoutines int
	opts        []aspen.Option
}

var itervars = []kvSetVars{
	{
		numNodes: 2,
		numOps:   100,
	},
	{
		numNodes: 4,
		numOps:   100,
	},
}

var _ = Describe("KvPerf", Serial, func() {
	var (
		builder *mock.Builder
		logger  *zap.SugaredLogger
	)
	Describe("Set", func() {
		log, _ := zap.NewDevelopment()
		logger = log.Sugar()
		builder = &mock.Builder{
			PortRangeStart: 22546,
			DataDir:        "./testdata",
			DefaultOptions: []aspen.Option{aspen.WithLogger(logger)},
		}
	})
	iter := alamos.IterVars(itervars)
	p := alamos.NewParametrize(iter)
	p.Template(func(i int, vars kvSetVars) {
		It(fmt.Sprintf("Should be able to disseminate %d operations with %d nodes", vars.numOps, vars.numNodes), func() {
			var dbs []aspen.DB
			for i := 0; i < vars.numNodes; i++ {
				db, err := builder.New(vars.opts...)
				Expect(err).ToNot(HaveOccurred())
				dbs = append(dbs, db)
			}
			leaseDB := dbs[0]
			for i := 0; i < vars.numOps; i++ {
				Expect(leaseDB.Set([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))).To(Succeed())
			}
		})
	})
})
