package aspen_test

import (
	"github.com/arya-analytics/aspen"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/alamos"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"os"
)

var _ = Describe("Open", func() {
	var (
		db1    aspen.DB
		db2    aspen.DB
		logger *zap.SugaredLogger
		exp    alamos.Experiment
	)
	BeforeEach(func() {
		log := zap.NewNop()
		logger = log.Sugar()
		exp = alamos.New("aspen_join_test")
		var err error
		db1, err = aspen.Open(
			"",
			"localhost:22646",
			[]address.Address{},
			aspen.Bootstrap(),
			aspen.WithLogger(logger),
			aspen.WithExperiment(alamos.Sub(exp, "db1")),
			aspen.MemBacked(),
		)
		Expect(err).ToNot(HaveOccurred())
		db2, err = aspen.Open(
			"",
			"localhost:22647",
			[]address.Address{"localhost:22646"},
			aspen.WithLogger(logger),
			aspen.WithExperiment(alamos.Sub(exp, "db2")),
			aspen.MemBacked(),
		)
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		Expect(db1.Close()).To(Succeed())
		Expect(db2.Close()).To(Succeed())
		f, err := os.Create("aspen_join_test_report.json")
		defer func() {
			Expect(f.Close()).To(Succeed())
		}()
		Expect(err).ToNot(HaveOccurred())
		Expect(exp.Report().WriteJSON(f)).To(Succeed())
	})
	It("Should be able to join two clusters", func() {
		Expect(len(db1.Nodes())).To(Equal(2))
		Expect(len(db2.Nodes())).To(Equal(2))
	})
})
