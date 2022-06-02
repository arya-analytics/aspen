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

var _ = Describe("Join", func() {
	var (
		db1    aspen.DB
		db2    aspen.DB
		logger *zap.SugaredLogger
		exp    alamos.Experiment
	)
	BeforeEach(func() {
		log, _ := zap.NewDevelopment()
		logger = log.Sugar()
		exp = alamos.New("aspen_join_test")
		var err error
		db1, err = aspen.Join(
			"./testdata/db1",
			"localhost:22546",
			[]address.Address{},
			aspen.Bootstrap(),
			aspen.WithLogger(logger),
			aspen.WithExperiment(alamos.Sub(exp, "db1")),
		)
		Expect(err).ToNot(HaveOccurred())
		db2, err = aspen.Join(
			"./testdata/db2",
			"localhost:22547",
			[]address.Address{"localhost:22546"},
			aspen.WithLogger(logger),
			aspen.WithExperiment(alamos.Sub(exp, "db2")),
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
	FIt("Should be able to join two clusters", func() {
		Expect(len(db1.Nodes())).To(Equal(2))
		Expect(len(db2.Nodes())).To(Equal(2))
	})
})
