package member_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMembership(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Membership Suite")
}
