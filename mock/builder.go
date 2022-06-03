package mock

import (
	"github.com/arya-analytics/aspen"
	"github.com/arya-analytics/x/address"
	"os"
	"path/filepath"
	"strconv"
)

type Builder struct {
	PortRangeStart int
	DataDir        string
	DefaultOptions []aspen.Option
	peerAddresses  []address.Address
	tmpDir         string
}

func (b *Builder) Dir() string {
	if b.tmpDir == "" {
		var err error
		b.tmpDir, err = os.MkdirTemp(b.DataDir, "aspen")
		if err != nil {
			panic(err)
		}
	}
	return b.tmpDir
}

func (b *Builder) New(opts ...aspen.Option) (aspen.DB, error) {
	dir := filepath.Join(b.Dir(), strconv.Itoa(len(b.peerAddresses)))
	addr := address.Address("localhost:" + strconv.Itoa(b.PortRangeStart+len(b.peerAddresses)))
	if len(b.peerAddresses) == 0 {
		opts = append(opts, aspen.Bootstrap())
	}
	return aspen.Open(dir, addr, b.peerAddresses, append(b.DefaultOptions, opts...)...)
}

func (b *Builder) Cleanup() error { return os.RemoveAll(b.Dir()) }
