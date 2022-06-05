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
	TmpDirs        map[aspen.NodeID]string
	tmpDir         string
	addressFactory *address.Factory
	Contexts       map[aspen.NodeID]Context
}

type Context struct {
	Addr address.Address
	Dir  string
	DB   aspen.DB
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

func (b *Builder) AddressFactory() *address.Factory {
	if b.addressFactory == nil {
		b.addressFactory = address.NewLocalFactory(b.PortRangeStart)
	}
	return b.addressFactory
}

func (b *Builder) New(opts ...aspen.Option) (aspen.DB, error) {
	dir := filepath.Join(b.Dir(), strconv.Itoa(len(b.peerAddresses)))
	if len(b.Contexts) == 0 {
		opts = append(opts, aspen.Bootstrap())
	}
	addr := b.AddressFactory().Next()
	db, err := aspen.Open(dir, addr, b.peerAddresses, append(b.DefaultOptions, opts...)...)
	if err != nil {
		return nil, err
	}
	b.Contexts[db.HostID()] = Context{
		Addr: addr,
		Dir:  dir,
		DB:   db,
	}
	b.peerAddresses = append(b.peerAddresses, addr)
	return db, nil
}

func (b *Builder) Cleanup() error { return os.RemoveAll(b.Dir()) }
