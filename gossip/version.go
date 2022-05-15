package gossip

import (
	"github.com/arya-analytics/aspen/internal/address"
	"github.com/arya-analytics/aspen/internal/version"
)

type VersionMiddleware struct {
	version.Map[address.Address]
}

func (v VersionMiddleware[T]) Exec(m Message) error {
	if v.Map.YoungerThan(m.Address(), m.Version()) {
		return version.ErrOld
	}
	return nil
}
