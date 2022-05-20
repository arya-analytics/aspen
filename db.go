package aspen

import "github.com/arya-analytics/x/kv"

type (
	Reader = kv.Reader
	Closer = kv.Closer
)

// Writer is a writable key-value store.
type Writer interface {
	Set(key []byte, leaseholder NodeID, value []byte) error
	Delete(key []byte) error
}

type DB interface {
	Reader
	Writer
	Closer
}
