package operation

import (
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/util/errutil"
	"github.com/arya-analytics/x/version"
	"io"
)

type Variant byte

const (
	Set Variant = iota
	Delete
)

type State byte

const (
	Infected State = iota
	Recovered
)

type Executor func(op Operation) error

type Operation struct {
	Key         []byte
	Value       []byte
	Variant     Variant
	Version     version.Counter
	Leaseholder node.ID
	State       State
	Error       error
}

// Load implements the kv.Loader interface.
func (o *Operation) Load(r io.Reader) error {
	c := errutil.NewCatchRead(r)
	c.Read(&o.Variant)
	c.Read(&o.Version)
	c.Read(&o.Leaseholder)
	return c.Error()
}

// Flush implements the kv.Flusher interface.
func (o Operation) Flush(w io.Writer) error {
	c := errutil.NewCatchWrite(w)
	c.Write(o.Variant)
	c.Write(o.Version)
	c.Write(o.Leaseholder)
	return c.Error()
}

type Segment interface {
	InPipe() chan<- Operation
	OutPipe() <-chan Operation
	Start() <-chan error
}

const key = "op"

func Key(key []byte) (opKey []byte, err error) {
	return kv.CompositeKey(key, key)
}

func Load(kve kv.Reader, key []byte) (op Operation, err error) {
	opKey, err := Key(key)
	if err != nil {
		return op, err
	}
	return op, kv.Load(kve, opKey, &op)
}
