package leasholder

import (
	"context"
	"errors"
	"github.com/arya-analytics/aspen/internal/kv/operation"
	"github.com/arya-analytics/aspen/internal/node"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/transport"
	"go/types"
)

var ErrLeaseNotTransferable = errors.New("cannot transfer lease")

// Writer is a writable key-value store.
type Writer interface {
	// SetWithLease is similar to Set, but also takes an id for a leaseholder node.
	// If the leaseholder node is not the host, the request will be forwarded to the
	// leaseholder for execution. Only the leaseholder node will be able to perform
	// set and delete operations on the requested key.
	SetWithLease(key []byte, leaseholder node.ID, value []byte) error
	// Writer represents the same interface to a typical key-value store.
	// kv.Write.Set operations call SetWithLease internally and mark the leaseholder as
	// the host.
	kv_.Writer
}

type Transport = transport.Unary[operation.Operation, types.Nil]

type Leaseholder struct {
	Config
	host node.ID
}

func New(cfg Config) *Leaseholder {
	lh := &Leaseholder{Config: cfg}
	lh.host = lh.Cluster.Host().ID
	lh.Transport.Handle(lh.handle)
	return lh
}

func (l *Leaseholder) Set(key []byte, value []byte) error { return l.SetWithLease(key, l.host, value) }

func (l *Leaseholder) SetWithLease(key []byte, leaseholder node.ID, value []byte) error {
	if leaseholder == 0 {
		leaseholder = l.host
	}
	if err := l.validateLease(key, leaseholder); err != nil {
		return err
	}
	return l.process(operation.Operation{
		Key:         key,
		Leaseholder: leaseholder,
		Value:       value,
		Variant:     operation.Set,
	})
}

func (l *Leaseholder) Delete(key []byte) error {
	leaseholder, err := l.getLease(key)
	if err != nil {
		return err
	}
	return l.process(operation.Operation{Key: key, Leaseholder: leaseholder, Variant: operation.Delete})
}

func (l *Leaseholder) process(op operation.Operation) error {
	if op.Leaseholder != l.host {
		return l.forward(op)
	}
	return l.Executor(op)
}

func (l *Leaseholder) handle(ctx context.Context, op operation.Operation) (types.Nil, error) {
	if ctx.Err() != nil {
		return types.Nil{}, ctx.Err()
	}
	return types.Nil{}, l.process(op)
}

func (l *Leaseholder) validateLease(key []byte, leaseholder node.ID) error {
	lease, err := l.getLease(key)
	if err != nil {
		return err
	}
	if err == kv_.ErrNotFound || lease == leaseholder {
		return nil
	}
	return ErrLeaseNotTransferable
}

func (l *Leaseholder) getLease(key []byte) (node.ID, error) {
	op, err := operation.Load(l.Reader, key)
	return op.Leaseholder, err
}

func (l *Leaseholder) forward(op operation.Operation) error {
	n, ok := l.Cluster.Member(op.Leaseholder)
	if !ok {
		return kv_.ErrNotFound
	}
	_, err := l.Transport.Send(context.Background(), n.Address, op)
	return err

}
