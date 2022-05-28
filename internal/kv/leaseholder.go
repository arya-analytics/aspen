package kv

import (
	"context"
	"errors"
	"github.com/arya-analytics/aspen/internal/node"
	kv_ "github.com/arya-analytics/x/kv"
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

type leaseProxy struct {
	Config
	host node.ID
}

func newLeaseProxy(cfg Config) *leaseProxy {
	lh := &leaseProxy{Config: cfg}
	lh.host = lh.Cluster.Host().ID
	lh.LeaseTransport.Handle(lh.handle)
	return lh
}

func (l *leaseProxy) Set(key []byte, value []byte) error { return l.SetWithLease(key, l.host, value) }

func (l *leaseProxy) SetWithLease(key []byte, leaseholder node.ID, value []byte) error {
	if leaseholder == 0 {
		leaseholder = l.host
	}
	if err := l.validateLease(key, leaseholder); err != nil {
		return err
	}
	return l.process(Operation{
		Key:         key,
		Leaseholder: leaseholder,
		Value:       value,
		Variant:     Set,
	})
}

func (l *leaseProxy) Delete(key []byte) error {
	leaseholder, err := l.getLease(key)
	if err != nil {
		return err
	}
	return l.process(Operation{Key: key, Leaseholder: leaseholder, Variant: Delete})
}

func (l *leaseProxy) process(op Operation) error {
	if op.Leaseholder != l.host {
		return l.forward(op)
	}
	return l.Executor(op)
}

func (l *leaseProxy) handle(ctx context.Context, msg LeaseMessage) (types.Nil, error) {
	if ctx.Err() != nil {
		return types.Nil{}, ctx.Err()
	}
	return types.Nil{}, l.process(msg.Operation)
}

func (l *leaseProxy) validateLease(key []byte, leaseholder node.ID) error {
	lease, err := l.getLease(key)
	if err != nil {
		return err
	}
	if err == kv_.ErrNotFound || lease == leaseholder {
		return nil
	}
	return ErrLeaseNotTransferable
}

func (l *leaseProxy) getLease(key []byte) (node.ID, error) {
	op, err := Load(l.Engine, key)
	return op.Leaseholder, err
}

func (l *leaseProxy) forward(op Operation) error {
	n, ok := l.Cluster.Member(op.Leaseholder)
	if !ok {
		return kv_.ErrNotFound
	}
	_, err := l.LeaseTransport.Send(context.Background(), n.Address, op)
	return err

}
