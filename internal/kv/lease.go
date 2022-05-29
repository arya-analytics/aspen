package kv

import (
	"context"
	"errors"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"go/types"
)

var ErrLeaseNotTransferable = errors.New("cannot transfer lease")

const DefaultLeaseholder = 0

type LeaseMessage struct {
	Operation Operation
}

type leaseProxy struct {
	Config
	host     node.ID
	localTo  address.Address
	remoteTo address.Address
	confluence.Switch[batch]
}

func newLeaseProxy(cfg Config, localTo address.Address, remoteTo address.Address) *leaseProxy {
	lp := &leaseProxy{Config: cfg}
	lp.host = lp.Cluster.Host().ID
	lp.Switch.Switch = lp._switch
	return lp
}

func (lp *leaseProxy) _switch(_ confluence.Context, batch batch) address.Address {
	if len(batch.Operations) != 1 {
		panic("cannot process more than one op at a time")
	}
	var (
		op    = batch.Operations[0]
		err   error
		local bool
	)
	switch op.Variant {
	case Set:
		local, err = lp.processSet(op)
	case Delete:
		local, err = lp.processDelete(op)
	}
	if err != nil {
		batch.Errors <- err
		close(batch.Errors)
		return ""
	}
	if local {
		return lp.localTo
	}
	return lp.remoteTo
}

func (lp *leaseProxy) processSet(op Operation) (bool, error) {
	if op.Leaseholder == DefaultLeaseholder {
		op.Leaseholder = lp.host
	}
	return op.Leaseholder == lp.host, lp.validateLease(op.Key, op.Leaseholder)
}

func (lp *leaseProxy) processDelete(op Operation) (bool, error) {
	leaseholder, err := lp.getLease(op.Key)
	return leaseholder == lp.host, err
}

func (lp *leaseProxy) validateLease(key []byte, leaseholder node.ID) error {
	lease, err := lp.getLease(key)
	if err != nil {
		return err
	}
	if err == kv_.ErrNotFound || lease == leaseholder {
		return nil
	}
	return ErrLeaseNotTransferable
}

func (lp *leaseProxy) getLease(key []byte) (node.ID, error) {
	op, err := loadMetadata(lp.Engine, key)
	return op.Leaseholder, err
}

type leaseSender struct {
	Config
	confluence.CoreSink[batch]
}

func newLeaseSender(cfg Config) segment { return &leaseSender{Config: cfg} }

func (lf *leaseSender) sink(ctx confluence.Context, batch batch) {
	defer close(batch.Errors)
	if len(batch.Operations) != 1 {
		panic("cannot process more than one op at a time")
	}
	op := batch.Operations[0]
	addr, err := lf.Cluster.Resolve(op.Leaseholder)
	if err != nil {
		batch.Errors <- err
		return
	}
	if _, err = lf.Config.LeaseTransport.Send(ctx.Ctx, addr, LeaseMessage{Operation: op}); err != nil {
		batch.Errors <- err
	}
}

type leaseReceiver struct {
	Config
	confluence.CoreSource[batch]
}

func newLeaseReceiver(cfg Config) segment { return &leaseReceiver{Config: cfg} }

func (lr *leaseReceiver) Flow(ctx confluence.Context) { lr.LeaseTransport.Handle(lr.handle) }

func (lr *leaseReceiver) handle(ctx context.Context, msg LeaseMessage) (types.Nil, error) {
	batch := batch{Errors: make(chan error, 1), Operations: []Operation{msg.Operation}}
	if ctx.Err() != nil {
		return types.Nil{}, ctx.Err()
	}
	for _, inlet := range lr.Out {
		inlet.Inlet() <- batch
	}
	return types.Nil{}, <-batch.Errors
}
