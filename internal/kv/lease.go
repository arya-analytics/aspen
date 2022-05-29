package kv

import (
	"context"
	"errors"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"go.uber.org/zap"
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
	lp := &leaseProxy{Config: cfg, localTo: localTo, remoteTo: remoteTo}
	lp.host = lp.Cluster.Host().ID
	lp.Switch.Switch = lp._switch
	return lp
}

func (lp *leaseProxy) _switch(_ confluence.Context, batch batch) address.Address {
	if len(batch.operations) != 1 {
		panic("cannot process more than one op at a time")
	}
	var (
		op    = batch.operations[0]
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
		batch.errors <- err
		close(batch.errors)
		return ""
	}
	lp.Logger.Debug("lease proxy", zap.Bool("local", local),
		zap.String("localTo", string(lp.localTo)),
		zap.String("remoteTo", string(lp.remoteTo)),
	)
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
	if err == kv_.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if lease != leaseholder {
		return ErrLeaseNotTransferable
	}
	return nil
}

func (lp *leaseProxy) getLease(key []byte) (node.ID, error) {
	op, err := getDigestFromKV(lp.Engine, key)
	return op.Leaseholder, err
}

type leaseSender struct {
	Config
	confluence.CoreSink[batch]
}

func newLeaseSender(cfg Config) segment {
	ls := &leaseSender{Config: cfg}
	ls.Sink = ls.forward
	return ls
}

func (lf *leaseSender) forward(ctx confluence.Context, batch batch) {
	defer close(batch.errors)
	if len(batch.operations) != 1 {
		panic("cannot process more than one op at a time")
	}
	op := batch.operations[0]
	lf.Logger.Debug("leaseSender: forwarding Op",
		zap.Int("variant", int(op.Variant)),
		zap.String("key", string(op.Key)),
		zap.Uint32("forwardingTo (leaseholder)", uint32(op.Leaseholder)),
	)
	addr, err := lf.Cluster.Resolve(op.Leaseholder)
	if err != nil {
		batch.errors <- err
		return
	}
	if _, err = lf.Config.LeaseTransport.Send(ctx.Ctx, addr, LeaseMessage{Operation: op}); err != nil {
		batch.errors <- err
	}
}

type leaseReceiver struct {
	Config
	confluence.CoreSource[batch]
}

func newLeaseReceiver(cfg Config) segment { return &leaseReceiver{Config: cfg} }

func (lr *leaseReceiver) Flow(ctx confluence.Context) { lr.LeaseTransport.Handle(lr.handle) }

func (lr *leaseReceiver) handle(ctx context.Context, msg LeaseMessage) (types.Nil, error) {
	b := batch{errors: make(chan error, 1), operations: []Operation{msg.Operation}}
	if ctx.Err() != nil {
		return types.Nil{}, ctx.Err()
	}
	lr.Logger.Debug("leaseReceiver: received Op",
		zap.Int("variant", int(msg.Operation.Variant)),
		zap.String("key", string(msg.Operation.Key)),
		zap.Uint32("receivedFrom", uint32(msg.Operation.Leaseholder)),
	)
	for _, inlet := range lr.Out {
		inlet.Inlet() <- b
	}
	return types.Nil{}, <-b.errors
}
