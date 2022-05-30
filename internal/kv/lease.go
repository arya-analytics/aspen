package kv

import (
	"context"
	"errors"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/transport"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go/types"
)

var ErrLeaseNotTransferable = errors.New("cannot transfer lease")

const DefaultLeaseholder = 0

type leaseProxy struct {
	Config
	localTo  address.Address
	remoteTo address.Address
	confluence.Switch[batch]
}

func newLeaseProxy(cfg Config, localTo address.Address, remoteTo address.Address) *leaseProxy {
	lp := &leaseProxy{Config: cfg, localTo: localTo, remoteTo: remoteTo}
	lp.Switch.Switch = lp._switch
	return lp
}

func (lp *leaseProxy) _switch(_ confluence.Context, batch batch) address.Address {
	var (
		op    = batch.single()
		err   error
		local bool
	)
	switch op.Variant {
	case Set:
		op, err = lp.processSet(op)
	case Delete:
		op, err = lp.processDelete(op)
	}
	if err != nil {
		lp.Logger.Error("leaseProxy failed to process operation", zap.Error(err))
		batch.errors <- err
		close(batch.errors)
		return ""
	}
	lp.Logger.Debug("lease proxy", zap.Bool("local", local))
	if op.Leaseholder == lp.Cluster.HostID() {
		return lp.localTo
	}
	return lp.remoteTo
}

func (lp *leaseProxy) processSet(op Operation) (Operation, error) {
	if op.Leaseholder == DefaultLeaseholder {
		leaseholder, err := lp.getLease(op.Key)
		if err == kv_.ErrNotFound {
			op.Leaseholder = lp.Cluster.HostID()
		} else if err != nil {
			return op, err
		} else {
			op.Leaseholder = leaseholder
		}
	}
	return op, lp.validateLease(op.Key, op.Leaseholder)
}

func (lp *leaseProxy) processDelete(op Operation) (Operation, error) {
	var err error
	op.Leaseholder, err = lp.getLease(op.Key)
	return op, err
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
	digest, err := getDigestFromKV(lp.Engine, key)
	log.Info(digest, err)
	return digest.Leaseholder, err
}

type LeaseMessage struct {
	Operation Operation
}

func (l LeaseMessage) toBatch() batch { return batch{operations: []Operation{l.Operation}} }

type LeaseTransport = transport.Unary[LeaseMessage, types.Nil]

type leaseSender struct {
	Config
	confluence.CoreSink[batch]
}

func newLeaseSender(cfg Config) segment {
	ls := &leaseSender{Config: cfg}
	ls.Sink = ls.send
	return ls
}

func (lf *leaseSender) send(ctx confluence.Context, batch batch) {
	defer close(batch.errors)
	op := batch.single()

	lf.Logger.Debug("sending lease operation",
		zap.Binary("key", op.Key),
		zap.Stringer("host", lf.Cluster.HostID()),
		zap.Stringer("peer (leaseholder)", op.Leaseholder),
	)

	addr, err := lf.Cluster.Resolve(op.Leaseholder)
	if err != nil {
		lf.Logger.Error("failed to resolve leaseholder", zap.Stringer("peer", op.Leaseholder), zap.Error(err))
		batch.errors <- err
		return
	}
	if _, err = lf.Config.LeaseTransport.Send(ctx.Ctx, addr, LeaseMessage{Operation: op}); err != nil {
		lf.Logger.Error("failed to send lease operation", zap.Stringer("peer", op.Leaseholder), zap.Error(err))
		batch.errors <- err
	}
}

type leaseReceiver struct {
	Config
	confluence.UnarySource[batch]
}

func newLeaseReceiver(cfg Config) segment {
	lr := &leaseReceiver{Config: cfg}
	lr.LeaseTransport.Handle(lr.receive)
	return lr
}

func (lr *leaseReceiver) receive(ctx context.Context, msg LeaseMessage) (types.Nil, error) {

	if ctx.Err() != nil {
		return types.Nil{}, ctx.Err()
	}

	b := msg.toBatch()
	b.errors = make(chan error, 1)

	lr.Logger.Debug("received lease operation",
		zap.Binary("key", msg.Operation.Key),
		zap.Stringer("peer", msg.Operation.Leaseholder),
		zap.Stringer("host", lr.Cluster.HostID()),
	)

	lr.Out.Inlet() <- b
	err := <-b.errors
	if err != nil {
		lr.Logger.Error("lease failed to process operation", zap.Error(err))
	}

	return types.Nil{}, err
}
