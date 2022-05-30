package kv

import (
	"context"
	"errors"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kv_ "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/transport"
	"go.uber.org/zap"
	"go/types"
)

var ErrLeaseNotTransferable = errors.New("cannot transfer lease")

const DefaultLeaseholder = 0

type leaseAssigner struct {
	Config
	confluence.Transform[batch]
}

func newLeaseAssigner(cfg Config) segment {
	la := &leaseAssigner{Config: cfg}
	la.Transform.Transform = la.assignLease
	return la
}

func (la *leaseAssigner) assignLease(ctx confluence.Context, b batch) (batch, bool) {
	op := b.single()
	lh, err := la.getLease(op.Key)
	if err == nil {
		if op.Leaseholder == DefaultLeaseholder {
			// If the leaseholder is the default, assign it to the stored leaseholder.
			op.Leaseholder = lh
		} else if lh != op.Leaseholder {
			// If we get a nil error, that means this key is in the KV store. If the leaseholder doesn't match
			// the previous leaseholder, we return an error.
			b.errors <- ErrLeaseNotTransferable
			return b, false
		}
	} else if err == kv_.ErrNotFound && op.Variant == Set {
		if op.Leaseholder == DefaultLeaseholder {
			// If we can't find the leaseholder, and the op doesn't have a leaseholder assigned,
			// we assign the lease to the cluster host.
			op.Leaseholder = la.Cluster.HostID()
		}
	} else {
		// For any other case, we return an error.
		b.errors <- err
		return b, false
	}
	b.operations[0] = op
	return b, true
}

func (la *leaseAssigner) getLease(key []byte) (node.ID, error) {
	digest, err := getDigestFromKV(la.Engine, key)
	return digest.Leaseholder, err
}

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
	if batch.single().Leaseholder == lp.Cluster.HostID() {
		return lp.localTo
	}
	return lp.remoteTo
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
		zap.String("key", string(op.Key)),
		zap.Stringer("host", lf.Cluster.HostID()),
		zap.Stringer("leaseholder", op.Leaseholder),
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
		zap.String("key", string(msg.Operation.Key)),
		zap.Stringer("leaseholder", msg.Operation.Leaseholder),
		zap.Stringer("host", lr.Cluster.HostID()),
	)

	lr.Out.Inlet() <- b
	err := <-b.errors
	if err != nil {
		lr.Logger.Error("lease failed to process operation", zap.Error(err))
	}

	return types.Nil{}, err
}
