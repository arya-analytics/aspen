package kv

import (
	"context"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	kvx "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
	"go/types"
)

var ErrLeaseNotTransferable = errors.New("[kv] - cannot transfer leaseAlloc")

const DefaultLeaseholder node.ID = 0

type leaseAllocator struct{ Config }

func (la *leaseAllocator) allocate(op Operation) (Operation, error) {
	lh, err := la.getLease(op.Key)
	// If we get a nil error, that means this key has been set before.
	if err == nil {
		if op.Leaseholder == DefaultLeaseholder {
			op.Leaseholder = lh
		} else if lh != op.Leaseholder {
			// If the Leaseholder doesn't match the previous Leaseholder,
			//we return an error.
			return op, ErrLeaseNotTransferable
		}
	} else if err == kvx.NotFound && op.Variant == Set {
		if op.Leaseholder == DefaultLeaseholder {
			// If we can't find the Leaseholder, and the op doesn't have a Leaseholder assigned,
			// we assign the leaseAlloc to the cluster host.
			op.Leaseholder = la.Cluster.HostID()
		}
		// If we can't find the Leaseholder, and the op has a Leaseholder assigned,
		// that means it's a new key, so we let it choose its own leaseAlloc.
	} else {
		return op, err
	}
	return op, nil
}

func (la *leaseAllocator) getLease(key []byte) (node.ID, error) {
	digest, err := getDigestFromKV(la.Engine, key)
	return digest.Leaseholder, err
}

type leaseProxy struct {
	Config
	localTo  address.Address
	remoteTo address.Address
	confluence.Switch[BatchRequest]
}

func newLeaseProxy(cfg Config, localTo address.Address, remoteTo address.Address) segment {
	lp := &leaseProxy{Config: cfg, localTo: localTo, remoteTo: remoteTo}
	lp.Switch.ApplySwitch = lp._switch
	return lp
}

func (lp *leaseProxy) _switch(
	_ signal.Context,
	b BatchRequest,
) (address.Address, bool, error) {
	if b.Leaseholder == lp.Cluster.HostID() {
		return lp.localTo, true, nil
	}
	return lp.remoteTo, true, nil
}

type LeaseTransport = transport.Unary[BatchRequest, types.Nil]

type leaseSender struct {
	Config
	confluence.UnarySink[BatchRequest]
}

func newLeaseSender(cfg Config) sink {
	ls := &leaseSender{Config: cfg}
	ls.Sink = ls.send
	return ls
}

func (lf *leaseSender) send(ctx signal.Context, br BatchRequest) error {
	lf.Logger.Debugw("sending leased BatchRequest",
		"host", lf.Cluster.HostID(),
		"Leaseholder", br.Leaseholder,
		"numOps", len(br.Operations),
	)
	addr, err := lf.Cluster.Resolve(br.Leaseholder)
	if err != nil {
		return err
	}
	_, err = lf.Config.LeaseTransport.Send(ctx, addr, br)
	if br.done != nil {
		br.done(err)
	}
	return err
}

type leaseReceiver struct {
	Config
	confluence.AbstractUnarySource[BatchRequest]
	confluence.EmptyFlow
}

func newLeaseReceiver(cfg Config) source {
	lr := &leaseReceiver{Config: cfg}
	lr.LeaseTransport.Handle(lr.receive)
	return lr
}

func (lr *leaseReceiver) receive(ctx context.Context, br BatchRequest) (types.Nil, error) {
	lr.Logger.Debugw("received leaseAlloc operation",
		"Leaseholder", br.Leaseholder,
		"host", lr.Cluster.HostID(),
		"size", br.size(),
	)
	bc := batchCoordinator{}
	bc.add(&br)
	lr.Out.Inlet() <- br
	return types.Nil{}, bc.wait()
}
