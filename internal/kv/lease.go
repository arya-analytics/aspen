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

var ErrLeaseNotTransferable = errors.New("[kv] - cannot transfer lease")

const DefaultLeaseholder node.ID = 0

type leaseAssigner struct {
	Config
	confluence.Transform[batch]
}

func newLeaseAssigner(cfg Config) segment {
	la := &leaseAssigner{Config: cfg}
	la.Transform.Transform = la.assignLease
	return la
}

func (la *leaseAssigner) assignLease(ctx signal.Context, b batch) (batch, bool, error) {
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
			return b, false, nil
		}
	} else if err == kvx.ErrNotFound && op.Variant == Set {
		if op.Leaseholder == DefaultLeaseholder {
			// If we can't find the leaseholder, and the op doesn't have a leaseholder assigned,
			// we assign the lease to the cluster host.
			op.Leaseholder = la.Cluster.HostID()
		}
	} else {
		// For any other case, we return an error.
		b.errors <- err
		return b, false, nil
	}
	b.operations[0] = op
	return b, true, nil
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

func (lp *leaseProxy) _switch(_ signal.Context, batch batch) (address.Address, error) {
	if batch.single().Leaseholder == lp.Cluster.HostID() {
		return lp.localTo, nil
	}
	return lp.remoteTo, nil
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

func (lf *leaseSender) send(ctx signal.Context, batch batch) error {
	defer close(batch.errors)
	op := batch.single()

	lf.Logger.Debugw("sending lease operation",
		"key", op.Key, "host", lf.Cluster.HostID(),
		"leaseholder", op.Leaseholder,
	)

	addr, err := lf.Cluster.Resolve(op.Leaseholder)
	if err != nil {
		batch.errors <- err
		return nil
	}
	_, err = lf.Config.LeaseTransport.Send(context.TODO(), addr, LeaseMessage{Operation: op})
	if err != nil {
		batch.errors <- errors.Wrap(err, "[kv.leaseSender] - failed to send operation")
	}
	return nil
}

type leaseReceiver struct {
	Config
	confluence.UnarySource[batch]
	ctx signal.Context
}

func newLeaseReceiver(cfg Config) segment {
	lr := &leaseReceiver{Config: cfg}
	lr.LeaseTransport.Handle(lr.receive)
	return lr
}

func (lr *leaseReceiver) Flow(ctx signal.Context) { lr.ctx = ctx }

func (lr *leaseReceiver) receive(ctx context.Context, msg LeaseMessage) (types.Nil, error) {
	b := msg.toBatch()
	b.errors = make(chan error, 1)

	lr.Logger.Debugw("received lease operation",
		"key", msg.Operation.Key,
		"leaseholder", msg.Operation.Leaseholder,
		"host", lr.Cluster.HostID(),
	)

	lr.Out.Inlet() <- b
	return types.Nil{}, <-b.errors
}
