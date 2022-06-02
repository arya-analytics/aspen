// Package pledge provides a system for pledging a node to a jury of candidates. The pledge uses quorum consensus
// to assign the node a unique ID.
//
// ToAddr pledge a new node to a jury, call Pledge() with a set of peer addresses. ToAddr register a node as a candidate,
// use Arbitrate().
//
// Vocabulary:
//
//  Pledge - Used as both a verb and noun. A "Pledge" is a node that has 'pledged' itself to the cluster. 'Pledging' is
//  the entire process of contacting a peer, proposing an ID to a jury, and returning it to the pledge.
//  Responsible - A node that is responsible for coordinating the Pledge process. A responsible node is the first peer
//  that accepts the Pledge request from the pledge node.
// 	Candidates - A pool of nodes that can be selected to form a jury that can arbitrate a Pledge.
//  Jury - A quorum (numCandidates/2 + 1) of candidates arbitrate a Pledge. All jurors must accept the Pledge for
//  the node to be inducted.
//
// The following RFC provides details on how the pledging algorithm
// is implemented. https://github.com/arya-analytics/delta/blob/DA-153-aspen-rfc/docs/rfc/220518-aspen-p2p-network.md#adding-a-member.
//
package pledge

import (
	"context"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/filter"
	"github.com/arya-analytics/x/iter"
	"github.com/arya-analytics/x/rand"
	xtime "github.com/arya-analytics/x/util/time"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"sync"
)

var (
	// ErrQuorumUnreachable is returned when a quorum jury cannot be safely assembled.
	ErrQuorumUnreachable = errors.New("quorum unreachable")
	// errProposalRejected is an internal error returned when a juror rejects a pledge proposal from a responsible node.
	errProposalRejected = errors.New("proposal rejected")
)

// Pledge pledges a node to a Jury selected from candidates for membership. Membership behaves in a similar
// manner to a one way mutex. A Pledge will submit a request to a peer in peers, this peer will then request for a
// random, quorum jury from candidates. If the jury approves the pledge, the node will be given membership, assigned a
// unique ID, and allowed to Arbitrate in future proposals. See algorithm in package level documentation for
// implementation details. Although IDs are guaranteed to be unique, they are not guarantee to be sequential. Pledge
// will continue to contact peers in cfg.peerAddresses at a scaling interval until the provided context is cancelled.
func Pledge(ctx context.Context, peers []address.Address, candidates func() node.Group, cfg Config) (id node.ID, err error) {
	if len(peers) == 0 {
		return id, errors.New("[pledge] no peers provided")
	}

	cfg.peerAddresses, cfg.candidates = peers, candidates
	cfg = cfg.Merge(DefaultConfig())
	if err = cfg.Validate(); err != nil {
		return id, err
	}
	alamos.AttachReporter(cfg.Experiment, "pledge", alamos.Debug, cfg)

	nextAddr := iter.InfiniteSlice(cfg.peerAddresses)

	t := xtime.NewScaledTicker(cfg.PledgeBaseRetry, cfg.PledgeRetryScale)
	defer t.Stop()

	for range t.C {
		addr := nextAddr()
		cfg.Logger.Debug("pledging to peer", "address", addr)
		reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		id, err = cfg.Transport.Send(reqCtx, addr, 0)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			break
		}
		cfg.Logger.Error("failed to contact peer. retrying with next")
	}

	if err != nil {
		cfg.Logger.Error("failed", err)
		return 0, err
	}

	cfg.Logger.Debug("successful", "id", id)

	// If the pledge node has been inducted successfully, allow it to arbitrate in future pledges.
	return id, Arbitrate(candidates, cfg)

}

// Arbitrate registers a node to arbitrate future pledges. When a node calls Arbitrate, it will be made available
// to become a Responsible or Juror node. Any node that calls arbitrate should also be a member of candidates.
func Arbitrate(candidates func() node.Group, cfg Config) error {
	cfg.candidates = candidates
	cfg = cfg.Merge(DefaultConfig())
	if err := cfg.Validate(); err != nil {
		return err
	}
	alamos.AttachReporter(cfg.Experiment, "pledge", alamos.Debug, cfg)
	j := &juror{Config: cfg}
	cfg.Transport.Handle(func(ctx context.Context, id node.ID) (node.ID, error) {
		if id == 0 {
			t := &responsible{Config: cfg}
			return t.propose(ctx)
		}
		return 0, j.verdict(ctx, id)
	})
	return nil
}

// |||||| RESPONSIBLE ||||||

type responsible struct {
	Config
	candidates  node.Group
	_proposedID node.ID
}

func (r *responsible) propose(ctx context.Context) (id node.ID, err error) {
	r.Logger.Debug("[pledge] responsible received pledge. starting proposal process.")
	var propC int
	for propC = 0; propC < r.MaxProposals; propC++ {
		if err = ctx.Err(); err != nil {
			break
		}

		// pull in the latest candidates. We manually refresh candidates
		// to provide a consistent view across the entire responsible.
		r.refreshCandidates()

		// Increment the proposed ID unconditionally. Quorum juror's store each
		// approved request. If one node in the quorum is unreachable, other candidates
		// may have already approved the request. This means that if we retry the request
		// without incrementing the proposed ID, we'll get a rejection from the candidates that
		// approved the request last time. This will result in marginally higher IDs being
		// assigned, but it's better than adding a lot of extra responsible logic.
		id = r.idToPropose()
		logID := zap.Uint32("id", uint32(id))

		quorum, qErr := r.buildQuorum()
		if qErr != nil {
			err = qErr
			break
		}

		r.Logger.Debug("responsible proposing id", logID, "quorumCount", quorum)

		// If any node returns an error, it means we need to retry the responsible with a new ID.
		if err = r.consultQuorum(ctx, id, quorum); err != nil {
			r.Logger.Error("quorum rejected proposal. retrying.", zap.Error(err))
			continue
		}

		r.Logger.Debug("quorum accepted pledge", logID)

		// If no candidates return an error, it means we reached a quorum approval,
		// and we can safely return the new ID to the caller.
		return id, nil
	}
	r.Logger.Error("responsible failed to build healthy quorum", zap.Int("proposals", propC), zap.Error(err))
	return id, err
}

func (r *responsible) refreshCandidates() { r.candidates = r.Config.candidates() }

func (r *responsible) buildQuorum() (node.Group, error) {
	presentCandidates := r.candidates.WhereActive()
	size := len(presentCandidates)/2 + 1
	healthy := presentCandidates.WhereState(node.StateHealthy)
	if len(healthy) < size {
		return node.Group{}, ErrQuorumUnreachable
	}
	return rand.SubMap(healthy, size), nil
}

func (r *responsible) idToPropose() node.ID {
	if r._proposedID == 0 {
		r._proposedID = filter.MaxMapKey(r.candidates) + 1
	} else {
		r._proposedID++
	}
	return r._proposedID
}

func (r *responsible) consultQuorum(ctx context.Context, id node.ID, quorum node.Group) error {
	reqCtx, cancel := context.WithTimeout(ctx, r.RequestTimeout)
	defer cancel()
	wg := errgroup.Group{}
	for _, n := range quorum {
		n_ := n
		wg.Go(func() error {
			_, err := r.Transport.Send(reqCtx, n_.Address, id)
			// If any node returns an error, we need to retry the entire responsible,
			// so we need to cancel all running requests.
			if err != nil {
				cancel()
			}
			return err
		})
	}
	return wg.Wait()
}

// |||||| JUROR ||||||

type juror struct {
	Config
	mu        sync.Mutex
	approvals []node.ID
}

func (j *juror) verdict(ctx context.Context, id node.ID) (err error) {
	logID := zap.Uint32("id", uint32(id))
	j.Logger.Debug("juror received proposal. making verdict.", logID)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, appID := range j.approvals {
		if appID == id {
			j.Logger.Error("juror rejected proposal. already approved for a different pledge.", logID)
			return errProposalRejected
		}
	}
	if id > filter.MaxMapKey(j.candidates()) {
		j.approvals = append(j.approvals, id)
		j.Logger.Debug("juror approved proposal.", logID)
		return nil
	}
	j.Logger.Error("juror rejected proposal. id out of range", logID)
	return errProposalRejected
}
