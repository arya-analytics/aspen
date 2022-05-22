// Package pledge provides a system for pledging a node to a jury of candidates. The pledge uses quorum consensus
// to assign the node a unique ID.
//
// To pledge a new node to a jury, call Pledge() with a set of peer addresses. To register a node as a candidate,
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
	"errors"
	. "github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/filter"
	"github.com/arya-analytics/x/iter"
	"github.com/arya-analytics/x/rand"
	"github.com/arya-analytics/x/transport"
	xtime "github.com/arya-analytics/x/util/time"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

var (
	// ErrQuorumUnreachable is returned when a quorum jury cannot be safely assembled.
	ErrQuorumUnreachable = errors.New("quorum unreachable")
	// ErrNoPeers is returned when no peers are provided to the Pledge.
	ErrNoPeers = errors.New("no peers")
	// errProposalRejected is an internal error returned when a juror rejects a pledge proposal from a responsible node.
	errProposalRejected = errors.New("proposal rejected")
)

// Pledge pledges a node to a Jury selected from candidates for membership. Membership behaves in a similar
// manner to a one way mutex. A Pledge will submit a request to a peer in peers, this peer will then request for a
// random, quorum jury from candidates. If the jury approves the pledge, the node will be given membership, assigned a
// unique ID, and allowed to Arbitrate in future proposals. See algorithm in package level documentation for
// implementation details. Although IDs are guaranteed to be unique, they are not guarantee to be sequential. Pledge
// will continue to contact peers in cfg.peerAddresses at a scaling interval until the provided context is cancelled.
func Pledge(ctx context.Context, peers []address.Address, candidates func() Group, cfg Config) (id ID, err error) {
	if len(peers) == 0 {
		return id, ErrNoPeers
	}

	cfg.peerAddresses, cfg.candidates = peers, candidates
	cfg = cfg.Merge(DefaultConfig())

	nextAddr := iter.InfiniteSlice(cfg.peerAddresses)

	t := xtime.NewScaledTicker(cfg.PledgeBaseRetry, cfg.PledgeRetryScale)
	defer t.Stop()

	for range t.C {
		addr := nextAddr()
		cfg.Logger.Debug("pledging to peer", zap.String("address", string(addr)))
		reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		id, err = cfg.Transport.Send(reqCtx, addr, 0)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			break
		}
		cfg.Logger.Error("failed to contact peer. retrying with next")
	}

	if err != nil {
		cfg.Logger.Error("pledge failed", zap.Error(err))
		return 0, err

	}
	cfg.Logger.Debug("pledge successful", zap.Uint32("id", uint32(id)))
	// If the pledge node has been inducted successfully, allow it to arbitrate in future pledges.
	Arbitrate(candidates, cfg)
	return id, err

}

// Arbitrate registers a node to arbitrate future pledges. When a node calls Arbitrate, it will be made available
// to become a Responsible or Juror node. Any node that calls arbitrate should also be a member of candidates.
func Arbitrate(candidates func() Group, cfg Config) {
	cfg.candidates = candidates
	cfg = cfg.Merge(DefaultConfig())
	j := &juror{Config: cfg}
	cfg.Transport.Handle(func(ctx context.Context, id ID) (ID, error) {
		if id == 0 {
			t := &responsible{Config: cfg}
			return t.propose(ctx)
		}
		return 0, j.verdict(ctx, id)
	})
}

// Config is used for configuring a pledge based membership network.
type Config struct {
	// Transport is used for sending pledge information over the network.
	Transport transport.Unary[ID, ID]
	// RequestTimeout is the timeout for a peer to respond to a pledge or proposal request.
	// If the request is not responded to before the timeout, a new jury will be formed and the request will be retried.
	RequestTimeout time.Duration
	// PledgeBaseRetry sets the initial retry interval for a Pledge to a peer.
	PledgeBaseRetry time.Duration
	// PledgeInterval scale sets how quickly the time in-between retries will increase during a Pledge to a peer. For example,
	// a value of 2 would result in a retry interval of 1,2, 4, 8, 16, 32, 64, ... seconds.
	PledgeRetryScale float64
	// Logger is where the pledge process will log to.
	Logger *zap.Logger
	// MaxProposals is the maximum number of proposals a responsible will make to a quorum before giving up.
	MaxProposals int
	// candidates is a Group of nodes to contact for as candidates for the formation of a jury.
	candidates func() Group
	// peerAddresses is a set of addresses a pledge can contact.
	peerAddresses []address.Address
}

func (cfg Config) Merge(override Config) Config {
	if cfg.PledgeBaseRetry == 0 {
		cfg.PledgeBaseRetry = override.PledgeBaseRetry
	}
	if cfg.PledgeRetryScale == 0 {
		cfg.PledgeRetryScale = override.PledgeRetryScale
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = override.RequestTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = override.Logger
	}
	if cfg.MaxProposals == 0 {
		cfg.MaxProposals = override.MaxProposals
	}
	return cfg
}

func DefaultConfig() Config {
	return Config{
		RequestTimeout:   5 * time.Second,
		PledgeBaseRetry:  1 * time.Second,
		PledgeRetryScale: 1.5,
		Logger:           zap.NewNop(),
		MaxProposals:     10,
	}
}

// |||||| RESPONSIBLE ||||||

type responsible struct {
	Config
	candidates  Group
	_proposedID ID
}

func (r *responsible) propose(ctx context.Context) (id ID, err error) {
	r.Logger.Debug("responsible received pledge. starting proposal process.")
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

		r.Logger.Debug("responsible proposing id", logID, zap.Int("quorumCount", len(quorum)))

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

func (r *responsible) buildQuorum() (Group, error) {
	presentCandidates := r.candidates.Where(func(_ ID, n Node) bool { return n.State != StateLeft })
	size := len(presentCandidates)/2 + 1
	healthy := presentCandidates.WhereState(StateHealthy)
	if len(healthy) < size {
		return Group{}, ErrQuorumUnreachable
	}
	return rand.MapSub(healthy, size), nil
}

func (r *responsible) idToPropose() ID {
	if r._proposedID == 0 {
		r._proposedID = filter.MaxMapKey(r.candidates) + 1
	} else {
		r._proposedID++
	}
	return r._proposedID
}

func (r *responsible) consultQuorum(ctx context.Context, id ID, quorum Group) error {
	reqCtx, cancel := context.WithTimeout(ctx, r.RequestTimeout)
	defer cancel()
	wg := errgroup.Group{}
	for _, n := range quorum {
		node := n
		wg.Go(func() error {
			_, err := r.Transport.Send(reqCtx, node.Address, id)
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
	approvals []ID
}

func (j *juror) verdict(ctx context.Context, id ID) (err error) {
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
