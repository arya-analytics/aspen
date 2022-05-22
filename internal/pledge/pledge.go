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
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

const (
	pledgeBaseRetry  = 1 * time.Second
	pledgeRetryScale = 1.5
	requestTimeout   = 5 * time.Second
)

var (
	errProposalRejected  = errors.New("proposal rejected")
	ErrQuorumUnreachable = errors.New("quorum unreachable")
	ErrNoPeers           = errors.New("no peers")
)

// Pledge pledges a node to a Jury selected from candidates for membership. Membership behaves in a similar
// manner to a one way mutex. A Pledge will submit a request to a peer in peers, this peer will then request for a
// random, quorum jury from candidates. If the jury approves the pledge, the node will be given membership, assigned a
// unique ID, and allowed to Arbitrate in future proposals. See algorithm in package level documentation for
// implementation details.
//
// Although IDs are guaranteed to be unique, they are not guarantee to be sequential.
//
// Pledge will continue to contact peers in cfg.peerAddresses at a scaling interval until the provided context is cancelled.
//
func Pledge(ctx context.Context, peers []address.Address, candidates func() Group, cfg Config) (id ID, err error) {
	if len(peers) == 0 {
		return 0, ErrNoPeers
	}
	cfg.peerAddresses, cfg.candidates = peers, candidates
	cfg = cfg.Merge(DefaultConfig())
	nextAddr := iter.InfiniteSlice(cfg.peerAddresses)
	t := xtime.NewScaledTicker(cfg.PledgeBaseRetry, cfg.PledgeRetryScale)
	defer t.Stop()
	for range t.C {
		reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		id, err = cfg.Transport.Send(reqCtx, nextAddr(), 0)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		Arbitrate(cfg)
	}
	return id, err
}

func Arbitrate(cfg Config) {
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
	return cfg
}

func DefaultConfig() Config {
	return Config{
		RequestTimeout:   requestTimeout,
		PledgeBaseRetry:  pledgeBaseRetry,
		PledgeRetryScale: pledgeRetryScale,
	}
}

// |||||| RESPONSIBLE ||||||

type responsible struct {
	Config
	candidates  Group
	_proposedID ID
}

func (r *responsible) propose(ctx context.Context) (id ID, err error) {
	for {
		select {
		case <-ctx.Done():
			return id, ctx.Err()
		default:
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

		quorum, err := r.buildQuorum()
		if err != nil {
			return 0, err
		}
		// If any node returns an error, it means we need to retry the responsible with a new ID.
		if err = r.consultQuorum(ctx, id, quorum); err != nil {
			continue
		}

		// If no candidates return an error, it means we reached a quorum approval,
		// and we can safely return the new ID to the caller.
		return id, nil
	}
}

func (r *responsible) refreshCandidates() {
	r.candidates = r.Config.candidates()
}

func (r *responsible) buildQuorum() (Group, error) {
	size := len(r.candidates)/2 + 1
	healthy := r.candidates.WhereState(StateHealthy)
	if len(healthy) < size {
		return Group{}, ErrQuorumUnreachable
	}
	return rand.MapSub(healthy, size), nil
}

func (r *responsible) idToPropose() ID {
	if r._proposedID == 0 {
		r._proposedID = filter.MaxMapKey(r.candidates)
	} else {
		r._proposedID++
	}
	return r._proposedID
}

func (r *responsible) consultQuorum(ctx context.Context, id ID, quorum Group) error {
	ctx, cancel := context.WithTimeout(ctx, r.RequestTimeout)
	defer cancel()
	wg := errgroup.Group{}
	for _, n := range quorum {
		wg.Go(func() error {
			_, err := r.Transport.Send(ctx, n.Address, id)
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
	if ctx.Err() != nil {
		return ctx.Err()
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, appID := range j.approvals {
		if appID == id {
			return errProposalRejected
		}
	}
	if filter.MaxMapKey(j.candidates()) > id {
		j.approvals = append(j.approvals, id)
		return nil
	}
	return errProposalRejected
}
