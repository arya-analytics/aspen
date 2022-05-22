package member

import (
	"context"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/filter"
	"github.com/arya-analytics/x/rand"
	"golang.org/x/sync/errgroup"
)

type responsible struct {
	Config
}

func (r *responsible) processPledge(ctx context.Context, _ PledgeRequest) (PledgeResponse, error) {
	t := &transaction{Config: r.Config}
	id, err := t.exec(ctx)
	return PledgeResponse{ID: id}, err
}

type transaction struct {
	Config
	nodes       node.Group
	_proposedID node.ID
}

func (t *transaction) exec(ctx context.Context) (node.ID, error) {
	t.source()
	id := t.proposedID()
	for {
		quorum, err := t.quorum()
		if err != nil {
			return 0, err
		}
		// If any node returns an error, it means we need to retry
		// the transaction, incrementing the proposed ID.
		if err = t.propose(ctx, id, quorum); err != nil {
			// re-source the nodes so we get a current view of cluster state.
			t.source()

			// Increment the proposed ID unconditionally. Quorum juror's store each
			// approved request. If one node in the quorum is unreachable, other nodes
			// may have already approved the request. This means that if we retry the request
			// without incrementing the proposed ID, we'll get a rejection from the nodes that
			// approved the request last time. This will result in marginally higher IDs being
			// assigned, but it's better than adding a lot of extra transaction logic.
			id = t.proposedID()

			continue
		}
		// If no nodes return an error, it means we reached a quorum approval,
		// and we can safely return the new ID to the caller.
		return id, nil
	}
}

func (t *transaction) source() {
	t.nodes = t.Config.Source()
}

func (t *transaction) quorum() (node.Group, error) {
	size := len(t.nodes)/2 + 1
	healthy := t.nodes.WhereState(node.StateHealthy)
	if len(healthy) < size {
		return node.Group{}, errQuorumUnreachable
	}
	return rand.MapSub(healthy, size), nil
}

func (t *transaction) proposedID() node.ID {
	if t._proposedID == 0 {
		t._proposedID = filter.MaxMapKey(t.nodes)
	} else {
		t._proposedID++
	}
	return t._proposedID
}

func (t *transaction) propose(ctx context.Context, id node.ID, quorum node.Group) error {
	ctx, cancel := context.WithTimeout(ctx, t.RequestTimeout)
	defer cancel()
	wg := errgroup.Group{}
	req := ProposalRequest{ID: id}
	for _, n := range quorum {
		wg.Go(func() error {
			_, err := t.ProposalTransport.Send(ctx, n.Address, req)
			// If any node returns an error, we need to retry the entire transaction,
			// so we need to cancel all running requests.
			if err != nil {
				cancel()
			}
			return err
		})
	}
	return wg.Wait()
}
