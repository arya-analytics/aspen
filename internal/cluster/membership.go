package cluster

import (
	"context"
	"errors"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/filter"
	"github.com/arya-analytics/x/rand"
	"golang.org/x/sync/errgroup"
	"sync"
)

type MembershipRequest struct {
	Responsible NodeID
	ProposedID  NodeID
}

type MembershipJuror struct {
	Cluster   Cluster
	mu        sync.Mutex
	approvals []MembershipRequest
}

func (m *MembershipJuror) Approve(req MembershipRequest) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, app := range m.approvals {
		if app.ProposedID == req.ProposedID {
			return false
		}
	}

	approved := filter.MaxMapKey(m.Cluster.Snapshot()) > req.ProposedID
	if approved {
		m.approvals = append(m.approvals, req)
	}

	return approved
}

type MembershipTransport interface {
	Send(ctx context.Context, addr address.Address, req MembershipRequest) error
	Handle(func(req MembershipRequest) error)
}

type MembershipResponsible struct {
	Cluster   Cluster
	Transport MembershipTransport
	Excluded  []NodeID
}

func (m *MembershipResponsible) Request() (NodeID, error) {
	for {
		snap := m.Cluster.Snapshot()

		quorumCount := len(snap)/2 + 1

		healthy := snap.WhereState(NodeStateHealthy).Where(func(id NodeID, _ Node) bool {
			return !filter.ElementOf(m.Excluded, id)
		})

		if len(healthy) < quorumCount {
			return 0, errors.New("cluster does not have enough healthy nodes")
		}

		quorum := rand.MapSub(healthy, quorumCount)

		// propose an ID to the quorum
		proposedID := filter.MaxMapKey(snap) + 1

		ctx, cancel := context.WithCancel(context.Background())

		wg := errgroup.Group{}

		for _, node := range quorum {
			wg.Go(func() error {
				err := m.Transport.Send(ctx, node.Address, MembershipRequest{
					Responsible: m.Cluster.Host().ID,
					ProposedID:  proposedID,
				})
				if err != nil {
					cancel()
				}
				if errors.Is(err, ErrUnreachable) {
					m.Excluded = append(m.Excluded, node.ID)
				}
				return err
			})

		}

		err := wg.Wait()
		cancel()
		if err != nil {
			continue
		}
		return proposedID, nil
	}
}

var (
	ErrMembershipRequestDenied = errors.New("membership request denied")
	ErrUnreachable             = errors.New("unreachable")
)
