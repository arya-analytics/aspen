package member

import (
	"context"
	"github.com/arya-analytics/x/filter"
	"sync"
)

type juror struct {
	Config
	mu        sync.Mutex
	approvals []ProposalRequest
}

func (j *juror) processProposal(_ context.Context, req ProposalRequest) (ProposalResponse, error) {
	return ProposalResponse{}, j.approve(req)
}

func (j *juror) approve(req ProposalRequest) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, app := range j.approvals {
		if app.ID == req.ID {
			return errProposalRejected
		}
	}

	if filter.MaxMapKey(j.Source()) > req.ID {
		j.approvals = append(j.approvals, req)
		return nil
	}

	return errProposalRejected
}
