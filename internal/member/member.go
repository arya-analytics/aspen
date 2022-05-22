package member

import (
	"context"
	"errors"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/transport"
	"time"
)

var (
	errProposalRejected  = errors.New("proposal rejected")
	errQuorumUnreachable = errors.New("quorum unreachable")
)

type (
	ProposalRequest   struct{ ID node.ID }
	ProposalResponse  struct{}
	ProposalTransport transport.Unary[ProposalRequest, ProposalResponse]
)

type Source func() node.Group

type Config struct {
	PledgeTransport   PledgeTransport
	ProposalTransport ProposalTransport
	RequestTimeout    time.Duration
	Source            Source
}

// |||||| PLEDGE ||||||

type (
	PledgeRequest   struct{}
	PledgeResponse  struct{ ID node.ID }
	PledgeTransport transport.Unary[PledgeRequest, PledgeResponse]
)

type Member struct {
	Config
}

func (m *Member) Pledge(ctx context.Context, peerAddresses []address.Address) (node.ID, error) {
	return pledge(ctx, peerAddresses, m.Config)
}

func New(cfg Config) *Member {
	j := &juror{Config: cfg}
	if cfg.ProposalTransport != nil {
		cfg.ProposalTransport.Handle(j.processProposal)
	}
	cfg.PledgeTransport.Handle(func(ctx context.Context, _ PledgeRequest) (PledgeResponse, error) {
		resp := &responsible{Config: cfg}
		id, err := resp.exec(ctx)
		return PledgeResponse{ID: id}, err
	})
	return &Member{cfg}
}
