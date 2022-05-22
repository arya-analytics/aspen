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
	j, r := &juror{Config: cfg}, &responsible{Config: cfg}
	if cfg.ProposalTransport != nil {
		cfg.ProposalTransport.Handle(j.processProposal)
	}
	if cfg.PledgeTransport != nil {
		cfg.PledgeTransport.Handle(r.processPledge)
	}
	return &Member{cfg}
}
