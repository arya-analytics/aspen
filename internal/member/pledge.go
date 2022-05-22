package member

import (
	"context"
	"errors"
	"github.com/arya-analytics/aspen/internal/node"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/iter"
	xtime "github.com/arya-analytics/x/util/time"
	"time"
)

const (
	pledgeBaseInterval = 1 * time.Second
	pledgeScale        = 1.5
)

func pledge(ctx context.Context, peerAddresses []address.Address, cfg Config) (id node.ID, err error) {
	nextAddr := iter.Slice(peerAddresses)
	t := xtime.NewScaledTicker(pledgeBaseInterval, pledgeScale)
	for range t.C {
		ctx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		var res PledgeResponse
		res, err = cfg.PledgeTransport.Send(ctx, nextAddr(), PledgeRequest{})
		id = res.ID
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			break
		}
	}
	return id, err
}
