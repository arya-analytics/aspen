package gossip

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"time"
)

type Config struct {
	OperationInlet  confluence.Inlet[Operation]
	OperationOutlet confluence.Outlet[Operation]
	Transport       Transport
	Shutdown        shutdown.Shutdown
	Interval        time.Duration
	Logger          *zap.Logger
}
