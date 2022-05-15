package gossip

import (
	"errors"
	"github.com/arya-analytics/aspen/internal/version"
)

type SIRState int

const (
	SIRStateSusceptible SIRState = iota
	SIRStateInfected
	SIRStateRemoved
)

type SIR[M Message] struct {
	State int
	Base[M]
}

func NewSIR[M Message](transport Transport[M]) *SIR[M] {
	return &SIR[M]{
		Base: Base[M]{
			Middleware: MiddlewareChain{
				VersionMiddleware{},
			},
			Transport: transport,
		},
	}
}

func (sir *SIR[M]) Gossip(m Message) error {
	err := sir.Base.Gossip(m)
	if errors.Is(err, version.ErrOld) {

	}
}

func (sir *SIR[M])
