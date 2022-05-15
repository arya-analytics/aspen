package gossip

type Transport[M Message] interface {
	Send(m M) error
	Handle(func(m M) error)
}
