package node

type Heartbeat struct {
	Generation uint64
	Version    uint64
}

func (n Heartbeat) YoungerThan(other Heartbeat) bool {
	return n.Generation < other.Generation ||
		(n.Generation == other.Generation && n.Version < other.Version)
}

func (n Heartbeat) EqualTo(other Heartbeat) bool {
	return n.Generation == other.Generation && n.Version == other.Version
}
