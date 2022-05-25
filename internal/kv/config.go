package kv

type Config struct{}

func (cfg Config) Merge(def Config) Config {
	return Config{}
}

func DefaultConfig() Config {
	return Config{}
}
