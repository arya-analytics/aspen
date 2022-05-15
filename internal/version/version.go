package version

import (
	"errors"
	"sync"
)

type Version interface {
	Increment()
	Decrement()
	YoungerThan(Version) bool
	OlderThan(Version) bool
	EqualTo(Version) bool
}

type Byte struct {
	Value []byte
}

type Counter struct {
	Value int32
	mu    sync.Mutex
}

func (c *Counter) Increment() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Value++
}

type Map[T comparable] map[T]Version

func (m Map[T]) YoungerThan(key T, v Version) bool {
	iv, ok := m[key]
	if ok {
		return m[key].YoungerThan(iv)
	}
	m[key] = v
	return true
}

func (m Map[T]) OlderThan(key T, v Version) bool {
	iv, ok := m[key]
	if ok {
		return m[key].OlderThan(iv)
	}
	m[key] = v
	return false
}

func (m Map[T]) EqualTo(key T, v Version) bool {
	iv, ok := m[key]
	if ok {
		return m[key].EqualTo(iv)
	}
	m[key] = v
	return false
}

var (
	ErrOld = errors.New("old version")
)
