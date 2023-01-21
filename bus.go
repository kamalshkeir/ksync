package ksync

import (
	"sync"
)

type Bus[T any] struct {
	Subs sync.Map
	mu   sync.RWMutex
}

func NewBus[T any]() *Bus[T] {
	return &Bus[T]{
		Subs: sync.Map{},
	}
}

func (b *Bus[T]) Subscribe(topic string, fn func(data T)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs, _ := b.Subs.LoadOrStore(topic, make(map[string]chan T))
	channels := subs.(map[string]chan T)
	ch := make(chan T, 64)
	channels[topic] = ch
	go func() {
		for v := range ch {
			fn(v)
		}
	}()
}

func (b *Bus[T]) Publish(topic string, data T) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	subs, ok := b.Subs.Load(topic)
	if !ok {
		return
	}
	channels := subs.(map[string]chan T)
	for _, ch := range channels {
		ch <- data
	}
}

func (b *Bus[T]) Unsubscribe(topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs, ok := b.Subs.Load(topic)
	if !ok {
		return
	}
	channels := subs.(map[string]chan T)
	delete(channels, topic)
}
