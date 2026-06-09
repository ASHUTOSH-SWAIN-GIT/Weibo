package bus

import (
	"context"
	"sync"
)

type InMem struct {
	mu     sync.RWMutex
	subs   map[string]map[chan Event]bool
	events chan Event
	done   chan struct{}
}

func NewInMem() *InMem {
	b := &InMem{
		subs:   make(map[string]map[chan Event]bool),
		events: make(chan Event, 256),
		done:   make(chan struct{}),
	}
	go b.fanout()
	return b
}

func (b *InMem) Publish(_ context.Context, channel string, event Event) error {
	event.Channel = channel
	select {
	case b.events <- event:
		return nil
	default:
		return nil
	}
}

func (b *InMem) Events() <-chan Event {
	return b.events
}

func (b *InMem) Close() error {
	close(b.done)
	return nil
}

func (b *InMem) fanout() {
	for {
		select {
		case <-b.done:
			return
		case event := <-b.events:
			b.mu.RLock()
			chs := b.subs[event.Channel]
			for ch := range chs {
				select {
				case ch <- event:
				default:
				}
			}
			b.mu.RUnlock()

			b.mu.RLock()
			allChs := b.subs["*"]
			for ch := range allChs {
				select {
				case ch <- event:
				default:
				}
			}
			b.mu.RUnlock()
		}
	}
}
