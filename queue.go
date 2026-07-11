package sseeventbus

import (
	"context"
	"sync"
)

type eventQueue struct {
	mu         sync.Mutex
	items      []*ClientEvent
	capacity   int
	itemReady  chan struct{}
	spaceReady chan struct{}
}

func newEventQueue(capacity int) *eventQueue {
	return &eventQueue{capacity: capacity, itemReady: make(chan struct{}, 1), spaceReady: make(chan struct{}, 1)}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (q *eventQueue) push(ctx context.Context, stop <-chan struct{}, event *ClientEvent) error {
	for {
		q.mu.Lock()
		if len(q.items) < q.capacity {
			q.items = append(q.items, event)
			signal(q.itemReady)
			q.mu.Unlock()
			return nil
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stop:
			return ErrClosed
		case <-q.spaceReady:
		}
	}
}

func (q *eventQueue) pop(stop <-chan struct{}) (*ClientEvent, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			event := q.items[0]
			copy(q.items, q.items[1:])
			q.items[len(q.items)-1] = nil
			q.items = q.items[:len(q.items)-1]
			signal(q.spaceReady)
			if len(q.items) > 0 {
				signal(q.itemReady)
			}
			q.mu.Unlock()
			return event, true
		}
		q.mu.Unlock()
		select {
		case <-stop:
			return nil, false
		case <-q.itemReady:
		}
	}
}

func (q *eventQueue) len() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.items) }
func (q *eventQueue) drain() []*ClientEvent {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := q.items
	q.items = nil
	signal(q.spaceReady)
	return result
}
func (q *eventQueue) remove(match func(*ClientEvent) bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	kept := q.items[:0]
	for _, event := range q.items {
		if !match(event) {
			kept = append(kept, event)
		}
	}
	for i := len(kept); i < len(q.items); i++ {
		q.items[i] = nil
	}
	q.items = kept
	signal(q.spaceReady)
}
