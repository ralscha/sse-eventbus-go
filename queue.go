package sseeventbus

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type eventQueue struct {
	mu         sync.Mutex
	items      []*ClientEvent
	head       int
	capacity   int
	itemReady  chan struct{}
	spaceReady chan struct{}
	closed     bool
	closedCh   chan struct{}
}

func newEventQueue(capacity int) *eventQueue {
	return &eventQueue{
		capacity:   capacity,
		itemReady:  make(chan struct{}, 1),
		spaceReady: make(chan struct{}, 1),
		closedCh:   make(chan struct{}),
	}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (q *eventQueue) push(ctx context.Context, event *ClientEvent) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrClosed
		}
		if q.sizeLocked() < q.capacity {
			event.sendOrder = event.client.assignOrder()
			q.items = append(q.items, event)
			signal(q.itemReady)
			// A single notification may represent several newly available slots.
			// Pass it on so blocked producers can fill all of them.
			if q.sizeLocked() < q.capacity {
				signal(q.spaceReady)
			}
			q.mu.Unlock()
			return nil
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.closedCh:
			return ErrClosed
		case <-q.spaceReady:
		}
	}
}

func (q *eventQueue) pop() (*ClientEvent, bool) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, false
		}
		if q.sizeLocked() > 0 {
			event := q.items[q.head]
			q.items[q.head] = nil
			q.head++
			q.compactLocked()
			signal(q.spaceReady)
			if q.sizeLocked() > 0 {
				signal(q.itemReady)
			}
			q.mu.Unlock()
			return event, true
		}
		q.mu.Unlock()
		select {
		case <-q.closedCh:
			return nil, false
		case <-q.itemReady:
		}
	}
}

func (q *eventQueue) close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.closedCh)
	}
	q.mu.Unlock()
}

func (q *eventQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.sizeLocked()
}

func (q *eventQueue) drain() []*ClientEvent {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := append([]*ClientEvent(nil), q.items[q.head:]...)
	for i := q.head; i < len(q.items); i++ {
		q.items[i] = nil
	}
	q.items = nil
	q.head = 0
	signal(q.spaceReady)
	return result
}

func (q *eventQueue) remove(match func(*ClientEvent) bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	kept := q.items[:0]
	for _, event := range q.items[q.head:] {
		if match(event) {
			event.client.cancelTurn(event.sendOrder)
			continue
		}
		kept = append(kept, event)
	}
	for i := len(kept); i < len(q.items); i++ {
		q.items[i] = nil
	}
	q.items = kept
	q.head = 0
	signal(q.spaceReady)
}

func (q *eventQueue) sizeLocked() int { return len(q.items) - q.head }

func (q *eventQueue) compactLocked() {
	if q.head == len(q.items) {
		q.items = nil
		q.head = 0
		return
	}
	if q.head >= 1024 && q.head*2 >= len(q.items) {
		copy(q.items, q.items[q.head:])
		q.items = q.items[:len(q.items)-q.head]
		q.head = 0
	}
}

// retryQueue is a bounded priority queue. A single scheduler removes due
// events; failed-send workers only add events, so the scheduler never blocks
// trying to reinsert an item into its own queue.
type retryQueue struct {
	mu         sync.Mutex
	items      retryHeap
	capacity   int
	sequence   uint64
	changed    chan struct{}
	spaceReady chan struct{}
	closed     bool
	closedCh   chan struct{}
}

func newRetryQueue(capacity int) *retryQueue {
	return &retryQueue{
		capacity:   capacity,
		changed:    make(chan struct{}, 1),
		spaceReady: make(chan struct{}, 1),
		closedCh:   make(chan struct{}),
	}
}

func (q *retryQueue) push(ctx context.Context, event *ClientEvent) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrClosed
		}
		if len(q.items) < q.capacity {
			q.sequence++
			event.retryOrder = q.sequence
			heap.Push(&q.items, event)
			signal(q.changed)
			if len(q.items) < q.capacity {
				signal(q.spaceReady)
			}
			q.mu.Unlock()
			return nil
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.closedCh:
			return ErrClosed
		case <-q.spaceReady:
		}
	}
}

// popDue returns one due event, the next due time when no event is ready, and
// false after the queue has closed.
func (q *retryQueue) popDue(now time.Time) (*ClientEvent, time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, time.Time{}, false
	}
	if len(q.items) == 0 {
		return nil, time.Time{}, true
	}
	next := q.items[0]
	if now.Before(next.retryAfter) {
		return nil, next.retryAfter, true
	}
	event := heap.Pop(&q.items).(*ClientEvent)
	signal(q.spaceReady)
	return event, time.Time{}, true
}

func (q *retryQueue) remove(match func(*ClientEvent) bool) {
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
	heap.Init(&q.items)
	signal(q.changed)
	signal(q.spaceReady)
}

func (q *retryQueue) close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.closedCh)
	}
	q.mu.Unlock()
}

func (q *retryQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *retryQueue) clear() {
	q.mu.Lock()
	for i := range q.items {
		q.items[i] = nil
	}
	q.items = nil
	q.mu.Unlock()
}

func (q *retryQueue) changes() <-chan struct{} { return q.changed }

type retryHeap []*ClientEvent

func (h retryHeap) Len() int { return len(h) }
func (h retryHeap) Less(i, j int) bool {
	if h[i].retryAfter.Equal(h[j].retryAfter) {
		return h[i].retryOrder < h[j].retryOrder
	}
	return h[i].retryAfter.Before(h[j].retryAfter)
}
func (h retryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *retryHeap) Push(value any) {
	*h = append(*h, value.(*ClientEvent))
}
func (h *retryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = nil
	*h = old[:last]
	return value
}
