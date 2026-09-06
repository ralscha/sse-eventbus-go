package sseeventbus

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestReplayCompleteAfterMessageStopsSuccessfully(t *testing.T) {
	store := NewMemoryReplayStore()
	bus := newSyncBus(t, WithReplay(store, time.Minute, time.Minute))
	for _, id := range []string{"1", "2", "3"} {
		store.Store(ReplayEvent{ClientID: "c", Event: Event{ID: id}, ConvertedValue: id, HasConverted: true, StoredAt: time.Now()})
	}
	conn := &recordingConnection{}
	if err := bus.Register("c", conn, SubscribeTo(DefaultEvent), ReplayFrom(""), CompleteAfterMessage()); err != nil {
		t.Fatalf("successful one-message replay returned %v", err)
	}
	if got := conn.snapshot(); len(got) != 1 || got[0].ID != "1" || !conn.isClosed() {
		t.Fatalf("replay = %#v, closed = %v", got, conn.isClosed())
	}
	if got := store.EventsSince("c", "1"); len(got) != 2 {
		t.Fatalf("remaining replay history = %#v", got)
	}
}

func TestReplaySkipsExpiredEventsBeforeCleanup(t *testing.T) {
	store := NewMemoryReplayStore()
	bus := newSyncBus(t, WithReplay(store, time.Minute, time.Hour))
	store.Store(ReplayEvent{ClientID: "c", Event: Event{ID: "expired"}, StoredAt: time.Now().Add(-time.Hour)})
	store.Store(ReplayEvent{ClientID: "c", Event: Event{ID: "recent"}, StoredAt: time.Now()})
	conn := &recordingConnection{}
	if err := bus.Register("c", conn, SubscribeTo(DefaultEvent), ReplayFrom("")); err != nil {
		t.Fatal(err)
	}
	if got := conn.snapshot(); len(got) != 1 || got[0].ID != "recent" {
		t.Fatalf("replay = %#v", got)
	}
}

func TestCanceledQueuePushDoesNotEnqueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	queue := newEventQueue(1)
	if err := queue.push(ctx, &ClientEvent{client: &client{}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("push = %v, want context.Canceled", err)
	}
	if queue.len() != 0 {
		t.Fatal("canceled event was queued")
	}
}

func TestQueueCompactionReleasesUnusedReferences(t *testing.T) {
	queue := newEventQueue(2050)
	for range 2050 {
		if err := queue.push(context.Background(), &ClientEvent{client: &client{}}); err != nil {
			t.Fatal(err)
		}
	}
	for range 1025 {
		queue.pop()
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for _, event := range queue.items[:cap(queue.items)][len(queue.items):] {
		if event != nil {
			t.Fatal("compacted queue retained an event beyond its length")
		}
	}
}

type interfaceFieldConnection struct{ value any }

func (interfaceFieldConnection) Send(Message) error { return nil }
func (interfaceFieldConnection) Close() error       { return nil }

func TestReconnectAcceptsConnectionWithNonComparableInterfaceField(t *testing.T) {
	bus := newSyncBus(t)
	conn := interfaceFieldConnection{value: []int{1}}
	if err := bus.Register("c", conn); err != nil {
		t.Fatal(err)
	}
	if err := bus.Register("c", conn); err != nil {
		t.Fatal(err)
	}
}

func TestRetryBackpressureMakesProgress(t *testing.T) {
	bus, err := New(WithQueueCapacities(1, 1), WithSendAttempts(3), WithRetryBackoff(time.Millisecond, time.Millisecond), WithoutClientExpiration())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := bus.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Independent clients ensure one client's exhaustion cannot make the test
	// pass by simply dropping all remaining messages for that same client.
	for i := range 20 {
		id := strconv.Itoa(i)
		if err := bus.Register(id, &recordingConnection{failures: 1}, SubscribeTo(DefaultEvent)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bus.Publish(ctx, NewEvent("payload")); err != nil {
		t.Fatalf("full send and retry queues stalled publication: %v", err)
	}
	for {
		complete := true
		for _, id := range bus.ClientIDs() {
			bus.mu.RLock()
			conn := bus.clients[id].connection.(*recordingConnection)
			bus.mu.RUnlock()
			complete = complete && len(conn.snapshot()) == 1
		}
		if complete {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("delivery stalled with both queues full")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestDisconnectPreservesHistoryAndProtectsReplacement(t *testing.T) {
	store := NewMemoryReplayStore()
	bus := newSyncBus(t, WithReplay(store, time.Minute, time.Minute))
	old := &recordingConnection{}
	if err := bus.Register("c", old, SubscribeTo(DefaultEvent)); err != nil {
		t.Fatal(err)
	}
	if !bus.Disconnect("c", old) || !old.isClosed() {
		t.Fatal("matching connection was not disconnected")
	}
	if !bus.IsClientRegistered("c") || !bus.HasSubscribers(DefaultEvent) {
		t.Fatal("disconnect removed logical client state")
	}
	if err := bus.Publish(context.Background(), Event{ID: "offline", Data: "saved"}); err != nil {
		t.Fatal(err)
	}
	current := &recordingConnection{}
	if err := bus.Register("c", current, ReplayFrom("")); err != nil {
		t.Fatal(err)
	}
	if got := current.snapshot(); len(got) != 1 || got[0].ID != "offline" {
		t.Fatalf("offline replay = %#v", got)
	}
	if bus.Disconnect("c", old) || current.isClosed() {
		t.Fatal("stale disconnect affected the replacement")
	}
	if bus.Disconnect("missing", current) || bus.Disconnect("c", nil) {
		t.Fatal("disconnect accepted a missing client or connection")
	}
}

func TestClosedConnectionStillPublishesRemotely(t *testing.T) {
	transport := &loopTransport{}
	bus := newSyncBus(t, WithDistributedTransport(transport))
	conn := &recordingConnection{closed: true}
	if err := bus.Register("c", conn, SubscribeTo(DefaultEvent)); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), NewEvent("value")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish = %v", err)
	}
	if len(transport.published) != 1 || !bus.IsClientRegistered("c") {
		t.Fatal("closed connection prevented remote publication or lost client state")
	}
}

func TestOperationsCancelWhileWaitingForClientLock(t *testing.T) {
	bus := newSyncBus(t, WithReplay(NewMemoryReplayStore(), time.Minute, time.Minute))
	if err := bus.Register("c", &recordingConnection{}, SubscribeTo(DefaultEvent)); err != nil {
		t.Fatal(err)
	}
	for name, operation := range map[string]func(context.Context) error{
		"register": func(ctx context.Context) error { return bus.RegisterContext(ctx, "c", &recordingConnection{}) },
		"publish":  func(ctx context.Context) error { return bus.Publish(ctx, NewEvent("value")) },
		"replay":   func(ctx context.Context) error { return bus.ReplayMissedEvents(ctx, "c", "") },
	} {
		t.Run(name, func(t *testing.T) {
			unlock := bus.lockClient("c")
			defer unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- operation(ctx) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("operation = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("context did not cancel lifecycle lock wait")
			}
		})
	}
	bus.clientLockMu.Lock()
	defer bus.clientLockMu.Unlock()
	if len(bus.clientLocks) != 0 {
		t.Fatal("cancellation leaked client locks")
	}
}

func TestRetryExhaustionDoesNotWaitOnPublishingClient(t *testing.T) {
	bus, err := New(WithQueueCapacities(1, 1), WithSendAttempts(1), WithoutClientExpiration())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	if err := bus.Register("c", &recordingConnection{failures: 1}, SubscribeTo(DefaultEvent)); err != nil {
		t.Fatal(err)
	}
	unlock := bus.lockClient("c")
	var once sync.Once
	defer once.Do(unlock)
	bus.mu.RLock()
	target := bus.clients["c"]
	bus.mu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range 10 {
		if err := bus.sendQueue.push(ctx, &ClientEvent{ClientID: "c", client: target, Event: NewEvent("value")}); err != nil {
			t.Fatalf("worker stalled waiting for lifecycle lock: %v", err)
		}
	}
	once.Do(unlock)
	for bus.IsClientRegistered("c") {
		select {
		case <-ctx.Done():
			t.Fatal("exhausted client was not removed")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestSynchronousReplayObserverCanAccessClientLifecycle(t *testing.T) {
	store := NewMemoryReplayStore()
	store.Store(ReplayEvent{ClientID: "c", Event: Event{ID: "1"}, StoredAt: time.Now()})
	var bus *Bus
	bus = newSyncBus(t, WithReplay(store, time.Minute, time.Minute), WithObserver(ObserverFunc(func(_ context.Context, o Observation) {
		if o.Operation == OperationSendEvent {
			bus.Unregister(o.ClientID)
		}
	})))
	done := make(chan error, 1)
	go func() { done <- bus.Register("c", &recordingConnection{}, SubscribeTo(DefaultEvent), ReplayFrom("")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("send observer ran under lifecycle lock")
	}
}
