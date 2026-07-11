package sseeventbus

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingConnection struct {
	mu       sync.Mutex
	messages []Message
	failures int
	closed   bool
}

func (c *recordingConnection) Send(message Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failures > 0 {
		c.failures--
		return errors.New("send failed")
	}
	if c.closed {
		return ErrClosed
	}
	c.messages = append(c.messages, message)
	return nil
}
func (c *recordingConnection) Close() error { c.mu.Lock(); c.closed = true; c.mu.Unlock(); return nil }
func (c *recordingConnection) snapshot() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Message(nil), c.messages...)
}
func (c *recordingConnection) isClosed() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.closed }

func newSyncBus(t *testing.T, options ...Option) *Bus {
	t.Helper()
	options = append([]Option{WithSynchronousDelivery()}, options...)
	bus, err := New(options...)
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
	return bus
}

func TestPublishSubscriptionTargetingAndExclusion(t *testing.T) {
	bus := newSyncBus(t)
	one, two, three := &recordingConnection{}, &recordingConnection{}, &recordingConnection{}
	for id, connection := range map[string]*recordingConnection{"1": one, "2": two, "3": three} {
		if err := bus.Register(id, connection, SubscribeTo("orders")); err != nil {
			t.Fatal(err)
		}
	}
	event := NewNamedEventWithData("orders", "all")
	event.ExcludeClientIDs = []string{"2"}
	if err := bus.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(one.snapshot()) != 1 || len(two.snapshot()) != 0 || len(three.snapshot()) != 1 {
		t.Fatalf("unexpected broadcast counts: %d %d %d", len(one.snapshot()), len(two.snapshot()), len(three.snapshot()))
	}
	direct := NewNamedEventWithData("orders", "direct")
	direct.ClientIDs = []string{"2", "2"}
	direct.ExcludeClientIDs = []string{"2"}
	if err := bus.Publish(context.Background(), direct); err != nil {
		t.Fatal(err)
	}
	if got := two.snapshot(); len(got) != 1 || got[0].Data != "direct" {
		t.Fatalf("direct event did not override exclusion: %#v", got)
	}
	bus.Unsubscribe("2", "orders")
	if err := bus.Publish(context.Background(), direct); err != nil {
		t.Fatal(err)
	}
	if len(two.snapshot()) != 1 {
		t.Fatal("direct delivery must still require a subscription")
	}
}

func TestSynchronousPublishContinuesAfterClientFailure(t *testing.T) {
	bus := newSyncBus(t)
	failing := &recordingConnection{failures: 1}
	healthy := &recordingConnection{}
	if err := bus.Register("bad", failing, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	if err := bus.Register("good", healthy, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	event := NewNamedEventWithData("orders", "payload")
	event.ClientIDs = []string{"bad", "good"}
	if err := bus.Publish(context.Background(), event); err == nil || !strings.Contains(err.Error(), `client "bad"`) {
		t.Fatalf("Publish error=%v", err)
	}
	if got := healthy.snapshot(); len(got) != 1 || got[0].Data != "payload" {
		t.Fatalf("healthy client did not receive event: %#v", got)
	}
}

func TestInvalidEventIsRejectedBeforeDeliveryOrDistribution(t *testing.T) {
	transport := &loopTransport{}
	bus := newSyncBus(t, WithDistributedTransport(transport))
	connection := &recordingConnection{}
	if err := bus.Register("c", connection, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	for _, event := range []Event{
		{Name: "orders\ndata:injected", Data: "x"},
		{Name: "orders", ID: "bad\x00id", Data: "x"},
		{Name: "orders", Retry: -time.Second, Data: "x"},
	} {
		if err := bus.Publish(context.Background(), event); !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("Publish(%#v) error=%v", event, err)
		}
	}
	if len(connection.snapshot()) != 0 || len(transport.published) != 0 {
		t.Fatal("invalid event was delivered or distributed")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Publish(canceled, NewNamedEventWithData("orders", "x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Publish error=%v", err)
	}
	if len(transport.published) != 0 {
		t.Fatal("canceled event was distributed")
	}
	if err := bus.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), NewNamedEventWithData("orders", "x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close error=%v", err)
	}
	if len(transport.published) != 0 {
		t.Fatal("event published remotely after Close")
	}
}

func TestRegistrationReplacementSubscriptionsAndQueries(t *testing.T) {
	bus := newSyncBus(t)
	first, second := &recordingConnection{}, &recordingConnection{}
	if err := bus.Register("client", first, SubscribeTo("a", "b")); err != nil {
		t.Fatal(err)
	}
	if err := bus.Register("client", second, ReplaceSubscriptions("b", "c")); err != nil {
		t.Fatal(err)
	}
	if !first.isClosed() {
		t.Fatal("old connection was not closed")
	}
	if got, want := bus.Events(), []string{"b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events=%v want %v", got, want)
	}
	if bus.HasSubscribers("a") || bus.CountSubscribers("b") != 1 || !reflect.DeepEqual(bus.ClientIDs(), []string{"client"}) {
		t.Fatal("query methods returned incorrect state")
	}
	bus.SubscribeOnly("client", DefaultEvent)
	if got := bus.Events(); !reflect.DeepEqual(got, []string{DefaultEvent}) {
		t.Fatalf("subscribe only left events: %v", got)
	}
}

func TestReplayKnownUnknownAndUnregister(t *testing.T) {
	store := NewMemoryReplayStore()
	bus := newSyncBus(t, WithReplay(store, time.Minute, time.Minute))
	live := &recordingConnection{}
	if err := bus.Register("c", live, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"1", "2", "3"} {
		event := NewNamedEventWithData("orders", id)
		event.ID = id
		if err := bus.Publish(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	reconnected := &recordingConnection{}
	if err := bus.Register("c", reconnected, ReplayFrom("1")); err != nil {
		t.Fatal(err)
	}
	if got := reconnected.snapshot(); len(got) != 2 || got[0].ID != "2" || got[1].ID != "3" {
		t.Fatalf("known ID replay=%#v", got)
	}
	all := &recordingConnection{}
	if err := bus.Register("c", all, ReplayFrom("missing")); err != nil {
		t.Fatal(err)
	}
	if got := all.snapshot(); len(got) != 3 {
		t.Fatalf("unknown ID should replay all, got %d", len(got))
	}
	if !bus.Unregister("c") {
		t.Fatal("client was not removed")
	}
	empty := &recordingConnection{}
	if err := bus.Register("c", empty, SubscribeTo("orders"), ReplayFrom("")); err != nil {
		t.Fatal(err)
	}
	if len(empty.snapshot()) != 0 {
		t.Fatal("explicit unregister did not clear replay history")
	}
}

type blockingReplayStore struct {
	*MemoryReplayStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingReplayStore) EventsSince(clientID, lastEventID string) []ReplayEvent {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.MemoryReplayStore.EventsSince(clientID, lastEventID)
}

func TestReconnectReplayBlocksConcurrentLivePublish(t *testing.T) {
	store := &blockingReplayStore{MemoryReplayStore: NewMemoryReplayStore(), entered: make(chan struct{}), release: make(chan struct{})}
	bus := newSyncBus(t, WithReplay(store, time.Minute, time.Minute))
	if err := bus.Register("c", &recordingConnection{}, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	reconnected := &recordingConnection{}
	registerDone := make(chan error, 1)
	go func() { registerDone <- bus.Register("c", reconnected, ReplayFrom("last")) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("replay did not begin")
	}
	publishDone := make(chan error, 1)
	go func() { publishDone <- bus.Publish(context.Background(), NewNamedEventWithData("orders", "live")) }()
	select {
	case err := <-publishDone:
		t.Fatalf("live publish passed reconnect/replay lock: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(store.release)
	if err := <-registerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-publishDone; err != nil {
		t.Fatal(err)
	}
	if got := reconnected.snapshot(); len(got) != 1 || got[0].Data != "live" {
		t.Fatalf("reconnected messages=%#v", got)
	}
}

func TestRegisterContextCancellationAndClosedBus(t *testing.T) {
	bus := newSyncBus(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.RegisterContext(ctx, "c", &recordingConnection{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RegisterContext error=%v", err)
	}
	if bus.IsClientRegistered("c") {
		t.Fatal("canceled registration changed bus state")
	}
	if err := bus.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := bus.Register("c", &recordingConnection{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("registration after Close error=%v", err)
	}
}

func TestCloseClosesConnectionsAndReportsErrors(t *testing.T) {
	closeFailure := errors.New("close failed")
	connection := &closeErrorConnection{closeErr: closeFailure}
	bus, err := New(WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.Register("c", connection); err != nil {
		t.Fatal(err)
	}
	if err := bus.Close(context.Background()); !errors.Is(err, closeFailure) {
		t.Fatalf("Close error=%v", err)
	}
	if !connection.closed {
		t.Fatal("connection was not closed")
	}
}

type closeErrorConnection struct {
	closed   bool
	closeErr error
}

func (*closeErrorConnection) Send(Message) error { return nil }
func (c *closeErrorConnection) Close() error {
	c.closed = true
	return c.closeErr
}

type recordingListener struct {
	NopListener
	mu      sync.Mutex
	retries []bool
	removed []string
}

type panickingListener struct{ NopListener }

func (panickingListener) AfterEventQueued(*ClientEvent, bool) { panic("listener failed") }

func TestPanicHandlerReportsExtensionPanicsWithoutStoppingDelivery(t *testing.T) {
	recovered := make(chan any, 1)
	bus := newSyncBus(t, WithListener(panickingListener{}), WithPanicHandler(func(value any) { recovered <- value }))
	connection := &recordingConnection{}
	if err := bus.Register("c", connection, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), NewNamedEventWithData("orders", "payload")); err != nil {
		t.Fatal(err)
	}
	if len(connection.snapshot()) != 1 {
		t.Fatal("listener panic stopped delivery")
	}
	select {
	case value := <-recovered:
		if value != "listener failed" {
			t.Fatalf("recovered value=%v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("panic handler was not called")
	}
}

func (l *recordingListener) AfterEventQueued(_ *ClientEvent, first bool) {
	l.mu.Lock()
	l.retries = append(l.retries, first)
	l.mu.Unlock()
}
func (l *recordingListener) AfterClientsUnregistered(ids []string) {
	l.mu.Lock()
	l.removed = append(l.removed, ids...)
	l.mu.Unlock()
}

func TestAsyncRetryExhaustion(t *testing.T) {
	listener := &recordingListener{}
	bus, err := New(WithSendAttempts(2), WithSchedulerDelay(5*time.Millisecond), WithRetryBackoff(10*time.Millisecond, 20*time.Millisecond), WithListener(listener), WithClientExpiration(time.Hour, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = bus.Close(ctx)
	})
	connection := &recordingConnection{failures: 10}
	if err := bus.Register("c", connection, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), NewNamedEventWithData("orders", "x")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for bus.IsClientRegistered("c") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if bus.IsClientRegistered("c") {
		t.Fatal("client was not removed after retry exhaustion")
	}
	listener.mu.Lock()
	defer listener.mu.Unlock()
	if len(listener.retries) < 2 || !listener.retries[0] || listener.retries[1] {
		t.Fatalf("queue callbacks=%v", listener.retries)
	}
	if !reflect.DeepEqual(listener.removed, []string{"c"}) {
		t.Fatalf("unregistered callback=%v", listener.removed)
	}
}

type blockingConnection struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingConnection) Send(Message) error {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return nil
}
func (*blockingConnection) Close() error { return nil }

func TestQueueListenerRunsOnlyAfterSuccessfulEnqueue(t *testing.T) {
	listener := &recordingListener{}
	bus, err := New(WithQueueCapacities(1, 1), WithListener(listener), WithClientExpiration(time.Hour, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	connection := &blockingConnection{started: make(chan struct{}), release: make(chan struct{})}
	defer close(connection.release)
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	if err := bus.Register("c", connection, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), NewNamedEventWithData("orders", "first")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connection.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start first send")
	}
	if err := bus.Publish(context.Background(), NewNamedEventWithData("orders", "second")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Publish(ctx, NewNamedEventWithData("orders", "not-queued")); !errors.Is(err, context.Canceled) {
		t.Fatalf("third Publish error=%v", err)
	}
	listener.mu.Lock()
	queued := len(listener.retries)
	listener.mu.Unlock()
	if queued != 2 {
		t.Fatalf("listener observed %d queued events, want 2", queued)
	}
}

func TestStaleRemovalDoesNotRemoveRefreshedClient(t *testing.T) {
	bus := newSyncBus(t)
	if err := bus.Register("c", &recordingConnection{}); err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now()
	bus.mu.RLock()
	registered := bus.clients["c"]
	bus.mu.RUnlock()
	registered.touch()
	if bus.unregister("c", &cutoff, false) {
		t.Fatal("refreshed client was removed as stale")
	}
	if !bus.IsClientRegistered("c") {
		t.Fatal("refreshed client is no longer registered")
	}
}

type loopTransport struct {
	consumer  func(Event)
	published []Event
}

func (t *loopTransport) SetRemoteEventConsumer(consumer func(Event)) error {
	t.consumer = consumer
	return nil
}
func (t *loopTransport) PublishRemote(_ context.Context, event Event) error {
	t.published = append(t.published, event)
	return nil
}

func TestDistributedAndObservations(t *testing.T) {
	transport := &loopTransport{}
	var mu sync.Mutex
	var observations []Observation
	bus := newSyncBus(t, WithDistributedTransport(transport), WithObserver(ObserverFunc(func(_ context.Context, o Observation) { mu.Lock(); observations = append(observations, o); mu.Unlock() })))
	connection := &recordingConnection{}
	if err := bus.Register("c", connection, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), NewNamedEventWithData("orders", "local")); err != nil {
		t.Fatal(err)
	}
	if len(transport.published) != 1 {
		t.Fatal("local event not distributed")
	}
	transport.consumer(NewNamedEventWithData("orders", "remote"))
	if len(transport.published) != 1 {
		t.Fatal("remote event was rebroadcast")
	}
	if len(connection.snapshot()) != 2 {
		t.Fatal("remote event was not delivered locally")
	}
	mu.Lock()
	defer mu.Unlock()
	foundReceive := false
	for _, o := range observations {
		if o.Operation == OperationReceiveRemote {
			foundReceive = true
		}
	}
	if !foundReceive {
		t.Fatal("receive_remote observation missing")
	}
}

type prefixConverter struct{}

func (prefixConverter) Supports(Event) bool { return true }
func (prefixConverter) Convert(event Event) (string, error) {
	return "converted:" + event.Data.(stringer).String(), nil
}

type stringer struct{ value string }

func (s stringer) String() string { return s.value }

func TestConverterHeartbeatAndExpiration(t *testing.T) {
	bus, err := New(WithSynchronousDelivery(), WithConverters(prefixConverter{}), WithHeartbeat(5*time.Millisecond, "pulse"), WithClientExpiration(time.Hour, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = bus.Close(ctx)
	})
	connection := &recordingConnection{}
	if err := bus.Register("c", connection, SubscribeTo("orders")); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), NewNamedEventWithData("orders", stringer{"value"})); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(connection.snapshot()) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	messages := connection.snapshot()
	if len(messages) < 2 || messages[0].Data != "converted:value" {
		t.Fatalf("messages=%#v", messages)
	}
	foundHeartbeat := false
	for _, message := range messages {
		if message.Comment == "pulse" {
			foundHeartbeat = true
		}
	}
	if !foundHeartbeat {
		t.Fatal("heartbeat was not sent")
	}

	listener := &recordingListener{}
	expireBus, err := New(WithSynchronousDelivery(), WithListener(listener), WithClientExpiration(20*time.Millisecond, 5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = expireBus.Close(ctx)
	})
	if err := expireBus.Register("c", &recordingConnection{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for expireBus.IsClientRegistered("c") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if expireBus.IsClientRegistered("c") {
		t.Fatal("client did not expire")
	}
	listener.mu.Lock()
	defer listener.mu.Unlock()
	if !reflect.DeepEqual(listener.removed, []string{"c"}) {
		t.Fatalf("expiration callback=%v", listener.removed)
	}
}
