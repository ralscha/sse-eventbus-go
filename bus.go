package sseeventbus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrClosed indicates that the bus or connection has closed.
	ErrClosed = errors.New("sse event bus is closed")
	// ErrInvalidClientID indicates that registration was attempted without an ID.
	ErrInvalidClientID = errors.New("client ID cannot be empty")
	// ErrNilConnection indicates that registration was attempted without a connection.
	ErrNilConnection = errors.New("connection cannot be nil")
)

type client struct {
	mu                   sync.RWMutex
	connection           Connection
	lastTransfer         time.Time
	completeAfterMessage bool
}

func (c *client) state() (Connection, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connection, c.completeAfterMessage
}
func (c *client) replace(connection Connection, complete bool) Connection {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.connection
	c.connection = connection
	c.completeAfterMessage = complete
	c.lastTransfer = time.Now()
	return old
}
func (c *client) touch()              { c.mu.Lock(); c.lastTransfer = time.Now(); c.mu.Unlock() }
func (c *client) lastSeen() time.Time { c.mu.RLock(); defer c.mu.RUnlock(); return c.lastTransfer }

// ClientEvent is the delivery unit passed to Listener callbacks.
type ClientEvent struct {
	ClientID   string
	Event      Event
	Message    Message
	client     *client
	attempts   atomic.Int32
	retryAfter time.Time
}

type deliveryNotification struct {
	event *ClientEvent
	sent  bool
	err   error
}

type clientReplayLock struct {
	mu              sync.Mutex
	references      int
	removeRequested bool
}

// Attempts returns the number of failed send attempts.
func (e *ClientEvent) Attempts() int { return int(e.attempts.Load()) }

// Bus tracks SSE clients and delivers published events.
type Bus struct {
	config                config
	mu                    sync.RWMutex
	clients               map[string]*client
	sendQueue, errorQueue *eventQueue
	replayMu              sync.Mutex
	replayLocks           map[string]*clientReplayLock
	stop                  chan struct{}
	closeOnce             sync.Once
	shutdownDone          chan struct{}
	closed                bool
	shutdownErr           error
	wg                    sync.WaitGroup
}

// New creates and starts an event bus.
func New(options ...Option) (*Bus, error) {
	configuration := defaultConfig()
	for _, option := range options {
		if option == nil {
			return nil, errors.New("option cannot be nil")
		}
		if err := option(&configuration); err != nil {
			return nil, err
		}
	}
	bus := &Bus{
		config:       configuration,
		clients:      make(map[string]*client),
		sendQueue:    newEventQueue(configuration.sendCapacity),
		errorQueue:   newEventQueue(configuration.errorCapacity),
		replayLocks:  make(map[string]*clientReplayLock),
		stop:         make(chan struct{}),
		shutdownDone: make(chan struct{}),
	}
	if configuration.distributed != nil {
		if err := configuration.distributed.SetRemoteEventConsumer(func(event Event) { _ = bus.handle(context.Background(), event, OperationReceiveRemote) }); err != nil {
			return nil, fmt.Errorf("configure distributed transport: %w", err)
		}
	}
	if !configuration.synchronous {
		for range configuration.workers {
			bus.wg.Add(1)
			go bus.worker()
		}
		bus.wg.Add(1)
		go bus.retryLoop()
	}
	if configuration.expiration > 0 {
		bus.wg.Add(1)
		go bus.expirationLoop()
	}
	if configuration.heartbeatInterval > 0 {
		bus.wg.Add(1)
		go bus.heartbeatLoop()
	}
	if configuration.replayStore != nil {
		bus.wg.Add(1)
		go bus.replayCleanupLoop()
	}
	return bus, nil
}

// Register registers or atomically reconnects a client.
func (b *Bus) Register(clientID string, connection Connection, options ...RegistrationOption) error {
	return b.RegisterContext(context.Background(), clientID, connection, options...)
}

// RegisterContext registers or atomically reconnects a client. The context
// controls waiting for queue capacity while replaying retained events.
func (b *Bus) RegisterContext(ctx context.Context, clientID string, connection Connection, options ...RegistrationOption) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if clientID == "" {
		return ErrInvalidClientID
	}
	if isNil(connection) {
		return ErrNilConnection
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	registration := registrationConfig{}
	for _, option := range options {
		if option != nil {
			option(&registration)
		}
	}
	unlock := b.lockReplay(clientID)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		unlock(false)
		return ErrClosed
	}
	existing := b.clients[clientID]
	var old Connection
	if existing == nil {
		existing = &client{connection: connection, lastTransfer: time.Now(), completeAfterMessage: registration.complete}
		b.clients[clientID] = existing
	} else {
		old = existing.replace(connection, registration.complete)
	}
	b.mu.Unlock()
	if registration.replace {
		b.UnsubscribeFromAll(clientID, registration.events...)
	}
	for _, event := range registration.events {
		b.Subscribe(clientID, event)
	}
	var replayObservation *Observation
	var replayNotifications []deliveryNotification
	var replayErr error
	if registration.replay {
		observation, notifications, err := b.replayMissedEventsLocked(ctx, clientID, registration.lastEventID)
		replayObservation, replayNotifications, replayErr = &observation, notifications, err
	}
	unlock(false)
	b.observe(ctx, Observation{Operation: OperationRegisterClient, Outcome: "success", ClientID: clientID, CompleteAfterMessage: registration.complete})
	if old != nil && old != connection {
		_ = old.Close()
	}
	if replayObservation != nil {
		b.notifyDeliveries(replayNotifications)
		b.observe(ctx, *replayObservation)
		return replayErr
	}
	return nil
}

// Unregister removes a client, its subscriptions, pending replay events, and replay history.
func (b *Bus) Unregister(clientID string) bool {
	return b.unregister(clientID, nil, true)
}

func (b *Bus) unregister(clientID string, staleBefore *time.Time, observeNoop bool) bool {
	unlock := b.lockReplay(clientID)
	b.mu.Lock()
	removed := b.clients[clientID]
	if removed != nil && staleBefore != nil && !removed.lastSeen().Before(*staleBefore) {
		b.mu.Unlock()
		unlock(false)
		return false
	}
	if removed != nil {
		delete(b.clients, clientID)
	}
	b.mu.Unlock()
	if removed == nil {
		unlock(true)
		if observeNoop {
			b.observe(context.Background(), Observation{Operation: OperationUnregisterClient, Outcome: "noop", ClientID: clientID})
		}
		return false
	}
	b.config.registry.UnsubscribeAll(clientID)
	b.removePendingReplayable(clientID)
	if b.config.replayStore != nil {
		b.config.replayStore.ClearClient(clientID)
	}
	connection, _ := removed.state()
	unlock(true)
	var closeErr error
	if connection != nil {
		closeErr = connection.Close()
	}
	observation := Observation{Operation: OperationUnregisterClient, Outcome: "success", ClientID: clientID, Err: closeErr}
	if closeErr != nil {
		observation.Outcome = "error"
	}
	b.observe(context.Background(), observation)
	return true
}

func (b *Bus) lockReplay(clientID string) func(bool) {
	b.replayMu.Lock()
	lock := b.replayLocks[clientID]
	if lock == nil {
		lock = &clientReplayLock{}
		b.replayLocks[clientID] = lock
	}
	lock.references++
	b.replayMu.Unlock()
	lock.mu.Lock()
	return func(remove bool) {
		lock.mu.Unlock()
		b.replayMu.Lock()
		lock.references--
		lock.removeRequested = lock.removeRequested || remove
		if lock.references == 0 && lock.removeRequested && b.replayLocks[clientID] == lock {
			delete(b.replayLocks, clientID)
		}
		b.replayMu.Unlock()
	}
}

func (b *Bus) Subscribe(clientID string, events ...string) {
	if len(events) == 0 {
		events = []string{DefaultEvent}
	}
	for _, event := range events {
		if event != "" {
			b.config.registry.Subscribe(clientID, event)
		}
	}
}
func (b *Bus) SubscribeOnly(clientID, event string) {
	b.UnsubscribeFromAll(clientID, event)
	b.Subscribe(clientID, event)
}
func (b *Bus) Unsubscribe(clientID, event string) { b.config.registry.Unsubscribe(clientID, event) }
func (b *Bus) UnsubscribeFromAll(clientID string, keepEvents ...string) {
	keep := make(map[string]struct{}, len(keepEvents))
	for _, event := range keepEvents {
		keep[event] = struct{}{}
	}
	for _, event := range b.config.registry.AllEvents() {
		if _, ok := keep[event]; !ok {
			b.config.registry.Unsubscribe(clientID, event)
		}
	}
}

// Publish delivers an event locally and then publishes it to the distributed transport.
func (b *Bus) Publish(ctx context.Context, event Event) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	event = normalizeEvent(event)
	localErr := b.handle(ctx, event, OperationHandleEvent)
	if errors.Is(localErr, ErrInvalidEvent) || errors.Is(localErr, ErrClosed) {
		return localErr
	}
	var remoteErr error
	if b.config.distributed != nil {
		remoteErr = b.config.distributed.PublishRemote(ctx, event)
		observation := Observation{Operation: OperationPublishRemote, Outcome: "success", EventName: event.Name, Err: remoteErr}
		if remoteErr != nil {
			observation.Outcome = "error"
		}
		b.observe(ctx, observation)
	}
	return errors.Join(localErr, remoteErr)
}

func (b *Bus) handle(ctx context.Context, event Event, operation Operation) error {
	select {
	case <-b.stop:
		return ErrClosed
	default:
	}
	event = normalizeEvent(event)
	if err := validateEvent(event); err != nil {
		b.observe(ctx, Observation{Operation: operation, Outcome: "error", EventName: event.Name, Err: err})
		return err
	}
	direct := len(event.ClientIDs) > 0
	ids := event.ClientIDs
	if !direct {
		ids = b.config.registry.Subscribers(event.Name)
	}
	excluded := make(map[string]struct{}, len(event.ExcludeClientIDs))
	if !direct {
		for _, id := range event.ExcludeClientIDs {
			excluded[id] = struct{}{}
		}
	}
	var converted string
	hasConverted := false
	conversionDone := false
	deliveryCount := 0
	var deliveryErrors []error
	for _, id := range ids {
		if _, skip := excluded[id]; skip {
			continue
		}
		if !b.config.registry.IsSubscribed(id, event.Name) {
			continue
		}
		b.mu.RLock()
		target := b.clients[id]
		b.mu.RUnlock()
		if target == nil {
			continue
		}
		if !conversionDone {
			var err error
			converted, hasConverted, err = b.convert(event)
			if err != nil {
				b.observe(ctx, Observation{Operation: operation, Outcome: "error", EventName: event.Name, Direct: direct, Err: err})
				return err
			}
			conversionDone = true
		}
		message := Message{Event: event.Name, Retry: event.Retry, ID: event.ID, Comment: event.Comment, Data: converted, HasData: hasConverted}
		clientEvent := &ClientEvent{ClientID: id, Event: event, Message: message, client: target}
		unlock := func(bool) {}
		if b.config.replayStore != nil {
			unlock = b.lockReplay(id)
			if event.ID != "" {
				b.config.replayStore.Store(ReplayEvent{ClientID: id, Event: event, ConvertedValue: converted, HasConverted: hasConverted, StoredAt: time.Now()})
			}
		}
		sent, err := b.deliver(ctx, clientEvent)
		if b.config.replayStore != nil {
			unlock(false)
		}
		if err == nil || sent {
			b.notifyQueued(clientEvent, true)
		}
		if sent {
			b.notifySent(clientEvent, err)
		}
		if err != nil {
			if b.config.synchronous {
				deliveryCount++
				deliveryErrors = append(deliveryErrors, fmt.Errorf("send to client %q: %w", id, err))
				continue
			}
			b.observe(ctx, Observation{Operation: operation, Outcome: "error", EventName: event.Name, Direct: direct, Replay: b.config.replayStore != nil && event.ID != "", DeliveryCount: deliveryCount, Err: err})
			return err
		}
		deliveryCount++
	}
	if err := errors.Join(deliveryErrors...); err != nil {
		b.observe(ctx, Observation{Operation: operation, Outcome: "error", EventName: event.Name, Direct: direct, Replay: b.config.replayStore != nil && event.ID != "", DeliveryCount: deliveryCount, Err: err})
		return err
	}
	b.observe(ctx, Observation{Operation: operation, Outcome: "success", EventName: event.Name, Direct: direct, Replay: b.config.replayStore != nil && event.ID != "", DeliveryCount: deliveryCount})
	return nil
}

func (b *Bus) convert(event Event) (string, bool, error) {
	if event.Data == nil {
		return "", false, nil
	}
	if value, ok := event.Data.(string); ok {
		return value, true, nil
	}
	for _, converter := range b.config.converters {
		if converter.Supports(event) {
			value, err := converter.Convert(event)
			return value, err == nil, err
		}
	}
	return "", false, fmt.Errorf("no data converter supports event %q", event.Name)
}

func (b *Bus) deliver(ctx context.Context, event *ClientEvent) (bool, error) {
	if b.config.synchronous {
		return true, b.send(event)
	}
	if err := b.sendQueue.push(ctx, b.stop, event); err != nil {
		return false, err
	}
	return false, nil
}

func (b *Bus) send(event *ClientEvent) error {
	connection, complete := event.client.state()
	attempt := int(event.attempts.Add(1))
	err := connection.Send(event.Message)
	observation := Observation{Operation: OperationSendEvent, Outcome: "success", ClientID: event.ClientID, EventName: event.Event.Name, Replay: event.Event.ID != "", CompleteAfterMessage: complete, Attempt: attempt, Err: err}
	if err != nil {
		observation.Outcome = "error"
	} else {
		event.client.touch()
		if complete {
			_ = connection.Close()
		}
	}
	b.observe(context.Background(), observation)
	return err
}

func (b *Bus) worker() {
	defer b.wg.Done()
	for {
		event, ok := b.sendQueue.pop(b.stop)
		if !ok {
			return
		}
		if event.Attempts() >= b.config.attempts {
			if b.Unregister(event.ClientID) {
				b.notifyUnregistered([]string{event.ClientID})
			}
			continue
		}
		err := b.send(event)
		b.notifySent(event, err)
		if err != nil {
			delay := b.config.retryBase
			for range min(event.Attempts()-1, 62) {
				if delay >= b.config.retryMax/2 {
					delay = b.config.retryMax
					break
				}
				delay *= 2
			}
			if delay > b.config.retryMax {
				delay = b.config.retryMax
			}
			event.retryAfter = time.Now().Add(delay)
			if pushErr := b.errorQueue.push(context.Background(), b.stop, event); pushErr != nil {
				return
			}
		}
	}
}

func (b *Bus) retryLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.config.schedulerDelay)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case now := <-ticker.C:
			for _, event := range b.errorQueue.drain() {
				if !b.IsClientRegistered(event.ClientID) {
					continue
				}
				if now.Before(event.retryAfter) {
					if b.errorQueue.push(context.Background(), b.stop, event) != nil {
						return
					}
					continue
				}
				if b.sendQueue.push(context.Background(), b.stop, event) != nil {
					return
				}
				b.notifyQueued(event, false)
			}
		}
	}
}

func (b *Bus) expirationLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.config.expirationScan)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case now := <-ticker.C:
			cutoff := now.Add(-b.config.expiration)
			var stale []string
			b.mu.RLock()
			for id, client := range b.clients {
				if client.lastSeen().Before(cutoff) {
					stale = append(stale, id)
				}
			}
			b.mu.RUnlock()
			var removed []string
			for _, id := range stale {
				if b.unregister(id, &cutoff, false) {
					removed = append(removed, id)
				}
			}
			if len(removed) > 0 {
				b.notifyUnregistered(removed)
			}
		}
	}
}

func (b *Bus) heartbeatLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.config.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			b.mu.RLock()
			clients := make([]*client, 0, len(b.clients))
			for _, client := range b.clients {
				clients = append(clients, client)
			}
			b.mu.RUnlock()
			for _, client := range clients {
				connection, _ := client.state()
				if connection.Send(Message{Comment: b.config.heartbeatComment}) == nil {
					client.touch()
				}
			}
		}
	}
}

func (b *Bus) replayCleanupLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.config.replayCleanup)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case now := <-ticker.C:
			b.config.replayStore.PurgeExpired(now.Add(-b.config.replayRetention))
		}
	}
}

// ReplayMissedEvents replays retained events after lastEventID to a registered client.
func (b *Bus) ReplayMissedEvents(ctx context.Context, clientID, lastEventID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		b.observe(ctx, Observation{Operation: OperationReplayEvents, Outcome: "error", ClientID: clientID, Replay: true, LastEventIDPresent: lastEventID != "", Err: err})
		return err
	}
	if b.config.replayStore == nil {
		observation := Observation{Operation: OperationReplayEvents, Outcome: "noop", ClientID: clientID, Replay: true, LastEventIDPresent: lastEventID != ""}
		b.observe(ctx, observation)
		return nil
	}
	unlock := b.lockReplay(clientID)
	observation, notifications, err := b.replayMissedEventsLocked(ctx, clientID, lastEventID)
	unlock(false)
	b.notifyDeliveries(notifications)
	b.observe(ctx, observation)
	return err
}

// replayMissedEventsLocked requires the per-client replay lock. Keeping
// registration and replay in one critical section prevents live events from
// slipping between reconnect and replay and being delivered twice.
func (b *Bus) replayMissedEventsLocked(ctx context.Context, clientID, lastEventID string) (Observation, []deliveryNotification, error) {
	observation := Observation{Operation: OperationReplayEvents, ClientID: clientID, Replay: true, LastEventIDPresent: lastEventID != ""}
	if err := ctx.Err(); err != nil {
		observation.Outcome = "error"
		observation.Err = err
		return observation, nil, err
	}
	if b.config.replayStore == nil {
		observation.Outcome = "noop"
		return observation, nil, nil
	}
	b.mu.RLock()
	target := b.clients[clientID]
	b.mu.RUnlock()
	if target == nil {
		observation.Outcome = "noop"
		return observation, nil, nil
	}
	b.removePendingReplayable(clientID)
	var replayErrors []error
	var notifications []deliveryNotification
	for _, retained := range b.config.replayStore.EventsSince(clientID, lastEventID) {
		retained.Event = normalizeEvent(retained.Event)
		if !b.config.registry.IsSubscribed(clientID, retained.Event.Name) {
			continue
		}
		if err := validateEvent(retained.Event); err != nil {
			observation.Outcome = "error"
			observation.Err = err
			return observation, notifications, err
		}
		message := Message{Event: retained.Event.Name, Data: retained.ConvertedValue, HasData: retained.HasConverted, Retry: retained.Event.Retry, ID: retained.Event.ID, Comment: retained.Event.Comment}
		clientEvent := &ClientEvent{ClientID: clientID, Event: retained.Event, Message: message, client: target}
		sent, err := b.deliver(ctx, clientEvent)
		if err == nil || sent {
			notifications = append(notifications, deliveryNotification{event: clientEvent, sent: sent, err: err})
		}
		if err != nil {
			if b.config.synchronous {
				observation.DeliveryCount++
				replayErrors = append(replayErrors, fmt.Errorf("replay to client %q: %w", clientID, err))
				continue
			}
			observation.Outcome = "error"
			observation.Err = err
			return observation, notifications, err
		}
		observation.DeliveryCount++
	}
	if err := errors.Join(replayErrors...); err != nil {
		observation.Outcome = "error"
		observation.Err = err
		return observation, notifications, err
	}
	observation.Outcome = "success"
	return observation, notifications, nil
}

func (b *Bus) notifyDeliveries(notifications []deliveryNotification) {
	for _, notification := range notifications {
		b.notifyQueued(notification.event, true)
		if notification.sent {
			b.notifySent(notification.event, notification.err)
		}
	}
}

func (b *Bus) removePendingReplayable(clientID string) {
	match := func(event *ClientEvent) bool { return event.ClientID == clientID && event.Event.ID != "" }
	b.sendQueue.remove(match)
	b.errorQueue.remove(match)
}

func (b *Bus) notifyQueued(event *ClientEvent, first bool) {
	defer b.recoverExtensionPanic()
	b.config.listener.AfterEventQueued(event.listenerSnapshot(), first)
}
func (b *Bus) notifySent(event *ClientEvent, err error) {
	defer b.recoverExtensionPanic()
	b.config.listener.AfterEventSent(event.listenerSnapshot(), err)
}

func (e *ClientEvent) listenerSnapshot() *ClientEvent {
	snapshot := &ClientEvent{ClientID: e.ClientID, Event: normalizeEvent(e.Event), Message: e.Message}
	snapshot.attempts.Store(e.attempts.Load())
	return snapshot
}
func (b *Bus) notifyUnregistered(ids []string) {
	defer b.recoverExtensionPanic()
	b.config.listener.AfterClientsUnregistered(append([]string(nil), ids...))
}
func (b *Bus) observe(ctx context.Context, observation Observation) {
	if b.config.observer == nil {
		return
	}
	defer b.recoverExtensionPanic()
	b.config.observer.Observe(ctx, observation)
}

func (b *Bus) recoverExtensionPanic() {
	value := recover()
	if value == nil || b.config.panicHandler == nil {
		return
	}
	func() {
		defer func() { _ = recover() }()
		b.config.panicHandler(value)
	}()
}

func (b *Bus) IsClientRegistered(clientID string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.clients[clientID] != nil
}
func (b *Bus) ClientIDs() []string {
	b.mu.RLock()
	result := make([]string, 0, len(b.clients))
	for id := range b.clients {
		result = append(result, id)
	}
	b.mu.RUnlock()
	slices.Sort(result)
	return result
}
func (b *Bus) Events() []string {
	result := append([]string(nil), b.config.registry.AllEvents()...)
	slices.Sort(result)
	return result
}
func (b *Bus) Subscriptions() map[string][]string {
	subscriptions := b.config.registry.AllSubscriptions()
	result := make(map[string][]string, len(subscriptions))
	for event, ids := range subscriptions {
		result[event] = append([]string(nil), ids...)
		slices.Sort(result[event])
	}
	return result
}
func (b *Bus) Subscribers(event string) []string {
	result := append([]string(nil), b.config.registry.Subscribers(event)...)
	slices.Sort(result)
	return result
}
func (b *Bus) CountSubscribers(event string) int { return b.config.registry.CountSubscribers(event) }
func (b *Bus) HasSubscribers(event string) bool  { return b.config.registry.HasSubscribers(event) }
func (b *Bus) ClientCount() int                  { b.mu.RLock(); defer b.mu.RUnlock(); return len(b.clients) }
func (b *Bus) SendQueueSize() int                { return b.sendQueue.len() }
func (b *Bus) ErrorQueueSize() int               { return b.errorQueue.len() }

// Close stops background work and flushes queued events synchronously.
func (b *Bus) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		close(b.stop)
		go func() {
			b.wg.Wait()
			var shutdownErrors []error
			for _, event := range b.sendQueue.drain() {
				if event.Attempts() < b.config.attempts {
					err := b.send(event)
					b.notifySent(event, err)
					if err != nil {
						shutdownErrors = append(shutdownErrors, fmt.Errorf("flush event for client %q: %w", event.ClientID, err))
					}
				}
			}
			b.mu.RLock()
			connections := make(map[string]Connection, len(b.clients))
			for id, client := range b.clients {
				connections[id], _ = client.state()
			}
			b.mu.RUnlock()
			for id, connection := range connections {
				if connection != nil {
					if err := connection.Close(); err != nil {
						shutdownErrors = append(shutdownErrors, fmt.Errorf("close client %q: %w", id, err))
					}
				}
			}
			b.mu.Lock()
			b.shutdownErr = errors.Join(shutdownErrors...)
			b.mu.Unlock()
			close(b.shutdownDone)
		}()
	})
	select {
	case <-b.shutdownDone:
		b.mu.RLock()
		defer b.mu.RUnlock()
		return b.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
