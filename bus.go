package sseeventbus

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
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
	ErrNilConnection  = errors.New("connection cannot be nil")
	errInactiveClient = errors.New("client generation is inactive")
)

type client struct {
	mu                   sync.Mutex
	connection           Connection
	lastTransfer         time.Time
	completeAfterMessage bool
	inactive             atomic.Bool
	connectionClosed     bool
	replayGeneration     atomic.Uint64
	orderMu              sync.Mutex
	orderCond            *sync.Cond
	nextOrder            uint64
	nextTurn             uint64
	canceledOrders       map[uint64]struct{}
}

func (c *client) retire(closeConnection bool) error {
	c.inactive.Store(true)
	c.mu.Lock()
	var connection Connection
	if closeConnection && !c.connectionClosed {
		c.connectionClosed = true
		connection = c.connection
	}
	c.mu.Unlock()
	if connection != nil {
		return connection.Close()
	}
	return nil
}

func (c *client) replaceWith(connection Connection) error {
	c.inactive.Store(true)
	c.mu.Lock()
	var old Connection
	if !sameConnection(c.connection, connection) && !c.connectionClosed {
		c.connectionClosed = true
		old = c.connection
	}
	c.mu.Unlock()
	if old != nil {
		return old.Close()
	}
	return nil
}
func (c *client) touch()              { c.mu.Lock(); c.lastTransfer = time.Now(); c.mu.Unlock() }
func (c *client) lastSeen() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.lastTransfer }

// ClientEvent is the delivery unit passed to Listener callbacks.
type ClientEvent struct {
	ClientID   string
	Event      Event
	Message    Message
	client     *client
	attempts   atomic.Int32
	retryAfter time.Time
	retryOrder uint64
	generation uint64
	sendOrder  uint64
}

type deliveryNotification struct {
	event *ClientEvent
	sent  bool
	err   error
}

type clientLock struct {
	mu         sync.Mutex
	references int
}

// Attempts returns the total number of send attempts, including a successful
// final attempt.
func (e *ClientEvent) Attempts() int { return int(e.attempts.Load()) }

// Bus tracks SSE clients and delivers published events.
type Bus struct {
	config       config
	mu           sync.RWMutex
	clients      map[string]*client
	sendQueue    *eventQueue
	retryQueue   *retryQueue
	clientLockMu sync.Mutex
	clientLocks  map[string]*clientLock
	stop         chan struct{}
	closeOnce    sync.Once
	shutdownDone chan struct{}
	closed       bool
	shutdownErr  error
	wg           sync.WaitGroup
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
		retryQueue:   newRetryQueue(configuration.retryCapacity),
		clientLocks:  make(map[string]*clientLock),
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
	unlock := b.lockClient(clientID)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		unlock()
		return ErrClosed
	}
	existing := b.clients[clientID]
	b.mu.Unlock()
	target := &client{connection: connection, lastTransfer: time.Now(), completeAfterMessage: registration.complete}
	if existing != nil {
		_ = existing.replaceWith(connection)
		b.removePending(existing, false)
	}
	if registration.replace {
		b.UnsubscribeFromAll(clientID, registration.events...)
	}
	for _, event := range registration.events {
		b.Subscribe(clientID, event)
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = target.retire(true)
		unlock()
		return ErrClosed
	}
	b.clients[clientID] = target
	b.mu.Unlock()
	var replayObservation *Observation
	var replayNotifications []deliveryNotification
	var replayErr error
	if registration.replay {
		observation, notifications, err := b.replayMissedEventsLocked(ctx, clientID, registration.lastEventID)
		replayObservation, replayNotifications, replayErr = &observation, notifications, err
	}
	unlock()
	b.observe(ctx, Observation{Operation: OperationRegisterClient, Outcome: "success", ClientID: clientID, CompleteAfterMessage: registration.complete})
	if replayObservation != nil {
		b.notifyDeliveries(replayNotifications)
		b.observe(ctx, *replayObservation)
		return replayErr
	}
	return nil
}

func sameConnection(left, right Connection) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftType, rightType := reflect.TypeOf(left), reflect.TypeOf(right)
	if leftType != rightType {
		return false
	}
	if leftType.Comparable() {
		return left == right
	}
	leftValue, rightValue := reflect.ValueOf(left), reflect.ValueOf(right)
	switch leftType.Kind() {
	case reflect.Map:
		return leftValue.Pointer() == rightValue.Pointer()
	case reflect.Slice:
		return leftValue.Pointer() == rightValue.Pointer() && leftValue.Len() == rightValue.Len() && leftValue.Cap() == rightValue.Cap()
	default:
		return false
	}
}

// Unregister removes a client, its subscriptions, all pending sends, and replay
// history.
func (b *Bus) Unregister(clientID string) bool {
	return b.unregister(clientID, nil, nil, true)
}

func (b *Bus) unregister(clientID string, expected *client, staleBefore *time.Time, observeNoop bool) bool {
	unlock := b.lockClient(clientID)
	b.mu.Lock()
	removed := b.clients[clientID]
	if expected != nil && removed != expected {
		b.mu.Unlock()
		unlock()
		return false
	}
	if removed != nil && staleBefore != nil && !removed.lastSeen().Before(*staleBefore) {
		b.mu.Unlock()
		unlock()
		return false
	}
	if removed != nil {
		delete(b.clients, clientID)
	}
	b.mu.Unlock()
	if removed == nil {
		unlock()
		if observeNoop {
			b.observe(context.Background(), Observation{Operation: OperationUnregisterClient, Outcome: "noop", ClientID: clientID})
		}
		return false
	}
	b.config.registry.UnsubscribeAll(clientID)
	b.removePending(removed, false)
	if b.config.replayStore != nil {
		b.config.replayStore.ClearClient(clientID)
	}
	closeErr := removed.retire(true)
	unlock()
	observation := Observation{Operation: OperationUnregisterClient, Outcome: "success", ClientID: clientID, Err: closeErr}
	if closeErr != nil {
		observation.Outcome = "error"
	}
	b.observe(context.Background(), observation)
	return true
}

func (b *Bus) lockClient(clientID string) func() {
	b.clientLockMu.Lock()
	lock := b.clientLocks[clientID]
	if lock == nil {
		lock = &clientLock{}
		b.clientLocks[clientID] = lock
	}
	lock.references++
	b.clientLockMu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		b.clientLockMu.Lock()
		lock.references--
		if lock.references == 0 && b.clientLocks[clientID] == lock {
			delete(b.clientLocks, clientID)
		}
		b.clientLockMu.Unlock()
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
		unlock := b.lockClient(id)
		if !b.config.registry.IsSubscribed(id, event.Name) {
			unlock()
			continue
		}
		b.mu.RLock()
		target = b.clients[id]
		b.mu.RUnlock()
		if target == nil {
			unlock()
			continue
		}
		select {
		case <-b.stop:
			unlock()
			return ErrClosed
		default:
		}
		message := Message{Event: event.Name, Retry: event.Retry, ID: event.ID, Comment: event.Comment, Data: converted, HasData: hasConverted}
		clientEvent := &ClientEvent{ClientID: id, Event: event, Message: message, client: target, generation: target.replayGeneration.Load()}
		if b.config.replayStore != nil && event.ID != "" {
			b.config.replayStore.Store(ReplayEvent{ClientID: id, Event: event, ConvertedValue: converted, HasConverted: hasConverted, StoredAt: time.Now()})
		}
		if !target.isActive() {
			unlock()
			continue
		}
		var sent bool
		var err error
		if b.config.synchronous {
			unlock()
			sent, err = b.deliver(ctx, clientEvent)
		} else {
			sent, err = b.deliver(ctx, clientEvent)
			unlock()
		}
		if errors.Is(err, errInactiveClient) {
			continue
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
		return b.send(event)
	}
	if err := b.sendQueue.push(ctx, event); err != nil {
		return false, err
	}
	return false, nil
}

func (b *Bus) send(event *ClientEvent) (bool, error) {
	client := event.client
	client.mu.Lock()
	if client.inactive.Load() {
		client.mu.Unlock()
		return false, errInactiveClient
	}
	connection := client.connection
	complete := client.completeAfterMessage
	attempt := int(event.attempts.Add(1))
	err := connection.Send(event.Message)
	var closeConnection Connection
	if err == nil {
		client.lastTransfer = time.Now()
		if complete {
			client.inactive.Store(true)
			if !client.connectionClosed {
				client.connectionClosed = true
				closeConnection = connection
			}
		}
	}
	client.mu.Unlock()
	if closeConnection != nil {
		_ = closeConnection.Close()
	}
	observation := Observation{Operation: OperationSendEvent, Outcome: "success", ClientID: event.ClientID, EventName: event.Event.Name, Replay: event.Event.ID != "", CompleteAfterMessage: complete, Attempt: attempt, Err: err}
	if err != nil {
		observation.Outcome = "error"
	}
	b.observe(context.Background(), observation)
	return true, err
}

func (c *client) isActive() bool {
	return !c.inactive.Load()
}

func (c *client) heartbeat(message Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inactive.Load() {
		return false
	}
	if c.connection.Send(message) != nil {
		return false
	}
	c.lastTransfer = time.Now()
	return true
}

func (c *client) assignOrder() uint64 {
	c.orderMu.Lock()
	c.nextOrder++
	order := c.nextOrder
	if c.nextTurn == 0 {
		c.nextTurn = 1
	}
	c.orderMu.Unlock()
	return order
}

func (c *client) waitForTurn(order uint64) bool {
	c.orderMu.Lock()
	defer c.orderMu.Unlock()
	if c.orderCond == nil {
		c.orderCond = sync.NewCond(&c.orderMu)
	}
	for order > c.nextTurn {
		c.orderCond.Wait()
	}
	return order == c.nextTurn
}

func (c *client) finishTurn(order uint64) {
	c.orderMu.Lock()
	if order == c.nextTurn {
		c.nextTurn++
		c.advanceCanceledLocked()
		if c.orderCond != nil {
			c.orderCond.Broadcast()
		}
	}
	c.orderMu.Unlock()
}

func (c *client) cancelTurn(order uint64) {
	if order == 0 {
		return
	}
	c.orderMu.Lock()
	if order == c.nextTurn {
		c.nextTurn++
		c.advanceCanceledLocked()
		if c.orderCond != nil {
			c.orderCond.Broadcast()
		}
	} else if order > c.nextTurn {
		if c.canceledOrders == nil {
			c.canceledOrders = make(map[uint64]struct{})
		}
		c.canceledOrders[order] = struct{}{}
	}
	c.orderMu.Unlock()
}

func (c *client) advanceCanceledLocked() {
	for {
		if _, canceled := c.canceledOrders[c.nextTurn]; !canceled {
			return
		}
		delete(c.canceledOrders, c.nextTurn)
		c.nextTurn++
	}
}

func (b *Bus) isCurrentEvent(event *ClientEvent) bool {
	b.mu.RLock()
	current := b.clients[event.ClientID] == event.client
	b.mu.RUnlock()
	if !current || event.Event.ID == "" {
		return current
	}
	return event.generation == event.client.replayGeneration.Load()
}

func (b *Bus) worker() {
	defer b.wg.Done()
	for {
		event, ok := b.sendQueue.pop()
		if !ok {
			return
		}
		if !event.client.waitForTurn(event.sendOrder) {
			continue
		}
		stopWorker := func() bool {
			defer event.client.finishTurn(event.sendOrder)
			if !b.isCurrentEvent(event) {
				return false
			}
			if event.Attempts() >= b.config.attempts {
				if b.unregister(event.ClientID, event.client, nil, false) {
					b.notifyUnregistered([]string{event.ClientID})
				}
				return false
			}
			attempted, err := b.send(event)
			if !attempted && errors.Is(err, errInactiveClient) {
				return false
			}
			b.notifySent(event, err)
			if err != nil {
				if !b.isCurrentEvent(event) {
					return false
				}
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
				if pushErr := b.retryQueue.push(context.Background(), event); pushErr != nil {
					return true
				}
			}
			return false
		}()
		if stopWorker {
			return
		}
	}
}

func (b *Bus) retryLoop() {
	defer b.wg.Done()
	timer := time.NewTimer(time.Hour)
	stopTimer(timer)
	defer timer.Stop()
	for {
		event, next, open := b.retryQueue.popDue(time.Now())
		if !open {
			return
		}
		if event != nil {
			if !b.isCurrentEvent(event) {
				continue
			}
			if b.sendQueue.push(context.Background(), event) != nil {
				return
			}
			b.notifyQueued(event, false)
			continue
		}
		var timerC <-chan time.Time
		if !next.IsZero() {
			delay := min(max(time.Until(next), 0), b.config.schedulerDelay)
			stopTimer(timer)
			timer.Reset(delay)
			timerC = timer.C
		} else {
			stopTimer(timer)
		}
		select {
		case <-b.stop:
			return
		case <-b.retryQueue.changes():
		case <-timerC:
		}
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
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
				if b.unregister(id, nil, &cutoff, false) {
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
				client.heartbeat(Message{Comment: b.config.heartbeatComment})
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
	select {
	case <-b.stop:
		return ErrClosed
	default:
	}
	if b.config.replayStore == nil {
		observation := Observation{Operation: OperationReplayEvents, Outcome: "noop", ClientID: clientID, Replay: true, LastEventIDPresent: lastEventID != ""}
		b.observe(ctx, observation)
		return nil
	}
	unlock := b.lockClient(clientID)
	observation, notifications, err := b.replayMissedEventsLocked(ctx, clientID, lastEventID)
	unlock()
	b.notifyDeliveries(notifications)
	b.observe(ctx, observation)
	return err
}

// replayMissedEventsLocked requires the per-client lifecycle lock. Keeping
// registration and replay in one critical section prevents live events from
// slipping between reconnect and replay and being queued twice.
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
	if !target.isActive() {
		observation.Outcome = "noop"
		return observation, nil, nil
	}
	generation := target.replayGeneration.Add(1)
	b.removePending(target, true)
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
		clientEvent := &ClientEvent{ClientID: clientID, Event: retained.Event, Message: message, client: target, generation: generation}
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

func (b *Bus) removePending(client *client, replayableOnly bool) {
	match := func(event *ClientEvent) bool {
		return event.client == client && (!replayableOnly || event.Event.ID != "")
	}
	b.sendQueue.remove(match)
	b.retryQueue.remove(match)
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

// RetryQueueSize returns the number of sends waiting for their retry time.
func (b *Bus) RetryQueueSize() int { return b.retryQueue.len() }

// ErrorQueueSize is retained for compatibility.
// Deprecated: use RetryQueueSize.
func (b *Bus) ErrorQueueSize() int { return b.RetryQueueSize() }

// Close stops background work, discards scheduled retries, and flushes events
// already waiting in the send queue synchronously.
func (b *Bus) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		b.sendQueue.close()
		b.retryQueue.close()
		close(b.stop)
		go func() {
			b.wg.Wait()
			b.retryQueue.clear()
			var shutdownErrors []error
			for _, event := range b.sendQueue.drain() {
				if !event.client.waitForTurn(event.sendOrder) {
					continue
				}
				func() {
					defer event.client.finishTurn(event.sendOrder)
					if b.isCurrentEvent(event) && event.Attempts() < b.config.attempts {
						attempted, err := b.send(event)
						if attempted {
							b.notifySent(event, err)
						}
						if attempted && err != nil {
							shutdownErrors = append(shutdownErrors, fmt.Errorf("flush event for client %q: %w", event.ClientID, err))
						}
					}
				}()
			}
			b.mu.RLock()
			clients := make(map[string]*client, len(b.clients))
			maps.Copy(clients, b.clients)
			b.mu.RUnlock()
			for id, client := range clients {
				if err := client.retire(true); err != nil {
					shutdownErrors = append(shutdownErrors, fmt.Errorf("close client %q: %w", id, err))
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
