package sseeventbus

import (
	"errors"
	"reflect"
	"time"
)

type config struct {
	workers, sendCapacity, errorCapacity, attempts int
	synchronous                                    bool
	schedulerDelay, expiration, expirationScan     time.Duration
	retryBase, retryMax                            time.Duration
	heartbeatInterval                              time.Duration
	heartbeatComment                               string
	replayStore                                    ReplayStore
	replayRetention, replayCleanup                 time.Duration
	converters                                     []DataConverter
	registry                                       SubscriptionRegistry
	listener                                       Listener
	observer                                       Observer
	panicHandler                                   func(any)
	distributed                                    DistributedTransport
}

func defaultConfig() config {
	return config{
		workers: 1, sendCapacity: 10_000, errorCapacity: 10_000, attempts: 40,
		schedulerDelay: 500 * time.Millisecond, expiration: 24 * time.Hour,
		retryBase: time.Second, retryMax: 30 * time.Second,
		expirationScan: 24 * time.Hour, heartbeatComment: "heartbeat",
		replayRetention: 5 * time.Minute, replayCleanup: 5 * time.Minute,
		converters: []DataConverter{JSONConverter{}}, registry: NewMemorySubscriptionRegistry(), listener: NopListener{},
	}
}

// Option configures a Bus.
type Option func(*config) error

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// WithWorkerCount sets the number of asynchronous send workers.
func WithWorkerCount(count int) Option {
	return func(c *config) error {
		if count < 1 {
			return errors.New("worker count must be positive")
		}
		c.workers = count
		return nil
	}
}

// WithSynchronousDelivery sends in the publishing goroutine without retries.
func WithSynchronousDelivery() Option {
	return func(c *config) error { c.synchronous = true; return nil }
}

// WithQueueCapacities sets the bounded send and retry queue capacities.
func WithQueueCapacities(send, retry int) Option {
	return func(c *config) error {
		if send < 1 || retry < 1 {
			return errors.New("queue capacities must be positive")
		}
		c.sendCapacity, c.errorCapacity = send, retry
		return nil
	}
}

// WithSendAttempts sets the maximum sends attempted before a client is removed.
func WithSendAttempts(attempts int) Option {
	return func(c *config) error {
		if attempts < 1 {
			return errors.New("send attempts must be positive")
		}
		c.attempts = attempts
		return nil
	}
}

// WithSchedulerDelay sets how often failed sends are checked for retry.
func WithSchedulerDelay(delay time.Duration) Option {
	return func(c *config) error {
		if delay <= 0 {
			return errors.New("scheduler delay must be positive")
		}
		c.schedulerDelay = delay
		return nil
	}
}

// WithRetryBackoff configures the initial exponential retry delay and its cap.
func WithRetryBackoff(initial, maximum time.Duration) Option {
	return func(c *config) error {
		if initial <= 0 || maximum < initial {
			return errors.New("retry backoff must be positive and maximum must not be less than initial")
		}
		c.retryBase, c.retryMax = initial, maximum
		return nil
	}
}

// WithClientExpiration sets the inactivity limit and its scan interval.
func WithClientExpiration(expiration, scanInterval time.Duration) Option {
	return func(c *config) error {
		if expiration <= 0 || scanInterval <= 0 {
			return errors.New("client expiration and scan interval must be positive")
		}
		c.expiration, c.expirationScan = expiration, scanInterval
		return nil
	}
}

// WithoutClientExpiration disables automatic removal of inactive clients.
func WithoutClientExpiration() Option {
	return func(c *config) error {
		c.expiration, c.expirationScan = 0, 0
		return nil
	}
}

// WithHeartbeat enables periodic SSE comments. A zero interval disables them.
func WithHeartbeat(interval time.Duration, comment string) Option {
	return func(c *config) error {
		if interval < 0 {
			return errors.New("heartbeat interval cannot be negative")
		}
		c.heartbeatInterval = interval
		if comment != "" {
			c.heartbeatComment = comment
		}
		return nil
	}
}

// WithReplay enables retained event replay using store.
func WithReplay(store ReplayStore, retention, cleanupInterval time.Duration) Option {
	return func(c *config) error {
		if isNil(store) {
			return errors.New("replay store cannot be nil")
		}
		if retention <= 0 || cleanupInterval <= 0 {
			return errors.New("replay retention and cleanup interval must be positive")
		}
		c.replayStore, c.replayRetention, c.replayCleanup = store, retention, cleanupInterval
		return nil
	}
}

// WithConverters replaces the converter chain in priority order.
func WithConverters(converters ...DataConverter) Option {
	return func(c *config) error {
		if len(converters) == 0 {
			return errors.New("at least one converter is required")
		}
		for _, converter := range converters {
			if isNil(converter) {
				return errors.New("converter cannot be nil")
			}
		}
		c.converters = append([]DataConverter(nil), converters...)
		return nil
	}
}

// WithSubscriptionRegistry replaces the in-memory subscription registry.
func WithSubscriptionRegistry(registry SubscriptionRegistry) Option {
	return func(c *config) error {
		if isNil(registry) {
			return errors.New("subscription registry cannot be nil")
		}
		c.registry = registry
		return nil
	}
}

// WithListener installs synchronous lifecycle callbacks.
func WithListener(listener Listener) Option {
	return func(c *config) error {
		if isNil(listener) {
			return errors.New("listener cannot be nil")
		}
		c.listener = listener
		return nil
	}
}

// WithObserver installs a structured operation observer.
func WithObserver(observer Observer) Option {
	return func(c *config) error {
		if isNil(observer) {
			return errors.New("observer cannot be nil")
		}
		c.observer = observer
		return nil
	}
}

// WithPanicHandler reports panics recovered from Listener and Observer calls.
func WithPanicHandler(handler func(any)) Option {
	return func(c *config) error {
		if handler == nil {
			return errors.New("panic handler cannot be nil")
		}
		c.panicHandler = handler
		return nil
	}
}

// WithDistributedTransport enables cross-node event delivery.
func WithDistributedTransport(transport DistributedTransport) Option {
	return func(c *config) error {
		if isNil(transport) {
			return errors.New("distributed transport cannot be nil")
		}
		c.distributed = transport
		return nil
	}
}

type registrationConfig struct {
	events      []string
	replace     bool
	complete    bool
	lastEventID string
	replay      bool
}

// RegistrationOption configures one client registration.
type RegistrationOption func(*registrationConfig)

// SubscribeTo subscribes the client to events in addition to existing subscriptions.
func SubscribeTo(events ...string) RegistrationOption {
	return func(c *registrationConfig) { c.events = append(c.events, events...) }
}

// ReplaceSubscriptions removes subscriptions not included in events.
func ReplaceSubscriptions(events ...string) RegistrationOption {
	return func(c *registrationConfig) { c.replace = true; c.events = append(c.events, events...) }
}

// CompleteAfterMessage closes the connection after its first successful send.
func CompleteAfterMessage() RegistrationOption {
	return func(c *registrationConfig) { c.complete = true }
}

// ReplayFrom replays retained events following lastEventID after registration.
func ReplayFrom(lastEventID string) RegistrationOption {
	return func(c *registrationConfig) { c.lastEventID = lastEventID; c.replay = true }
}
