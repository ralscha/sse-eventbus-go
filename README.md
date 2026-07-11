# sse-eventbus-go

`sse-eventbus-go` tracks connected Server-Sent Events clients and broadcasts
events to their subscriptions. It is a Go implementation of
[`sse-eventbus`](https://github.com/ralscha/sse-eventbus) with an HTTP-agnostic
core and a standard-library adapter.

The core has no third-party dependencies. A custom HTTP framework only needs to
implement the small `Connection` interface.

## Getting started

Every connecting client must send a non-empty ID that uniquely identifies its
logical SSE connection. The event bus uses this ID to track the active
connection and its subscriptions, target events at a specific client, and find
the correct retained history when that client reconnects. A reconnecting client
must therefore reuse the same ID. If another connection registers the same ID,
it replaces the previous connection.

The adapter deliberately does not prescribe how to transport or generate the
ID. In this example the client puts it in the request path. A browser can create
one with the Web Crypto API and keep it for subsequent reconnects:

```js
const clientID = crypto.randomUUID();
const events = new EventSource(`/events/${encodeURIComponent(clientID)}`);
```

Use a different ID for clients that need independent subscriptions, such as
separate browser tabs. If the ID comes from an untrusted client, validate it and
apply authorization independently; the ID identifies event-bus state and is not
an authentication credential.

The application extracts the ID and passes it to `httpadapter.Serve`:

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/ralscha/sse-eventbus-go"
	"github.com/ralscha/sse-eventbus-go/httpadapter"
)

func main() {
	bus, err := sseeventbus.New(
		sseeventbus.WithHeartbeat(30*time.Second, "keep-alive"),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := bus.Close(context.Background()); err != nil {
			log.Printf("close event bus: %v", err)
		}
	}()

	http.HandleFunc("/events/", func(w http.ResponseWriter, r *http.Request) {
		clientID := strings.TrimPrefix(r.URL.Path, "/events/")
		err := httpadapter.Serve(w, r, bus, clientID,
			httpadapter.WithRegistration(
				sseeventbus.ReplaceSubscriptions("orders", "news"),
				sseeventbus.ReplayFrom(r.Header.Get("Last-Event-ID")),
			),
		)
		if err != nil && !errors.Is(err, sseeventbus.ErrClosed) &&
			!errors.Is(err, context.Canceled) {
			// The response may already have started; log the error here.
			log.Printf("serve SSE client %q: %v", clientID, err)
		}
	})

	_ = http.ListenAndServe(":8080", nil)
}
```

`Serve` uses a three-minute default connection timeout. Use
`httpadapter.WithTimeout(0)` to rely only on the request context, or provide a
different duration. A disconnected request leaves the logical client registered
for reconnection and replay. Call `bus.Unregister(clientID)` when that state
should be permanently removed.

## Configuration options

There are three kinds of options. Bus options configure the event bus for its
whole lifetime, registration options configure one logical client, and HTTP
adapter options configure one SSE request.

### Bus options

Pass these options to `sseeventbus.New`. Invalid values cause `New` to return an
error.

| Option | Description and default |
| --- | --- |
| `WithWorkerCount(count)` | Sets the number of asynchronous send workers. The default is `1`. More workers reduce head-of-line blocking but may relax global delivery ordering. |
| `WithSynchronousDelivery()` | Sends events in the publishing goroutine instead of using the send and retry queues. Send errors are returned directly and automatic retries are disabled. |
| `WithQueueCapacities(send, retry)` | Sets the bounded send and retry queue capacities. Both default to `10,000`. A publisher waits when the send queue is full and can cancel that wait through its context. |
| `WithSendAttempts(attempts)` | Sets the maximum number of send attempts before a failing client is unregistered. The default is `40`. |
| `WithSchedulerDelay(delay)` | Sets how often the retry queue is checked. The default is `500ms`. |
| `WithRetryBackoff(initial, maximum)` | Sets the initial exponential retry delay and its maximum. The defaults are `1s` and `30s`. |
| `WithClientExpiration(expiration, scanInterval)` | Removes clients that have not had a successful send or heartbeat within `expiration`. Both values default to `24h`. |
| `WithoutClientExpiration()` | Disables automatic removal of inactive clients. This is useful for fully application-managed client lifecycles and goroutine-free synchronous buses. |
| `WithHeartbeat(interval, comment)` | Sends periodic SSE comments to keep idle connections alive. Heartbeats are disabled by default; the default comment is `heartbeat`. An interval of `0` disables them. |
| `WithReplay(store, retention, cleanupInterval)` | Enables replay with a `ReplayStore`. Replay is disabled by default; callers must supply positive retention and cleanup durations. |
| `WithConverters(converters...)` | Replaces the data-converter chain. The default is `JSONConverter`. Converters are checked in the order supplied, so include a fallback converter last if required. |
| `WithSubscriptionRegistry(registry)` | Replaces the default concurrent in-memory subscription registry. |
| `WithListener(listener)` | Installs synchronous queue, send, and automatic-unregister callbacks. The default listener does nothing. |
| `WithObserver(observer)` | Receives structured completed-operation observations. No observer is installed by default. |
| `WithPanicHandler(handler)` | Reports panics recovered from listener and observer callbacks. By default these panics are isolated silently so they cannot terminate delivery workers. |
| `WithDistributedTransport(transport)` | Enables cross-node event publication. The transport must prevent events from being echoed to their originating node. |

For example:

```go
bus, err := sseeventbus.New(
	sseeventbus.WithWorkerCount(4),
	sseeventbus.WithQueueCapacities(20_000, 10_000),
	sseeventbus.WithRetryBackoff(time.Second, 30*time.Second),
	sseeventbus.WithHeartbeat(30*time.Second, "keep-alive"),
)
```

### Client registration options

Pass these options to `bus.Register` or `bus.RegisterContext`, or wrap them in
`httpadapter.WithRegistration` when using `httpadapter.Serve`.

| Option | Description |
| --- | --- |
| `SubscribeTo(events...)` | Adds the client to the listed event subscriptions without removing its existing subscriptions. Calling `bus.Subscribe(clientID)` separately subscribes it to the default `message` event. |
| `ReplaceSubscriptions(events...)` | Makes the supplied list authoritative: the client is removed from every subscription not in the list, then subscribed to every listed event. Passing no events unsubscribes it from everything. This is useful when a reconnect request contains the client's complete desired topic list. |
| `CompleteAfterMessage()` | Closes the current connection after its first successful event. The logical client remains registered for reconnect and replay. |
| `ReplayFrom(lastEventID)` | After registration and subscription updates, replays retained subscribed events following `lastEventID`. A missing or unknown ID replays all retained events. It has no effect unless replay was enabled with `WithReplay`, and only published events with non-empty IDs can be replayed. |

`SubscribeTo` and `ReplaceSubscriptions` both contribute events to the same
registration. Prefer one of them per registration so it is clear whether the
request is additive or authoritative.

For reconnectable HTTP clients, a typical registration is:

```go
httpadapter.WithRegistration(
	sseeventbus.ReplaceSubscriptions("orders", "news"),
	sseeventbus.ReplayFrom(r.Header.Get("Last-Event-ID")),
)
```

The subscriptions are updated before replay starts, so retained events are
replayed only for the topics the client currently subscribes to.

### HTTP adapter options

Pass these options directly to `httpadapter.Serve`.

| Option | Description and default |
| --- | --- |
| `WithTimeout(timeout)` | Sets the lifetime of this HTTP streaming request. The default is `3m`; a non-positive duration disables the adapter timeout and relies on request cancellation. |
| `WithRegistration(options...)` | Passes one or more client registration options through to `bus.Register`. |
| `WithLastEventID(lastEventID)` | Shorthand for `WithRegistration(sseeventbus.ReplayFrom(lastEventID))`. |

## Publishing

```go
event := sseeventbus.NewNamedEventWithData("orders", order)
event.ID = "order-4711" // events need a non-empty ID to be replayable
if err := bus.Publish(ctx, event); err != nil {
	// A synchronous send or queue backpressure may return an error.
}
```

By default an event is sent to every connected client subscribed to its name.
Use `ClientIDs` for direct delivery or `ExcludeClientIDs` for broadcast
exclusions. Direct delivery still requires a matching subscription and ignores
the exclusion list.

```go
event.ClientIDs = []string{"client-1", "client-2"}
// or
event.ExcludeClientIDs = []string{"client-3"}
```

Strings are sent directly. Other values are serialized by `JSONConverter`.
Install custom converters, in priority order, with `WithConverters`.
Event names cannot contain line breaks, event IDs cannot contain nulls or line
breaks, and retry durations cannot be negative. Invalid events return
`ErrInvalidEvent` before local or distributed delivery.

## Replay and lifecycle

Replay is opt-in:

```go
store := sseeventbus.NewMemoryReplayStore()
bus, err := sseeventbus.New(
		sseeventbus.WithReplay(store, 10*time.Minute, time.Minute),
)
```

Only events with IDs are retained. A known last ID replays subsequent events;
an empty or unknown ID replays all retained events. Explicit unregister and
client expiration clear retained history.

The bus starts its workers in `New`. Always call `Close(ctx)` to stop maintenance
jobs, flush the send queue, and close every registered connection. `Close`
returns flush and connection-close errors and may be called more than once.
Defaults are: one send worker, 10,000-item send and retry queues, 40 send
attempts, one-day client expiration, disabled heartbeat, and disabled replay.
Use `WithSynchronousDelivery` when sends should run in the publishing goroutine.
Retry timing can be tuned with `WithSchedulerDelay` and `WithRetryBackoff`.

## Custom transports and integrations

A non-`net/http` framework supplies a concurrent-safe connection:

```go
type Connection interface {
	Send(sseeventbus.Message) error
	Close() error
}
```

Register it with `bus.Register`, or use `bus.RegisterContext` when replay queue
backpressure should be cancelable. `Message` already contains converted data
and the SSE event, ID, retry, and comment fields. Connection methods must be
concurrent-safe, and `Close` should be idempotent.

The following dependency-free extension points exist in the core package:

- `SubscriptionRegistry` for custom subscription storage
- `ReplayStore` for retained event storage
- `Listener` for queue, send, and automatic unregister callbacks
- `Observer` for structured telemetry callbacks
- `DistributedTransport` for cross-node publication

Distributed transports receive a local-delivery callback once during bus
construction. They must attach origin information and suppress messages emitted
by the receiving node; inbound events are delivered locally without being
published again.

## License

MIT License. See [LICENSE](LICENSE) for details.
