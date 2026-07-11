package sseeventbus

import (
	"context"
	"encoding/json"
)

// Connection is implemented by an SSE transport. Implementations must be safe
// for use by the configured number of send workers.
type Connection interface {
	Send(Message) error
	Close() error
}

// DataConverter serializes non-string event data. Implementations must be safe
// for concurrent calls from publishers.
type DataConverter interface {
	Supports(Event) bool
	Convert(Event) (string, error)
}

// JSONConverter serializes values with encoding/json.
type JSONConverter struct{}

func (JSONConverter) Supports(Event) bool { return true }
func (JSONConverter) Convert(event Event) (string, error) {
	if event.Data == nil {
		return "", nil
	}
	value, err := json.Marshal(event.Data)
	return string(value), err
}

// Listener receives synchronous lifecycle notifications. Callbacks may run
// concurrently when multiple workers are configured. Panics are isolated from
// event delivery; implementations should handle and report their own failures.
type Listener interface {
	AfterEventQueued(*ClientEvent, bool)
	AfterEventSent(*ClientEvent, error)
	AfterClientsUnregistered([]string)
}

// NopListener can be embedded by listeners interested in only some callbacks.
type NopListener struct{}

func (NopListener) AfterEventQueued(*ClientEvent, bool) {}
func (NopListener) AfterEventSent(*ClientEvent, error)  {}
func (NopListener) AfterClientsUnregistered([]string)   {}

// DistributedTransport bridges events between nodes. It is responsible for
// origin tracking, must not echo an event back to its originating node, and
// must be safe for concurrent publication and inbound delivery.
type DistributedTransport interface {
	PublishRemote(context.Context, Event) error
	SetRemoteEventConsumer(func(Event)) error
}

// Operation identifies an observable event-bus operation.
type Operation string

const (
	OperationRegisterClient   Operation = "register_client"
	OperationUnregisterClient Operation = "unregister_client"
	OperationHandleEvent      Operation = "handle_event"
	OperationSendEvent        Operation = "send_event"
	OperationReplayEvents     Operation = "replay_events"
	OperationPublishRemote    Operation = "publish_remote"
	OperationReceiveRemote    Operation = "receive_remote"
)

// Observation is emitted once when an operation completes.
type Observation struct {
	Operation            Operation
	Outcome              string
	ClientID             string
	EventName            string
	Direct               bool
	Replay               bool
	CompleteAfterMessage bool
	LastEventIDPresent   bool
	DeliveryCount        int
	Attempt              int
	Err                  error
}

// Observer receives dependency-free structured observations. Observe may be
// called concurrently and must return promptly.
type Observer interface {
	Observe(context.Context, Observation)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Observation)

func (f ObserverFunc) Observe(ctx context.Context, observation Observation) { f(ctx, observation) }
