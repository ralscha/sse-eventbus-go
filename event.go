package sseeventbus

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// DefaultEvent is the event name used by the EventSource "message" handler.
const DefaultEvent = "message"

// ErrInvalidEvent indicates that an event contains values that cannot be
// represented safely on an SSE wire.
var ErrInvalidEvent = errors.New("invalid SSE event")

// Event describes one logical server-sent event. Callers should treat an Event
// as immutable after passing it to the bus.
type Event struct {
	ClientIDs        []string
	ExcludeClientIDs []string
	Name             string
	Data             any
	Retry            time.Duration
	ID               string
	Comment          string
}

// NewEvent creates an event with the default event name.
func NewEvent(data any) Event { return Event{Name: DefaultEvent, Data: data} }

// NewNamedEvent creates a named event with an empty data value.
func NewNamedEvent(name string) Event { return Event{Name: name, Data: ""} }

// NewNamedEventWithData creates a named event containing data.
func NewNamedEventWithData(name string, data any) Event { return Event{Name: name, Data: data} }

func normalizeEvent(event Event) Event {
	if event.Name == "" {
		event.Name = DefaultEvent
	}
	event.ClientIDs = uniqueStrings(event.ClientIDs)
	event.ExcludeClientIDs = uniqueStrings(event.ExcludeClientIDs)
	return event
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func validateEvent(event Event) error {
	if strings.ContainsAny(event.Name, "\r\n") {
		return fmt.Errorf("%w: name contains a line break", ErrInvalidEvent)
	}
	if strings.ContainsAny(event.ID, "\x00\r\n") {
		return fmt.Errorf("%w: ID contains a null or line break", ErrInvalidEvent)
	}
	if event.Retry < 0 {
		return fmt.Errorf("%w: retry duration is negative", ErrInvalidEvent)
	}
	return nil
}

// Message is the transport-ready representation sent to a Connection.
type Message struct {
	Event   string
	Data    string
	HasData bool
	Retry   time.Duration
	ID      string
	Comment string
}
