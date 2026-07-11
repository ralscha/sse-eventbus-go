// Package httpadapter bridges sseeventbus to net/http without imposing a URL scheme.
package httpadapter

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ralscha/sse-eventbus-go"
)

var ErrStreamingUnsupported = errors.New("response writer does not support streaming")

// ErrInvalidMessage indicates that a Message cannot be encoded safely as SSE.
var ErrInvalidMessage = errors.New("invalid SSE message")

type config struct {
	timeout      time.Duration
	registration []sseeventbus.RegistrationOption
}

// Option configures Serve.
type Option func(*config)

// WithTimeout controls how long the handler stays open. A non-positive value disables the adapter timeout.
func WithTimeout(timeout time.Duration) Option { return func(c *config) { c.timeout = timeout } }

// WithRegistration passes options to Bus.Register.
func WithRegistration(options ...sseeventbus.RegistrationOption) Option {
	return func(c *config) { c.registration = append(c.registration, options...) }
}

// WithLastEventID requests replay after the given event ID.
func WithLastEventID(lastEventID string) Option {
	return WithRegistration(sseeventbus.ReplayFrom(lastEventID))
}

type connection struct {
	mu          sync.Mutex
	w           http.ResponseWriter
	flush       func() error
	requestDone <-chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	closed      bool
}

func (c *connection) Send(message sseeventbus.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return sseeventbus.ErrClosed
	}
	select {
	case <-c.requestDone:
		return sseeventbus.ErrClosed
	default:
	}
	if strings.ContainsAny(message.Event, "\r\n") {
		return fmt.Errorf("%w: event contains a line break", ErrInvalidMessage)
	}
	if strings.ContainsAny(message.ID, "\x00\r\n") {
		return fmt.Errorf("%w: ID contains a null or line break", ErrInvalidMessage)
	}
	var builder strings.Builder
	if message.Comment != "" {
		for _, line := range splitLines(message.Comment) {
			builder.WriteString(":")
			builder.WriteString(line)
			builder.WriteByte('\n')
		}
	}
	if message.Event != "" && message.Event != sseeventbus.DefaultEvent {
		builder.WriteString("event:")
		builder.WriteString(message.Event)
		builder.WriteByte('\n')
	}
	if message.ID != "" {
		builder.WriteString("id:")
		builder.WriteString(message.ID)
		builder.WriteByte('\n')
	}
	if message.Retry > 0 {
		builder.WriteString("retry:")
		builder.WriteString(strconv.FormatInt(message.Retry.Milliseconds(), 10))
		builder.WriteByte('\n')
	}
	if message.HasData {
		for _, line := range splitLines(message.Data) {
			builder.WriteString("data:")
			builder.WriteString(line)
			builder.WriteByte('\n')
		}
	}
	builder.WriteByte('\n')
	written, err := io.WriteString(c.w, builder.String())
	if err != nil {
		return err
	}
	if written != builder.Len() {
		return io.ErrShortWrite
	}
	return c.flush()
}

func splitLines(value string) []string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.Split(value, "\n")
}

func (c *connection) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

func (c *connection) openStream() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	// A flushed header-only response can still be buffered by reverse proxies.
	// An empty SSE comment makes the stream observable without dispatching an
	// application event to the client.
	written, err := io.WriteString(c.w, ":\n\n")
	if err != nil {
		return err
	}
	if written != 3 {
		return io.ErrShortWrite
	}
	return c.flush()
}

// Serve registers a net/http SSE connection and blocks until the request ends,
// the configured timeout expires, or complete-after-message closes it.
func Serve(w http.ResponseWriter, r *http.Request, bus *sseeventbus.Bus, clientID string, options ...Option) error {
	flush, ok := findFlusher(w)
	if !ok {
		http.Error(w, ErrStreamingUnsupported.Error(), http.StatusInternalServerError)
		return ErrStreamingUnsupported
	}
	configuration := config{timeout: 3 * time.Minute}
	for _, option := range options {
		if option != nil {
			option(&configuration)
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	if w.Header().Get("Cache-Control") == "" {
		w.Header().Set("Cache-Control", "no-cache")
	}
	if w.Header().Get("X-Accel-Buffering") == "" {
		w.Header().Set("X-Accel-Buffering", "no")
	}
	conn := &connection{w: w, flush: flush, requestDone: r.Context().Done(), done: make(chan struct{})}
	if err := bus.RegisterContext(r.Context(), clientID, conn, configuration.registration...); err != nil {
		_ = conn.Close()
		return err
	}
	if err := conn.openStream(); err != nil {
		_ = conn.Close()
		return fmt.Errorf("open SSE stream: %w", err)
	}
	var timeout <-chan time.Time
	var timer *time.Timer
	if configuration.timeout > 0 {
		timer = time.NewTimer(configuration.timeout)
		timeout = timer.C
		defer timer.Stop()
	}
	select {
	case <-r.Context().Done():
	case <-conn.done:
	case <-timeout:
	}
	return conn.Close()
}

type flushErrorer interface {
	FlushError() error
}

type unwrapper interface {
	Unwrap() http.ResponseWriter
}

func findFlusher(writer http.ResponseWriter) (func() error, bool) {
	for writer != nil {
		if flusher, ok := writer.(flushErrorer); ok {
			return flusher.FlushError, true
		}
		if flusher, ok := writer.(http.Flusher); ok {
			return func() error { flusher.Flush(); return nil }, true
		}
		wrapper, ok := writer.(unwrapper)
		if !ok {
			break
		}
		next := wrapper.Unwrap()
		if next == writer {
			break
		}
		writer = next
	}
	return nil, false
}
