// Package httpadapter bridges sseeventbus to net/http without imposing a URL scheme.
package httpadapter

import (
	"context"
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
	writeTimeout time.Duration
	registration []sseeventbus.RegistrationOption
}

// Option configures Serve.
type Option func(*config)

// WithTimeout controls how long the handler stays open. A non-positive value disables the adapter timeout.
func WithTimeout(timeout time.Duration) Option { return func(c *config) { c.timeout = timeout } }

// WithWriteTimeout bounds each write and flush. A non-positive value leaves
// write deadlines to the HTTP server. The ResponseWriter must support deadlines.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(c *config) { c.writeTimeout = timeout }
}

// WithRegistration passes options to Bus.Register.
func WithRegistration(options ...sseeventbus.RegistrationOption) Option {
	return func(c *config) { c.registration = append(c.registration, options...) }
}

// WithLastEventID requests replay after the given event ID.
func WithLastEventID(lastEventID string) Option {
	return WithRegistration(sseeventbus.ReplayFrom(lastEventID))
}

type connection struct {
	mu               sync.Mutex
	w                http.ResponseWriter
	flush            func() error
	setWriteDeadline func(time.Time) error
	writeTimeout     time.Duration
	requestDone      <-chan struct{}
	done             chan struct{}
	closed           bool
	err              error
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
	if message.Retry < 0 {
		return fmt.Errorf("%w: retry duration is negative", ErrInvalidMessage)
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
		writeField(&builder, "event", message.Event)
	}
	if message.ID != "" {
		writeField(&builder, "id", message.ID)
	}
	if message.Retry > 0 {
		builder.WriteString("retry:")
		builder.WriteString(strconv.FormatInt(message.Retry.Milliseconds(), 10))
		builder.WriteByte('\n')
	}
	if message.HasData {
		for _, line := range splitLines(message.Data) {
			writeField(&builder, "data", line)
		}
	}
	builder.WriteByte('\n')
	if err := c.writeFrame(builder.String()); err != nil {
		return errors.Join(sseeventbus.ErrClosed, err)
	}
	return nil
}

// writeFrame requires c.mu. Transport failures terminate the stream: retrying a
// partially written SSE frame on the same response could corrupt the next event.
func (c *connection) writeFrame(frame string) (err error) {
	defer func() {
		if err != nil {
			c.closeLocked(err)
		}
	}()
	if c.writeTimeout > 0 {
		if err := c.setWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return err
		}
		defer func() { _ = c.setWriteDeadline(time.Time{}) }()
	}
	written, err := io.WriteString(c.w, frame)
	if err != nil {
		return err
	}
	if written != len(frame) {
		return io.ErrShortWrite
	}
	return c.flush()
}

func writeField(builder *strings.Builder, name, value string) {
	builder.WriteString(name)
	builder.WriteByte(':')
	// SSE parsers strip one leading space from a field value.
	if strings.HasPrefix(value, " ") {
		builder.WriteByte(' ')
	}
	builder.WriteString(value)
	builder.WriteByte('\n')
}

func splitLines(value string) []string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.Split(value, "\n")
}

func (c *connection) Close() error {
	c.mu.Lock()
	c.closeLocked(nil)
	c.mu.Unlock()
	return nil
}

func (c *connection) closeLocked(err error) {
	if !c.closed {
		c.closed = true
		c.err = err
		close(c.done)
	}
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
	return c.writeFrame(":\n\n")
}

// Serve registers a net/http SSE connection and blocks until the request ends,
// the configured timeout expires, or complete-after-message closes it.
// A non-empty Last-Event-ID request header enables replay; explicit registration
// options override that cursor. Disconnect preserves subscriptions and history.
func Serve(w http.ResponseWriter, r *http.Request, bus *sseeventbus.Bus, clientID string, options ...Option) error {
	configuration := config{timeout: 3 * time.Minute}
	if lastEventID := r.Header.Get("Last-Event-ID"); lastEventID != "" {
		configuration.registration = append(configuration.registration, sseeventbus.ReplayFrom(lastEventID))
	}
	for _, option := range options {
		if option != nil {
			option(&configuration)
		}
	}
	if clientID == "" {
		return sseeventbus.ErrInvalidClientID
	}
	if err := r.Context().Err(); err != nil {
		return err
	}
	ctx := r.Context()
	if configuration.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, configuration.timeout)
		defer cancel()
	}
	w.Header().Set("Content-Type", "text/event-stream")
	if w.Header().Get("Cache-Control") == "" {
		w.Header().Set("Cache-Control", "no-cache")
	}
	if w.Header().Get("X-Accel-Buffering") == "" {
		w.Header().Set("X-Accel-Buffering", "no")
	}
	controller := http.NewResponseController(w)
	flush := func() error {
		if err := controller.Flush(); err != nil {
			if errors.Is(err, http.ErrNotSupported) {
				return ErrStreamingUnsupported
			}
			return err
		}
		return nil
	}
	conn := &connection{
		w: w, flush: flush, setWriteDeadline: controller.SetWriteDeadline,
		writeTimeout: configuration.writeTimeout, requestDone: ctx.Done(), done: make(chan struct{}),
	}
	defer func() {
		_ = conn.Close()
		bus.Disconnect(clientID, conn)
	}()
	if err := conn.openStream(); err != nil {
		return fmt.Errorf("open SSE stream: %w", err)
	}
	if err := bus.RegisterContext(ctx, clientID, conn, configuration.registration...); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case <-conn.done:
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	return conn.err
}
