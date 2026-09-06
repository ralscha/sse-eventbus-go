package httpadapter

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ralscha/sse-eventbus-go"
)

func TestServeWireFormatAndHeaders(t *testing.T) {
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = bus.Close(ctx)
	}()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = Serve(w, r, bus, "c", WithRegistration(sseeventbus.SubscribeTo("orders"), sseeventbus.CompleteAfterMessage()))
	}))
	defer server.Close()
	responseCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		response, requestErr := http.Get(server.URL)
		if requestErr != nil {
			errCh <- requestErr
			return
		}
		responseCh <- response
	}()
	deadline := time.Now().Add(time.Second)
	for !bus.IsClientRegistered("c") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	event := sseeventbus.NewNamedEventWithData("orders", "line 1\nline 2")
	event.ID = "id-1"
	event.Retry = 1500 * time.Millisecond
	event.Comment = "note"
	if err := bus.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		t.Fatal(err)
	case response := <-responseCh:
		defer func() {
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Errorf("close response body: %v", closeErr)
			}
		}()
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		want := ":\n\n:note\nevent:orders\nid:id-1\nretry:1500\ndata:line 1\ndata:line 2\n\n"
		if string(body) != want {
			t.Fatalf("body=%q want %q", body, want)
		}
		if response.Header.Get("Content-Type") != "text/event-stream" || response.Header.Get("Cache-Control") != "no-cache" {
			t.Fatalf("headers=%v", response.Header)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP response did not complete")
	}
}

func TestServeTimeoutLeavesLogicalClientRegistered(t *testing.T) {
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		bus.Unregister("c")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = bus.Close(ctx)
	}()
	request := httptest.NewRequest(http.MethodGet, "/events", nil)
	recorder := httptest.NewRecorder()
	if err := Serve(recorder, request, bus, "c", WithTimeout(10*time.Millisecond), WithRegistration(sseeventbus.SubscribeTo("orders"))); err != nil {
		t.Fatal(err)
	}
	if !bus.IsClientRegistered("c") {
		t.Fatal("disconnect removed logical client")
	}
	if got := recorder.Body.String(); got != ":\n\n" {
		t.Fatalf("initial stream body=%q", got)
	}
}

func TestConnectionDefaultEventAndEmptyData(t *testing.T) {
	recorder := httptest.NewRecorder()
	conn := &connection{w: recorder, flush: func() error { recorder.Flush(); return nil }, requestDone: make(chan struct{}), done: make(chan struct{})}
	if err := conn.Send(sseeventbus.Message{Event: sseeventbus.DefaultEvent, Data: "", HasData: true}); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(strings.NewReader(recorder.Body.String()))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if len(lines) != 2 || lines[0] != "data:" || lines[1] != "" {
		t.Fatalf("lines=%v", lines)
	}
}

func TestConnectionNormalizesAllSSELineEndings(t *testing.T) {
	recorder := httptest.NewRecorder()
	conn := &connection{w: recorder, flush: func() error { recorder.Flush(); return nil }, requestDone: make(chan struct{}), done: make(chan struct{})}
	message := sseeventbus.Message{Event: "orders", Comment: "one\r\ntwo\rthree", Data: "a\r\nb\rc\n", HasData: true}
	if err := conn.Send(message); err != nil {
		t.Fatal(err)
	}
	want := ":one\n:two\n:three\nevent:orders\ndata:a\ndata:b\ndata:c\ndata:\n\n"
	if got := recorder.Body.String(); got != want {
		t.Fatalf("wire output=%q want %q", got, want)
	}
}

func TestConnectionRejectsLineInjection(t *testing.T) {
	recorder := httptest.NewRecorder()
	conn := &connection{w: recorder, flush: func() error { return nil }, requestDone: make(chan struct{}), done: make(chan struct{})}
	for _, message := range []sseeventbus.Message{{Event: "orders\ndata:injected"}, {ID: "id\rretry:0"}, {ID: "id\x00ignored"}, {Retry: -time.Second}} {
		if err := conn.Send(message); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("Send(%#v) error=%v", message, err)
		}
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("invalid message wrote %q", recorder.Body.String())
	}
}

func TestConnectionPreservesLeadingSpaces(t *testing.T) {
	recorder := httptest.NewRecorder()
	conn := &connection{w: recorder, flush: func() error { return nil }, done: make(chan struct{})}
	if err := conn.Send(sseeventbus.Message{Event: " orders", ID: " id", Data: " first\n  second", HasData: true}); err != nil {
		t.Fatal(err)
	}
	// EventSource removes exactly one space after each field's colon.
	fields := map[string]string{}
	for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
		name, value, ok := strings.Cut(line, ":")
		if ok {
			fields[name] += strings.TrimPrefix(value, " ") + "\n"
		}
	}
	if fields["event"] != " orders\n" || fields["id"] != " id\n" || fields["data"] != " first\n  second\n" {
		t.Fatalf("decoded fields = %#v", fields)
	}
}

type wrappedWriter struct{ http.ResponseWriter }

func (w wrappedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestServeSupportsWrappedWritersAndPreservesCachePolicy(t *testing.T) {
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	request := httptest.NewRequest(http.MethodGet, "/events", nil)
	recorder := httptest.NewRecorder()
	recorder.Header().Set("Cache-Control", "no-store")
	if err := Serve(wrappedWriter{recorder}, request, bus, "c", WithTimeout(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control=%q", recorder.Header().Get("Cache-Control"))
	}
	if recorder.Header().Get("Connection") != "" {
		t.Fatalf("hop-by-hop Connection header was set: %q", recorder.Header().Get("Connection"))
	}
}

type nonFlushingWriter struct {
	header http.Header
	body   strings.Builder
	status int
}

func (w *nonFlushingWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *nonFlushingWriter) Write(value []byte) (int, error) { return w.body.Write(value) }
func (w *nonFlushingWriter) WriteHeader(status int)          { w.status = status }

func TestServeRejectsWriterWithoutStreamingSupportBeforeRegistration(t *testing.T) {
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	request := httptest.NewRequest(http.MethodGet, "/events", nil)
	writer := &nonFlushingWriter{}
	err = Serve(writer, request, bus, "c")
	if !errors.Is(err, ErrStreamingUnsupported) {
		t.Fatalf("Serve error=%v, want ErrStreamingUnsupported", err)
	}
	if bus.IsClientRegistered("c") {
		t.Fatal("unsupported response writer registered a client")
	}
}

func TestServeAutomaticallyReplaysLastEventID(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []Option
		wantID  string
	}{
		{name: "header", wantID: "2"},
		{name: "explicit cursor", options: []Option{WithLastEventID("2")}, wantID: "3"},
		{name: "explicit empty cursor", options: []Option{WithLastEventID("")}, wantID: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := sseeventbus.NewMemoryReplayStore()
			for _, id := range []string{"1", "2", "3"} {
				store.Store(sseeventbus.ReplayEvent{ClientID: "c", Event: sseeventbus.Event{ID: id}, ConvertedValue: id, HasConverted: true, StoredAt: time.Now()})
			}
			bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery(), sseeventbus.WithReplay(store, time.Minute, time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bus.Close(context.Background()) })
			r := httptest.NewRequest(http.MethodGet, "/events", nil)
			r.Header.Set("Last-Event-ID", "1")
			w := httptest.NewRecorder()
			options := append([]Option{WithRegistration(sseeventbus.SubscribeTo(sseeventbus.DefaultEvent), sseeventbus.CompleteAfterMessage())}, tc.options...)
			if err := Serve(w, r, bus, "c", options...); err != nil {
				t.Fatal(err)
			}
			if want := ":\n\nid:" + tc.wantID + "\ndata:" + tc.wantID + "\n\n"; w.Body.String() != want {
				t.Fatalf("body = %q, want %q", w.Body.String(), want)
			}
		})
	}
}

type gatedConnection struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *gatedConnection) Send(sseeventbus.Message) error {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return nil
}
func (*gatedConnection) Close() error { return nil }

func TestServeTimeoutCancelsReplayBackpressure(t *testing.T) {
	store := sseeventbus.NewMemoryReplayStore()
	store.Store(sseeventbus.ReplayEvent{ClientID: "c", Event: sseeventbus.Event{ID: "1"}, StoredAt: time.Now()})
	bus, err := sseeventbus.New(sseeventbus.WithQueueCapacities(1, 1), sseeventbus.WithReplay(store, time.Minute, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	blocker := &gatedConnection{started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(blocker.release); _ = bus.Close(context.Background()) })
	if err := bus.Register("blocker", blocker, sseeventbus.SubscribeTo(sseeventbus.DefaultEvent)); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), sseeventbus.NewEvent("first")); err != nil {
		t.Fatal(err)
	}
	<-blocker.started
	if err := bus.Publish(context.Background(), sseeventbus.NewEvent("second")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- Serve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/events", nil), bus, "c",
			WithTimeout(10*time.Millisecond), WithLastEventID(""), WithRegistration(sseeventbus.SubscribeTo(sseeventbus.DefaultEvent)))
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Serve = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve timeout did not cancel replay")
	}
	if !bus.IsClientRegistered("c") || len(store.EventsSince("c", "")) != 1 {
		t.Fatal("timed out replay lost logical client state")
	}
}

func TestServeDisconnectRetainsOfflineEvents(t *testing.T) {
	store := sseeventbus.NewMemoryReplayStore()
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery(), sseeventbus.WithReplay(store, time.Minute, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	if err := Serve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/events", nil), bus, "c",
		WithTimeout(time.Millisecond), WithRegistration(sseeventbus.SubscribeTo(sseeventbus.DefaultEvent))); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), sseeventbus.Event{ID: "offline", Data: "saved"}); err != nil {
		t.Fatalf("offline publish tried to send on the ended HTTP response: %v", err)
	}
	if !bus.IsClientRegistered("c") || len(store.EventsSince("c", "")) != 1 {
		t.Fatal("disconnected client lost its retained event")
	}
}

type pipeWriter struct {
	net.Conn
	header http.Header
}

func (w *pipeWriter) Header() http.Header { return w.header }
func (*pipeWriter) WriteHeader(int)       {}
func (*pipeWriter) FlushError() error     { return nil }

func TestServeWriteTimeoutBoundsBlockedStream(t *testing.T) {
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	w := &pipeWriter{Conn: server, header: make(http.Header)}
	done := make(chan error, 1)
	go func() {
		done <- Serve(w, httptest.NewRequest(http.MethodGet, "/events", nil), bus, "c", WithWriteTimeout(10*time.Millisecond))
	}()
	select {
	case err := <-done:
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("Serve = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write timeout did not release blocked stream")
	}
	if bus.IsClientRegistered("c") {
		t.Fatal("failed stream was registered")
	}
}

func TestServeRejectsUnsupportedWriteDeadlines(t *testing.T) {
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	w := httptest.NewRecorder()
	err = Serve(w, httptest.NewRequest(http.MethodGet, "/events", nil), bus, "c", WithWriteTimeout(time.Second))
	if !errors.Is(err, http.ErrNotSupported) || w.Body.Len() != 0 || bus.IsClientRegistered("c") {
		t.Fatalf("unsupported deadlines: err = %v, body = %q", err, w.Body.String())
	}
}

type failingFlushWriter struct {
	*httptest.ResponseRecorder
	calls int
	err   error
}

func (w *failingFlushWriter) FlushError() error {
	w.calls++
	if w.calls > 1 {
		return w.err
	}
	return nil
}

func TestServeReturnsWriteFailureAndPreservesHistory(t *testing.T) {
	store := sseeventbus.NewMemoryReplayStore()
	bus, err := sseeventbus.New(sseeventbus.WithSendAttempts(1), sseeventbus.WithReplay(store, time.Minute, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	failed := errors.New("flush failed")
	w := &failingFlushWriter{ResponseRecorder: httptest.NewRecorder(), err: failed}
	done := make(chan error, 1)
	go func() {
		done <- Serve(w, httptest.NewRequest(http.MethodGet, "/events", nil), bus, "c", WithRegistration(sseeventbus.SubscribeTo(sseeventbus.DefaultEvent)))
	}()
	deadline := time.Now().Add(time.Second)
	for !bus.IsClientRegistered("c") {
		if time.Now().After(deadline) {
			t.Fatal("client did not register")
		}
		time.Sleep(time.Millisecond)
	}
	if err := bus.Publish(context.Background(), sseeventbus.Event{ID: "1", Data: "saved"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, failed) {
			t.Fatalf("Serve = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write failure did not terminate Serve")
	}
	if !bus.IsClientRegistered("c") || len(store.EventsSince("c", "")) != 1 {
		t.Fatal("failed stream lost logical client state")
	}
	if err := bus.Publish(context.Background(), sseeventbus.Event{ID: "2", Data: "offline"}); err != nil {
		t.Fatal(err)
	}
	if w.calls != 2 {
		t.Fatalf("failed stream was written again: %d flushes", w.calls)
	}
}
