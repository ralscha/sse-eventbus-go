package httpadapter

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	for _, message := range []sseeventbus.Message{{Event: "orders\ndata:injected"}, {ID: "id\rretry:0"}, {ID: "id\x00ignored"}} {
		if err := conn.Send(message); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("Send(%#v) error=%v", message, err)
		}
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("invalid message wrote %q", recorder.Body.String())
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
