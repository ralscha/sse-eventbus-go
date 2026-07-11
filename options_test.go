package sseeventbus

import (
	"errors"
	"testing"
)

func TestTypedNilExtensionsAreRejected(t *testing.T) {
	var connection *recordingConnection
	bus := newSyncBus(t)
	if err := bus.Register("client", connection); !errors.Is(err, ErrNilConnection) {
		t.Fatalf("typed nil connection error=%v", err)
	}

	var registry *MemorySubscriptionRegistry
	if _, err := New(WithSubscriptionRegistry(registry)); err == nil {
		t.Fatal("typed nil subscription registry was accepted")
	}
	var store *MemoryReplayStore
	if _, err := New(WithReplay(store, 1, 1)); err == nil {
		t.Fatal("typed nil replay store was accepted")
	}
}

func TestWithoutClientExpiration(t *testing.T) {
	configuration := defaultConfig()
	if err := WithoutClientExpiration()(&configuration); err != nil {
		t.Fatal(err)
	}
	if configuration.expiration != 0 || configuration.expirationScan != 0 {
		t.Fatalf("expiration configuration=%v/%v", configuration.expiration, configuration.expirationScan)
	}
}
