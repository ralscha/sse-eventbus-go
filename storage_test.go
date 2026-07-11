package sseeventbus

import (
	"reflect"
	"testing"
	"time"
)

func TestMemoryRegistryZeroValueIsUsable(t *testing.T) {
	var registry MemorySubscriptionRegistry
	registry.Subscribe("client", "orders")
	if !registry.IsSubscribed("client", "orders") {
		t.Fatal("zero-value registry did not store subscription")
	}
	registry.UnsubscribeAll("client")
	if registry.HasSubscribers("orders") {
		t.Fatal("unsubscribe all left a subscription")
	}
}

func TestMemoryReplayStoreZeroValueCopiesEvents(t *testing.T) {
	var store MemoryReplayStore
	clientIDs := []string{"client"}
	event := Event{Name: "orders", ID: "1", ClientIDs: clientIDs, Data: "payload"}
	store.Store(ReplayEvent{ClientID: "client", Event: event, StoredAt: time.Now()})
	clientIDs[0] = "mutated"

	first := store.EventsSince("client", "")
	if len(first) != 1 || !reflect.DeepEqual(first[0].Event.ClientIDs, []string{"client"}) {
		t.Fatalf("stored event was mutated: %#v", first)
	}
	first[0].Event.ClientIDs[0] = "also-mutated"
	second := store.EventsSince("client", "")
	if !reflect.DeepEqual(second[0].Event.ClientIDs, []string{"client"}) {
		t.Fatalf("returned event aliases store: %#v", second)
	}
}

func TestMemoryReplayStorePurgesOutOfOrderEvents(t *testing.T) {
	var store MemoryReplayStore
	now := time.Now()
	store.Store(ReplayEvent{ClientID: "client", Event: Event{Name: "x", ID: "new"}, StoredAt: now})
	store.Store(ReplayEvent{ClientID: "client", Event: Event{Name: "x", ID: "old"}, StoredAt: now.Add(-time.Hour)})
	store.PurgeExpired(now.Add(-time.Minute))
	events := store.EventsSince("client", "")
	if len(events) != 1 || events[0].Event.ID != "new" {
		t.Fatalf("purged events=%#v", events)
	}
}
