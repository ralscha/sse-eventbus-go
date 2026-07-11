package sseeventbus

import (
	"sync"
	"time"
)

// ReplayEvent is one retained, already-converted event for a client.
type ReplayEvent struct {
	ClientID       string
	Event          Event
	ConvertedValue string
	HasConverted   bool
	StoredAt       time.Time
}

// ReplayStore stores events for reconnecting clients. Implementations must be
// safe for concurrent use and return events in original delivery order.
type ReplayStore interface {
	Store(ReplayEvent)
	EventsSince(clientID, lastEventID string) []ReplayEvent
	ClearClient(clientID string)
	PurgeExpired(before time.Time)
}

// MemoryReplayStore is the default in-memory replay implementation.
type MemoryReplayStore struct {
	mu     sync.RWMutex
	events map[string][]ReplayEvent
}

func NewMemoryReplayStore() *MemoryReplayStore {
	return &MemoryReplayStore{events: make(map[string][]ReplayEvent)}
}

func (s *MemoryReplayStore) Store(event ReplayEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.events == nil {
		s.events = make(map[string][]ReplayEvent)
	}
	event.Event = normalizeEvent(event.Event)
	s.events[event.ClientID] = append(s.events[event.ClientID], event)
}

func (s *MemoryReplayStore) EventsSince(clientID, lastEventID string) []ReplayEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := s.events[clientID]
	if len(events) == 0 {
		return nil
	}
	start := 0
	if lastEventID != "" {
		found := false
		for i, event := range events {
			if event.Event.ID == lastEventID {
				start, found = i+1, true
				break
			}
		}
		if !found {
			start = 0
		}
	}
	result := append([]ReplayEvent(nil), events[start:]...)
	for i := range result {
		result[i].Event = normalizeEvent(result[i].Event)
	}
	return result
}

func (s *MemoryReplayStore) ClearClient(clientID string) {
	s.mu.Lock()
	delete(s.events, clientID)
	s.mu.Unlock()
}

func (s *MemoryReplayStore) PurgeExpired(before time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, events := range s.events {
		kept := events[:0]
		for _, event := range events {
			if !event.StoredAt.Before(before) {
				kept = append(kept, event)
			}
		}
		for i := len(kept); i < len(events); i++ {
			events[i] = ReplayEvent{}
		}
		if len(kept) == 0 {
			delete(s.events, id)
		} else {
			s.events[id] = kept
		}
	}
}
