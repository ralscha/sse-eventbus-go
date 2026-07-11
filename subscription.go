package sseeventbus

import "sync"

// SubscriptionRegistry stores event-to-client subscriptions. Implementations
// must be safe for concurrent use and must not return mutable internal storage.
type SubscriptionRegistry interface {
	Subscribe(clientID, event string)
	Unsubscribe(clientID, event string)
	UnsubscribeAll(clientID string)
	IsSubscribed(clientID, event string) bool
	AllEvents() []string
	AllSubscriptions() map[string][]string
	Subscribers(event string) []string
	CountSubscribers(event string) int
	HasSubscribers(event string) bool
}

// MemorySubscriptionRegistry is the default concurrent registry.
type MemorySubscriptionRegistry struct {
	mu   sync.RWMutex
	subs map[string]map[string]struct{}
}

func NewMemorySubscriptionRegistry() *MemorySubscriptionRegistry {
	return &MemorySubscriptionRegistry{subs: make(map[string]map[string]struct{})}
}

func (r *MemorySubscriptionRegistry) Subscribe(clientID, event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.subs == nil {
		r.subs = make(map[string]map[string]struct{})
	}
	clients := r.subs[event]
	if clients == nil {
		clients = make(map[string]struct{})
		r.subs[event] = clients
	}
	clients[clientID] = struct{}{}
}

func (r *MemorySubscriptionRegistry) Unsubscribe(clientID, event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if clients := r.subs[event]; clients != nil {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(r.subs, event)
		}
	}
}

func (r *MemorySubscriptionRegistry) UnsubscribeAll(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for event, clients := range r.subs {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(r.subs, event)
		}
	}
}

func (r *MemorySubscriptionRegistry) IsSubscribed(clientID, event string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.subs[event][clientID]
	return ok
}

func (r *MemorySubscriptionRegistry) AllEvents() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]string, 0, len(r.subs))
	for event := range r.subs {
		result = append(result, event)
	}
	return result
}

func (r *MemorySubscriptionRegistry) AllSubscriptions() map[string][]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string][]string, len(r.subs))
	for event, clients := range r.subs {
		ids := make([]string, 0, len(clients))
		for id := range clients {
			ids = append(ids, id)
		}
		result[event] = ids
	}
	return result
}

func (r *MemorySubscriptionRegistry) Subscribers(event string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	clients := r.subs[event]
	result := make([]string, 0, len(clients))
	for id := range clients {
		result = append(result, id)
	}
	return result
}

func (r *MemorySubscriptionRegistry) CountSubscribers(event string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.subs[event])
}
func (r *MemorySubscriptionRegistry) HasSubscribers(event string) bool {
	return r.CountSubscribers(event) > 0
}
