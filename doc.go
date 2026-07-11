// Package sseeventbus tracks Server-Sent Events clients and delivers events to
// their subscriptions through transport-neutral Connection implementations.
//
// A Bus is safe for concurrent use. Custom Connection, SubscriptionRegistry,
// ReplayStore, Listener, Observer, DataConverter, and DistributedTransport
// implementations must also be safe for the concurrency documented by their
// interfaces.
package sseeventbus
