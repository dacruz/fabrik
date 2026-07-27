// Package pubsub provides a typed, process-local publish/subscribe bus for
// long-lived Go processes.
//
// Create a bus with NewBus, create an exact named topic with NewTopic, and use
// Subscribe and Publish to connect consumers and producers. Topics are
// registered lazily by the first Subscribe or Publish. A topic name is opaque
// and exact; one name may be used with only one Go event type on a bus.
//
// Subscriptions expose 100-capacity buffered channels. Publish is
// non-blocking: a full subscriber drops that delivery while other subscribers
// continue receiving, and Publish returns an inspectable DeliveryError.
// Unsubscribe is idempotent and closes that subscription's channel. Shutdown
// rejects new work, drains already queued values, and then closes active
// subscription channels. Shutdown is graceful and idempotent; its context
// controls waiting to start or join shutdown, not an already-running teardown.
//
// The bus does not cross process boundaries and does not execute message
// handlers. Consumers read subscription channels and define their own handler,
// acknowledgement, retry, and ordering semantics.
package pubsub
