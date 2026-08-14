// Package fabrik provides a small, process-local, typed publish/subscribe bus.
//
// A Bus routes payloads by the StreamType returned by each payload. Every
// active subscription for a stream receives each emitted event, subject to its
// independent bounded queue. Delivery is at-most-once: a full queue drops
// only that subscription's delivery and Emit reports a DeliveryError.
//
// The bus does not run application handlers. Consumers read Subscription.Events
// and decide how to process each Event. Payloads are not cloned, while event
// headers are copied at each delivery boundary.
package fabrik
