# Plan 002: Subscription lifecycle

**Status:** Completed

## Feature completed

Callers can subscribe to a typed topic and later unsubscribe safely. Each subscription starts empty, exposes a receive-only channel with capacity 100, and is removed from future deliveries without closing a channel during an active publish.

## Depends on

Plan 001: Bus and typed topic registry.

## Implementation

- [x] Add `Subscription[T any]` with `Events <-chan T`, an internal channel, subscriber identifier, topic reference, and once-only lifecycle state.
- [x] Implement `Subscribe[T](b *Bus, topic Topic[T]) (*Subscription[T], error)` using the registration and type validation from Plan 001, so a topic-type conflict is reported at subscription time.
- [x] Add `(*Subscription[T]).Unsubscribe()` as an idempotent, concurrency-safe operation.
- [x] Under the topic mutex, add and remove subscribers atomically. Ensure an unsubscribe cannot close a channel while a publisher is sending to it.
- [x] Define channel behavior for unsubscribe: remove the subscriber and close its channel after removal, allowing already queued values to drain.
- [x] Keep an empty topic registered after its last subscriber leaves.

## Tests

- [x] New subscriptions have no queued values and capacity 100.
- [x] Two subscriptions to one topic are independently tracked.
- [x] Unsubscribe is safe when called twice and concurrently from many goroutines.
- [x] After `Unsubscribe` returns, later publishes cannot queue to that subscription.
- [x] Values queued before unsubscribe remain readable and the channel eventually closes according to the documented behavior.
- [x] Removing the last subscriber does not prevent a later subscription.

## Completion criteria

Subscription creation and teardown work through the public API without send/close panics, and lifecycle tests pass under `go test -race`.
