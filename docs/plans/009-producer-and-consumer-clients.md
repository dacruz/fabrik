# Plan 009: Producer and consumer clients

**Status:** Completed

## Feature

Provide clear, role-specific client interfaces for applications that publish
one event type on one topic and consume another event type from a different
topic. The clients hide bus and topic wiring from application code while
retaining Fabrik's typed, process-local delivery semantics.

## Depends on

Plans 001 through 007.

## Proposed package and API

Add a small `client` package that adapts the low-level `pubsub` API.
The package should expose capability-oriented generic interfaces:

```go
type ProducerClient[T any] interface {
	Publish(T) error
}

type ConsumerClient[T any] interface {
	Run(context.Context, Handler[T]) error
	Close()
}

type Handler[T any] func(context.Context, T) error
```

Constructors bind the bus and exact topic name:

```go
func NewProducerClient[T any](b *pubsub.Bus, topicName string) ProducerClient[T]
func NewConsumerClient[T any](b *pubsub.Bus, topicName string) (ConsumerClient[T], error)
```

`ProducerClient` wraps `pubsub.Publish`. Publishing remains non-blocking and
returns `DeliveryError` when subscriber deliveries are dropped. It does not
accept a context because the underlying operation does not wait.

`ConsumerClient` subscribes during construction, owns that subscription, and
processes events through `Run`. `Run` returns when its context is canceled, the
bus closes the subscription, or the handler returns an error. `Close` is
idempotent and releases the subscription. A consumer has one active `Run`
loop at a time; concurrent `Run` calls are unsupported.

The constructors should not expose `pubsub.Topic[T]` to callers. This keeps
topic names and event types together at the client boundary and makes it
impossible for a caller to accidentally publish or consume through the wrong
role-specific topic.

## Implementation

- [x] Add the `client` package and generic producer and consumer client
  implementations.
- [x] Bind each client to one exact topic name and one event type.
- [x] Preserve `ErrNilBus`, `ErrBusClosed`, topic type conflicts, and
  `DeliveryError` from the underlying package.
- [x] Define and test consumer lifecycle behavior for context cancellation,
  bus shutdown, handler errors, repeated `Close`, and a nil subscription.
- [x] Document that clients share a `*pubsub.Bus`; the application that owns
  the bus remains responsible for `pubsub.Shutdown`.
- [x] Add package and README examples showing independent producer and
  consumer clients for topic A and topic B.

## Tests

- [x] Add unit tests for `ProducerClient` publishing typed values to its bound
  topic and rejecting a closed or nil bus.
- [x] Add unit tests for `ConsumerClient` receiving only values from its bound
  topic and preserving delivery order.
- [x] Test that a producer on topic A cannot deliver to a consumer on topic B,
  including when both event types are otherwise compatible.
- [x] Test consumer cancellation, handler errors, bus shutdown, buffered
  draining, and idempotent close.
- [x] Migrate the root-level `integration` workflow tests to construct and use
  producer and consumer clients instead of directly calling `Subscribe`,
  `Publish`, and managing raw subscriptions.
- [x] Keep focused low-level `pubsub` unit tests for bus, topic, subscription,
  backpressure, locking, and shutdown internals; client-facing integration
  tests should exercise only the client interfaces and public lifecycle.
- [x] Run `go test ./...`, `go test -race ./...`, and repeated stress runs for
  the client workflows.

## Completion criteria

An application can create a `ProducerClient[A]` for topic A and a
`ConsumerClient[B]` for topic B, use them without direct bus/topic plumbing,
and receive predictable typed, isolated, cancellable behavior. The migrated
integration suite passes under the race detector and the documentation shows
the client-oriented workflow.
