# Plan 011: Client constructors accept typed topics

**Status:** Planned

## Feature

Change the role-specific client constructors to receive an existing typed
`pubsub.Topic[T]` instead of a topic name string. This keeps topic declaration
in the low-level `pubsub` package while allowing clients to bind directly to
the caller's typed topic.

The public API should become:

```go
func NewProducerClient[T any](b *pubsub.Bus, topic pubsub.Topic[T]) ProducerClient[T]
func NewConsumerClient[T any](b *pubsub.Bus, topic pubsub.Topic[T]) (ConsumerClient[T], error)
```

Usage should look like:

```go
b := pubsub.NewBus()
orders := pubsub.NewTopic[OrderCreated]("orders.created")

producer := client.NewProducerClient(b, orders)
consumer, err := client.NewConsumerClient(b, orders)
```

The change must preserve exact topic-name matching, one event type per topic
name, non-blocking publication, delivery errors, consumer cancellation,
handler errors, idempotent close, and graceful bus shutdown.

## Depends on

Plan 010: Top-level client package.

## Implementation

- [ ] Change `NewProducerClient` to accept `pubsub.Topic[T]` rather than a
  topic-name string.
- [ ] Change `NewConsumerClient` to accept `pubsub.Topic[T]` rather than a
  topic-name string.
- [ ] Store the supplied typed topic directly in each client; do not recreate
  a topic from its name inside the client package.
- [ ] Preserve the existing producer and consumer interfaces and method
  behavior.
- [ ] Preserve nil-bus handling, topic registration, topic-type conflicts,
  `DeliveryError`, and `ErrBusClosed` behavior.
- [ ] Update all client tests, examples, integration tests, and README usage
  to construct `pubsub.Topic[T]` values explicitly.
- [ ] Document that a topic's type and exact name are declared with
  `pubsub.NewTopic` and then passed to the appropriate client constructor.
- [ ] Keep client code dependent on `pubsub` without introducing an import
  cycle.

## Tests

- [ ] Verify producer and consumer clients bind to the supplied typed topic.
- [ ] Verify a client cannot accidentally use a different event type for the
  same topic name through the typed constructor API.
- [ ] Verify topic isolation when multiple typed topics are passed to clients.
- [ ] Verify producer publishing, consumer ordering, cancellation, handler
  errors, repeated close, buffered draining, and shutdown behavior.
- [ ] Search for and remove all stale constructor calls that pass topic-name
  strings.
- [ ] Run `go test ./...`, `go test -race ./...`, and `go vet ./...`.
- [ ] Run repeated race-detector stress tests for client workflows.

## Completion criteria

Applications declare typed topics once with `pubsub.NewTopic` and pass those
topics directly to `client.NewProducerClient` and
`client.NewConsumerClient`. The clients retain predictable typed, isolated,
cancellable behavior, and the complete test suite passes under the race
detector.
