# Fabrik v2: in-process events

Status: Draft

This document is the working design for v2. It is intentionally isolated from
the v1 packages. Until the design is settled, every v2 artifact belongs under
`./v2`.

## Goal

Provide a small, process-local pub/sub framework for sending typed events from
one or more producer goroutines to one or more consumer goroutines.

The message is one event value containing both metadata and payload. There is
no separate public `Envelope` value to construct or pass around.

## Proposed first-cut API

The preferred v2 API uses the payload's Go type as the topic identity. A
`Bus` owns delivery state and lifecycle. Callers do not name a topic or
construct an event when emitting:

```go
package v2

type Event[T any] struct {
	ID            string
	OccurredAt    time.Time
	CorrelationID string
	CausationID   string
	Headers       map[string]string
	Payload       T
}

type PayloadType string

type EventPayload interface {
	PayloadType() PayloadType
}

type Options func(*EventMetadata)

type EventMetadata struct {
	ID            string
	OccurredAt    time.Time
	CorrelationID string
	CausationID   string
	Headers       map[string]string
}

type Bus struct { /* registry, queues, and lifecycle */ }

func NewBus() *Bus
func (b *Bus) Emit(ctx context.Context, payload EventPayload, opts ...Options) error

func WithID(id string) Options
func WithOccurredAt(at time.Time) Options
func WithCorrelationID(id string) Options
func WithCausationID(id string) Options
func WithHeaders(headers map[string]string) Options

func Subscribe[T EventPayload](ctx context.Context, b *Bus) (*Subscription[T], error)
func WithBufferSize(size int) SubscriptionOption

type Subscription[T any] struct {
	Events <-chan Event[T]
}

func (s *Subscription[T]) Close() error
func (b *Bus) Shutdown(ctx context.Context) error
```

`Emit` creates the event internally:

```go
func (b *Bus) Emit(ctx context.Context, payload EventPayload, opts ...Options) error {
	metadata := EventMetadata{
		ID:         newEventID(),
		OccurredAt: time.Now(),
	}
	for _, option := range opts {
		option(&metadata)
	}
	event := Event[EventPayload]{
		ID:            metadata.ID,
		OccurredAt:    metadata.OccurredAt,
		CorrelationID: metadata.CorrelationID,
		CausationID:   metadata.CausationID,
		Headers:       metadata.Headers,
		Payload:       payload,
	}
	return b.emit(ctx, event)
}
```

Application payloads provide a stable logical stream identity:

```go
const OrderCreatedType pubsub.PayloadType = "orders.created"

type OrderCreated struct {
	OrderID string
}

func (OrderCreated) PayloadType() pubsub.PayloadType {
	return OrderCreatedType
}
```

When options are omitted, the generated event receives an ID and the current
time. Options override generated metadata:

```go
	bus := pubsub.NewBus()
	bus.Emit(ctx, order,
		pubsub.WithCorrelationID(requestID),
		pubsub.WithHeaders(map[string]string{"tenant": tenantID}),
	)
```

Options are applied from left to right. Later options replace earlier scalar
values. `WithHeaders` merges keys, with later values winning for duplicate
keys. The implementation should copy headers before queueing the event.

Example:

```go
b := NewBus()

sub, err := Subscribe[OrderCreated](context.Background(), b)
if err != nil {
	return err
}

	go func() {
	defer sub.Close()
	for event := range sub.Events {
		process(event)
	}
}()

err = b.Emit(context.Background(), OrderCreated{ID: "order-123"})
```

Each `Bus` is independent. This supports separate applications, tests, and
lifecycle boundaries in one process. `Shutdown` closes only the bus it belongs
to.

## Semantics for v2.0

These are the recommended defaults. They should become executable tests before
implementation is considered complete.

- Each bus is process-local and safe for concurrent use.
- Each `PayloadType` has one implicit topic on a bus.
- Every active subscription to a topic receives every emitted event.
- Each subscription has an independent bounded queue.
- `Emit` does not wait for a slow consumer.
- A full consumer queue causes that delivery to be dropped and is reported by
  the returned error. Other consumers still receive the event.
- Events are delivered in emission order per topic. With concurrent emitters,
  order is the order at the topic's serialization point.
- Delivery is at-most-once. Queueing counts as delivery; there are no
  acknowledgements, retries, persistence, replay, or redelivery.
- Closing a subscription is idempotent and drains events already queued before
  the close becomes visible.
- Shutdown rejects new emits and subscriptions, then closes active
  subscriptions after queued events have been drained.
- `Emit` generates an event containing at least a unique event ID and
  `OccurredAt`.
- A consumer handler is application code. The bus only queues and delivers
  events; it does not recover panics, retry failures, or run handlers itself.
- `Event.Headers` is copied or treated as immutable at the delivery
  boundary. The implementation must not let one consumer mutate metadata seen
  by another consumer.

## Event metadata

The initial event metadata is deliberately small:

- `ID` identifies one event instance. The framework should generate a non-empty
  ID for every event emitted through `Emit`; the exact ID format is an
  implementation detail.
- `OccurredAt` is the application event time, not necessarily queue time.
- `CorrelationID` links related work, such as one request and all events
  produced during that workflow.
- `CausationID` identifies the immediate event or command that caused this
  event.
- `Headers` supports lightweight cross-cutting metadata such as tenant or
  trace values.

There is no separate public topic handle in this model. `PayloadType` is the
routing key, and each bus keeps one subscriber set for each value. The value is
an opaque, stable identifier such as `orders.created`.

The bus must reject an empty `PayloadType`. It must also reject attempts to use
one `PayloadType` with incompatible Go payload types. This prevents two
different payload structs from being silently delivered through the same typed
subscription.

The framework must not add received-at, attempt, or consumer identity fields
until those concepts exist in the delivery contract. Metadata that sounds
useful but has no defined semantics becomes a long-lived lie.

## Concurrency model

The bus owns the topic registry and lifecycle state. Each topic owns its own
subscriber set and serialization lock.

```text
Emit / Subscribe
        |
        v
  bus lifecycle lock  -- short registry operation only
        |
        v
  topic lock           -- registration or non-blocking fan-out
        |
        v
  subscriber queues
```

The global bus lock must not be held while delivering to subscriber queues.
Operations on unrelated topics should not wait on each other except during the
short lifecycle boundary. The topic lock may serialize emitters for one topic,
which is what gives the ordering guarantee.

The implementation should prefer ordinary mutexes and channels over clever
lock-free structures. The race detector is part of the design, not a final
polish step.

## Decisions previously deferred

These decisions are now settled:

1. `Event.ID` and `OccurredAt` are always non-zero on emitted events. The
   framework generates them when callers do not provide overrides.
2. The default subscription queue size is `128`, with a per-subscription
   override.
3. Full queues drop only that subscriber's delivery and return a
   `DeliveryError`. Emit never waits for consumer processing.
4. V2.0 is fan-out only. Consumer groups are out of scope.
5. Event headers use `map[string]string`, copied defensively at delivery.
6. A canceled context rejects a new `Emit` or `Subscribe`; it does not remove
   events already queued.
7. Nil payloads are rejected, including typed nil pointers, maps, slices, and
   interfaces.

The implicit-type design accepts the restriction that one `PayloadType` maps to
one stream per bus. Different Go payload types returning the same
`PayloadType` are rejected as a conflict.

## Non-goals for v2.0

- Cross-process or network transport.
- Durable storage, replay, or recovery after process failure.
- Exactly-once processing.
- Consumer acknowledgements and retries.
- Wildcard topic matching or topic hierarchies.
- Serialization formats such as JSON, protobuf, or CloudEvents compliance.
- A handler registry inside the bus.

## Implementation phases

### Phase 1: event primitives

- Add `Event[T]`, `Bus`, and the package-level generic subscription function
  under `./v2`.
- Define validation and error identities.
- Test zero values, nil payloads, header isolation, and topic type identity.

### Phase 2: bus and fan-out

- Add the topic registry and per-topic subscriber queues.
- Implement `Emit` and `Subscribe` with the semantics above.
- Test fan-out, topic isolation, ordering, full queues, and concurrent emitters.

### Phase 3: lifecycle

- Add idempotent subscription close and graceful bus shutdown.
- Test queued-event draining, concurrent close, shutdown races, and rejected
  operations after shutdown begins.

### Phase 4: public examples and hardening

- Add package documentation and examples under `./v2`.
- Run `go test ./...`, `go test -race ./...`, and `go vet ./...`.
- Add stress tests for multi-topic concurrent producers and consumers.
- Only after the contract is stable, decide whether v2 needs a handler/client
  convenience layer.

## First discussion

The v2.0 design is ready to implement once the remaining queue and metadata
details are settled. Consumer groups and multiple streams for one payload type
are explicitly outside v2.0. The sharpest boundary is this:

```text
v2.0 = type-routed fan-out buses with bounded, at-most-once delivery
```

Adding consumer groups or blocking delivery changes the mental model from
simple notification bus to work queue. That should be a deliberate future
versioned choice, not an option hidden in v2.0.
