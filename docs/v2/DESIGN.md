# Fabrik v2: in-process events

Status: Draft

This document is the working design for v2. It is intentionally isolated from
the v1 packages. The implementation will use the module root at
`github.com/dacruz/fabrik`; this document remains under `docs/v2`.

## Goal

Provide a small, process-local pub/sub framework for sending typed events from
one or more producer goroutines to one or more consumer goroutines.

The message is one event value containing both metadata and payload. There is
no separate public `Envelope` value to construct or pass around.

## Proposed first-cut API

The preferred v2 API uses a payload's logical `StreamType` as its stream
identity. A `Bus` owns delivery state and lifecycle. Callers do not name a
stream or
construct an event when emitting:

```go
package fabrik

type Event[T any] struct {
	ID            string
	OccurredAt    time.Time
	CorrelationID string
	CausationID   string
	Headers       map[string]string
	Payload       T
}

type StreamType string

type EventPayload interface {
	StreamType() StreamType
}

type EventOptions interface {
	applyEvent(*eventMetadata)
}

type eventMetadata struct {
	OccurredAt    time.Time
	CorrelationID string
	CausationID   string
	Headers       map[string]string
}

type Bus struct { /* registry, queues, and lifecycle */ }

func NewBus() *Bus
func (b *Bus) Emit(ctx context.Context, payload EventPayload, opts ...EventOptions) error

func WithOccurredAt(at time.Time) EventOptions
func WithCorrelationID(id string) EventOptions
func WithCausationID(id string) EventOptions
func WithHeaders(headers map[string]string) EventOptions

type SubscriptionOptions interface {
	applySubscription(*subscriptionConfig)
}

type subscriptionConfig struct {
	bufferSize int
}

func Subscribe[T EventPayload](ctx context.Context, b *Bus, opts ...SubscriptionOptions) (*Subscription[T], error)
func WithBufferSize(size int) SubscriptionOptions

type Subscription[T any] struct {
	Events <-chan Event[T]
}

func (s *Subscription[T]) Close()
func (b *Bus) Shutdown(ctx context.Context) error
```

`EventOptions` and `SubscriptionOptions` intentionally contain unexported
methods. This keeps the option sets closed: callers can use options provided
by Fabrik, but cannot define new options outside the package.

The bus may use an unexported type-erased queued record internally. This is
not part of the public API and is not a separate public `Envelope` type:

```go
type queuedEvent struct {
	id          string
	streamType  StreamType
	payloadType reflect.Type
	metadata    eventMetadata
	payload     EventPayload
}
```

The bus constructs a typed `Event[T]` from this record at each subscription
delivery boundary. The registry's exact Go-type check makes the payload
assertion to `T` safe.

The error API uses `Err...` names for stable categories and `...Error` names
for structured details. Error messages use the `fabrik:` prefix. The initial
error set is:

```go
var (
	ErrBusClosed           = errors.New("fabrik: bus is shut down")
	ErrDelivery            = errors.New("fabrik: delivery failed")
	ErrEmptyStreamType     = errors.New("fabrik: empty stream type")
	ErrEventIDGeneration   = errors.New("fabrik: event ID generation failed")
	ErrInvalidOccurredAt   = errors.New("fabrik: invalid occurred-at timestamp")
	ErrInvalidBufferSize   = errors.New("fabrik: invalid subscription buffer size")
	ErrNilBus              = errors.New("fabrik: nil bus")
	ErrNilContext          = errors.New("fabrik: nil context")
	ErrNilPayload          = errors.New("fabrik: nil payload")
	ErrStreamTypeConflict  = errors.New("fabrik: stream type conflict")
	ErrUnsupportedPayloadType = errors.New("fabrik: unsupported payload type")
)

type DeliveryError struct {
	StreamType StreamType
	Dropped    int
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("fabrik: dropped %d delivery(s) for stream type %q", e.Dropped, e.StreamType)
}

func (e *DeliveryError) Unwrap() error { return ErrDelivery }

type StreamTypeConflictError struct {
	StreamType StreamType
	Existing   reflect.Type
	Requested  reflect.Type
}

func (e *StreamTypeConflictError) Error() string {
	return fmt.Sprintf(
		"fabrik: stream type %q is registered for %v, cannot use %v",
		e.StreamType,
		e.Existing,
		e.Requested,
	)
}

func (e *StreamTypeConflictError) Unwrap() error {
	return ErrStreamTypeConflict
}
```

`errors.Is` matches stable categories such as `ErrDelivery` and
`ErrStreamTypeConflict`; `errors.As` exposes details such as the number of
dropped deliveries or the conflicting Go types. A canceled context returns
`ctx.Err()` directly. Closing a subscription is idempotent and does not
return an error or require a separate subscription-closed error.

`Emit` creates the event internally:

```go
func (b *Bus) Emit(ctx context.Context, payload EventPayload, opts ...EventOptions) error {
	// Validate b, ctx, and payload, and pass the bus admission boundary first.
	id, err := newEventID()
	if err != nil {
		return err
	}

	metadata := eventMetadata{
		OccurredAt: time.Now(),
	}
	for _, option := range opts {
		option.applyEvent(&metadata)
	}
	event := queuedEvent{
		id:          id,
		streamType:  payload.StreamType(),
		payloadType: reflect.TypeOf(payload),
		metadata:    metadata,
		payload:     payload,
	}
	return b.emit(ctx, event)
}
```

Application payloads provide a stable logical stream identity:

```go
const OrderCreatedStream fabrik.StreamType = "orders.created"

type OrderCreated struct {
	OrderID string
}

func (OrderCreated) StreamType() fabrik.StreamType {
	return OrderCreatedStream
}
```

`Subscribe` resolves the stream identity from the type parameter without a
prototype argument. Its implementation uses a `streamTypeOf[T]` helper. For
non-pointer payload types, the helper calls `StreamType` on the zero value of
`T`. For pointer payload types, it creates a non-nil zero pointee before
calling `StreamType`, so both pointer- and value-receiver methods can be
used:

```go
func streamTypeOf[T EventPayload]() (StreamType, error) {
	typ := reflect.TypeFor[T]()
	var payload T
	if typ.Kind() == reflect.Pointer {
		payload = reflect.New(typ.Elem()).Interface().(T)
	}
	stream := payload.StreamType()
	if stream == "" {
		return "", ErrEmptyStreamType
	}
	return stream, nil
}
```

`StreamType` methods must be pure identity methods: they must not depend on
payload state and must return a stable value. Interface type parameters are
rejected because they do not identify one concrete Go payload type.

When options are omitted, the generated event receives an internally generated
ID and the current time. Options can override the other event metadata:

```go
	bus := fabrik.NewBus()
	bus.Emit(ctx, order,
		fabrik.WithCorrelationID(requestID),
		fabrik.WithHeaders(map[string]string{"tenant": tenantID}),
	)
```

Options are applied from left to right. Later options replace earlier scalar
values. `WithHeaders` merges keys, with later values winning for duplicate
keys. It copies key/value pairs while applying the option and does not retain
the caller's map. The implementation creates an independent copy of the
final headers map for every subscriber delivery, so a consumer cannot mutate
headers observed by another consumer. Nil headers remain nil; non-nil empty
maps remain non-nil empty maps.

Nil options are ignored. `WithHeaders(nil)` is a no-op; a non-nil empty map
may initialize an empty, non-nil headers map. After all options run, `Emit`
validates that `Event.OccurredAt` is non-zero. The event ID is generated and
controlled internally; options cannot override it.

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

err = b.Emit(context.Background(), OrderCreated{OrderID: "order-123"})
```

Each `Bus` is independent. This supports separate applications, tests, and
lifecycle boundaries in one process. `Shutdown` closes only the bus it belongs
to.

## Semantics for v2.0

These are the recommended defaults. They should become executable tests before
implementation is considered complete.

- Each bus is process-local and safe for concurrent use.
- Each `StreamType` has one implicit stream on a bus.
- Every active subscription to a stream receives every emitted event.
- Each subscription has an independent bounded queue.
- `Emit` does not wait for a slow consumer.
- A full consumer queue causes that delivery to be dropped and is reported by
  the returned error. Other consumers still receive the event.
- Events are delivered in emission order per stream. With concurrent emitters,
  order is the order at the stream's serialization point.
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
- Event payloads are not cloned at the delivery boundary. If an emitted
  payload is a pointer or contains mutable reference data such as maps,
  slices, or pointers, the application is responsible for not mutating it
  after emission or for providing its own synchronization.

## Event metadata

The initial event metadata is deliberately small:

- `ID` identifies one event instance. The framework generates a non-empty,
  cryptographically random ID for every event emitted through `Emit`; the
  exact ID format is an implementation detail and cannot be overridden by
  callers. IDs are collision-resistant in practice rather than mathematically
  guaranteed unique.
- `OccurredAt` is the application event time, not necessarily queue time.
- `CorrelationID` links related work, such as one request and all events
  produced during that workflow.
- `CausationID` identifies the immediate event or command that caused this
  event.
- `Headers` supports lightweight cross-cutting metadata such as tenant or
  trace values.

There is no separate public stream handle in this model. `StreamType` is the
routing key, and each bus keeps one subscriber set for each value. The value is
an opaque, stable identifier such as `orders.created`.

The bus must reject an empty `StreamType`. It must also reject attempts to use
one `StreamType` with incompatible Go payload types. This prevents two
different payload structs from being silently delivered through the same typed
subscription.

Each bus keeps a registry from `StreamType` to the exact `reflect.Type` of
the payload. The first `Subscribe` or `Emit` for a `StreamType` registers its
Go type. Later uses must have the same type; pointer and value forms are
different types, so a bus cannot use both `OrderCreated` and
`*OrderCreated` for the same `StreamType`. A conflict returns
`*StreamTypeConflictError`, does not
deliver an event or create a subscription, and leaves the existing registry
entry unchanged. A registration remains on the bus after all subscriptions
close, and an emit with no subscribers still registers its payload type.

The framework must not add received-at, attempt, or consumer identity fields
until those concepts exist in the delivery contract. Metadata that sounds
useful but has no defined semantics becomes a long-lived lie.

## Concurrency model

The bus owns the stream registry and lifecycle state. Each stream owns its own
subscriber set and serialization lock.

```text
Emit / Subscribe
        |
        v
  bus lifecycle lock  -- short registry operation only
        |
        v
  stream lock          -- registration or non-blocking fan-out
        |
        v
  subscriber queues
```

The global bus lock must not be held while delivering to subscriber queues.
Operations on unrelated streams should not wait on each other except during the
short lifecycle boundary. The stream lock may serialize emitters for one stream,
which is what gives the ordering guarantee.

The implementation should prefer ordinary mutexes and channels over clever
lock-free structures. The race detector is part of the design, not a final
polish step.

### Lifecycle linearization

The bus has three lifecycle states: `open`, `shuttingDown`, and `closed`.
`Emit` and `Subscribe` acquire the bus lifecycle lock briefly to verify that
the bus is `open`. Once admitted, an operation is allowed to finish and
`Shutdown` waits for it before closing subscriptions. Operations that do not
pass this boundary return `ErrBusClosed`.

`Shutdown` transitions the bus to `shuttingDown`, preventing new emits and
subscriptions. After all admitted operations finish, it closes active
subscriptions. Closing a subscription preserves events already queued so
consumers can drain them before the channel closes. A subscription close is
serialized with stream delivery: once close becomes visible, later emissions
do not target that subscription.

A canceled context before an operation is admitted rejects that operation. If
the context is canceled after admission, the operation may still complete. If
the first `Shutdown` call starts successfully and its context later expires,
shutdown continues independently; that caller returns `ctx.Err()`. Concurrent
shutdown callers wait for the same shutdown result unless their own contexts
expire.

`Emit`, `Subscribe`, and `Shutdown` return `ErrNilBus` for a nil bus and
`ErrNilContext` for a nil context. A canceled or expired non-nil context
returns `ctx.Err()`. A nil subscription's `Close` method is a no-op. Canceling
the context passed to `Subscribe` after admission does not close the
subscription or remove events already queued.

## Decisions previously deferred

These decisions are now settled:

1. `Event.ID` and `OccurredAt` are always non-zero on emitted events. The
   framework generates both; callers can override `OccurredAt` but not `ID`.
   Event IDs use `crypto/rand`; a generation failure returns
   `ErrEventIDGeneration`.
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
8. Subscription queue sizes must be positive. Zero and negative buffer sizes
   are rejected; subscriptions otherwise use a default queue size of `128`.
9. Event payloads are delivered without cloning. Payload immutability and
   synchronization across consumers are the application's responsibility.
10. Header maps are copied when `WithHeaders` is applied and copied again for
    each subscriber delivery. Header mutation is isolated per delivered event.
11. Each bus registers the exact Go type associated with a `StreamType` on its
    first use. Conflicting types—including value versus pointer forms—are
    rejected, and registrations persist for the lifetime of the bus.
12. Nil options are ignored, headers are copied defensively, and emitted event
    timestamps are validated after all options are applied. Event IDs are
    generated and controlled internally.
13. Nil buses and contexts return `ErrNilBus` and `ErrNilContext`. A nil
    subscription can be closed safely, and subscription contexts do not own
    subscription lifetime after admission.

The implicit-type design accepts the restriction that one `StreamType` maps to
one stream per bus. Different Go payload types returning the same `StreamType`
are rejected as a conflict.

## Non-goals for v2.0

- Cross-process or network transport.
- Durable storage, replay, or recovery after process failure.
- Exactly-once processing.
- Consumer acknowledgements and retries.
- Wildcard stream matching or stream hierarchies.
- Serialization formats such as JSON, protobuf, or CloudEvents compliance.
- A handler registry inside the bus.

## Implementation phases

### Test matrix

The implementation must include tests for:

- Stream identity and exact Go-type conflicts.
- Pointer and value payload handling.
- Empty stream type and typed-nil payload rejection.
- Option ordering and final metadata validation.
- Header input copying and per-subscriber header isolation.
- Fan-out to every active subscription.
- Independent bounded queues, non-blocking delivery, and `DeliveryError`.
- Per-stream ordering and concurrent emitters.
- Idempotent subscription close and queued-event draining.
- Shutdown races and rejected operations after shutdown begins.

The concurrency tests must pass under the race detector.

### Phase 1: event primitives

- [x] Add `Event[T]`, `Bus`, and the package-level generic subscription function
  at the module root using `package fabrik`.
- [x] Define validation and error identities.
- [x] Test zero values, nil payloads, header isolation, and stream type identity.

### Phase 2: bus and fan-out

- [x] Add the stream registry and per-stream subscriber queues.
- [x] Implement `Emit` and `Subscribe` with the semantics above.
- [x] Test fan-out, stream isolation, ordering, full queues, and concurrent emitters.

### Phase 3: lifecycle

- [x] Add idempotent subscription close and graceful bus shutdown.
- [x] Test queued-event draining, concurrent close, shutdown races, and rejected
  operations after shutdown begins.

### Phase 4: public examples and hardening

- Add package documentation and examples at the module root.
- Run `go test ./...`, `go test -race ./...`, and `go vet ./...`.
- Add stress tests for multi-stream concurrent producers and consumers.
- Only after the contract is stable, decide whether v2 needs a handler/client
  convenience layer.

## First discussion

The v2.0 design is ready to implement. Consumer groups and multiple streams
for one payload type
are explicitly outside v2.0. The sharpest boundary is this:

```text
v2.0 = type-routed fan-out buses with bounded, at-most-once delivery
```

Adding consumer groups or blocking delivery changes the mental model from
simple notification bus to work queue. That should be a deliberate future
versioned choice, not an option hidden in v2.0.
