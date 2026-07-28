# In-process PubSub

## Purpose

Fabrik provides an in-process PubSub for communication between goroutines in a
single Go process. A published message is fanned out to every subscriber of
the message's topic.

This is an event-notification mechanism, not a durable or reliable message
queue. Messages are delivered at most once and are considered successfully
delivered when they are queued for a subscriber.

## Implementation status

Plans 001 through 006 are implemented. The current package supports creating a
bus and typed topics, lazy type-safe topic registration, buffered subscriptions,
idempotent, concurrency-safe unsubscription, non-blocking per-topic fan-out
with ordering, bounded delivery errors for dropped subscriber deliveries, and
graceful bus shutdown with buffered draining.

Integration hardening is implemented in plan 006.

## Project structure

The PubSub implementation lives in its own `pubsub` package. The current
layout is:

```text
fabrik/
├── go.mod
├── README.md
├── LICENSE
├── .gitignore
│
├── docs/
│   └── in-process-pubsub.md
│
└── pubsub/
    ├── bus.go
    ├── topic.go
    ├── subscription.go
    ├── state.go
    ├── errors.go
    │
    ├── bus_test.go
    ├── topic_test.go
    ├── publish_test.go
    ├── subscription_test.go
    ├── shutdown_test.go
    │
    └── integration_test/
        ├── fanout_test.go
        ├── backpressure_test.go
        └── shutdown_test.go
```

File responsibilities:

- `bus.go` — `Bus`, construction, topic registry, lifecycle, and shutdown.
- `topic.go` — generic `Topic[T]`, topic creation, type validation, and
  publishing.
- `subscription.go` — generic `Subscription[T]`, subscribe, and unsubscribe.
- `state.go` — internal topic state, subscriber bookkeeping, and
  synchronization.
- `errors.go` — topic-type, nil-bus, delivery, and shutdown errors.

Tests are grouped by observable behavior rather than implementation details.
The package should expose the public API as `fabrik/pubsub`; implementation
tests may use `package pubsub`, while selected API tests may use
`package pubsub_test`.

The root-level `integration_test` package contains system-level tests that exercise the
library as a whole through its public API. These tests create realistic
publisher and consumer goroutines and cover complete workflows such as
fan-out, backpressure, concurrent unsubscription, and graceful shutdown.
They should use `package integration_test` and should not depend on internal
implementation details.

## Bus

`Bus` is the central, process-local PubSub object. It is safe to share between
multiple goroutines and owns the topic registry, subscriptions, publishing,
and lifecycle state.

Conceptually, it contains global state plus independent state for each topic:

```go
type Bus struct {
	mu     sync.RWMutex
	topics map[string]*topicState
	state  busState
}

type topicState struct {
	mu          sync.Mutex
	eventType   reflect.Type
	subscribers map[uint64]subscriber
}
```

The target bus is responsible for:

- registering and looking up exact topics;
- ensuring a topic name is associated with only one event type;
- adding and removing subscribers;
- fanning out messages to subscribers;
- rejecting publishes after shutdown begins; and
- closing active subscriptions during shutdown.

The bus does not provide persistence, replay, cross-process delivery, or
durable message processing. Its state exists only for the lifetime of the Go
process.

## Target semantics

- Every subscriber receives every message published to its topic.
- Topics use exact matching. There are no wildcards or topic hierarchies.
- Each topic has one event type, represented through Go generics.
- Subscriber channels are bounded to 100 entries.
- `Publish` never waits for a slow subscriber.
- If a subscriber's channel is full, delivery to that subscriber is dropped
  and `Publish` returns an error.
- A publish continues to other subscribers even when one or more deliveries
  are dropped.
- Publishing to a topic with no subscribers succeeds; the message is dropped.
- Messages preserve publication order per topic.
- When multiple goroutines publish concurrently, order is defined by the order
  in which they acquire the topic's serialization point.

## Proposed API

Go does not support generic methods, so the public operations are generic
functions (or may be wrapped by a typed facade):

```go
type Topic[T any] struct {
	name string
	typ  reflect.Type
}

func NewTopic[T any](name string) Topic[T]

type Subscription[T any] struct {
	Events <-chan T
}

func Subscribe[T any](b *Bus, topic Topic[T]) (*Subscription[T], error)
func Publish[T any](b *Bus, topic Topic[T], value T) error
func (b *Bus) Shutdown(ctx context.Context) error
```

`Subscription` also provides an idempotent `Unsubscribe` operation:

```go
func (s *Subscription[T]) Unsubscribe()
```

The bus must validate that a topic name is not reused with a different event
type. Creating two topic values with the same name and the same event type is
valid.

## Delivery errors

Delivery errors describe partial fan-out. For example:

```go
type DeliveryError struct {
	Topic   string
	Dropped int
}
```

If `Dropped` is non-zero, some subscribers did not receive the message because
their bounded channels were full. Subscribers with available capacity still
receive the message.

No-subscriber publishing is not an error.

## Concurrency model

The bus maintains independent state for each exact topic. Each topic has its
own mutex and subscriber set:

```text
Bus
└── topic "orders"
    ├── topic mutex
    ├── subscriber A: chan Order, capacity 100
    ├── subscriber B: chan Order, capacity 100
    └── subscriber C: chan Order, capacity 100
```

Publishing takes the topic lock, iterates over the current subscribers, and
performs non-blocking sends:

```go
select {
case subscriber <- value:
	// queued
default:
	// subscriber is slow; record a dropped delivery
}
```

Unsubscription uses the same topic lock. This prevents a channel from being
closed while a publish is sending to it. Consumers must never close
subscription channels; the bus owns their lifecycle.

The topic lock serializes publishes for that topic and therefore establishes
publication order. Topics do not block one another.

## Unsubscription

Unsubscription is safe to call multiple times and may run concurrently with
publishing. Once unsubscription has completed, the subscriber receives no
future messages. The bus removes the subscriber and closes its channel while
holding the topic lock, so values queued before unsubscription remain readable
before the channel reports closed.

When the last subscriber leaves a topic, the topic remains registered but
future messages are discarded until a new subscriber joins.

## Shutdown

Shutdown is graceful from the bus's perspective:

1. The bus stops accepting new publishes.
2. Existing queued messages remain in subscriber channels.
3. Subscription channels are closed so consumers can drain them and finish.
4. Topic and subscriber resources are released.

`Shutdown` may accept a context and return when bus-side shutdown completes or
the context expires. It does not imply that arbitrary consumer work has
finished; waiting for application handlers to process events requires an
application-level `WaitGroup`, acknowledgement mechanism, or handler-based
subscription API. A canceled context prevents a new shutdown from starting or
stops a caller waiting for another shutdown; once a caller crosses the shutdown
boundary, that teardown completes before it returns. Repeated calls after
completion return the first shutdown result (normally `nil`).

## Testing requirements

The initial implementation should be covered primarily by unit tests, with
stress tests for concurrency. There is no separate service-level test required
because the bus is entirely process-local.

### Core behavior

- Every subscriber receives every message for its topic.
- Multiple subscribers receive the same messages.
- Subscribers for other topics receive nothing.
- Topic matching is exact; prefixes, suffixes, and topic hierarchies do not
  match.
- Publishing with no subscribers succeeds and drops the message.
- Messages are delivered at most once.

### Buffering and slow subscribers

- Every subscription channel has capacity 100.
- Publishing to a channel with available capacity queues the message.
- Publishing to a full channel never blocks.
- Publishing to a full channel returns a delivery error.
- A publish continues to subscribers whose channels have capacity when another
  subscriber's channel is full.
- The first 100 messages can be queued, and the next message is dropped for a
  subscriber that has not consumed anything.

Buffer tests should fill channels deterministically rather than relying on
timing assertions.

### Ordering

- Sequential publishes are received in publication order.
- All subscribers observe the same order for a given topic.
- Concurrent publishes produce one valid per-topic serialization order.
- Different topics have no required global ordering.
- A slow subscriber does not change the ordering observed by other subscribers.

Tests must not assume that concurrent goroutine start order is publication
order. The relevant order is the bus's per-topic publication linearization
order.

### Subscription lifecycle

- A new subscription starts empty.
- `Unsubscribe` is idempotent.
- Concurrent publish and unsubscribe do not panic or race.
- Concurrent calls to `Unsubscribe` are safe.
- After unsubscription completes, future publishes do not reach that
  subscriber.
- Messages already queued before unsubscription follow the documented channel
  closing behavior.
- Removing the last subscriber leaves the topic usable for future
  subscribers.
- A new subscriber does not receive messages published before it subscribed.

### Topic typing

- A typed topic accepts values of its declared type.
- Topic values with the same name and type are compatible.
- Reusing a topic name with a different event type is rejected.
- Different topics may use different event types.

The implementation should define whether type conflicts are reported during
topic registration, subscription, or publishing, and test that behavior
consistently. Errors should be inspectable with `errors.Is` or `errors.As`
rather than only by comparing error strings.

### Shutdown

- Shutdown prevents new publishes.
- Publishing after shutdown returns a defined shutdown error.
- Messages already queued remain readable.
- Subscription channels are closed according to the shutdown contract.
- Consumers can drain buffered messages with `range`.
- Shutdown is safe with no topics or subscribers.
- Shutdown is idempotent.
- Concurrent publish, subscribe, unsubscribe, and shutdown do not panic or
  race.
- Shutdown does not leave blocked goroutines or leak bus-owned resources.

Graceful bus shutdown does not guarantee that application handlers have
finished processing messages. That requires an application-level `WaitGroup`,
acknowledgement mechanism, or handler-based subscription API.

### Concurrency and race testing

Run the full test suite with the Go race detector:

```text
go test -race ./...
```

Stress tests should repeatedly publish from many goroutines, subscribe and
unsubscribe concurrently, fill and drain subscriber buffers, use multiple
topics, and shut down while publishers are active. They should assert absence
of panics, data races, deadlocks, and goroutine leaks without depending on
scheduler timing.

Suggested test files are:

```text
bus_test.go
topic_test.go
publish_test.go
subscription_test.go
shutdown_test.go
concurrency_test.go
```

The implementation is considered tested when the documented behavior is
covered, the race-detector suite passes, and shutdown tests demonstrate that
buffered messages drain without leaving blocked goroutines.

## Deliberate non-goals

The initial design does not provide:

- durable storage or replay;
- cross-process or network delivery;
- retries or acknowledgements;
- wildcard topic matching;
- guaranteed delivery to slow subscribers;
- global ordering across different topics.
