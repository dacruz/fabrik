# Fabrik

Fabrik is a small, process-local, typed publish/subscribe bus for Go.
Payloads define their logical stream identity, and every active subscription
is offered each emitted event. Delivery is best-effort: an event is dropped
for an individual subscription when that subscription's bounded queue is full.

## Install

Fabrik requires Go 1.26 or newer.

```sh
go get github.com/dacruz/fabrik/v2
```

## Quick start

```go
package main

import (
	"context"
	"fmt"

	"github.com/dacruz/fabrik/v2"
)

type OrderCreated struct {
	OrderID string
}

func (OrderCreated) StreamType() fabrik.StreamType {
	return "orders.created"
}

func main() {
	ctx := context.Background()
	bus := fabrik.NewBus()

	subscription, err := fabrik.Subscribe[OrderCreated](ctx, bus,
		fabrik.WithSubscriptionName("order-projector"),
	)
	if err != nil {
		panic(err)
	}
	defer subscription.Close()

	if err := bus.Emit(ctx, OrderCreated{OrderID: "order-123"},
		fabrik.WithCorrelationID("request-456"),
		fabrik.WithHeaders(map[string]string{"tenant": "acme"}),
	); err != nil {
		panic(err)
	}

	event := <-subscription.Events
	fmt.Println(event.Payload.OrderID, event.CorrelationID)
}
```

## How it works

- A payload implements `StreamType() fabrik.StreamType`.
- `StreamType` must be a stable, zero-value-safe identity method. It must not
  depend on payload fields because subscriptions resolve it from the payload
  type's zero value.
- A bus has one stream per `StreamType` value.
- Every subscription to a stream is offered each event independently.
- Subscription queues are bounded and default to 128 events.
- Delivery does not wait for subscriber consumption. Emitters for the same
  stream are serialized; a full queue drops only that subscriber's delivery
  and returns a `*fabrik.DeliveryError`.
- A delivery error is a partial-success result: other subscribers may already
  have received the event. Do not retry it as though emission were atomic.
- Every subscription has a bus-local unique ID, an optional name, and
  concurrency-safe delivered/dropped counters available through `Stats()`.
  Delivery errors identify subscriptions whose queues were full.
- Delivery is at-most-once and events are delivered in emission order per
  stream.
- Payloads are not cloned. Callers are responsible for synchronizing mutable
  payload data after emission.
- Header options snapshot their input maps when created. Event headers are also
  copied for every subscriber, so producers and consumers cannot mutate each
  other's header maps.

## Lifecycle

Call `Subscription.Close()` when a consumer no longer needs events. Closing is
idempotent, stops future delivery, and closes the event channel. Events that
were already buffered remain readable before the channel reports that it is
closed.

Call `Bus.Shutdown(ctx)` to stop the bus gracefully. New emits and
subscriptions are rejected once shutdown begins. Active subscriptions are
closed after admitted operations finish. Shutdown does not wait for application
consumers to process buffered events; consumers can continue reading those
events from the closed buffered channels, and applications should wait for
their consumer goroutines separately when processing completion is required.

The contexts passed to `Emit` and `Subscribe` are admission contexts. A context
that is already canceled rejects the operation. Once the operation has been
admitted, later cancellation does not interrupt stream registration or event
delivery and does not own the lifetime of a subscription. `Shutdown` is
different: each caller's context controls how long that caller waits, while a
shutdown that has already started continues in the background.

## Errors

Stable error categories are exposed through `errors.Is`, including:

- `fabrik.ErrBusClosed`
- `fabrik.ErrDelivery`
- `fabrik.ErrEmptyStreamType`
- `fabrik.ErrEventIDGeneration`
- `fabrik.ErrInvalidOccurredAt`
- `fabrik.ErrInvalidBufferSize`
- `fabrik.ErrNilBus`
- `fabrik.ErrNilContext`
- `fabrik.ErrNilPayload`
- `fabrik.ErrPayloadTypeConflict`
- `fabrik.ErrStreamTypeConflict`
- `fabrik.ErrUnsupportedPayloadType`

Use `errors.As` for structured details such as dropped delivery counts and
conflicting payload types. A `DeliveryError` includes the event ID and the
attempted, delivered, and dropped subscriber counts, plus the IDs and optional
names of subscriptions whose queues were full.

## Development

```sh
make test
make test-race
make test-cover
make vet
make benchmark
```

Fabrik is intentionally process-local. Durable storage, replay, retries,
consumer groups, wildcard streams, and network transport are outside the
current scope.

## Releasing

Merging a pull request creates a patch release by default. Apply exactly one of
the `release:patch`, `release:minor`, `release:major`, or `release:none` labels
to override that behavior. Major releases must also update the Go module path
before merge.
