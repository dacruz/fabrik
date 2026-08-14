# Fabrik

Fabrik is a small, process-local, typed publish/subscribe bus for Go.
Payloads define their logical stream identity, and every active subscription
to that stream receives each emitted event.

## Install

```sh
go get github.com/dacruz/fabrik
```

## Quick start

```go
package main

import (
	"context"
	"fmt"

	"github.com/dacruz/fabrik"
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

	subscription, err := fabrik.Subscribe[OrderCreated](ctx, bus)
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
- A bus has one stream per `StreamType` value.
- Every subscription to a stream receives each event independently.
- Subscription queues are bounded and default to 128 events.
- Emission is non-blocking. A full queue drops only that subscriber's delivery
  and returns a `*fabrik.DeliveryError`.
- Delivery is at-most-once and events are delivered in emission order per
  stream.
- Payloads are not cloned. Callers are responsible for synchronizing mutable
  payload data after emission.
- Event headers are copied for every subscriber, so consumers cannot mutate
  each other's header maps.

## Lifecycle

Call `Subscription.Close()` when a consumer no longer needs events. Closing is
idempotent and drains events that were already queued.

Call `Bus.Shutdown(ctx)` to stop the bus gracefully. New emits and
subscriptions are rejected once shutdown begins. Active subscriptions are
closed after admitted operations finish and queued events have been drained.

## Errors

Stable error categories are exposed through `errors.Is`, including:

- `fabrik.ErrBusClosed`
- `fabrik.ErrDelivery`
- `fabrik.ErrEmptyStreamType`
- `fabrik.ErrInvalidOccurredAt`
- `fabrik.ErrInvalidBufferSize`
- `fabrik.ErrNilBus`
- `fabrik.ErrNilContext`
- `fabrik.ErrNilPayload`
- `fabrik.ErrStreamTypeConflict`

Use `errors.As` for structured details such as dropped delivery counts and
conflicting payload types.

## Development

```sh
make test
make test-race
make test-cover
make vet
```

Fabrik is intentionally process-local. Durable storage, replay, retries,
consumer groups, wildcard streams, and network transport are outside the
current scope.
