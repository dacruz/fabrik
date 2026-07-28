# fabrik

[![CI](https://github.com/dacruz/fabrik/actions/workflows/ci.yml/badge.svg)](https://github.com/dacruz/fabrik/actions/workflows/ci.yml)
[![Build](https://img.shields.io/github/actions/workflow/status/dacruz/fabrik/ci.yml?branch=main&label=build)](https://github.com/dacruz/fabrik/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/github/actions/workflow/status/dacruz/fabrik/ci.yml?branch=main&label=tests)](https://github.com/dacruz/fabrik/actions/workflows/ci.yml)
[![Race detector](https://img.shields.io/github/actions/workflow/status/dacruz/fabrik/ci.yml?branch=main&label=race%20detector)](https://github.com/dacruz/fabrik/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/github/actions/workflow/status/dacruz/fabrik/ci.yml?branch=main&label=coverage)](https://github.com/dacruz/fabrik/actions/workflows/ci.yml)
[![Vet](https://img.shields.io/github/actions/workflow/status/dacruz/fabrik/ci.yml?branch=main&label=vet)](https://github.com/dacruz/fabrik/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/actions/workflow/status/dacruz/fabrik/release.yml?label=release)](https://github.com/dacruz/fabrik/actions/workflows/release.yml)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

Fabrik is a lightweight, typed, process-local Pub/Sub bus for decoupled
communication between producers and consumers in long-lived Go processes.

## Features

- Generic, type-safe topics with exact, opaque names.
- Lazy topic registration with one event type per name on each bus.
- Fan-out delivery to every current subscriber.
- Non-blocking publishes with independent buffered queues per subscriber.
- Bounded backpressure reporting through inspectable `DeliveryError` values.
- Idempotent, concurrency-safe unsubscription.
- Graceful, idempotent shutdown that preserves queued events for consumers to drain.
- Role-specific producer and consumer clients that hide topic wiring.

Fabrik is process-local: it does not provide a broker, persistence, replay,
network transport, or cross-process delivery. It delivers values through
channels; applications define their own handlers, acknowledgements, retries,
and higher-level ordering semantics.

## Requirements

- Go 1.26 or newer

## Installation

```sh
go get github.com/dacruz/fabrik
```

## Usage

For application code, the role-specific clients keep each event type and exact
topic name together. Producer and consumer clients share a `*pubsub.Bus`; the
application that owns the bus remains responsible for calling
`pubsub.Shutdown`.

```go
import (
	"context"
	"log"

	"github.com/dacruz/fabrik/pubsub"
	"github.com/dacruz/fabrik/client"
)

type OrderCreated struct{ ID string }
type OrderArchived struct{ ID string }

b := pubsub.NewBus()
producer := client.NewProducerClient[OrderCreated](b, "orders.created")
consumer, err := client.NewConsumerClient[OrderArchived](b, "orders.archived")
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

go func() {
	_ = consumer.Run(context.Background(), func(_ context.Context, event OrderArchived) error {
		log.Println(event.ID)
		return nil
	})
}()

if err := producer.Publish(OrderCreated{ID: "order-123"}); err != nil {
	// Handle a closed bus, a topic type conflict, or a DeliveryError.
	log.Println(err)
}
// The application owning b calls pubsub.Shutdown when the process stops.
```

Clients are intentionally capability-oriented: a producer only publishes and
a consumer only runs a handler for its bound topic. Use the lower-level API
below when direct topic and subscription management is needed.

The public API is in the `pubsub` package. Construct a bus and a typed topic.
Creating a topic does not register it; registration is lazy and happens on the
first `Subscribe` or `Publish`.

```go
import (
	"context"
	"errors"
	"log"

	"github.com/dacruz/fabrik/pubsub"
)

type OrderCreated struct {
	ID string
}

b := pubsub.NewBus()
orders := pubsub.NewTopic[OrderCreated]("orders.created")
```

Topic names are opaque and matched exactly. On one bus, a topic name can be
used with only one Go event type. A different type for the same name returns a
`TopicTypeConflictError`, which can be inspected with `errors.Is` and
`errors.As`.

Subscribe to receive a buffered channel. Each subscription has its own queue,
and every current subscriber receives each publish independently.

```go
sub, err := pubsub.Subscribe(b, orders)
if err != nil {
	// Handle a topic type conflict or a closed bus.
	return err
}

go func() {
	for order := range sub.Events {
		process(order)
	}
}()
```

Publish is non-blocking and returns after attempting delivery to all current
subscribers.

```go
if err := pubsub.Publish(b, orders, OrderCreated{ID: "o-123"}); err != nil {
	var dropped *pubsub.DeliveryError
	if errors.As(err, &dropped) {
		log.Printf("%d delivery dropped for %q", dropped.Dropped, dropped.Topic)
	}
}
```

Subscription channels have a capacity of 100 values. If a subscriber is full,
that delivery is dropped, publishing continues for subscribers with capacity,
and the publish returns a `DeliveryError`. The error is inspectable with
`errors.Is(err, pubsub.ErrDelivery)` or `errors.As` to read `Topic` and
`Dropped`. Publishing to a topic with no subscribers succeeds and drops the
value.

Unsubscribe is safe to call repeatedly or concurrently. It removes the
subscriber and closes its channel; already queued values can still be read
before the channel reports closed.

```go
sub.Unsubscribe()
```

Shutdown gracefully stops a bus. It rejects new subscriptions and publishes,
preserves and drains values already queued in active subscriptions, then closes
their channels. It is safe and idempotent to call concurrently. A context can
cancel a caller waiting for shutdown to begin or for another caller's shutdown
to finish; it does not cancel teardown already in progress.

```go
if err := pubsub.Shutdown(context.Background(), b); err != nil {
	return err
}
```

Fabrik is deliberately process-local: it does not deliver between processes or
provide a broker, persistence, or network transport. It only delivers values
through channels; it does not execute handlers or define acknowledgement,
retry, or application-level ordering semantics.

## Development

The Makefile provides the following development commands:

```sh
make deps        # Download Go dependencies
make build       # Build all packages
make test        # Run tests
make test-race   # Run tests with the race detector
make test-cover  # Run tests with coverage
make vet         # Run go vet
```

`make ci` runs the full CI sequence: dependency download, build, tests,
race-detector tests, coverage tests, and vet. `make verify` runs the release
verification sequence: dependency download, build, tests, and vet.

## License

Fabrik is available under the [BSD-3-Clause license](LICENSE).
