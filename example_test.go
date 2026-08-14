package fabrik_test

import (
	"context"
	"fmt"

	"github.com/dacruz/fabrik"
)

type orderCreated struct {
	OrderID string
}

func (orderCreated) StreamType() fabrik.StreamType { return "orders.created" }

func Example() {
	ctx := context.Background()
	bus := fabrik.NewBus()

	subscription, err := fabrik.Subscribe[orderCreated](ctx, bus)
	if err != nil {
		panic(err)
	}
	defer subscription.Close()

	if err := bus.Emit(ctx, orderCreated{OrderID: "order-123"}); err != nil {
		panic(err)
	}

	event := <-subscription.Events
	fmt.Println(event.Payload.OrderID)
	// Output: order-123
}

func ExampleBus_Emit() {
	ctx := context.Background()
	bus := fabrik.NewBus()
	subscription, err := fabrik.Subscribe[orderCreated](ctx, bus, fabrik.WithBufferSize(1))
	if err != nil {
		panic(err)
	}
	defer subscription.Close()

	if err := bus.Emit(ctx, orderCreated{OrderID: "order-456"},
		fabrik.WithCorrelationID("request-789"),
		fabrik.WithHeaders(map[string]string{"tenant": "acme"}),
	); err != nil {
		panic(err)
	}

	event := <-subscription.Events
	fmt.Println(event.Payload.OrderID, event.CorrelationID, event.Headers["tenant"])
	// Output: order-456 request-789 acme
}
