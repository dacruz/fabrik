package client_test

import (
	"context"
	"log"

	"github.com/dacruz/fabrik/client"
	"github.com/dacruz/fabrik/pubsub"
)

func Example() {
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
	_ = producer.Publish(OrderCreated{ID: "order-123"})
	// The application owning b calls pubsub.Shutdown when the process stops.
}
