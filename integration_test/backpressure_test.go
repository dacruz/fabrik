package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/dacruz/fabrik/client"
	"github.com/dacruz/fabrik/pubsub"
)

func TestWorkflowBackpressureDoesNotStarveHealthyConsumer(t *testing.T) {
	b := pubsub.NewBus()
	events := pubsub.NewTopic[int]("events")
	slow, err := client.NewConsumerClient(b, events)
	if err != nil {
		t.Fatal(err)
	}
	producer := client.NewProducerClient(b, events)
	for value := range 100 {
		if err := producer.Publish(value); err != nil {
			t.Fatal(err)
		}
	}
	healthy, err := client.NewConsumerClient(b, events)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	defer healthy.Close()

	err = producer.Publish(100)
	var delivery *pubsub.DeliveryError
	if !errors.As(err, &delivery) || delivery.Dropped != 1 {
		t.Fatalf("expected one deterministic drop, got %v", err)
	}
	assertClientValues(t, healthy, []int{100})
}

func TestWorkflowConcurrentUnsubscribeDuringPublishing(t *testing.T) {
	b := pubsub.NewBus()
	live := pubsub.NewTopic[int]("live")
	consumer, err := client.NewConsumerClient(b, live)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	firstPublish := make(chan struct{}, 1)
	stop := make(chan struct{})
	var publishers sync.WaitGroup
	for publisher := range 8 {
		publishers.Go(func() {
			<-start
			for value := range 1000 {
				if err := client.NewProducerClient(b, live).Publish(publisher*1000 + value); err != nil && !errors.Is(err, pubsub.ErrDelivery) {
					t.Errorf("publish: %v", err)
				}
				select {
				case firstPublish <- struct{}{}:
				default:
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		})
	}
	close(start)
	<-firstPublish
	consumer.Close()
	close(stop)
	publishers.Wait()
}

func TestWorkflowRepeatedSubscribeUnsubscribeAcrossTopics(t *testing.T) {
	b := pubsub.NewBus()
	topics := []pubsub.Topic[int]{
		pubsub.NewTopic[int]("topic-0"),
		pubsub.NewTopic[int]("topic-1"),
		pubsub.NewTopic[int]("topic-2"),
		pubsub.NewTopic[int]("topic-3"),
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := range 12 {
		workers.Go(func() {
			<-start
			for iteration := range 50 {
				topic := topics[(worker+iteration)%len(topics)]
				consumer, err := client.NewConsumerClient(b, topic)
				if err != nil {
					t.Errorf("subscribe: %v", err)
					continue
				}
				ready := make(chan struct{})
				received := make(chan int, 1)
				done := make(chan struct{})
				go func() {
					defer close(done)
					close(ready)
					_ = consumer.Run(context.Background(), func(_ context.Context, value int) error {
						if value == worker*1000+iteration {
							received <- value
						}
						return nil
					})
				}()
				<-ready
				value := worker*1000 + iteration
				if err := client.NewProducerClient(b, topic).Publish(value); err != nil && !errors.Is(err, pubsub.ErrDelivery) {
					t.Errorf("publish: %v", err)
				}
				consumer.Close()
				<-done
				select {
				case got := <-received:
					if got != value {
						t.Errorf("got %d, want %d", got, value)
					}
				default:
				}
			}
		})
	}
	close(start)
	workers.Wait()
	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
