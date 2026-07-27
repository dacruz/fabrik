package integration_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/dacruz/fabrik/pubsub"
)

func TestWorkflowBackpressureDoesNotStarveHealthyConsumer(t *testing.T) {
	b := pubsub.NewBus()
	topic := pubsub.NewTopic[int]("events")
	slow, err := pubsub.Subscribe(b, topic)
	if err != nil {
		t.Fatal(err)
	}
	for value := 0; value < cap(slow.Events); value++ {
		if err := pubsub.Publish(b, topic, value); err != nil {
			t.Fatal(err)
		}
	}
	healthy, err := pubsub.Subscribe(b, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Unsubscribe()
	defer healthy.Unsubscribe()

	err = pubsub.Publish(b, topic, cap(slow.Events))
	var delivery *pubsub.DeliveryError
	if !errors.As(err, &delivery) || delivery.Dropped != 1 {
		t.Fatalf("expected one deterministic drop, got %v", err)
	}
	assertValues(t, healthy.Events, []int{cap(slow.Events)})
}

func TestWorkflowConcurrentUnsubscribeDuringPublishing(t *testing.T) {
	b := pubsub.NewBus()
	topic := pubsub.NewTopic[int]("live")
	sub, err := pubsub.Subscribe(b, topic)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	firstPublish := make(chan struct{}, 1)
	stop := make(chan struct{})
	var publishers sync.WaitGroup
	for publisher := 0; publisher < 8; publisher++ {
		publishers.Add(1)
		go func(id int) {
			defer publishers.Done()
			<-start
			for value := 0; value < 1000; value++ {
				if err := pubsub.Publish(b, topic, id*1000+value); err != nil && !errors.Is(err, pubsub.ErrDelivery) {
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
		}(publisher)
	}
	close(start)
	<-firstPublish
	sub.Unsubscribe()
	close(stop)
	publishers.Wait()
	for range sub.Events {
	}
}

func TestWorkflowRepeatedSubscribeUnsubscribeAcrossTopics(t *testing.T) {
	b := pubsub.NewBus()
	topics := []pubsub.Topic[int]{pubsub.NewTopic[int]("topic-0"), pubsub.NewTopic[int]("topic-1"), pubsub.NewTopic[int]("topic-2"), pubsub.NewTopic[int]("topic-3")}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 12; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for iteration := 0; iteration < 50; iteration++ {
				topic := topics[(worker+iteration)%len(topics)]
				sub, err := pubsub.Subscribe(b, topic)
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
					for value := range sub.Events {
						if value == worker*1000+iteration {
							received <- value
						}
					}
				}()
				<-ready
				value := worker*1000 + iteration
				if err := pubsub.Publish(b, topic, value); err != nil && !errors.Is(err, pubsub.ErrDelivery) {
					t.Errorf("publish: %v", err)
				}
				sub.Unsubscribe()
				<-done
				select {
				case got := <-received:
					if got != value {
						t.Errorf("got %d, want %d", got, value)
					}
				default:
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	if err := pubsub.Shutdown(nil, b); err != nil {
		t.Fatal(err)
	}
}
