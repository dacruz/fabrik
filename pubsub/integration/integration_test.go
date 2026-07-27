package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dacruz/fabrik/pubsub"
)

func TestWorkflowFanOutAndTopicIsolation(t *testing.T) {
	b := pubsub.NewBus()
	orders := pubsub.NewTopic[int]("orders")
	archive := pubsub.NewTopic[int]("orders/archive")

	first, err := pubsub.Subscribe(b, orders)
	if err != nil {
		t.Fatal(err)
	}
	second, err := pubsub.Subscribe(b, orders)
	if err != nil {
		t.Fatal(err)
	}
	archiveSub, err := pubsub.Subscribe(b, archive)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Unsubscribe()
	defer second.Unsubscribe()
	defer archiveSub.Unsubscribe()

	for _, value := range []int{1, 2, 3} {
		if err := pubsub.Publish(b, orders, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := pubsub.Publish(b, archive, 99); err != nil {
		t.Fatal(err)
	}

	assertValues(t, first.Events, []int{1, 2, 3})
	assertValues(t, second.Events, []int{1, 2, 3})
	assertValues(t, archiveSub.Events, []int{99})
}

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

func TestWorkflowShutdownClosesActivePublishersAndDrainsConsumers(t *testing.T) {
	b := pubsub.NewBus()
	drainTopic := pubsub.NewTopic[int]("drain")
	activeTopic := pubsub.NewTopic[int]("active")
	sub, err := pubsub.Subscribe(b, drainTopic)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []int{10, 20, 30, 40} {
		if err := pubsub.Publish(b, drainTopic, value); err != nil {
			t.Fatal(err)
		}
	}

	consumerStarted := make(chan struct{})
	consumerRelease := make(chan struct{})
	consumed := make(chan []int, 1)
	go func() {
		close(consumerStarted)
		<-consumerRelease
		values := make([]int, 0, 4)
		for value := range sub.Events {
			values = append(values, value)
		}
		consumed <- values
	}()
	<-consumerStarted

	start := make(chan struct{})
	firstPublish := make(chan struct{}, 1)
	stop := make(chan struct{})
	var publishers sync.WaitGroup
	for publisher := 0; publisher < 4; publisher++ {
		publishers.Add(1)
		go func(id int) {
			defer publishers.Done()
			<-start
			for value := 0; value < 1000; value++ {
				if err := pubsub.Publish(b, activeTopic, id*1000+value); err != nil && !errors.Is(err, pubsub.ErrBusClosed) {
					t.Errorf("publish during shutdown: %v", err)
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

	if err := pubsub.Shutdown(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	close(stop)
	close(consumerRelease)
	publishers.Wait()

	select {
	case values := <-consumed:
		assertEqualValues(t, values, []int{10, 20, 30, 40})
	case <-time.After(time.Second):
		t.Fatal("consumer did not finish after shutdown")
	}
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
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("consumer did not stop after unsubscribe")
				}
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
	if err := pubsub.Shutdown(context.Background(), b); err != nil {
		t.Fatal(err)
	}
}

func assertValues[T comparable](t *testing.T, events <-chan T, want []T) {
	t.Helper()
	got := make([]T, 0, len(want))
	for range want {
		select {
		case value := <-events:
			got = append(got, value)
		default:
			t.Fatalf("received %d values, want %d", len(got), len(want))
		}
	}
	assertEqualValues(t, got, want)
}

func assertEqualValues[T comparable](t *testing.T, got, want []T) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
