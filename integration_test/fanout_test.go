package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/dacruz/fabrik/client"
	"github.com/dacruz/fabrik/pubsub"
)

func TestWorkflowFanOutAndTopicIsolation(t *testing.T) {
	b := pubsub.NewBus()
	orders := pubsub.NewTopic[int]("orders")
	archive := pubsub.NewTopic[int]("orders/archive")
	first, err := client.NewConsumerClient(b, orders)
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.NewConsumerClient(b, orders)
	if err != nil {
		t.Fatal(err)
	}
	archiveSub, err := client.NewConsumerClient(b, archive)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()
	defer archiveSub.Close()
	ordersProducer := client.NewProducerClient(b, orders)
	archiveProducer := client.NewProducerClient(b, archive)

	for _, value := range []int{1, 2, 3} {
		if err := ordersProducer.Publish(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := archiveProducer.Publish(99); err != nil {
		t.Fatal(err)
	}

	assertClientValues(t, first, []int{1, 2, 3})
	assertClientValues(t, second, []int{1, 2, 3})
	assertClientValues(t, archiveSub, []int{99})
}

func assertClientValues[T comparable](t *testing.T, consumer client.ConsumerClient[T], want []T) {
	t.Helper()
	got := make([]T, 0, len(want))
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(context.Background(), func(_ context.Context, value T) error {
			got = append(got, value)
			if len(got) == len(want) {
				consumer.Close()
			}
			return nil
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatalf("received %d values, want %d", len(got), len(want))
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
