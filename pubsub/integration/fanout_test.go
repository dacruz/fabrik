package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/dacruz/fabrik/pubsub"
	"github.com/dacruz/fabrik/pubsub/client"
)

func TestWorkflowFanOutAndTopicIsolation(t *testing.T) {
	b := pubsub.NewBus()
	first, err := client.NewConsumerClient[int](b, "orders")
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.NewConsumerClient[int](b, "orders")
	if err != nil {
		t.Fatal(err)
	}
	archiveSub, err := client.NewConsumerClient[int](b, "orders/archive")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()
	defer archiveSub.Close()
	orders := client.NewProducerClient[int](b, "orders")
	archive := client.NewProducerClient[int](b, "orders/archive")

	for _, value := range []int{1, 2, 3} {
		if err := orders.Publish(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Publish(99); err != nil {
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
