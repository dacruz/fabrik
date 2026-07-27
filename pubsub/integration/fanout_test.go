package integration_test

import (
	"testing"

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
