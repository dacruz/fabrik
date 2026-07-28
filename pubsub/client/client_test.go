package client

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/dacruz/fabrik/pubsub"
)

func TestProducerPublishesToBoundTopicAndPreservesErrors(t *testing.T) {
	b := pubsub.NewBus()
	consumer, err := NewConsumerClient[int](b, "orders")
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	producer := NewProducerClient[int](b, "orders")
	if err := producer.Publish(7); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := make(chan int, 1)
	go func() { _ = consumer.Run(ctx, func(_ context.Context, value int) error { got <- value; return nil }) }()
	select {
	case value := <-got:
		if value != 7 {
			t.Fatalf("got %d, want 7", value)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	if err := pubsub.Shutdown(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(8); !errors.Is(err, pubsub.ErrBusClosed) {
		t.Fatalf("got %v", err)
	}
	if err := NewProducerClient[int](nil, "orders").Publish(1); !errors.Is(err, pubsub.ErrNilBus) {
		t.Fatalf("got %v", err)
	}
}

func TestConsumerReceivesOnlyBoundTopicInOrder(t *testing.T) {
	b := pubsub.NewBus()
	consumer, err := NewConsumerClient[int](b, "orders")
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	orders := NewProducerClient[int](b, "orders")
	other := NewProducerClient[int](b, "archive")
	for _, value := range []int{1, 2, 3} {
		if err := orders.Publish(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := other.Publish(99); err != nil {
		t.Fatal(err)
	}
	got := make(chan []int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		values := []int{}
		err := consumer.Run(ctx, func(_ context.Context, value int) error {
			values = append(values, value)
			if len(values) == 3 {
				cancel()
			}
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
		got <- values
	}()
	select {
	case values := <-got:
		if !reflect.DeepEqual(values, []int{1, 2, 3}) {
			t.Fatalf("got %v", values)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not finish")
	}
}

func TestConsumerLifecycle(t *testing.T) {
	t.Run("handler error", func(t *testing.T) {
		b := pubsub.NewBus()
		c, err := NewConsumerClient[int](b, "events")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err := NewProducerClient[int](b, "events").Publish(1); err != nil {
			t.Fatal(err)
		}
		want := errors.New("stop")
		if got := c.Run(context.Background(), func(context.Context, int) error { return want }); !errors.Is(got, want) {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		b := pubsub.NewBus()
		c, err := NewConsumerClient[int](b, "events")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got := c.Run(ctx, func(context.Context, int) error { t.Fatal("handler called"); return nil }); !errors.Is(got, context.Canceled) {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("shutdown drains buffered values", func(t *testing.T) {
		b := pubsub.NewBus()
		c, err := NewConsumerClient[int](b, "events")
		if err != nil {
			t.Fatal(err)
		}
		p := NewProducerClient[int](b, "events")
		for _, value := range []int{4, 5, 6} {
			if err := p.Publish(value); err != nil {
				t.Fatal(err)
			}
		}
		if err := pubsub.Shutdown(context.Background(), b); err != nil {
			t.Fatal(err)
		}
		got := []int{}
		if err := c.Run(context.Background(), func(_ context.Context, value int) error { got = append(got, value); return nil }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, []int{4, 5, 6}) {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("close is idempotent", func(t *testing.T) {
		b := pubsub.NewBus()
		c, err := NewConsumerClient[int](b, "events")
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
		c.Close()
		if err := c.Run(context.Background(), func(context.Context, int) error { return nil }); err != nil {
			t.Fatalf("got %v", err)
		}
	})
	if err := (&consumer[int]{}).Run(context.Background(), func(context.Context, int) error { return nil }); err == nil {
		t.Fatal("nil subscription unexpectedly succeeded")
	}
	if _, err := NewConsumerClient[string](nil, "events"); !errors.Is(err, pubsub.ErrNilBus) {
		t.Fatalf("got %v", err)
	}
	b := pubsub.NewBus()
	first, err := NewConsumerClient[int](b, "same")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := NewConsumerClient[string](b, "same"); !errors.Is(err, pubsub.ErrTopicTypeConflict) {
		t.Fatalf("got %v", err)
	}
}
