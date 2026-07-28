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
	orders := pubsub.NewTopic[int]("orders")
	consumer, err := NewConsumerClient(b, orders)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	producer := NewProducerClient(b, orders)
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

	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(8); !errors.Is(err, pubsub.ErrBusClosed) {
		t.Fatalf("got %v", err)
	}
	if err := NewProducerClient(nil, orders).Publish(1); !errors.Is(err, pubsub.ErrNilBus) {
		t.Fatalf("got %v", err)
	}
}

func TestClientsBindToSuppliedTypedTopic(t *testing.T) {
	b := pubsub.NewBus()
	topic := pubsub.NewTopic[int]("supplied")
	consumer, err := NewConsumerClient(b, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	producer := NewProducerClient(b, topic)
	if err := producer.Publish(42); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := consumer.Run(ctx, func(_ context.Context, value int) error {
		if value != 42 {
			t.Errorf("got %d, want 42", value)
		}
		consumer.Close()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestConsumerReceivesOnlyBoundTopicInOrder(t *testing.T) {
	b := pubsub.NewBus()
	orders := pubsub.NewTopic[int]("orders")
	archive := pubsub.NewTopic[int]("archive")
	consumer, err := NewConsumerClient(b, orders)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	ordersProducer := NewProducerClient(b, orders)
	other := NewProducerClient(b, archive)
	for _, value := range []int{1, 2, 3} {
		if err := ordersProducer.Publish(value); err != nil {
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
		events := pubsub.NewTopic[int]("events")
		c, err := NewConsumerClient(b, events)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err := NewProducerClient(b, events).Publish(1); err != nil {
			t.Fatal(err)
		}
		want := errors.New("stop")
		if got := c.Run(context.Background(), func(context.Context, int) error { return want }); !errors.Is(got, want) {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		b := pubsub.NewBus()
		c, err := NewConsumerClient(b, pubsub.NewTopic[int]("events"))
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
		events := pubsub.NewTopic[int]("events")
		c, err := NewConsumerClient(b, events)
		if err != nil {
			t.Fatal(err)
		}
		p := NewProducerClient(b, events)
		for _, value := range []int{4, 5, 6} {
			if err := p.Publish(value); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Shutdown(context.Background()); err != nil {
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
		c, err := NewConsumerClient(b, pubsub.NewTopic[int]("events"))
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
	if _, err := NewConsumerClient(nil, pubsub.NewTopic[string]("events")); !errors.Is(err, pubsub.ErrNilBus) {
		t.Fatalf("got %v", err)
	}
	b := pubsub.NewBus()
	first, err := NewConsumerClient(b, pubsub.NewTopic[int]("same"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := NewConsumerClient(b, pubsub.NewTopic[string]("same")); !errors.Is(err, pubsub.ErrTopicTypeConflict) {
		t.Fatalf("got %v", err)
	}
}

func TestConsumerRequiresContext(t *testing.T) {
	b := pubsub.NewBus()
	c, err := NewConsumerClient(b, pubsub.NewTopic[int]("events"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.Run(nil, func(context.Context, int) error { return nil })
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("got %v", err)
	}
}
