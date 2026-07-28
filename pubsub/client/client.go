// Package client provides role-specific clients for a pubsub.Bus.
//
// A producer or consumer client is bound to one exact topic name and event
// type. Clients share the bus they are constructed with; the application that
// owns that bus remains responsible for calling pubsub.Shutdown.
package client

import (
	"context"
	"errors"

	"github.com/dacruz/fabrik/pubsub"
)

// ProducerClient publishes values to one typed topic.
type ProducerClient[T any] interface {
	Publish(T) error
}

// ConsumerClient receives values from one typed topic.
type ConsumerClient[T any] interface {
	Run(context.Context, Handler[T]) error
	Close()
}

// Handler processes one value delivered to a consumer.
type Handler[T any] func(context.Context, T) error

// ErrNilHandler is returned when Run is given no handler.
var ErrNilHandler = errors.New("pubsub/client: nil handler")

type producer[T any] struct {
	bus   *pubsub.Bus
	topic pubsub.Topic[T]
}

// NewProducerClient binds a producer to topicName on b.
func NewProducerClient[T any](b *pubsub.Bus, topicName string) ProducerClient[T] {
	return &producer[T]{bus: b, topic: pubsub.NewTopic[T](topicName)}
}

func (p *producer[T]) Publish(value T) error {
	return pubsub.Publish(p.bus, p.topic, value)
}

type consumer[T any] struct {
	sub *pubsub.Subscription[T]
}

// NewConsumerClient subscribes to topicName on b and owns the resulting
// subscription. It returns underlying pubsub errors, including nil-bus and
// topic-type-conflict errors.
func NewConsumerClient[T any](b *pubsub.Bus, topicName string) (ConsumerClient[T], error) {
	sub, err := pubsub.Subscribe(b, pubsub.NewTopic[T](topicName))
	if err != nil {
		return nil, err
	}
	return &consumer[T]{sub: sub}, nil
}

func (c *consumer[T]) Run(ctx context.Context, handler Handler[T]) error {
	if c == nil || c.sub == nil {
		return errors.New("pubsub/client: nil subscription")
	}
	if handler == nil {
		return ErrNilHandler
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case value, ok := <-c.sub.Events:
			if !ok {
				return nil
			}
			if err := handler(ctx, value); err != nil {
				return err
			}
		}
	}
}

func (c *consumer[T]) Close() {
	if c == nil {
		return
	}
	if c.sub != nil {
		c.sub.Unsubscribe()
	}
}
