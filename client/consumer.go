package client

import (
	"context"
	"errors"

	"github.com/dacruz/fabrik/pubsub"
)

// ConsumerClient receives values from one typed topic.
type ConsumerClient[T any] interface {
	Run(context.Context, Handler[T]) error
	Close()
}

// Handler processes one value delivered to a consumer.
type Handler[T any] func(context.Context, T) error

// ErrNilHandler is returned when Run is given no handler.
var ErrNilHandler = errors.New("client: nil handler")

// ErrNilContext is returned when Run is given no context.
var ErrNilContext = errors.New("client: nil context")

type consumer[T any] struct {
	sub *pubsub.Subscription[T]
}

// NewConsumerClient subscribes to topic on b and owns the resulting
// subscription. It returns underlying pubsub errors, including nil-bus and
// topic-type-conflict errors.
func NewConsumerClient[T any](b *pubsub.Bus, topic pubsub.Topic[T]) (ConsumerClient[T], error) {
	sub, err := pubsub.Subscribe(b, topic)
	if err != nil {
		return nil, err
	}
	return &consumer[T]{sub: sub}, nil
}

func (c *consumer[T]) Run(ctx context.Context, handler Handler[T]) error {
	if c == nil || c.sub == nil {
		return errors.New("client: nil subscription")
	}
	if handler == nil {
		return ErrNilHandler
	}
	if ctx == nil {
		return ErrNilContext
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
