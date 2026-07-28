package client

import "github.com/dacruz/fabrik/pubsub"

// ProducerClient publishes values to one typed topic.
type ProducerClient[T any] interface {
	Publish(T) error
}

type producer[T any] struct {
	bus   *pubsub.Bus
	topic pubsub.Topic[T]
}

// NewProducerClient binds a producer to topic on b.
func NewProducerClient[T any](b *pubsub.Bus, topic pubsub.Topic[T]) ProducerClient[T] {
	return &producer[T]{bus: b, topic: topic}
}

func (p *producer[T]) Publish(value T) error {
	return pubsub.Publish(p.bus, p.topic, value)
}
