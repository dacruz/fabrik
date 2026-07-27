package pubsub

import "reflect"

// Topic identifies events of type T by an opaque, exact name.
type Topic[T any] struct {
	name string
	typ  reflect.Type
}

// NewTopic creates a typed topic value. It does not register the topic on a
// bus; registration happens on the first Subscribe or Publish operation.
func NewTopic[T any](name string) Topic[T] {
	return Topic[T]{name: name, typ: reflect.TypeFor[T]()}
}

// Name returns the topic's exact, opaque name.
func (t Topic[T]) Name() string { return t.name }

// Type returns the event type represented by the topic.
func (t Topic[T]) Type() reflect.Type { return t.typ }

// Publish sends value to every current subscriber of topic without blocking.
// Values sent to a full subscriber channel are dropped and reported as a
// DeliveryError; publishing continues to subscribers with available capacity.
func Publish[T any](b *Bus, topic Topic[T], value T) error {
	if b == nil {
		return ErrNilBus
	}
	return b.withTopic(topic.name, topic.typ, func(state *topicState) error {
		dropped := state.publishLocked(value)
		if dropped > 0 {
			return &DeliveryError{Topic: topic.name, Dropped: dropped}
		}
		return nil
	})
}
