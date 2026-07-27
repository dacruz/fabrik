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
	return Topic[T]{name: name, typ: reflect.TypeOf((*T)(nil)).Elem()}
}

// Name returns the topic's exact, opaque name.
func (t Topic[T]) Name() string { return t.name }

// Type returns the event type represented by the topic.
func (t Topic[T]) Type() reflect.Type { return t.typ }

// Publish registers topic on b. Delivery behavior is intentionally deferred
// to Plan 003.
func Publish[T any](b *Bus, topic Topic[T], _ T) error {
	if b == nil {
		return ErrNilBus
	}
	_, err := b.register(topic.name, topic.typ)
	return err
}
