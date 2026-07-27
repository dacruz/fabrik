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

// Publish sends value to every current subscriber of topic without blocking.
// Values sent to a full subscriber channel are dropped; delivery errors for
// those drops are added in Plan 004.
func Publish[T any](b *Bus, topic Topic[T], value T) error {
	if b == nil {
		return ErrNilBus
	}
	state, err := b.register(topic.name, topic.typ)
	if err != nil {
		return err
	}
	state.publish(value)
	return nil
}
