package pubsub

import (
	"errors"
	"fmt"
	"reflect"
)

// ErrTopicTypeConflict identifies reuse of a topic name with another event
// type. Use errors.Is to match it or errors.As to inspect the details.
var ErrTopicTypeConflict = errors.New("pubsub topic type conflict")

// ErrNilBus is returned when an operation is attempted on a nil bus.
var ErrNilBus = errors.New("pubsub: nil bus")

// TopicTypeConflictError reports both types involved in an incompatible
// registration.
type TopicTypeConflictError struct {
	Topic         string
	ExistingType  reflect.Type
	RequestedType reflect.Type
}

func (e *TopicTypeConflictError) Error() string {
	return fmt.Sprintf("pubsub: topic %q is registered for %v, cannot use %v", e.Topic, e.ExistingType, e.RequestedType)
}

func (e *TopicTypeConflictError) Unwrap() error { return ErrTopicTypeConflict }
