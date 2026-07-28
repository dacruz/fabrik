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

// ErrNilContext is returned when an operation requiring a context receives nil.
var ErrNilContext = errors.New("pubsub: nil context")

// ErrBusClosed is returned when an operation is attempted after shutdown
// begins. Use errors.Is to inspect this lifecycle error.
var ErrBusClosed = errors.New("pubsub: bus is shut down")

// ErrDelivery identifies a publish where one or more subscriber deliveries
// were dropped because their bounded channels were full.
var ErrDelivery = errors.New("pubsub: delivery failed")

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

// DeliveryError reports the subscriber deliveries dropped by one publish.
type DeliveryError struct {
	Topic   string
	Dropped int
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("pubsub: dropped %d delivery(s) for topic %q", e.Dropped, e.Topic)
}

func (e *DeliveryError) Unwrap() error { return ErrDelivery }
