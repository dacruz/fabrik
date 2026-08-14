package fabrik

import (
	"crypto/rand"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// Event contains the metadata and payload for one emitted event.
type Event[T any] struct {
	ID            string
	OccurredAt    time.Time
	CorrelationID string
	CausationID   string
	Headers       map[string]string
	Payload       T
}

// StreamType is the logical routing identity of an event payload.
type StreamType string

// EventPayload is implemented by values that can be emitted through a Bus.
type EventPayload interface {
	StreamType() StreamType
}

var (
	ErrBusClosed              = errors.New("fabrik: bus is shut down")
	ErrDelivery               = errors.New("fabrik: delivery failed")
	ErrEmptyStreamType        = errors.New("fabrik: empty stream type")
	ErrEventIDGeneration      = errors.New("fabrik: event ID generation failed")
	ErrInvalidOccurredAt      = errors.New("fabrik: invalid occurred-at timestamp")
	ErrInvalidBufferSize      = errors.New("fabrik: invalid subscription buffer size")
	ErrNilBus                 = errors.New("fabrik: nil bus")
	ErrNilContext             = errors.New("fabrik: nil context")
	ErrNilPayload             = errors.New("fabrik: nil payload")
	ErrStreamTypeConflict     = errors.New("fabrik: stream type conflict")
	ErrUnsupportedPayloadType = errors.New("fabrik: unsupported payload type")
)

type DeliveryError struct {
	StreamType StreamType
	Dropped    int
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("fabrik: dropped %d delivery(s) for stream type %q", e.Dropped, e.StreamType)
}

func (e *DeliveryError) Unwrap() error { return ErrDelivery }

type StreamTypeConflictError struct {
	StreamType StreamType
	Existing   reflect.Type
	Requested  reflect.Type
}

func (e *StreamTypeConflictError) Error() string {
	return fmt.Sprintf("fabrik: stream type %q is registered for %v, cannot use %v", e.StreamType, e.Existing, e.Requested)
}

func (e *StreamTypeConflictError) Unwrap() error { return ErrStreamTypeConflict }

type eventMetadata struct {
	OccurredAt    time.Time
	CorrelationID string
	CausationID   string
	Headers       map[string]string
}

type EventOptions interface{ applyEvent(*eventMetadata) }

type eventOption func(*eventMetadata)

func (o eventOption) applyEvent(metadata *eventMetadata) { o(metadata) }

func WithOccurredAt(at time.Time) EventOptions {
	return eventOption(func(metadata *eventMetadata) { metadata.OccurredAt = at })
}

func WithCorrelationID(id string) EventOptions {
	return eventOption(func(metadata *eventMetadata) { metadata.CorrelationID = id })
}

func WithCausationID(id string) EventOptions {
	return eventOption(func(metadata *eventMetadata) { metadata.CausationID = id })
}

func WithHeaders(headers map[string]string) EventOptions {
	return eventOption(func(metadata *eventMetadata) {
		if headers == nil {
			return
		}
		if metadata.Headers == nil {
			metadata.Headers = make(map[string]string, len(headers))
		}
		for key, value := range headers {
			metadata.Headers[key] = value
		}
	})
}

func newEventID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", ErrEventIDGeneration
	}
	return fmt.Sprintf("%x", bytes), nil
}

func copyHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		return nil
	}
	copy := make(map[string]string, len(headers))
	for key, value := range headers {
		copy[key] = value
	}
	return copy
}
