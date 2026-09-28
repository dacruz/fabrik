package fabrik

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
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
// StreamType must return a stable identity that does not depend on payload
// fields, and it must be safe to call on the payload type's zero value.
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
	ErrPayloadTypeConflict    = errors.New("fabrik: payload type conflict")
	ErrStreamTypeConflict     = errors.New("fabrik: stream type conflict")
	ErrUnsupportedPayloadType = errors.New("fabrik: unsupported payload type")
)

// DeliveryError reports a partial fan-out delivery. Delivered subscribers have
// already received the event, so callers must not treat this as an atomic
// failure and blindly retry the emission.
type DeliveryError struct {
	EventID    string
	StreamType StreamType
	Attempted  int
	Delivered  int
	Dropped    int
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("fabrik: dropped %d delivery(s) for stream type %q", e.Dropped, e.StreamType)
}

func (e *DeliveryError) Unwrap() error { return ErrDelivery }

// StreamTypeConflictError reports an attempt to associate one stream identity
// with more than one concrete Go payload type.
type StreamTypeConflictError struct {
	StreamType StreamType
	Existing   reflect.Type
	Requested  reflect.Type
}

func (e *StreamTypeConflictError) Error() string {
	return fmt.Sprintf("fabrik: stream type %q is registered for %v, cannot use %v", e.StreamType, e.Existing, e.Requested)
}

func (e *StreamTypeConflictError) Unwrap() error { return ErrStreamTypeConflict }

// PayloadTypeConflictError reports an unstable StreamType implementation: one
// concrete Go payload type attempted to register more than one stream identity.
type PayloadTypeConflictError struct {
	PayloadType reflect.Type
	Existing    StreamType
	Requested   StreamType
}

func (e *PayloadTypeConflictError) Error() string {
	return fmt.Sprintf("fabrik: payload type %v is registered for stream type %q, cannot use %q", e.PayloadType, e.Existing, e.Requested)
}

func (e *PayloadTypeConflictError) Unwrap() error { return ErrPayloadTypeConflict }

type eventMetadata struct {
	OccurredAt    time.Time
	CorrelationID string
	CausationID   string
	Headers       map[string]string
}

// EventOption configures metadata for an emitted event.
type EventOption interface{ applyEvent(*eventMetadata) }

// EventOptions is retained for source compatibility.
// Deprecated: use EventOption.
type EventOptions = EventOption

type eventOption func(*eventMetadata)

func (o eventOption) applyEvent(metadata *eventMetadata) { o(metadata) }

// WithOccurredAt overrides the event occurrence time.
func WithOccurredAt(at time.Time) EventOption {
	return eventOption(func(metadata *eventMetadata) { metadata.OccurredAt = at })
}

// WithCorrelationID attaches a workflow correlation identifier.
func WithCorrelationID(id string) EventOption {
	return eventOption(func(metadata *eventMetadata) { metadata.CorrelationID = id })
}

// WithCausationID identifies the event or command that caused this event.
func WithCausationID(id string) EventOption {
	return eventOption(func(metadata *eventMetadata) { metadata.CausationID = id })
}

// WithHeaders merges a defensive copy of headers into the event metadata.
func WithHeaders(headers map[string]string) EventOption {
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
	return newEventIDFrom(rand.Reader)
}

func newEventIDFrom(reader io.Reader) (string, error) {
	var bytes [16]byte
	if _, err := io.ReadFull(reader, bytes[:]); err != nil {
		return "", fmt.Errorf("%w: %w", ErrEventIDGeneration, err)
	}
	return fmt.Sprintf("%x", bytes[:]), nil
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
