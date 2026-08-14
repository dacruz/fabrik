package fabrik

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"reflect"
	"sync"
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

// Bus owns event delivery state.
type Bus struct {
	mu      sync.Mutex
	streams map[StreamType]*stream
}

// NewBus creates an empty event bus.
func NewBus() *Bus { return &Bus{streams: make(map[StreamType]*stream)} }

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

type SubscriptionOptions interface{ applySubscription(*subscriptionConfig) }

type subscriptionConfig struct{ bufferSize int }

type subscriptionOption func(*subscriptionConfig)

func (o subscriptionOption) applySubscription(config *subscriptionConfig) { o(config) }

func WithBufferSize(size int) SubscriptionOptions {
	return subscriptionOption(func(config *subscriptionConfig) { config.bufferSize = size })
}

// Subscription is the typed event stream returned by Subscribe.
type Subscription[T any] struct {
	Events <-chan Event[T]
}

type queuedEvent struct {
	id          string
	streamType  StreamType
	payloadType reflect.Type
	metadata    eventMetadata
	payload     EventPayload
}

type stream struct {
	mu          sync.Mutex
	payloadType reflect.Type
	subscribers []*subscriber
}

type subscriber struct {
	deliver func(queuedEvent) bool
}

// Close is a placeholder for the Phase 3 lifecycle implementation.
func (s *Subscription[T]) Close() {}

func Subscribe[T EventPayload](ctx context.Context, b *Bus, opts ...SubscriptionOptions) (*Subscription[T], error) {
	if b == nil {
		return nil, ErrNilBus
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config := subscriptionConfig{bufferSize: 128}
	for _, option := range opts {
		if option != nil {
			option.applySubscription(&config)
		}
	}
	if config.bufferSize <= 0 {
		return nil, ErrInvalidBufferSize
	}
	streamType, err := streamTypeOf[T]()
	if err != nil {
		return nil, err
	}
	stream, err := b.registerStream(streamType, reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	channel := make(chan Event[T], config.bufferSize)
	subscription := &Subscription[T]{Events: channel}
	stream.mu.Lock()
	stream.subscribers = append(stream.subscribers, &subscriber{deliver: func(event queuedEvent) bool {
		delivered := Event[T]{
			ID:            event.id,
			OccurredAt:    event.metadata.OccurredAt,
			CorrelationID: event.metadata.CorrelationID,
			CausationID:   event.metadata.CausationID,
			Headers:       copyHeaders(event.metadata.Headers),
			Payload:       event.payload.(T),
		}
		select {
		case channel <- delivered:
			return true
		default:
			return false
		}
	}})
	stream.mu.Unlock()
	return subscription, nil
}

func (b *Bus) Emit(ctx context.Context, payload EventPayload, opts ...EventOptions) error {
	if b == nil {
		return ErrNilBus
	}
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNilPayload(payload) {
		return ErrNilPayload
	}
	if payload.StreamType() == "" {
		return ErrEmptyStreamType
	}
	metadata := eventMetadata{OccurredAt: time.Now()}
	for _, option := range opts {
		if option != nil {
			option.applyEvent(&metadata)
		}
	}
	if metadata.OccurredAt.IsZero() {
		return ErrInvalidOccurredAt
	}
	id, err := newEventID()
	if err != nil {
		return err
	}
	streamType := payload.StreamType()
	if streamType == "" {
		return ErrEmptyStreamType
	}
	payloadType := reflect.TypeOf(payload)
	stream, err := b.registerStream(streamType, payloadType)
	if err != nil {
		return err
	}
	event := queuedEvent{id: id, streamType: streamType, payloadType: payloadType, metadata: metadata, payload: payload}
	stream.mu.Lock()
	dropped := 0
	for _, subscriber := range stream.subscribers {
		if !subscriber.deliver(event) {
			dropped++
		}
	}
	stream.mu.Unlock()
	if dropped > 0 {
		return &DeliveryError{StreamType: streamType, Dropped: dropped}
	}
	return nil
}

func (b *Bus) registerStream(streamType StreamType, payloadType reflect.Type) (*stream, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.streams == nil {
		b.streams = make(map[StreamType]*stream)
	}
	if existing, ok := b.streams[streamType]; ok {
		if existing.payloadType != payloadType {
			return nil, &StreamTypeConflictError{StreamType: streamType, Existing: existing.payloadType, Requested: payloadType}
		}
		return existing, nil
	}
	stream := &stream{payloadType: payloadType}
	b.streams[streamType] = stream
	return stream, nil
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

func isNilPayload(payload EventPayload) bool {
	if payload == nil {
		return true
	}
	value := reflect.ValueOf(payload)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func streamTypeOf[T EventPayload]() (StreamType, error) {
	typ := reflect.TypeFor[T]()
	if typ.Kind() == reflect.Interface {
		return "", ErrUnsupportedPayloadType
	}
	var payload T
	if typ.Kind() == reflect.Pointer {
		payload = reflect.New(typ.Elem()).Interface().(T)
	}
	stream := payload.StreamType()
	if stream == "" {
		return "", ErrEmptyStreamType
	}
	return stream, nil
}
