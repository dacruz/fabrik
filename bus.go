package fabrik

import (
	"context"
	"reflect"
	"sync"
	"time"
)

// Bus owns event delivery state.
type Bus struct {
	mu      sync.Mutex
	streams map[StreamType]*stream
}

// NewBus creates an empty event bus.
func NewBus() *Bus { return &Bus{streams: make(map[StreamType]*stream)} }

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
	streamType := payload.StreamType()
	if streamType == "" {
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
