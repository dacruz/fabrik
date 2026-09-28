package fabrik

import (
	"context"
	"reflect"
	"sync"
	"time"
)

// Bus owns event delivery state.
type Bus struct {
	mu           sync.Mutex
	streams      map[StreamType]*stream
	streamTypes  map[reflect.Type]StreamType
	state        busState
	admitted     sync.WaitGroup
	shutdownDone chan struct{}
}

type busState uint8

const (
	busOpen busState = iota
	busShuttingDown
	busClosed
)

// NewBus creates an empty event bus.
func NewBus() *Bus {
	return &Bus{
		streams:     make(map[StreamType]*stream),
		streamTypes: make(map[reflect.Type]StreamType),
	}
}

type queuedEvent struct {
	id       string
	metadata eventMetadata
	payload  EventPayload
}

type stream struct {
	mu          sync.Mutex
	payloadType reflect.Type
	subscribers map[*subscriber]struct{}
}

type subscriber struct {
	stream  *stream
	deliver func(queuedEvent) bool
	closed  bool
	closeFn func()
}

// closeLocked closes and unregisters the subscriber. The stream lock must be
// held by the caller.
func (s *subscriber) closeLocked() {
	if s.closed {
		return
	}
	s.closed = true
	delete(s.stream.subscribers, s)
	if s.closeFn != nil {
		s.closeFn()
	}
	s.deliver = nil
	s.closeFn = nil
}

// Emit publishes a payload to every active subscription for its stream.
// Delivery to a full subscription queue is dropped and reported through a
// DeliveryError; other subscribers may already have received the event.
func (b *Bus) Emit(ctx context.Context, payload EventPayload, opts ...EventOption) error {
	if b == nil {
		return ErrNilBus
	}
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	release, err := b.admit(ctx)
	if err != nil {
		return err
	}
	defer release()
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
	event := queuedEvent{id: id, metadata: metadata, payload: payload}
	stream.mu.Lock()
	attempted := len(stream.subscribers)
	dropped := 0
	for subscriber := range stream.subscribers {
		if !subscriber.deliver(event) {
			dropped++
		}
	}
	stream.mu.Unlock()
	if dropped > 0 {
		return &DeliveryError{
			EventID:    id,
			StreamType: streamType,
			Attempted:  attempted,
			Delivered:  attempted - dropped,
			Dropped:    dropped,
		}
	}
	return nil
}

func (b *Bus) admit(ctx context.Context) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.state != busOpen {
		return nil, ErrBusClosed
	}
	b.admitted.Add(1)
	return b.admitted.Done, nil
}

// Shutdown stops admission, waits for admitted operations, and then closes
// subscriptions. Buffered events remain readable from their closed channels;
// Shutdown does not wait for application consumers to process them.
func (b *Bus) Shutdown(ctx context.Context) error {
	if b == nil {
		return ErrNilBus
	}
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	if b.state == busOpen {
		b.state = busShuttingDown
		b.shutdownDone = make(chan struct{})
		go b.finishShutdown(b.shutdownDone)
	}
	done := b.shutdownDone
	b.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Bus) finishShutdown(done chan struct{}) {
	b.admitted.Wait()

	b.mu.Lock()
	streams := make([]*stream, 0, len(b.streams))
	for _, stream := range b.streams {
		streams = append(streams, stream)
	}
	b.mu.Unlock()

	for _, stream := range streams {
		stream.mu.Lock()
		for subscriber := range stream.subscribers {
			subscriber.closeLocked()
		}
		stream.mu.Unlock()
	}

	b.mu.Lock()
	b.state = busClosed
	b.mu.Unlock()
	close(done)
}

func (b *Bus) registerStream(streamType StreamType, payloadType reflect.Type) (*stream, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.streams == nil {
		b.streams = make(map[StreamType]*stream)
	}
	if b.streamTypes == nil {
		b.streamTypes = make(map[reflect.Type]StreamType)
	}
	if existing, ok := b.streamTypes[payloadType]; ok && existing != streamType {
		return nil, &PayloadTypeConflictError{
			PayloadType: payloadType,
			Existing:    existing,
			Requested:   streamType,
		}
	}
	if existing, ok := b.streams[streamType]; ok {
		if existing.payloadType != payloadType {
			return nil, &StreamTypeConflictError{StreamType: streamType, Existing: existing.payloadType, Requested: payloadType}
		}
		b.streamTypes[payloadType] = streamType
		return existing, nil
	}
	stream := &stream{
		payloadType: payloadType,
		subscribers: make(map[*subscriber]struct{}),
	}
	b.streams[streamType] = stream
	b.streamTypes[payloadType] = streamType
	return stream, nil
}
