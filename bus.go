package fabrik

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// Bus owns event delivery state.
type Bus struct {
	lifecycleMu      sync.Mutex
	streamsMu        sync.RWMutex
	streams          map[StreamType]*stream
	registrations    sync.Map
	nextSubscriberID atomic.Uint64
	state            busState
	admitted         sync.WaitGroup
	shutdownDone     chan struct{}
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
		streams: make(map[StreamType]*stream),
	}
}

type streamRegistration struct {
	streamType StreamType
	stream     *stream
}

type queuedEvent struct {
	id       string
	metadata eventMetadata
	payload  EventPayload
}

type stream struct {
	mu              sync.Mutex
	payloadType     reflect.Type
	subscribers     map[*subscriber]struct{}
	subscriberCount atomic.Int64
}

type subscriber struct {
	stream    *stream
	id        uint64
	name      string
	delivered atomic.Uint64
	dropped   atomic.Uint64
	deliver   func(queuedEvent, map[string]string) bool
	closed    bool
	closeFn   func()
}

// closeLocked closes and unregisters the subscriber. The stream lock must be
// held by the caller.
func (s *subscriber) closeLocked() {
	if s.closed {
		return
	}
	s.closed = true
	delete(s.stream.subscribers, s)
	s.stream.subscriberCount.Add(-1)
	if s.closeFn != nil {
		s.closeFn()
	}
	s.deliver = nil
	s.closeFn = nil
}

// Emit publishes a payload to every active subscription for its stream.
// Delivery to a full subscription queue is dropped and reported through a
// DeliveryError; other subscribers may already have received the event.
// The context controls admission only; cancellation after admission does not
// interrupt delivery.
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
	var headerCopies []map[string]string
	if metadata.Headers != nil {
		headerCopies = makeHeaderCopies(metadata.Headers, int(stream.subscriberCount.Load()))
	}
	stream.mu.Lock()
	attempted := len(stream.subscribers)
	if metadata.Headers != nil {
		for len(headerCopies) < attempted {
			headerCopies = append(headerCopies, copyHeaders(metadata.Headers))
		}
	}
	dropped := 0
	var droppedSubscriptions []SubscriptionRef
	subscriberIndex := 0
	for subscriber := range stream.subscribers {
		var headers map[string]string
		if metadata.Headers != nil {
			headers = headerCopies[subscriberIndex]
		}
		if !subscriber.deliver(event, headers) {
			dropped++
			droppedSubscriptions = append(droppedSubscriptions, SubscriptionRef{
				ID:   subscriber.id,
				Name: subscriber.name,
			})
		}
		subscriberIndex++
	}
	stream.mu.Unlock()
	if dropped > 0 {
		return &DeliveryError{
			EventID:              id,
			StreamType:           streamType,
			Attempted:            attempted,
			Delivered:            attempted - dropped,
			Dropped:              dropped,
			DroppedSubscriptions: droppedSubscriptions,
		}
	}
	return nil
}

func (b *Bus) admit(ctx context.Context) (func(), error) {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
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
// Shutdown does not wait for application consumers to process them. A caller's
// context controls how long that caller waits, but an initiated shutdown
// continues independently.
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

	b.lifecycleMu.Lock()
	if b.state == busOpen {
		b.state = busShuttingDown
		b.shutdownDone = make(chan struct{})
		go b.finishShutdown(b.shutdownDone)
	}
	done := b.shutdownDone
	b.lifecycleMu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Bus) finishShutdown(done chan struct{}) {
	b.admitted.Wait()

	b.streamsMu.RLock()
	streams := make([]*stream, 0, len(b.streams))
	for _, stream := range b.streams {
		streams = append(streams, stream)
	}
	b.streamsMu.RUnlock()

	for _, stream := range streams {
		stream.mu.Lock()
		for subscriber := range stream.subscribers {
			subscriber.closeLocked()
		}
		stream.mu.Unlock()
	}

	b.lifecycleMu.Lock()
	b.state = busClosed
	b.lifecycleMu.Unlock()
	close(done)
}

func (b *Bus) registerStream(streamType StreamType, payloadType reflect.Type) (*stream, error) {
	if registration, ok := b.registrations.Load(payloadType); ok {
		return registeredStream(payloadType, streamType, registration.(streamRegistration))
	}

	b.streamsMu.Lock()
	defer b.streamsMu.Unlock()
	if registration, ok := b.registrations.Load(payloadType); ok {
		return registeredStream(payloadType, streamType, registration.(streamRegistration))
	}
	if b.streams == nil {
		b.streams = make(map[StreamType]*stream)
	}
	if existing, ok := b.streams[streamType]; ok {
		if existing.payloadType != payloadType {
			return nil, &StreamTypeConflictError{StreamType: streamType, Existing: existing.payloadType, Requested: payloadType}
		}
		b.registrations.Store(payloadType, streamRegistration{streamType: streamType, stream: existing})
		return existing, nil
	}
	stream := &stream{
		payloadType: payloadType,
		subscribers: make(map[*subscriber]struct{}),
	}
	b.streams[streamType] = stream
	b.registrations.Store(payloadType, streamRegistration{streamType: streamType, stream: stream})
	return stream, nil
}

func registeredStream(payloadType reflect.Type, requested StreamType, registration streamRegistration) (*stream, error) {
	if registration.streamType != requested {
		return nil, &PayloadTypeConflictError{
			PayloadType: payloadType,
			Existing:    registration.streamType,
			Requested:   requested,
		}
	}
	return registration.stream, nil
}

func makeHeaderCopies(headers map[string]string, count int) []map[string]string {
	if count <= 0 {
		return nil
	}
	copies := make([]map[string]string, count)
	for index := range copies {
		copies[index] = copyHeaders(headers)
	}
	return copies
}
