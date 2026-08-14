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
	state        busState
	admitted     sync.WaitGroup
	shutdownDone chan struct{}
	shutdownErr  error
}

type busState uint8

const (
	busOpen busState = iota
	busShuttingDown
	busClosed
)

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
	stream  *stream
	deliver func(queuedEvent) bool
	closed  bool
	closeFn func()
}

func (s *subscriber) close() {
	if s.closed {
		return
	}
	s.closed = true
	if s.closeFn != nil {
		s.closeFn()
	}
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
// subscriptions after their queued events have become drainable.
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
		b.mu.Lock()
		err := b.shutdownErr
		b.mu.Unlock()
		return err
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
		for _, subscriber := range stream.subscribers {
			subscriber.close()
		}
		stream.mu.Unlock()
	}

	b.mu.Lock()
	b.state = busClosed
	b.shutdownErr = nil
	b.mu.Unlock()
	close(done)
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
