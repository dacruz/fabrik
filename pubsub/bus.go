package pubsub

import (
	"context"
	"reflect"
	"runtime"
	"sync"
	"time"
)

type busState uint8

const (
	busOpen busState = iota
	busShuttingDown
	busClosed
)

// Bus owns the registry of topics for one process-local pub/sub instance.
//
// A Bus is safe for use by multiple goroutines. Topic registration is lazy:
// the first Subscribe or Publish for a topic adds it to the registry.
type Bus struct {
	mu            sync.RWMutex
	topics        map[string]*topicState
	state         busState
	shutdownDone  chan struct{}
	shutdownError error
}

// NewBus returns an empty, usable bus.
func NewBus() *Bus {
	return &Bus{topics: make(map[string]*topicState)}
}

// Shutdown gracefully closes b. It first rejects new publishes and
// subscriptions, then closes active subscription channels after queued values
// have been preserved. Shutdown is safe to call repeatedly and concurrently:
// calls after the first completed shutdown return the first shutdown result.
// A caller that waits for another shutdown may return ctx.Err() if its context
// expires; that does not cancel the shutdown already in progress. Once a
// caller has started shutdown, bus-side teardown completes before it returns.
func Shutdown(ctx context.Context, b *Bus) error {
	if b == nil {
		return ErrNilBus
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.lock(ctx); err != nil {
		return err
	}

	switch b.state {
	case busOpen:
		if err := ctx.Err(); err != nil {
			b.mu.Unlock()
			return err
		}
		b.state = busShuttingDown
		b.shutdownDone = make(chan struct{})
		done := b.shutdownDone
		topics := make([]*topicState, 0, len(b.topics))
		for _, state := range b.topics {
			topics = append(topics, state)
		}
		b.mu.Unlock()

		for _, state := range topics {
			state.closeSubscribers()
		}

		b.mu.Lock()
		b.topics = nil
		b.state = busClosed
		b.shutdownError = nil
		close(done)
		b.mu.Unlock()
		return nil
	case busShuttingDown:
		done := b.shutdownDone
		b.mu.Unlock()
		select {
		case <-done:
			b.mu.RLock()
			err := b.shutdownError
			b.mu.RUnlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case busClosed:
		err := b.shutdownError
		b.mu.Unlock()
		return err
	default:
		b.mu.Unlock()
		panic("pubsub: invalid bus state")
	}
}

// lock acquires the bus mutex without trapping a caller with an expired
// context. Bus operations hold the read lock only around non-blocking topic
// work, so this retry loop is bounded by the operation currently in progress.
func (b *Bus) lock(ctx context.Context) error {
	for {
		if b.mu.TryLock() {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
			runtime.Gosched()
		}
	}
}

// withTopic keeps the bus lifecycle read lock across topic lookup/creation and
// the operation. This makes the operation's linearization point precede the
// shutdown transition, preventing a send or subscriber registration from
// racing with channel closure.
func (b *Bus) withTopic(topicName string, eventType reflect.Type, operation func(*topicState) error) error {
	b.mu.RLock()
	if b.state != busOpen {
		b.mu.RUnlock()
		return ErrBusClosed
	}
	state := b.topics[topicName]
	if state != nil {
		if err := state.validate(eventType, topicName); err != nil {
			b.mu.RUnlock()
			return err
		}
		err := operation(state)
		b.mu.RUnlock()
		return err
	}
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != busOpen {
		return ErrBusClosed
	}
	state = &topicState{
		eventType:   eventType,
		subscribers: make(map[uint64]subscriber),
	}
	b.topics[topicName] = state
	return operation(state)
}
