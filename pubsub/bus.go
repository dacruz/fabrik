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
	activeOps     int
	activeDone    chan struct{}
	shutdownDone  chan struct{}
	shutdownError error
}

// NewBus returns an empty, usable bus.
func NewBus() *Bus {
	done := make(chan struct{})
	close(done)
	return &Bus{topics: make(map[string]*topicState), activeDone: done}
}

// Shutdown gracefully closes b. It first rejects new publishes and
// subscriptions, then closes active subscription channels after queued values
// have been preserved. Shutdown is safe to call repeatedly and concurrently:
// calls after the first completed shutdown return the first shutdown result.
// A caller that waits for another shutdown may return ctx.Err() if its context
// expires; that does not cancel the shutdown already in progress. Once a
// caller has started shutdown, bus-side teardown completes before it returns.
func (b *Bus) Shutdown(ctx context.Context) error {
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
		activeDone := b.activeDone
		b.mu.Unlock()
		<-activeDone

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
// context. Bus operations hold it only around registry and lifecycle work.
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

// withTopic holds the bus lock only long enough to cross the lifecycle
// boundary and find or register the topic. The operation then owns the topic
// lock, so delivery and subscription bookkeeping do not retain the bus lock.
func (b *Bus) withTopic(topicName string, eventType reflect.Type, operation func(*topicState) error) error {
	b.mu.Lock()
	if b.state != busOpen {
		b.mu.Unlock()
		return ErrBusClosed
	}
	state := b.topics[topicName]
	if state == nil {
		state = &topicState{eventType: eventType, subscribers: make(map[uint64]subscriber)}
		b.topics[topicName] = state
	}
	if b.activeOps == 0 {
		b.activeDone = make(chan struct{})
	}
	b.activeOps++
	b.mu.Unlock()
	defer b.finishOperation()
	return state.operate(eventType, topicName, operation)
}

func (b *Bus) finishOperation() {
	b.mu.Lock()
	b.activeOps--
	if b.activeOps == 0 {
		close(b.activeDone)
	}
	b.mu.Unlock()
}
