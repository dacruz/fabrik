package pubsub

import "sync"

// Bus owns the registry of topics for one process-local pub/sub instance.
//
// A Bus is safe for use by multiple goroutines. Topic registration is lazy:
// the first Subscribe or Publish for a topic adds it to the registry.
type Bus struct {
	mu     sync.RWMutex
	topics map[string]*topicState
}

// NewBus returns an empty, usable bus.
func NewBus() *Bus {
	return &Bus{topics: make(map[string]*topicState)}
}

// register returns the state for topic, creating it if necessary. A topic
// name is associated with exactly one event type for the lifetime of the bus.
func (b *Bus) register(topicName string, eventType reflectType) (*topicState, error) {
	b.mu.RLock()
	state := b.topics[topicName]
	b.mu.RUnlock()

	if state != nil {
		return state, state.validate(eventType, topicName)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if state = b.topics[topicName]; state == nil {
		state = &topicState{
			eventType:   eventType,
			subscribers: make(map[uint64]subscriber),
		}
		b.topics[topicName] = state
		return state, nil
	}
	return state, state.validate(eventType, topicName)
}
