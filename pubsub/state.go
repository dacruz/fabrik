package pubsub

import (
	"reflect"
	"sync"
)

type reflectType = reflect.Type

type subscriber struct {
	send  func(any) bool
	close func()
}

// topicState is deliberately ready for later subscription work: its state is
// independent per topic and its mutex never needs to be held for registry
// operations on another topic.
type topicState struct {
	mu          sync.Mutex
	eventType   reflect.Type
	subscribers map[uint64]subscriber
	nextID      uint64
}

// addSubscriberLocked adds subscriber to the topic and returns its identifier.
// The caller must hold s.mu.
func (s *topicState) addSubscriberLocked(subscriber subscriber) uint64 {
	s.nextID++
	id := s.nextID
	s.subscribers[id] = subscriber
	return id
}

func (s *topicState) removeSubscriber(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	subscriber, ok := s.subscribers[id]
	if !ok {
		return
	}
	delete(s.subscribers, id)
	subscriber.close()
}

// closeSubscribers removes every subscriber while holding the topic lock,
// excluding all sends. Channels are closed after the registrations have been
// detached, so a concurrent Unsubscribe observes the missing registration and
// cannot close a channel a second time.
func (s *topicState) closeSubscribers() {
	s.mu.Lock()
	subscribers := make([]subscriber, 0, len(s.subscribers))
	for id, subscriber := range s.subscribers {
		delete(s.subscribers, id)
		subscribers = append(subscribers, subscriber)
	}
	for _, subscriber := range subscribers {
		subscriber.close()
	}
	s.mu.Unlock()
}

// publishLocked delivers value to every subscriber that was registered when
// the topic lock was acquired. The non-blocking send keeps a slow subscriber
// from delaying other subscribers or publishers on unrelated topics. The
// caller must hold s.mu.
func (s *topicState) publishLocked(value any) int {
	dropped := 0
	for _, subscriber := range s.subscribers {
		if !subscriber.send(value) {
			dropped++
		}
	}
	return dropped
}

// operate validates the requested event type and runs operation while holding
// the topic lock.
func (s *topicState) operate(requested reflect.Type, name string, operation func(*topicState) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateLocked(requested, name); err != nil {
		return err
	}
	return operation(s)
}

// validateLocked checks the requested event type against the registered type.
// The caller must hold s.mu.
func (s *topicState) validateLocked(requested reflect.Type, name string) error {
	if s.eventType == requested {
		return nil
	}
	return &TopicTypeConflictError{
		Topic:         name,
		ExistingType:  s.eventType,
		RequestedType: requested,
	}
}
