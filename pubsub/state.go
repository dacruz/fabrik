package pubsub

import (
	"reflect"
	"sync"
)

type reflectType = reflect.Type

type subscriber struct {
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

func (s *topicState) addSubscriber(subscriber subscriber) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

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

func (s *topicState) validate(requested reflect.Type, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eventType == requested {
		return nil
	}
	return &TopicTypeConflictError{
		Topic:         name,
		ExistingType:  s.eventType,
		RequestedType: requested,
	}
}
