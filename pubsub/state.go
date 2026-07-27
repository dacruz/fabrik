package pubsub

import (
	"reflect"
	"sync"
)

type reflectType = reflect.Type

type subscriber struct{}

// topicState is deliberately ready for later subscription work: its state is
// independent per topic and its mutex never needs to be held for registry
// operations on another topic.
type topicState struct {
	mu          sync.Mutex
	eventType   reflect.Type
	subscribers map[uint64]subscriber
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
