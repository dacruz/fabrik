package pubsub

import "sync"

const subscriptionBufferSize = 100

// Subscription receives events published to one typed topic. Its channel is
// owned by the bus and is closed after Unsubscribe removes the subscription.
type Subscription[T any] struct {
	Events <-chan T

	events chan T
	id     uint64
	topic  *topicState
	once   sync.Once
}

// Subscribe registers topic on b and returns an empty buffered subscription.
// The topic remains registered even after all of its subscriptions leave.
func Subscribe[T any](b *Bus, topic Topic[T]) (*Subscription[T], error) {
	if b == nil {
		return nil, ErrNilBus
	}

	state, err := b.register(topic.name, topic.typ)
	if err != nil {
		return nil, err
	}

	events := make(chan T, subscriptionBufferSize)
	subscription := &Subscription[T]{
		Events: events,
		events: events,
		topic:  state,
	}
	subscription.id = state.addSubscriber(subscriber{
		send: func(value any) bool {
			select {
			case events <- value.(T):
				return true
			default:
				return false
			}
		},
		close: func() { close(events) },
	})
	return subscription, nil
}

// Unsubscribe removes the subscription and closes its channel. It is safe to
// call repeatedly and concurrently. Removal and closing are serialized with
// publishing by the topic mutex, so a publisher cannot send to a closed
// channel.
func (s *Subscription[T]) Unsubscribe() {
	if s == nil {
		return
	}

	s.once.Do(func() {
		s.topic.removeSubscriber(s.id)
	})
}
