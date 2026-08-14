package fabrik

import (
	"context"
	"reflect"
)

type SubscriptionOptions interface{ applySubscription(*subscriptionConfig) }

type subscriptionConfig struct{ bufferSize int }

type subscriptionOption func(*subscriptionConfig)

func (o subscriptionOption) applySubscription(config *subscriptionConfig) { o(config) }

func WithBufferSize(size int) SubscriptionOptions {
	return subscriptionOption(func(config *subscriptionConfig) { config.bufferSize = size })
}

// Subscription is the typed event stream returned by Subscribe.
type Subscription[T any] struct {
	Events     <-chan Event[T]
	subscriber *subscriber
}

// Close stops delivery to the subscription and closes its event channel.
// Events already queued remain available to the consumer.
func (s *Subscription[T]) Close() {
	if s == nil || s.subscriber == nil {
		return
	}
	s.subscriber.stream.mu.Lock()
	s.subscriber.close()
	s.subscriber.stream.mu.Unlock()
}

func Subscribe[T EventPayload](ctx context.Context, b *Bus, opts ...SubscriptionOptions) (*Subscription[T], error) {
	if b == nil {
		return nil, ErrNilBus
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	release, err := b.admit(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	config := subscriptionConfig{bufferSize: 128}
	for _, option := range opts {
		if option != nil {
			option.applySubscription(&config)
		}
	}
	if config.bufferSize <= 0 {
		return nil, ErrInvalidBufferSize
	}
	streamType, err := streamTypeOf[T]()
	if err != nil {
		return nil, err
	}
	stream, err := b.registerStream(streamType, reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	channel := make(chan Event[T], config.bufferSize)
	subscriber := &subscriber{stream: stream}
	subscription := &Subscription[T]{Events: channel, subscriber: subscriber}
	subscriber.closeFn = func() { close(channel) }
	stream.mu.Lock()
	subscriber.deliver = func(event queuedEvent) bool {
		if subscriber.closed {
			return true
		}
		delivered := Event[T]{
			ID:            event.id,
			OccurredAt:    event.metadata.OccurredAt,
			CorrelationID: event.metadata.CorrelationID,
			CausationID:   event.metadata.CausationID,
			Headers:       copyHeaders(event.metadata.Headers),
			Payload:       event.payload.(T),
		}
		select {
		case channel <- delivered:
			return true
		default:
			return false
		}
	}
	stream.subscribers = append(stream.subscribers, subscriber)
	stream.mu.Unlock()
	return subscription, nil
}

func isNilPayload(payload EventPayload) bool {
	if payload == nil {
		return true
	}
	value := reflect.ValueOf(payload)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func streamTypeOf[T EventPayload]() (StreamType, error) {
	typ := reflect.TypeFor[T]()
	if typ.Kind() == reflect.Interface {
		return "", ErrUnsupportedPayloadType
	}
	var payload T
	if typ.Kind() == reflect.Pointer {
		payload = reflect.New(typ.Elem()).Interface().(T)
	}
	stream := payload.StreamType()
	if stream == "" {
		return "", ErrEmptyStreamType
	}
	return stream, nil
}
