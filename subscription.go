package fabrik

import (
	"context"
	"reflect"
)

// SubscriptionOption configures a subscription.
type SubscriptionOption interface{ applySubscription(*subscriptionConfig) }

// SubscriptionOptions is retained for source compatibility.
// Deprecated: use SubscriptionOption.
type SubscriptionOptions = SubscriptionOption

type subscriptionConfig struct {
	bufferSize int
	name       string
}

type subscriptionOption func(*subscriptionConfig)

func (o subscriptionOption) applySubscription(config *subscriptionConfig) { o(config) }

// WithBufferSize sets the number of events that can be queued for a
// subscription before further deliveries are dropped.
func WithBufferSize(size int) SubscriptionOption {
	return subscriptionOption(func(config *subscriptionConfig) { config.bufferSize = size })
}

// WithSubscriptionName attaches an optional human-readable name to a
// subscription. Names are included in delivery errors but are not required to
// be unique; use Subscription.ID when a unique identity is needed.
func WithSubscriptionName(name string) SubscriptionOption {
	return subscriptionOption(func(config *subscriptionConfig) { config.name = name })
}

// SubscriptionStats reports queueing outcomes for a subscription. Delivered
// counts events accepted into the subscription queue, not events processed by
// application code.
type SubscriptionStats struct {
	Delivered uint64
	Dropped   uint64
}

// Subscription is the typed event stream returned by Subscribe.
type Subscription[T any] struct {
	Events     <-chan Event[T]
	subscriber *subscriber
}

// ID returns the subscription's bus-local unique identifier. It returns zero
// for a nil or uninitialized Subscription.
func (s *Subscription[T]) ID() uint64 {
	if s == nil || s.subscriber == nil {
		return 0
	}
	return s.subscriber.id
}

// Name returns the optional human-readable subscription name.
func (s *Subscription[T]) Name() string {
	if s == nil || s.subscriber == nil {
		return ""
	}
	return s.subscriber.name
}

// Stats returns a concurrency-safe snapshot of the subscription's queueing
// outcomes.
func (s *Subscription[T]) Stats() SubscriptionStats {
	if s == nil || s.subscriber == nil {
		return SubscriptionStats{}
	}
	return SubscriptionStats{
		Delivered: s.subscriber.delivered.Load(),
		Dropped:   s.subscriber.dropped.Load(),
	}
}

// Close stops delivery to the subscription and closes its event channel.
// Events already queued remain available to the consumer.
func (s *Subscription[T]) Close() {
	if s == nil || s.subscriber == nil {
		return
	}
	s.subscriber.stream.mu.Lock()
	s.subscriber.closeLocked()
	s.subscriber.stream.mu.Unlock()
}

// Subscribe creates a typed subscription for T's stream identity. The context
// controls admission only; cancellation after admission does not close the
// subscription.
func Subscribe[T EventPayload](ctx context.Context, b *Bus, opts ...SubscriptionOption) (*Subscription[T], error) {
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
	subscriber := &subscriber{
		stream: stream,
		id:     b.nextSubscriberID.Add(1),
		name:   config.name,
	}
	subscription := &Subscription[T]{Events: channel, subscriber: subscriber}
	subscriber.closeFn = func() { close(channel) }
	stream.mu.Lock()
	subscriber.deliver = func(event queuedEvent, headers map[string]string) bool {
		if subscriber.closed {
			return true
		}
		delivered := Event[T]{
			ID:            event.id,
			OccurredAt:    event.metadata.OccurredAt,
			CorrelationID: event.metadata.CorrelationID,
			CausationID:   event.metadata.CausationID,
			Headers:       headers,
			Payload:       event.payload.(T),
		}
		select {
		case channel <- delivered:
			subscriber.delivered.Add(1)
			return true
		default:
			subscriber.dropped.Add(1)
			return false
		}
	}
	stream.subscribers[subscriber] = struct{}{}
	stream.subscriberCount.Add(1)
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
