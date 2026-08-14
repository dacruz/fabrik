package fabrik

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deliveryEvent struct{ Number int }

func (deliveryEvent) StreamType() StreamType { return "delivery.events" }

type conflictingDeliveryEvent struct{}

func (conflictingDeliveryEvent) StreamType() StreamType { return "delivery.events" }

type separateDeliveryEvent struct{ Number int }

func (separateDeliveryEvent) StreamType() StreamType { return "separate.delivery.events" }

type blockingDeliveryEvent struct {
	entered chan struct{}
	release chan struct{}
	once    *sync.Once
}

func (event blockingDeliveryEvent) StreamType() StreamType {
	event.once.Do(func() { close(event.entered) })
	<-event.release
	return "blocking.delivery.events"
}

func TestEmitRejectsNilPayloads(t *testing.T) {
	b := NewBus()
	ctx := context.Background()

	var pointer *pointerTestPayload
	assert.ErrorIs(t, b.Emit(ctx, pointer), ErrNilPayload)

	var payload nilMapTestPayload
	assert.ErrorIs(t, b.Emit(ctx, payload), ErrNilPayload)
}

func TestBusRejectsNilBusAndContext(t *testing.T) {
	var nilBus *Bus
	assert.ErrorIs(t, nilBus.Emit(context.Background(), deliveryEvent{}), ErrNilBus)
	_, err := Subscribe[deliveryEvent](context.Background(), nilBus)
	assert.ErrorIs(t, err, ErrNilBus)
	assert.ErrorIs(t, nilBus.Shutdown(context.Background()), ErrNilBus)

	bus := NewBus()
	assert.ErrorIs(t, bus.Emit(nil, deliveryEvent{}), ErrNilContext)
	_, err = Subscribe[deliveryEvent](nil, bus)
	assert.ErrorIs(t, err, ErrNilContext)
	assert.ErrorIs(t, bus.Shutdown(nil), ErrNilContext)
}

func TestBusRejectsZeroOccurredAtAfterApplyingOptions(t *testing.T) {
	assert.ErrorIs(t, NewBus().Emit(context.Background(), testPayload{}, WithOccurredAt(time.Time{})), ErrInvalidOccurredAt)
}

func TestBusFanOutAndHeaderIsolation(t *testing.T) {
	bus := NewBus()
	first, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(2))
	require.NoError(t, err)
	second, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(2))
	require.NoError(t, err)

	err = bus.Emit(context.Background(), deliveryEvent{Number: 7}, WithHeaders(map[string]string{"source": "producer"}))
	require.NoError(t, err)
	firstEvent := <-first.Events
	secondEvent := <-second.Events
	firstEvent.Headers["source"] = "first"
	assert.Equal(t, 7, firstEvent.Payload.Number)
	assert.Equal(t, 7, secondEvent.Payload.Number)
	assert.Equal(t, "producer", secondEvent.Headers["source"])
	assert.NotEmpty(t, firstEvent.ID)
	assert.Equal(t, firstEvent.ID, secondEvent.ID)
}

func TestBusStreamIsolation(t *testing.T) {
	bus := NewBus()
	events, err := Subscribe[deliveryEvent](context.Background(), bus)
	require.NoError(t, err)
	separate, err := Subscribe[separateDeliveryEvent](context.Background(), bus)
	require.NoError(t, err)
	require.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: 1}))
	select {
	case got := <-events.Events:
		assert.Equal(t, 1, got.Payload.Number)
	case <-separate.Events:
		require.FailNow(t, "event crossed stream boundary")
	}
}

func TestBusRejectsConflictingPayloadTypesForStream(t *testing.T) {
	bus := NewBus()
	_, err := Subscribe[deliveryEvent](context.Background(), bus)
	require.NoError(t, err)
	_, err = Subscribe[conflictingDeliveryEvent](context.Background(), bus)
	var conflict *StreamTypeConflictError
	require.ErrorAs(t, err, &conflict)
	assert.ErrorIs(t, err, ErrStreamTypeConflict)
	assert.Equal(t, reflectType[deliveryEvent](), conflict.Existing)
	assert.Equal(t, reflectType[conflictingDeliveryEvent](), conflict.Requested)
}

func TestBusFullQueueReportsOnlyDroppedSubscriber(t *testing.T) {
	bus := NewBus()
	full, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(1))
	require.NoError(t, err)
	ready, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(2))
	require.NoError(t, err)
	require.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: 1}))
	err = bus.Emit(context.Background(), deliveryEvent{Number: 2})
	var delivery *DeliveryError
	require.ErrorAs(t, err, &delivery)
	assert.ErrorIs(t, err, ErrDelivery)
	assert.Equal(t, 1, delivery.Dropped)
	assert.Equal(t, 1, (<-ready.Events).Payload.Number)
	assert.Equal(t, 2, (<-ready.Events).Payload.Number)
	assert.Equal(t, 1, (<-full.Events).Payload.Number)
}

func TestBusPreservesPerStreamOrdering(t *testing.T) {
	bus := NewBus()
	subscription, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(64))
	require.NoError(t, err)
	for number := 0; number < 32; number++ {
		require.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: number}))
	}
	for expected := 0; expected < 32; expected++ {
		assert.Equal(t, expected, (<-subscription.Events).Payload.Number)
	}
}

func TestBusSerializesConcurrentEmittersPerStream(t *testing.T) {
	const emitters = 8
	const perEmitter = 20
	bus := NewBus()
	subscription, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(emitters*perEmitter))
	require.NoError(t, err)
	var group sync.WaitGroup
	for emitter := 0; emitter < emitters; emitter++ {
		group.Add(1)
		go func(emitter int) {
			defer group.Done()
			for number := 0; number < perEmitter; number++ {
				assert.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: emitter*perEmitter + number}))
			}
		}(emitter)
	}
	group.Wait()
	seen := make(map[int]bool, emitters*perEmitter)
	for number := 0; number < emitters*perEmitter; number++ {
		seen[(<-subscription.Events).Payload.Number] = true
	}
	assert.Len(t, seen, emitters*perEmitter)
}

func TestSubscriptionCloseIsIdempotentAndDrainsQueuedEvents(t *testing.T) {
	bus := NewBus()
	subscription, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(2))
	require.NoError(t, err)
	require.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: 1}))
	require.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: 2}))

	subscription.Close()
	subscription.Close()

	assert.Equal(t, 1, (<-subscription.Events).Payload.Number)
	assert.Equal(t, 2, (<-subscription.Events).Payload.Number)
	_, open := <-subscription.Events
	assert.False(t, open)
	assert.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: 3}))
}

func TestBusShutdownDrainsQueuedEventsAndRejectsNewOperations(t *testing.T) {
	bus := NewBus()
	subscription, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(2))
	require.NoError(t, err)
	require.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: 1}))
	require.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: 2}))

	require.NoError(t, bus.Shutdown(context.Background()))
	assert.Equal(t, 1, (<-subscription.Events).Payload.Number)
	assert.Equal(t, 2, (<-subscription.Events).Payload.Number)
	_, open := <-subscription.Events
	assert.False(t, open)

	assert.ErrorIs(t, bus.Emit(context.Background(), deliveryEvent{Number: 3}), ErrBusClosed)
	_, err = Subscribe[deliveryEvent](context.Background(), bus)
	assert.ErrorIs(t, err, ErrBusClosed)
	assert.NoError(t, bus.Shutdown(context.Background()))
}

func TestSubscriptionCloseIsSafeDuringConcurrentCalls(t *testing.T) {
	subscription, err := Subscribe[deliveryEvent](context.Background(), NewBus())
	require.NoError(t, err)

	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			subscription.Close()
		}()
	}
	group.Wait()

	_, open := <-subscription.Events
	assert.False(t, open)
}

func TestBusShutdownRacesConcurrentShutdownCallers(t *testing.T) {
	bus := NewBus()
	subscription, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(32))
	require.NoError(t, err)
	for number := 0; number < 8; number++ {
		require.NoError(t, bus.Emit(context.Background(), deliveryEvent{Number: number}))
	}

	const callers = 16
	errors := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			errors <- bus.Shutdown(context.Background())
		}()
	}
	group.Wait()
	close(errors)
	for shutdownErr := range errors {
		assert.NoError(t, shutdownErr)
	}

	for number := 0; number < 8; number++ {
		assert.Equal(t, number, (<-subscription.Events).Payload.Number)
	}
	_, open := <-subscription.Events
	assert.False(t, open)
}

func TestBusRejectsOperationsAfterShutdownBegins(t *testing.T) {
	bus := NewBus()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- bus.Shutdown(context.Background()) }()

	require.Eventually(t, func() bool {
		bus.mu.Lock()
		defer bus.mu.Unlock()
		return bus.state == busShuttingDown || bus.state == busClosed
	}, time.Second, time.Millisecond)

	assert.ErrorIs(t, bus.Emit(context.Background(), deliveryEvent{}), ErrBusClosed)
	_, err := Subscribe[deliveryEvent](context.Background(), bus)
	assert.ErrorIs(t, err, ErrBusClosed)
	assert.NoError(t, <-shutdownDone)
}

func TestBusShutdownContextCancellationDoesNotStopShutdown(t *testing.T) {
	bus := NewBus()
	blockingEvent := blockingDeliveryEvent{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		once:    &sync.Once{},
	}
	emitDone := make(chan error, 1)
	go func() {
		emitDone <- bus.Emit(context.Background(), blockingEvent)
	}()
	<-blockingEvent.entered

	ctx, cancel := context.WithCancel(context.Background())
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- bus.Shutdown(ctx) }()
	require.Eventually(t, func() bool {
		bus.mu.Lock()
		defer bus.mu.Unlock()
		return bus.state == busShuttingDown
	}, time.Second, time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-shutdownDone, context.Canceled)

	close(blockingEvent.release)
	assert.NoError(t, <-emitDone)
	assert.NoError(t, bus.Shutdown(context.Background()))
}

func reflectType[T any]() reflect.Type { return reflect.TypeFor[T]() }
