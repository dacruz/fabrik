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

func TestEmitRejectsNilPayloads(t *testing.T) {
	b := NewBus()
	ctx := context.Background()

	var pointer *pointerTestPayload
	assert.ErrorIs(t, b.Emit(ctx, pointer), ErrNilPayload)

	var payload nilMapTestPayload
	assert.ErrorIs(t, b.Emit(ctx, payload), ErrNilPayload)
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

func reflectType[T any]() reflect.Type { return reflect.TypeFor[T]() }
