package pubsub

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublish_ItShouldFanOutToEverySubscriber(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	first, err := Subscribe(b, topic)
	require.NoError(t, err)
	second, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer first.Unsubscribe()
	defer second.Unsubscribe()

	for value := 1; value <= 3; value++ {
		require.NoError(t, Publish(b, topic, value))
	}

	for _, sub := range []*Subscription[int]{first, second} {
		assert.Equal(t, []int{1, 2, 3}, receiveInts(t, sub, 3))
	}
}

func TestPublish_ItShouldDeliverOnlyToTheExactTopic(t *testing.T) {
	b := NewBus()
	orders := NewTopic[int]("orders")
	ordersArchive := NewTopic[int]("orders/archive")
	prefix := NewTopic[int]("orders-v2")
	orderSub, err := Subscribe(b, orders)
	require.NoError(t, err)
	archiveSub, err := Subscribe(b, ordersArchive)
	require.NoError(t, err)
	prefixSub, err := Subscribe(b, prefix)
	require.NoError(t, err)
	defer orderSub.Unsubscribe()
	defer archiveSub.Unsubscribe()
	defer prefixSub.Unsubscribe()

	require.NoError(t, Publish(b, orders, 7))

	assert.Equal(t, []int{7}, receiveInts(t, orderSub, 1))
	assertEmpty(t, archiveSub)
	assertEmpty(t, prefixSub)
}

func TestPublish_ItShouldNotDeliverEarlierValuesToNewSubscribers(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	first, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer first.Unsubscribe()
	require.NoError(t, Publish(b, topic, 1))

	second, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer second.Unsubscribe()
	require.NoError(t, Publish(b, topic, 2))

	assert.Equal(t, []int{1, 2}, receiveInts(t, first, 2))
	assert.Equal(t, []int{2}, receiveInts(t, second, 1))
}

func TestPublish_ItShouldNotBlockOnASlowSubscriber(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	slow, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer slow.Unsubscribe()

	for value := 0; value < cap(slow.Events); value++ {
		require.NoError(t, Publish(b, topic, value))
	}
	fast, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer fast.Unsubscribe()

	done := make(chan error, 1)
	go func() { done <- Publish(b, topic, cap(slow.Events)) }()
	select {
	case err := <-done:
		require.Error(t, err)
		delivery := &DeliveryError{}
		require.ErrorAs(t, err, &delivery)
		assert.Equal(t, 1, delivery.Dropped)
	case <-time.After(time.Second):
		t.Fatal("publish blocked on a full subscriber")
	}

	select {
	case value := <-fast.Events:
		assert.Equal(t, cap(slow.Events), value)
	case <-time.After(time.Second):
		t.Fatal("available subscriber did not receive the value")
	}
}

func TestPublish_ItShouldReportAFullSubscriberAfterExactly100QueuedValues(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	sub, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	for value := range subscriptionBufferSize {
		require.NoError(t, Publish(b, topic, value))
	}

	err = Publish(b, topic, subscriptionBufferSize)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDelivery)
	delivery := &DeliveryError{}
	require.ErrorAs(t, err, &delivery)
	assert.Equal(t, topic.Name(), delivery.Topic)
	assert.Equal(t, 1, delivery.Dropped)
	assert.Equal(t, []int{0, 1, 2, 3, 4}, receiveInts(t, sub, 5))
}

func TestPublish_ItShouldPreserveAllFirst100ValuesInOrder(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	sub, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	for value := range subscriptionBufferSize {
		require.NoError(t, Publish(b, topic, value))
	}

	assert.Equal(t, makeRange(subscriptionBufferSize), receiveInts(t, sub, subscriptionBufferSize))
}

func TestPublish_ItShouldContinueFanoutAfterDroppingOneDelivery(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	full, err := Subscribe(b, topic)
	require.NoError(t, err)
	available, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer full.Unsubscribe()
	defer available.Unsubscribe()

	for value := 0; value < cap(full.Events); value++ {
		full.events <- value
	}
	err = Publish(b, topic, 100)
	delivery := &DeliveryError{}
	require.ErrorAs(t, err, &delivery)
	assert.Equal(t, 1, delivery.Dropped)
	assert.Equal(t, 100, <-available.Events)
}

func TestPublish_ItShouldSucceedWithNoSubscribers(t *testing.T) {
	b := NewBus()
	assert.NoError(t, Publish(b, NewTopic[int]("orders"), 1))
}

func TestPublish_ItShouldNotBlockWhenSubscriberIsFull(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	sub, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer sub.Unsubscribe()
	for value := 0; value < cap(sub.Events); value++ {
		require.NoError(t, Publish(b, topic, value))
	}

	done := make(chan error, 1)
	go func() { done <- Publish(b, topic, subscriptionBufferSize) }()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrDelivery))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("publish blocked on a full subscriber")
	}
}

func TestPublish_ItShouldPreserveOneTopicOrderForConcurrentPublishers(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	sub, err := Subscribe(b, topic)
	require.NoError(t, err)
	second, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer sub.Unsubscribe()
	defer second.Unsubscribe()

	const publishers = 8
	const valuesPerPublisher = 10
	var wg sync.WaitGroup
	for publisher := range publishers {
		wg.Add(1)
		go func(publisher int) {
			defer wg.Done()
			for value := range valuesPerPublisher {
				assert.NoError(t, Publish(b, topic, publisher*valuesPerPublisher+value))
			}
		}(publisher)
	}
	wg.Wait()

	values := receiveInts(t, sub, publishers*valuesPerPublisher)
	secondValues := receiveInts(t, second, publishers*valuesPerPublisher)
	seen := make(map[int]bool, len(values))
	for _, value := range values {
		assert.False(t, seen[value], "value delivered more than once")
		seen[value] = true
	}
	assert.Len(t, seen, publishers*valuesPerPublisher)
	assert.Equal(t, values, secondValues)
}

func TestPublish_ItShouldHandleNilBus(t *testing.T) {
	err := Publish[int](nil, NewTopic[int]("orders"), 1)

	assert.ErrorIs(t, err, ErrNilBus)
}

func TestPublish_ItShouldReportAllDroppedDeliveries(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	first, err := Subscribe(b, topic)
	require.NoError(t, err)
	second, err := Subscribe(b, topic)
	require.NoError(t, err)
	available, err := Subscribe(b, topic)
	require.NoError(t, err)
	defer first.Unsubscribe()
	defer second.Unsubscribe()
	defer available.Unsubscribe()

	for range subscriptionBufferSize {
		first.events <- 1
		second.events <- 1
	}
	err = Publish(b, topic, 2)

	assert.ErrorIs(t, err, ErrDelivery)
	delivery := &DeliveryError{}
	require.ErrorAs(t, err, &delivery)
	assert.Equal(t, 2, delivery.Dropped)
	assert.Equal(t, 2, <-available.Events)
}

func TestPublish_ItShouldReportTopicTypeConflicts(t *testing.T) {
	b := NewBus()
	topicName := "orders"
	require.NoError(t, Publish(b, NewTopic[int](topicName), 1))

	err := Publish(b, NewTopic[string](topicName), "one")
	assert.ErrorIs(t, err, ErrTopicTypeConflict)
	conflict := &TopicTypeConflictError{}
	require.ErrorAs(t, err, &conflict)
	assert.Equal(t, topicName, conflict.Topic)
	assert.Equal(t, reflect.TypeFor[int](), conflict.ExistingType)
	assert.Equal(t, reflect.TypeFor[string](), conflict.RequestedType)
}

func receiveInts(t *testing.T, sub *Subscription[int], count int) []int {
	t.Helper()
	values := make([]int, 0, count)
	for range count {
		select {
		case value := <-sub.Events:
			values = append(values, value)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for published value")
		}
	}
	return values
}

func assertEmpty(t *testing.T, sub *Subscription[int]) {
	t.Helper()
	select {
	case value := <-sub.Events:
		t.Fatalf("unexpected value %d", value)
	default:
	}
}

func makeRange(count int) []int {
	values := make([]int, count)
	for value := range count {
		values[value] = value
	}
	return values
}
