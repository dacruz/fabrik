package pubsub

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscription_ItShouldStartEmptyAndBuffered(t *testing.T) {
	b := NewBus()
	sub, err := Subscribe(b, NewTopic[int]("orders"))
	require.NoError(t, err)
	defer sub.Unsubscribe()

	assert.Equal(t, subscriptionBufferSize, cap(sub.Events))
	select {
	case value := <-sub.Events:
		t.Fatalf("new subscription unexpectedly contained %d", value)
	default:
	}
}

func TestSubscription_ItShouldTrackTwoSubscriptionsIndependently(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	first, err := Subscribe(b, topic)
	require.NoError(t, err)
	second, err := Subscribe(b, topic)
	require.NoError(t, err)

	assert.Len(t, b.topics[topic.Name()].subscribers, 2)
	first.Unsubscribe()
	assert.Len(t, b.topics[topic.Name()].subscribers, 1)
	select {
	case _, ok := <-first.Events:
		assert.False(t, ok)
	default:
		t.Fatal("unsubscribed subscription channel is not closed")
	}
	assert.NotPanics(t, second.Unsubscribe)
}

func TestSubscription_ItShouldAllowIdempotentConcurrentUnsubscribe(t *testing.T) {
	b := NewBus()
	sub, err := Subscribe(b, NewTopic[int]("orders"))
	require.NoError(t, err)

	const workers = 100
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			sub.Unsubscribe()
		})
	}
	wg.Wait()
	sub.Unsubscribe()

	assert.Empty(t, b.topics["orders"].subscribers)
	value, ok := <-sub.Events
	assert.Zero(t, value)
	assert.False(t, ok)
}

func TestSubscription_ItShouldWaitForAnActivePublishCriticalSectionBeforeUnsubscribe(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	sub, err := Subscribe(b, topic)
	require.NoError(t, err)

	state := b.topics[topic.Name()]
	state.mu.Lock()
	sub.events <- 7
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		sub.Unsubscribe()
		close(done)
	}()
	<-started

	select {
	case <-done:
		t.Fatal("unsubscribe completed while the topic lock was held")
	default:
	}
	state.mu.Unlock()

	select {
	case value, ok := <-sub.Events:
		require.True(t, ok)
		assert.Equal(t, 7, value)
	case <-time.After(time.Second):
		t.Fatal("timed out reading queued value")
	}
	select {
	case _, ok := <-sub.Events:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("subscription channel did not close")
	}
	<-done
}

func TestSubscription_ItShouldExcludeLaterPublishesAfterUnsubscribe(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	sub, err := Subscribe(b, topic)
	require.NoError(t, err)
	sub.Unsubscribe()

	require.NoError(t, Publish(b, topic, 8))
	select {
	case value, ok := <-sub.Events:
		require.False(t, ok)
		assert.Zero(t, value)
	case <-time.After(time.Second):
		t.Fatal("unsubscribed subscription channel did not close")
	}
}

func TestSubscription_ItShouldKeepTheTopicRegisteredAfterRemovingTheLastSubscriber(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	first, err := Subscribe(b, topic)
	require.NoError(t, err)
	first.Unsubscribe()

	assert.Contains(t, b.topics, topic.Name())
	second, err := Subscribe(b, topic)
	require.NoError(t, err)
	assert.NotNil(t, second)
	second.Unsubscribe()
}

func TestSubscription_ItShouldHandleNilBus(t *testing.T) {
	sub, err := Subscribe[int](nil, NewTopic[int]("orders"))

	assert.Nil(t, sub)
	assert.ErrorIs(t, err, ErrNilBus)
}

func TestSubscription_ItShouldAllowNilUnsubscribe(t *testing.T) {
	var sub *Subscription[int]

	assert.NotPanics(t, sub.Unsubscribe)
}
