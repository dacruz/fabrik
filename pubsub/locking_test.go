package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBusTopicOperationDoesNotRetainBusLock(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- b.withTopic(topic.name, topic.typ, func(*topicState) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	busLockAcquired := make(chan struct{})
	go func() {
		b.mu.Lock()
		close(busLockAcquired)
		b.mu.Unlock()
	}()

	select {
	case <-busLockAcquired:
	case <-time.After(time.Second):
		t.Fatal("topic operation retained the global bus lock")
	}
	close(release)
	require.NoError(t, <-done)
}

func TestBusUnrelatedTopicsPublishConcurrently(t *testing.T) {
	b := NewBus()
	topicA := NewTopic[int]("a")
	topicB := NewTopic[int]("b")
	stateA := &topicState{eventType: topicA.typ, subscribers: make(map[uint64]subscriber)}
	b.mu.Lock()
	b.topics[topicA.name] = stateA
	b.mu.Unlock()

	stateA.mu.Lock()
	publishADone := make(chan error, 1)
	go func() { publishADone <- Publish(b, topicA, 1) }()
	waitForActiveOperations(t, b, 1)

	publishBDone := make(chan error, 1)
	go func() { publishBDone <- Publish(b, topicB, 2) }()
	select {
	case err := <-publishBDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("publishing on an unrelated topic was blocked")
	}

	stateA.mu.Unlock()
	require.NoError(t, <-publishADone)
}

func TestBusPublishSubscribeShutdownLifecycleBoundary(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		b := NewBus()
		topic := NewTopic[int]("orders")
		_, err := Subscribe(b, topic)
		require.NoError(t, err)

		start := make(chan struct{})
		var wg sync.WaitGroup
		shutdownErr := make(chan error, 1)
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			_ = Publish(b, topic, iteration)
		}()
		go func() {
			defer wg.Done()
			<-start
			sub, subscribeErr := Subscribe(b, topic)
			if subscribeErr == nil {
				sub.Unsubscribe()
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			shutdownErr <- b.Shutdown(context.Background())
		}()
		close(start)
		wg.Wait()
		require.NoError(t, <-shutdownErr)
	}
}

func waitForActiveOperations(t *testing.T, b *Bus, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		active := b.activeOps
		b.mu.Unlock()
		if active >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("bus did not reach %d active operations", want)
}
