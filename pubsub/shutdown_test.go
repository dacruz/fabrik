package pubsub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShutdownPreservesQueuedValuesBeforeClosingSubscriptions(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	sub, err := Subscribe(b, topic)
	require.NoError(t, err)

	for value := range 3 {
		value++
		require.NoError(t, Publish(b, topic, value))
	}
	require.NoError(t, b.Shutdown(context.Background()))

	values := make([]int, 0, 3)
	for value := range sub.Events {
		values = append(values, value)
	}
	assert.Equal(t, []int{1, 2, 3}, values)

	b.mu.RLock()
	assert.Equal(t, busClosed, b.state)
	assert.Nil(t, b.topics)
	b.mu.RUnlock()
	sub.Unsubscribe()
}

func TestShutdownRejectsPublishAndSubscribeAfterShutdown(t *testing.T) {
	b := NewBus()
	require.NoError(t, b.Shutdown(context.Background()))

	topic := NewTopic[int]("orders")
	err := Publish(b, topic, 1)
	assert.ErrorIs(t, err, ErrBusClosed)

	sub, err := Subscribe(b, topic)
	assert.Nil(t, sub)
	assert.ErrorIs(t, err, ErrBusClosed)
}

func TestShutdownIsIdempotentAndSafeConcurrently(t *testing.T) {
	b := NewBus()
	sub, err := Subscribe(b, NewTopic[int]("orders"))
	require.NoError(t, err)

	const callers = 100
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			errs <- b.Shutdown(context.Background())
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err)
	}

	_, ok := <-sub.Events
	assert.False(t, ok)
	assert.NoError(t, b.Shutdown(context.Background()), "calls after completion return the first result")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NoError(t, b.Shutdown(canceled), "completed shutdown remains idempotent")
}

func TestShutdownHonorsCancellationBeforeStarting(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.Shutdown(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NoError(t, Publish(b, topic, 1), "canceled shutdown did not cross the lifecycle boundary")
	assert.NoError(t, b.Shutdown(context.Background()))
}

func TestShutdownRejectsNilContext(t *testing.T) {
	b := NewBus()

	assert.ErrorIs(t, b.Shutdown(nil), ErrNilContext)
	assert.NoError(t, Publish(b, NewTopic[int]("orders"), 1))
}

func TestShutdownHonorsCancellationWhileWaitingForTheBusLock(t *testing.T) {
	b := NewBus()
	b.mu.Lock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Shutdown(ctx) }()

	time.Sleep(5 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("shutdown did not honor cancellation while waiting for the bus lock")
	}
	b.mu.Unlock()
	assert.NoError(t, b.Shutdown(context.Background()))
}

func TestShutdownHonorsCancellationWhileWaitingForAnotherShutdown(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	sub, err := Subscribe(b, topic)
	require.NoError(t, err)
	state := b.topics[topic.name]
	state.mu.Lock()

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- b.Shutdown(context.Background())
	}()
	waitForBusState(t, b, busShuttingDown)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = b.Shutdown(ctx)
	assert.ErrorIs(t, err, context.Canceled)

	state.mu.Unlock()
	assert.NoError(t, <-firstDone)
	assert.Eventually(t, func() bool {
		select {
		case _, ok := <-sub.Events:
			return !ok
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestConsumersFinishAfterBusTeardown(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	sub, err := Subscribe(b, topic)
	require.NoError(t, err)
	require.NoError(t, Publish(b, topic, 1))

	consumed := make(chan []int, 1)
	go func() {
		values := make([]int, 0, 1)
		for value := range sub.Events {
			values = append(values, value)
		}
		consumed <- values
	}()

	require.NoError(t, b.Shutdown(context.Background()))
	select {
	case values := <-consumed:
		assert.Equal(t, []int{1}, values)
	case <-time.After(time.Second):
		t.Fatal("consumer remained blocked after shutdown")
	}
}

func TestConcurrentTeardownExcludesSendsAndRegistrations(t *testing.T) {
	b := NewBus()
	topic := NewTopic[int]("orders")
	initial, err := Subscribe(b, topic)
	require.NoError(t, err)

	const publishers = 8
	const publishesPerWorker = 200
	const subscriptionWorkers = 8
	const subscriptionsPerWorker = 100
	operationErrors := make(chan error, publishers*publishesPerWorker+subscriptionWorkers*subscriptionsPerWorker)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for publisher := range publishers {
		wg.Go(func() {
			<-start
			for value := range publishesPerWorker {
				err := Publish(b, topic, publisher*publishesPerWorker+value)
				if err != nil && !errors.Is(err, ErrBusClosed) && !errors.Is(err, ErrDelivery) {
					operationErrors <- err
				}
			}
		})
	}
	for range subscriptionWorkers {
		wg.Go(func() {
			<-start
			for range subscriptionsPerWorker {
				sub, err := Subscribe(b, topic)
				if err != nil {
					if !errors.Is(err, ErrBusClosed) {
						operationErrors <- err
					}
					continue
				}
				sub.Unsubscribe()
			}
		})
	}

	close(start)
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- b.Shutdown(context.Background()) }()
	wg.Go(func() {
		initial.Unsubscribe()
	})

	wg.Wait()
	assert.NoError(t, <-shutdownDone)
	close(operationErrors)
	for err := range operationErrors {
		t.Errorf("unexpected concurrent operation error: %v", err)
	}
	assert.ErrorIs(t, Publish(b, topic, 0), ErrBusClosed)
}

func waitForBusState(t *testing.T, b *Bus, want busState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		got := b.state
		b.mu.RUnlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("bus did not reach state %d", want)
}
