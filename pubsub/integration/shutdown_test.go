package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dacruz/fabrik/pubsub"
)

func TestWorkflowShutdownClosesActivePublishersAndDrainsConsumers(t *testing.T) {
	b := pubsub.NewBus()
	drainTopic := pubsub.NewTopic[int]("drain")
	activeTopic := pubsub.NewTopic[int]("active")
	sub, err := pubsub.Subscribe(b, drainTopic)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []int{10, 20, 30, 40} {
		if err := pubsub.Publish(b, drainTopic, value); err != nil {
			t.Fatal(err)
		}
	}
	consumerStarted := make(chan struct{})
	consumerRelease := make(chan struct{})
	consumed := make(chan []int, 1)
	go func() {
		close(consumerStarted)
		<-consumerRelease
		values := make([]int, 0, 4)
		for value := range sub.Events {
			values = append(values, value)
		}
		consumed <- values
	}()
	<-consumerStarted

	start := make(chan struct{})
	firstPublish := make(chan struct{}, 1)
	stop := make(chan struct{})
	var publishers sync.WaitGroup
	for publisher := 0; publisher < 4; publisher++ {
		publishers.Add(1)
		go func(id int) {
			defer publishers.Done()
			<-start
			for value := 0; value < 1000; value++ {
				if err := pubsub.Publish(b, activeTopic, id*1000+value); err != nil && !errors.Is(err, pubsub.ErrBusClosed) {
					t.Errorf("publish during shutdown: %v", err)
				}
				select {
				case firstPublish <- struct{}{}:
				default:
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}(publisher)
	}
	close(start)
	<-firstPublish
	if err := pubsub.Shutdown(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	close(stop)
	close(consumerRelease)
	publishers.Wait()

	select {
	case values := <-consumed:
		assertEqualValues(t, values, []int{10, 20, 30, 40})
	case <-time.After(time.Second):
		t.Fatal("consumer did not finish after shutdown")
	}
}
