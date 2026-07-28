package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dacruz/fabrik/client"
	"github.com/dacruz/fabrik/pubsub"
)

func TestWorkflowShutdownClosesActivePublishersAndDrainsConsumers(t *testing.T) {
	b := pubsub.NewBus()
	drain := pubsub.NewTopic[int]("drain")
	active := pubsub.NewTopic[int]("active")
	consumer, err := client.NewConsumerClient(b, drain)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []int{10, 20, 30, 40} {
		if err := client.NewProducerClient(b, drain).Publish(value); err != nil {
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
		err := consumer.Run(context.Background(), func(_ context.Context, value int) error { values = append(values, value); return nil })
		if err != nil {
			t.Errorf("consumer run: %v", err)
		}
		consumed <- values
	}()
	<-consumerStarted

	start := make(chan struct{})
	firstPublish := make(chan struct{}, 1)
	stop := make(chan struct{})
	var publishers sync.WaitGroup
	for publisher := range 4 {
		publishers.Go(func() {
			<-start
			for value := range 1000 {
				if err := client.NewProducerClient(b, active).Publish(publisher*1000 + value); err != nil && !errors.Is(err, pubsub.ErrBusClosed) {
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
		})
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
