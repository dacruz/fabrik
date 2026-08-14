package fabrik

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stressOrder struct{ Number int }

func (stressOrder) StreamType() StreamType { return "stress.orders" }

type stressInvoice struct{ Number int }

func (stressInvoice) StreamType() StreamType { return "stress.invoices" }

type stressShipment struct{ Number int }

func (stressShipment) StreamType() StreamType { return "stress.shipments" }

func TestBusHandlesConcurrentProducersAndConsumersAcrossStreams(t *testing.T) {
	type streamConfig struct {
		streamIndex int
		emit        func(context.Context, int) error
		consume     func() int
		totalEvents int
	}
	type streamResult struct {
		streamIndex int
		values      []int
	}

	const producers = 4
	const eventsPerProducer = 100
	totalEvents := producers * eventsPerProducer
	ctx := context.Background()
	bus := NewBus()

	orders, err := Subscribe[stressOrder](ctx, bus, WithBufferSize(totalEvents))
	require.NoError(t, err)
	invoices, err := Subscribe[stressInvoice](ctx, bus, WithBufferSize(totalEvents))
	require.NoError(t, err)
	shipments, err := Subscribe[stressShipment](ctx, bus, WithBufferSize(totalEvents))
	require.NoError(t, err)

	streams := []streamConfig{
		{
			streamIndex: 0, totalEvents: totalEvents,
			emit: func(ctx context.Context, number int) error {
				return bus.Emit(ctx, stressOrder{Number: number})
			},
			consume: func() int {
				return (<-orders.Events).Payload.Number
			},
		},
		{
			streamIndex: 1, totalEvents: totalEvents,
			emit: func(ctx context.Context, number int) error {
				return bus.Emit(ctx, stressInvoice{Number: number})
			},
			consume: func() int {
				return (<-invoices.Events).Payload.Number
			},
		},
		{
			streamIndex: 2, totalEvents: totalEvents,
			emit: func(ctx context.Context, number int) error {
				return bus.Emit(ctx, stressShipment{Number: number})
			},
			consume: func() int {
				return (<-shipments.Events).Payload.Number
			},
		},
	}

	producerErrors := make(chan error, len(streams)*producers*eventsPerProducer)
	consumerValues := make(chan streamResult, len(streams))
	var consumers sync.WaitGroup
	for _, stream := range streams {
		stream := stream
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			values := make([]int, 0, stream.totalEvents)
			for range stream.totalEvents {
				values = append(values, stream.consume())
			}
			consumerValues <- streamResult{streamIndex: stream.streamIndex, values: values}
		}()
	}

	var producersGroup sync.WaitGroup
	for streamIndex, stream := range streams {
		streamIndex, stream := streamIndex, stream
		for producer := range producers {
			producer := producer
			producersGroup.Add(1)
			go func() {
				defer producersGroup.Done()
				for eventNumber := range eventsPerProducer {
					number := streamIndex*totalEvents + producer*eventsPerProducer + eventNumber
					producerErrors <- stream.emit(ctx, number)
				}
			}()
		}
	}
	producersGroup.Wait()
	close(producerErrors)
	for producerErr := range producerErrors {
		assert.NoError(t, producerErr)
	}

	consumers.Wait()
	close(consumerValues)
	for result := range consumerValues {
		assert.Len(t, result.values, totalEvents)
		expected := make([]int, totalEvents)
		for number := range totalEvents {
			expected[number] = result.streamIndex*totalEvents + number
		}
		assert.ElementsMatch(t, expected, result.values)
	}
}
