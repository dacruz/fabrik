package fabrik

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkBusEmitFanOut(b *testing.B) {
	for _, subscriberCount := range []int{0, 1, 10, 100} {
		for _, withHeaders := range []bool{false, true} {
			name := fmt.Sprintf("subscribers=%d/headers=%t", subscriberCount, withHeaders)
			b.Run(name, func(b *testing.B) {
				bus := NewBus()
				subscriptions := make([]*Subscription[deliveryEvent], 0, subscriberCount)
				for range subscriberCount {
					subscription, err := Subscribe[deliveryEvent](context.Background(), bus, WithBufferSize(1))
					if err != nil {
						b.Fatal(err)
					}
					subscriptions = append(subscriptions, subscription)
				}

				var options []EventOption
				if withHeaders {
					options = []EventOption{WithHeaders(map[string]string{
						"tenant": "acme",
						"trace":  "01JTEST",
						"region": "eu-west",
					})}
				}

				b.ReportAllocs()
				b.ResetTimer()
				for number := 0; number < b.N; number++ {
					if err := bus.Emit(context.Background(), deliveryEvent{Number: number}, options...); err != nil {
						b.Fatal(err)
					}
					for _, subscription := range subscriptions {
						<-subscription.Events
					}
				}
			})
		}
	}
}

func BenchmarkBusEmitParallel(b *testing.B) {
	for _, streamCount := range []int{1, 3} {
		b.Run(fmt.Sprintf("streams=%d", streamCount), func(b *testing.B) {
			bus := NewBus()
			ctx := context.Background()
			if err := bus.Emit(ctx, stressOrder{}); err != nil {
				b.Fatal(err)
			}
			if streamCount == 3 {
				if err := bus.Emit(ctx, stressInvoice{}); err != nil {
					b.Fatal(err)
				}
				if err := bus.Emit(ctx, stressShipment{}); err != nil {
					b.Fatal(err)
				}
			}

			var sequence atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					number := int(sequence.Add(1))
					var err error
					switch number % streamCount {
					case 0:
						err = bus.Emit(ctx, stressOrder{Number: number})
					case 1:
						err = bus.Emit(ctx, stressInvoice{Number: number})
					case 2:
						err = bus.Emit(ctx, stressShipment{Number: number})
					}
					if err != nil {
						b.Error(err)
					}
				}
			})
		})
	}
}
