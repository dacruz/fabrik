package pubsub

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type orderCreated struct{ ID int }
type orderCancelled struct{ ID int }

func TestNewBusIsUsableAndEmpty(t *testing.T) {
	b := NewBus()
	if !assert.NotNil(t, b, "NewBus should return a usable bus") {
		return
	}
	assert.NoError(t, Publish(b, NewTopic[string]("ready"), ""))
}

func TestTopicsRegisterByExactNameAndType(t *testing.T) {
	b := NewBus()
	_, err := Subscribe(b, NewTopic[orderCreated]("orders"))
	assert.NoError(t, err)
	_, err = Subscribe(b, NewTopic[orderCreated]("orders"))
	assert.NoError(t, err, "same type should be compatible")
	assert.NoError(t, Publish(b, NewTopic[orderCancelled]("orders-cancelled"), orderCancelled{}))
}

func TestTopicTypeConflictIsInspectable(t *testing.T) {
	b := NewBus()
	name := "orders"
	assert.NoError(t, Publish(b, NewTopic[orderCreated](name), orderCreated{}))
	_, err := Subscribe(b, NewTopic[orderCancelled](name))
	assert.ErrorIs(t, err, ErrTopicTypeConflict)
	conflict := &TopicTypeConflictError{}
	assert.ErrorAs(t, err, &conflict)
	assert.Equal(t, name, conflict.Topic)
	assert.Equal(t, reflect.TypeFor[orderCreated](), conflict.ExistingType)
	assert.Equal(t, reflect.TypeFor[orderCancelled](), conflict.RequestedType)
}

func TestEmptyTopicNameIsOpaque(t *testing.T) {
	b := NewBus()
	assert.NoError(t, Publish(b, NewTopic[int](""), 1))
	assert.NoError(t, Publish(b, NewTopic[int](""), 2), "empty name should remain compatible")
	assert.ErrorIs(t, Publish(b, NewTopic[string](""), ""), ErrTopicTypeConflict)
}

func TestConcurrentRegistration(t *testing.T) {
	b := NewBus()
	const workers = 100
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Go(func() {
			if i%2 == 0 {
				_, err := Subscribe(b, NewTopic[orderCreated]("orders"))
				errs <- err
				return
			}
			errs <- Publish(b, NewTopic[orderCreated]("orders"), orderCreated{})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err, "compatible concurrent registration should succeed")
	}
}

func TestNilBusDuringShutdownIsHandled(t *testing.T) {
	assert.ErrorIs(t, Shutdown(context.Background(), nil), ErrNilBus)
}

func TestPublicErrorsAreFormatted(t *testing.T) {
	conflict := &TopicTypeConflictError{
		Topic:         "orders",
		ExistingType:  reflect.TypeFor[int](),
		RequestedType: reflect.TypeFor[string](),
	}
	delivery := &DeliveryError{Topic: "orders", Dropped: 2}

	assert.Equal(t, `pubsub: topic "orders" is registered for int, cannot use string`, conflict.Error())
	assert.Equal(t, `pubsub: dropped 2 delivery(s) for topic "orders"`, delivery.Error())
}
