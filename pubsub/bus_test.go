package pubsub

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

type orderCreated struct{ ID int }
type orderCancelled struct{ ID int }

func TestNewBusIsUsableAndEmpty(t *testing.T) {
	b := NewBus()
	if b == nil {
		t.Fatal("NewBus returned nil")
	}
	if err := Publish(b, NewTopic[string]("ready"), ""); err != nil {
		t.Fatalf("first registration failed: %v", err)
	}
}

func TestTopicsRegisterByExactNameAndType(t *testing.T) {
	b := NewBus()
	if _, err := Subscribe(b, NewTopic[orderCreated]("orders")); err != nil {
		t.Fatal(err)
	}
	if _, err := Subscribe(b, NewTopic[orderCreated]("orders")); err != nil {
		t.Fatalf("same type should be compatible: %v", err)
	}
	if err := Publish(b, NewTopic[orderCancelled]("orders-cancelled"), orderCancelled{}); err != nil {
		t.Fatal(err)
	}
}

func TestTopicTypeConflictIsInspectable(t *testing.T) {
	b := NewBus()
	name := "orders"
	if err := Publish(b, NewTopic[orderCreated](name), orderCreated{}); err != nil {
		t.Fatal(err)
	}
	_, err := Subscribe(b, NewTopic[orderCancelled](name))
	if !errors.Is(err, ErrTopicTypeConflict) {
		t.Fatalf("errors.Is conflict = false: %v", err)
	}
	var conflict *TopicTypeConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("errors.As conflict = false: %v", err)
	}
	if conflict.Topic != name || conflict.ExistingType != reflect.TypeOf(orderCreated{}) || conflict.RequestedType != reflect.TypeOf(orderCancelled{}) {
		t.Fatalf("unexpected conflict details: %#v", conflict)
	}
}

func TestEmptyTopicNameIsOpaque(t *testing.T) {
	b := NewBus()
	if err := Publish(b, NewTopic[int](""), 1); err != nil {
		t.Fatal(err)
	}
	if err := Publish(b, NewTopic[int](""), 2); err != nil {
		t.Fatalf("empty name should remain compatible: %v", err)
	}
	if err := Publish(b, NewTopic[string](""), ""); !errors.Is(err, ErrTopicTypeConflict) {
		t.Fatalf("empty name conflict = %v", err)
	}
}

func TestTopicTypeIsStableForNamedAndPointerTypes(t *testing.T) {
	type named string
	if got := NewTopic[named]("named").Type(); got != reflect.TypeOf(named("")) {
		t.Fatalf("named type = %v", got)
	}
	if got := NewTopic[*orderCreated]("pointer").Type(); got != reflect.TypeOf((*orderCreated)(nil)) {
		t.Fatalf("pointer type = %v", got)
	}
}

func TestConcurrentRegistration(t *testing.T) {
	b := NewBus()
	const workers = 100
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_, err := Subscribe(b, NewTopic[orderCreated]("orders"))
				errs <- err
				return
			}
			errs <- Publish(b, NewTopic[orderCreated]("orders"), orderCreated{})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("compatible concurrent registration failed: %v", err)
		}
	}
}
