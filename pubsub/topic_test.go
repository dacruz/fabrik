package pubsub

import (
	"reflect"
	"testing"
)

func TestTopic_ItShouldPreserveNamedAndPointerTypes(t *testing.T) {
	type named string
	if got := NewTopic[named]("named").Type(); got != reflect.TypeOf(named("")) {
		t.Fatalf("named type = %v", got)
	}
	if got := NewTopic[*orderCreated]("pointer").Type(); got != reflect.TypeOf((*orderCreated)(nil)) {
		t.Fatalf("pointer type = %v", got)
	}
}
