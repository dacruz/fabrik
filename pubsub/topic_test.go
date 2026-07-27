package pubsub

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopic_ItShouldPreserveNamedAndPointerTypes(t *testing.T) {
	type named string
	assert.Equal(t, reflect.TypeOf(named("")), NewTopic[named]("named").Type())
	assert.Equal(t, reflect.TypeOf((*orderCreated)(nil)), NewTopic[*orderCreated]("pointer").Type())
}
