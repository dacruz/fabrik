package pubsub

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicTypeIsStableForNamedAndPointerTypes(t *testing.T) {
	type named string
	assert.Equal(t, reflect.TypeFor[named](), NewTopic[named]("named").typ)
	assert.Equal(t, reflect.TypeFor[*orderCreated](), NewTopic[*orderCreated]("pointer").typ)
}
