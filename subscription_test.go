package fabrik

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionResolvesValueAndPointerStreamIdentity(t *testing.T) {
	valueStream, err := streamTypeOf[testPayload]()
	require.NoError(t, err)
	assert.Equal(t, StreamType("test.payload"), valueStream)

	pointerStream, err := streamTypeOf[*pointerTestPayload]()
	require.NoError(t, err)
	assert.Equal(t, StreamType("pointer.test"), pointerStream)

	assert.Equal(t, reflect.Pointer, reflect.TypeFor[*pointerTestPayload]().Kind())
}

func TestSubscriptionRejectsInvalidBufferSize(t *testing.T) {
	_, err := Subscribe[testPayload](context.Background(), NewBus(), WithBufferSize(0))
	assert.ErrorIs(t, err, ErrInvalidBufferSize)
}
