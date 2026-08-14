package fabrik

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type emptyStreamPayload struct{}

func (emptyStreamPayload) StreamType() StreamType { return "" }

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

func TestSubscriptionRejectsUnsupportedAndEmptyPayloadTypes(t *testing.T) {
	_, err := Subscribe[EventPayload](context.Background(), NewBus())
	assert.ErrorIs(t, err, ErrUnsupportedPayloadType)

	_, err = Subscribe[emptyStreamPayload](context.Background(), NewBus())
	assert.ErrorIs(t, err, ErrEmptyStreamType)
}

func TestNilSubscriptionCloseIsSafe(t *testing.T) {
	var subscription *Subscription[testPayload]
	assert.NotPanics(t, subscription.Close)
}
