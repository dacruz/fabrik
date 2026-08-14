package fabrik

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testPayload struct{}

func (testPayload) StreamType() StreamType { return "test.payload" }

type pointerTestPayload struct{}

func (*pointerTestPayload) StreamType() StreamType { return "pointer.test" }

type nilMapTestPayload map[string]string

func (nilMapTestPayload) StreamType() StreamType { return "nil-map.test" }

func TestEventZeroValue(t *testing.T) {
	var event Event[testPayload]

	assert.Empty(t, event.ID)
	assert.True(t, event.OccurredAt.IsZero())
	assert.Empty(t, event.CorrelationID)
	assert.Empty(t, event.CausationID)
	assert.Nil(t, event.Headers)
	assert.Equal(t, testPayload{}, event.Payload)
}

func TestEventHeadersCopyInputAndIsolateConsumers(t *testing.T) {
	input := map[string]string{"tenant": "one"}
	metadata := eventMetadata{}
	WithHeaders(input).applyEvent(&metadata)
	input["tenant"] = "mutated"

	assert.Equal(t, "one", metadata.Headers["tenant"])

	first := cloneHeaders(metadata.Headers)
	second := cloneHeaders(metadata.Headers)
	first["tenant"] = "consumer-one"
	assert.Equal(t, "consumer-one", first["tenant"])
	assert.Equal(t, "one", second["tenant"])
	assert.Equal(t, "one", metadata.Headers["tenant"])

	empty := eventMetadata{}
	WithHeaders(map[string]string{}).applyEvent(&empty)
	assert.NotNil(t, empty.Headers)

	nilHeaders := eventMetadata{}
	WithHeaders(nil).applyEvent(&nilHeaders)
	assert.Nil(t, nilHeaders.Headers)
}

func TestEventOptionsApplyMetadataInOrder(t *testing.T) {
	at := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	metadata := eventMetadata{}
	WithOccurredAt(at).applyEvent(&metadata)
	WithCorrelationID("request-1").applyEvent(&metadata)
	WithCausationID("event-1").applyEvent(&metadata)
	WithHeaders(map[string]string{"tenant": "acme", "region": "eu"}).applyEvent(&metadata)
	WithHeaders(map[string]string{"region": "us"}).applyEvent(&metadata)

	assert.Equal(t, at, metadata.OccurredAt)
	assert.Equal(t, "request-1", metadata.CorrelationID)
	assert.Equal(t, "event-1", metadata.CausationID)
	assert.Equal(t, map[string]string{"tenant": "acme", "region": "us"}, metadata.Headers)
}

func TestStructuredErrorsExposeStableMessagesAndCategories(t *testing.T) {
	delivery := &DeliveryError{StreamType: "orders", Dropped: 2}
	assert.Equal(t, `fabrik: dropped 2 delivery(s) for stream type "orders"`, delivery.Error())
	assert.ErrorIs(t, delivery, ErrDelivery)

	conflict := &StreamTypeConflictError{
		StreamType: "orders",
		Existing:   reflect.TypeFor[testPayload](),
		Requested:  reflect.TypeFor[conflictingTestPayload](),
	}
	assert.Equal(t, `fabrik: stream type "orders" is registered for fabrik.testPayload, cannot use fabrik.conflictingTestPayload`, conflict.Error())
	assert.ErrorIs(t, conflict, ErrStreamTypeConflict)
	assert.True(t, errors.Is(conflict, ErrStreamTypeConflict))
}

type conflictingTestPayload struct{}

func (conflictingTestPayload) StreamType() StreamType { return "conflicting.test" }

func cloneHeaders(headers map[string]string) map[string]string {
	copy := make(map[string]string, len(headers))
	for key, value := range headers {
		copy[key] = value
	}
	return copy
}
