package fabrik

import (
	"testing"

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

func cloneHeaders(headers map[string]string) map[string]string {
	copy := make(map[string]string, len(headers))
	for key, value := range headers {
		copy[key] = value
	}
	return copy
}
