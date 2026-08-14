package fabrik

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type phaseOnePayload struct{}

func (phaseOnePayload) StreamType() StreamType { return "phase1.payload" }

type phaseOnePointerPayload struct{}

func (*phaseOnePointerPayload) StreamType() StreamType { return "phase1.pointer" }

type phaseOneNilPayload map[string]string

func (phaseOneNilPayload) StreamType() StreamType { return "phase1.nil-map" }

func TestEventZeroValue(t *testing.T) {
	var event Event[phaseOnePayload]

	if event.ID != "" || !event.OccurredAt.IsZero() || event.CorrelationID != "" || event.CausationID != "" {
		t.Fatalf("unexpected zero event metadata: %#v", event)
	}
	if event.Headers != nil || event.Payload != (phaseOnePayload{}) {
		t.Fatalf("unexpected zero event values: %#v", event)
	}
}

func TestEmitRejectsNilPayloads(t *testing.T) {
	b := NewBus()
	ctx := context.Background()

	var pointer *phaseOnePointerPayload
	if err := b.Emit(ctx, pointer); !errors.Is(err, ErrNilPayload) {
		t.Fatalf("typed nil pointer error = %v, want ErrNilPayload", err)
	}

	var payload phaseOneNilPayload
	if err := b.Emit(ctx, payload); !errors.Is(err, ErrNilPayload) {
		t.Fatalf("typed nil map error = %v, want ErrNilPayload", err)
	}
}

func TestWithHeadersCopiesAndIsolatesMetadata(t *testing.T) {
	input := map[string]string{"tenant": "one"}
	metadata := eventMetadata{}
	WithHeaders(input).applyEvent(&metadata)
	input["tenant"] = "mutated"

	if metadata.Headers["tenant"] != "one" {
		t.Fatalf("option retained caller map: %#v", metadata.Headers)
	}

	first := cloneHeaders(metadata.Headers)
	second := cloneHeaders(metadata.Headers)
	first["tenant"] = "consumer-one"
	if second["tenant"] != "one" || metadata.Headers["tenant"] != "one" {
		t.Fatalf("header copies are not isolated: first=%v second=%v source=%v", first, second, metadata.Headers)
	}

	empty := eventMetadata{}
	WithHeaders(map[string]string{}).applyEvent(&empty)
	if empty.Headers == nil {
		t.Fatal("non-nil empty headers must remain non-nil")
	}

	nilHeaders := eventMetadata{}
	WithHeaders(nil).applyEvent(&nilHeaders)
	if nilHeaders.Headers != nil {
		t.Fatal("nil headers must be a no-op")
	}
}

func TestStreamTypeIdentitySupportsValuesAndPointers(t *testing.T) {
	valueStream, err := streamTypeOf[phaseOnePayload]()
	if err != nil || valueStream != "phase1.payload" {
		t.Fatalf("value stream = %q, err = %v", valueStream, err)
	}

	pointerStream, err := streamTypeOf[*phaseOnePointerPayload]()
	if err != nil || pointerStream != "phase1.pointer" {
		t.Fatalf("pointer stream = %q, err = %v", pointerStream, err)
	}

	if got := reflect.TypeFor[*phaseOnePointerPayload](); got.Kind() != reflect.Pointer {
		t.Fatalf("test payload type kind = %v, want pointer", got.Kind())
	}
}

func TestPhaseOneValidationIdentities(t *testing.T) {
	if err := NewBus().Emit(context.Background(), phaseOnePayload{}, WithOccurredAt(time.Time{})); !errors.Is(err, ErrInvalidOccurredAt) {
		t.Fatalf("invalid timestamp error = %v, want ErrInvalidOccurredAt", err)
	}
	if _, err := Subscribe[phaseOnePayload](context.Background(), NewBus(), WithBufferSize(0)); !errors.Is(err, ErrInvalidBufferSize) {
		t.Fatalf("invalid buffer error = %v, want ErrInvalidBufferSize", err)
	}
}

func cloneHeaders(headers map[string]string) map[string]string {
	copy := make(map[string]string, len(headers))
	for key, value := range headers {
		copy[key] = value
	}
	return copy
}
