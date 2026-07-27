# Plan 001: Bus and typed topic registry

**Status:** Completed

## Feature completed

Create the process-local bus and typed topic model. A caller can construct a bus, create topics, and register compatible topics while incompatible reuse of a topic name is rejected with an inspectable error.

## Depends on

Nothing. This is the foundation for the remaining plans.

## Implementation

- [x] Add the `pubsub` package and a bus constructor (for example, `NewBus() *Bus`) with an empty topic registry and an open lifecycle state.
- [x] Add `Topic[T any]` and `NewTopic[T any](name string) Topic[T]`.
- [x] Store the event type in a topic using `reflect.Type`; use a stable type representation for named and pointer types.
- [x] Add internal per-topic state with its own mutex and subscriber map, while protecting the bus registry with `sync.RWMutex`.
- [x] Define sentinel/typed errors in `errors.go`, including a topic-type conflict error that works with `errors.Is` or `errors.As` and reports the topic name and both types.
- [x] Choose and document registration semantics: registration occurs on the first `Subscribe` or `Publish` operation, because `NewTopic` cannot return an error. Reusing a name with the same type is valid.
- [x] Treat topic names as opaque exact strings, including allowing an empty name; cover that decision with a test.

## Tests

- [x] Constructor creates a usable empty bus.
- [x] Topics with distinct names and types coexist.
- [x] Same name plus same type is compatible.
- [x] Same name plus different type is rejected through the chosen registration path, with `errors.Is`/`errors.As` assertions rather than string matching.
- [x] Registry operations are safe under concurrent registration.

## Completion criteria

The package compiles and its public topic/registry tests pass; no publishing or subscription behavior is included in this plan.
