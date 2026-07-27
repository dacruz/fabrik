# Plan 006: Concurrent multi-topic lifecycle workflow

**Status:** Pending

## Feature completed

The complete public PubSub workflow works safely across many topics and goroutines, including realistic publisher/consumer lifecycles, concurrent teardown, and graceful shutdown under load.

## Depends on

Plans 001 through 005.

## Implementation

- Add `pubsub/integration` tests using `package integration_test` and only the public API; this package is the user-visible workflow proving the feature.
- Cover a fan-out workflow with multiple publishers and consumers, exact topic isolation, deterministic buffer filling, concurrent unsubscription, and graceful shutdown with draining consumers.
- Add stress tests across many goroutines, multiple topics, repeated subscribe/unsubscribe cycles, full and draining buffers, and shutdown while publishers are active.
- Use synchronization primitives and explicit handshakes rather than sleeps or assumptions about goroutine scheduling.
- Run `go test ./...`, `go test -race ./...`, and suitable repeated stress runs; investigate and fix any race, deadlock, panic, or goroutine leak found.
- Add concise public API and lifecycle documentation to package comments and update `README.md` with construction, subscribe, publish, unsubscribe, backpressure, and shutdown examples.
- Verify the final file layout matches the spec and that tests assert behavior, not internal implementation details.

## Tests

- Integration fan-out and topic isolation.
- Backpressure with one slow and one healthy consumer.
- Concurrent unsubscribe during active publishing.
- Shutdown while publishers and consumers are active, including buffered drain.
- Repeated race-detector and stress runs with no timing-dependent failures.

## Completion criteria

The documented public workflow passes unit, integration, and race-detector tests, and the README/package docs accurately describe all supported behavior and deliberate non-goals.
