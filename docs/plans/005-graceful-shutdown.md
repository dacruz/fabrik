# Plan 005: Graceful bus shutdown

**Status:** Completed

## Feature completed

The bus can shut down gracefully: it rejects new publishes, closes active subscription channels after preserving queued values, releases bus-owned state, and supports idempotent/concurrent shutdown with context cancellation.

## Depends on

Plans 001 through 004.

## Implementation

- [x] Add a closed/shutting-down bus state guarded by the bus mutex and a sentinel shutdown error usable with `errors.Is`.
- [x] Implement `Shutdown(ctx context.Context, b *Bus) error`.
- [x] Transition to shutdown before closing subscriptions so no new publish or subscription can race past the lifecycle boundary.
- [x] Snapshot topics/subscribers under the appropriate locks, remove subscriber registrations, and close each subscription channel exactly once after all sends for that subscriber are excluded.
- [x] Leave queued channel values available for consumers to drain with `range`.
- [x] Make repeated and concurrent shutdown calls safe. Define whether later calls return nil or the first result and test that contract.
- [x] Honor context expiry while waiting for bus-side shutdown work; do not claim that application handlers have finished processing.
- [x] Ensure shutdown with no topics/subscribers is successful.

## Tests

- [x] Publish, shut down, and drain all queued values before channel closure.
- [x] Publish after shutdown returns the shutdown error.
- [x] Subscribe after shutdown is rejected consistently.
- [x] Shutdown is idempotent and safe when called concurrently.
- [x] Concurrent publish, subscribe, unsubscribe, and shutdown produce no panic or send-on-closed-channel race.
- [x] Context cancellation returns the documented context error where applicable.
- [x] Verify no bus-owned goroutines remain blocked after shutdown.

## Completion criteria

Consumers can drain and finish cleanly after shutdown, lifecycle errors are inspectable, and shutdown tests pass with `go test -race ./...`.
