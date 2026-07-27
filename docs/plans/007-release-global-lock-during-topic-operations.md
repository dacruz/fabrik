# Plan 007: Release the global bus lock during topic operations

**Status:** Completed

## Feature

Remove the global bus lock from the publish and subscribe critical paths after
topic lookup or registration. Topic delivery and subscription bookkeeping must
use the topic's own mutex, while the bus lock remains responsible only for the
topic registry and lifecycle boundary.

This aligns the implementation with Plan 003: unrelated topics must remain
independent, and topic fan-out must not execute while holding the global bus
lock.

## Problem

`Bus.withTopic` currently holds `b.mu.RLock()` while it calls the supplied
operation. That means `Publish` holds the global read lock through the complete
fan-out, and `Subscribe` holds it while adding a subscriber. Shutdown cannot
transition the bus to `busShuttingDown` until those operations return.

The current sends are non-blocking, so the existing tests pass. The locking
contract is still wrong, and the coupling makes shutdown latency grow with
topic work. It also makes future changes to delivery behavior unsafe unless
they happen to remain non-blocking.

## Constraints and required semantics

- Preserve the existing public API and error identities.
- Preserve exact topic/type registration semantics.
- Preserve per-topic publication ordering.
- Preserve the guarantee that shutdown cannot close a subscription channel
  while a publish is sending to it.
- Preserve the guarantee that a publish or subscription operation crossing the
  lifecycle boundary is completed before shutdown closes affected channels.
- Do not hold `Bus.mu` while calling `topicState.publish`,
  `topicState.addSubscriber`, or any subscriber send/close callback.
- Keep unrelated topics independently publishable.
- Keep repeated and concurrent shutdown safe and idempotent.

## Implementation

- [x] Split topic lookup/registration from topic operation execution in
  `bus.go`.
- [x] Define the synchronization boundary that prevents a topic operation from
  starting after shutdown transitions to `busShuttingDown`.
- [x] Move publish and subscribe execution behind the topic mutex, without
  retaining the global bus lock during the operation.
- [x] Ensure shutdown snapshots the registered topics under `Bus.mu`, then
  closes subscribers under each topic mutex after all operations that crossed
  the boundary are excluded.
- [x] Re-check lifecycle state at every point where a new topic can be created
  or an existing topic can be operated on.
- [x] Keep topic type validation atomic with the topic operation so a type
  conflict cannot be bypassed during concurrent lookup or shutdown.
- [x] Update comments in `bus.go` and package documentation if the final
  locking model differs from the current wording.

## Tests

- [x] Add a regression test proving a publish on topic A does not retain the
  global bus lock while topic A delivery is in progress.
- [x] Add a multi-topic concurrency test showing topic B can publish while
  topic A is active.
- [x] Add a shutdown race test covering publish, subscribe, and shutdown at
  the lifecycle boundary, asserting no send-on-closed-channel panic or race.
- [x] Retain and run the existing per-topic ordering, unsubscribe, backpressure,
  and buffered shutdown tests.
- [x] Run `go test ./...`, `go test -race ./...`, `go vet ./...`, and repeated
  stress runs such as `go test -race -count=50 ./...`.

## Completion criteria

- `Bus.mu` is not held during topic delivery or subscriber callbacks.
- Concurrent operations on unrelated topics do not wait on the global bus
  lock.
- Shutdown remains race-free, idempotent, and able to close all active
  subscriptions after the lifecycle boundary.
- All existing and new tests pass under the race detector.
- The implementation and docs no longer claim a lock behavior that the code
  does not provide.
