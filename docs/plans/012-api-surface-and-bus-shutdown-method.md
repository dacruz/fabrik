# Plan 012: API surface cleanup and bus shutdown method

**Status:** Completed

## Feature

Tighten the public API around the role-specific clients and make shutdown a
method of the bus that owns the lifecycle.

The resulting shutdown API is:

```go
func (b *Bus) Shutdown(ctx context.Context) error
```

Callers use `b.Shutdown(ctx)`. The context controls waiting to acquire the bus
lock or join another shutdown already in progress; once teardown starts,
cancellation does not interrupt bus-side cleanup.

## Depends on

Plans 001 through 011.

## Implementation

- [x] Remove the exported `Topic.Name` and `Topic.Type` introspection methods;
  topic names and reflected event types remain private implementation details.
- [x] Convert package-level `Shutdown(ctx, b)` into `(*Bus).Shutdown(ctx)`.
- [x] Preserve nil-bus handling, idempotency, concurrent shutdown behavior,
  context cancellation semantics, and queued-value draining.
- [x] Update client and integration callers to use `b.Shutdown(ctx)`.
- [x] Update README, package documentation, and prior plan references.
- [x] Keep `Subscribe`, `Publish`, and subscription teardown available to the
  separate `client` package, which depends on `pubsub` for its implementation.

## Tests

- [x] Update in-package tests to use private topic fields.
- [x] Verify a nil `*Bus` can still return `ErrNilBus` through the method call.
- [x] Run `go test ./...`.
- [x] Run `go vet ./...`.

## Completion criteria

Topic implementation details are no longer exposed as public methods, bus
shutdown is expressed as an operation on its owning bus, existing lifecycle
semantics are preserved, and the full test and vet suites pass.
