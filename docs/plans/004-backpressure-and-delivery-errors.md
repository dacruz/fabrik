# Plan 004: Bounded backpressure and partial-delivery errors

**Status:** Pending

## Feature completed

Slow subscribers are bounded to 100 queued events. Publishing never waits for a full subscriber; it reports dropped deliveries while still delivering to subscribers that have capacity.

## Depends on

Plan 003: Non-blocking publish fan-out and ordering.

## Implementation

- Add `DeliveryError` with `Topic` and `Dropped` fields and an inspectable error identity/type.
- Count failed non-blocking sends during one publish. Return `nil` when all deliveries were queued or when there were no subscribers; return a `DeliveryError` when one or more deliveries were dropped.
- Preserve the per-topic lock and continue iterating after a failed send.
- Keep the error semantics precise: `Dropped` counts subscriber deliveries for that publish, not messages globally and not subscribers permanently removed.
- Ensure a concurrent unsubscribe cannot turn a full-channel send into a panic or falsely report an unrelated failure.

## Tests

- Fill a subscription deterministically with exactly 100 values.
- Confirm the 101st publish returns a delivery error and does not block.
- Confirm the first 100 values remain in publication order.
- With one full and one available subscriber, confirm the available subscriber receives the value and the error reports one drop.
- Confirm no-subscriber publishing remains successful.
- Add a bounded-time regression test proving a full subscriber never blocks `Publish`.

## Completion criteria

Backpressure behavior is deterministic and observable through the public API; the full-buffer and partial-fan-out tests pass under the race detector.
