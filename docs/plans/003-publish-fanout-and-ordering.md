# Plan 003: Non-blocking publish fan-out and ordering

**Status:** Completed

## Feature completed

Publishing a typed value fans it out to every current subscriber of the exact topic without blocking. Sequential publication order is preserved, and all subscribers observe the same per-topic order.

## Depends on

Plans 001 and 002.

## Implementation

- [x] Implement `Publish[T any](b *Bus, topic Topic[T], value T) error`.
- [x] Resolve/register the topic and validate its type before delivery.
- [x] Take the topic lock as the per-topic serialization point. Iterate over a stable subscriber set and use a non-blocking channel send for each subscriber.
- [x] Do not hold the global bus lock while delivering; unrelated topics must be able to publish independently.
- [x] Treat publishing with no subscribers as success and discard the value.
- [x] Add the defined shutdown error path, even if shutdown itself is implemented in Plan 005, so the publish API has one stable lifecycle contract.

## Tests

- [x] One subscriber receives every sequentially published value.
- [x] Multiple subscribers each receive every value.
- [x] A subscriber on another exact topic receives nothing.
- [x] Prefixes, suffixes, and topic-like hierarchies do not match.
- [x] A new subscriber receives no values published before it subscribed.
- [x] Sequential values arrive in order for every subscriber.
- [x] A slow subscriber does not block delivery to a subscriber with capacity.
- [x] Concurrent publishers produce one valid order per topic; tests do not assume goroutine start order.

## Completion criteria

The bus provides useful end-to-end event notification for subscribers, with non-blocking sends and deterministic per-topic serialization semantics.
