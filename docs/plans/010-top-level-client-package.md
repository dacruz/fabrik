# Plan 010: Top-level client package

**Status:** Planned

## Feature

Restructure the public packages so the low-level bus API remains available at
`github.com/dacruz/fabrik/pubsub` while the role-specific producer and consumer
clients are available at `github.com/dacruz/fabrik/client`.

The public usage should become:

```go
import (
	"github.com/dacruz/fabrik/pubsub"
	"github.com/dacruz/fabrik/client"
)
```

The move is organizational and must preserve the existing typed delivery,
backpressure, lifecycle, and shutdown behavior.

## Depends on

Plan 009: Producer and consumer clients.

## Target structure

```text
pubsub/
    bus.go
    topic.go
    subscription.go
    errors.go
    doc.go
client/
    producer.go
    consumer.go
    client_test.go
    example_test.go
pubsub/integration/
    ...
```

## Implementation

- [x] Move the role-specific client implementation from the nested pubsub
  client package to a
  top-level `client` package.
- [x] Preserve the public `github.com/dacruz/fabrik/pubsub` package and all of
  its existing low-level APIs and error identities.
- [x] Keep `client` dependent on `pubsub` without introducing an import cycle.
- [x] Split client implementation files by responsibility where useful,
  keeping producer and consumer behavior easy to locate.
- [x] Update all client tests, examples, integration tests, and README imports
  to use `github.com/dacruz/fabrik/client`.
- [x] Update package comments and examples to document the two public package
  paths and their responsibilities.
- [x] Remove the old nested client package after all references are migrated.
- [x] Keep unrelated low-level `pubsub` tests and implementation unchanged
  except where import or package documentation updates are required.

## Tests

- [ ] Verify the top-level `client` package builds and exposes the same
  producer, consumer, handler, and constructor APIs.
- [ ] Verify client lifecycle, topic isolation, ordering, backpressure, and
  shutdown tests still pass.
- [ ] Search the repository for stale nested client package imports or
  references.
- [ ] Run `go test ./...`, `go test -race ./...`, and `go vet ./...`.
- [ ] Run repeated race-detector stress tests for client and integration
  workflows.

## Completion criteria

Applications can import the low-level API from `github.com/dacruz/fabrik/pubsub`
and role-specific clients from `github.com/dacruz/fabrik/client`. No stale
package references remain, all behavior is preserved, and the full test suite
passes under the race detector.
