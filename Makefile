.PHONY: deps build test test-race test-cover benchmark vet ci verify

deps:
	go mod download

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

test-cover:
	go test -cover ./...

benchmark:
	go test -bench=. -benchmem ./...

vet:
	go vet ./...

ci: deps build test test-race test-cover vet

verify: deps build test vet
