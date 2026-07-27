.PHONY: deps build test test-race test-cover vet ci verify

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

vet:
	go vet ./...

ci: deps build test test-race test-cover vet

verify: deps build test vet
