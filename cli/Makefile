VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE    := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -s -w \
	-X github.com/neutron-build/neutron/cli/cmd.version=$(VERSION) \
	-X github.com/neutron-build/neutron/cli/cmd.commit=$(COMMIT) \
	-X github.com/neutron-build/neutron/cli/cmd.date=$(DATE)

.PHONY: build install test lint clean snapshot

build:
	go build -ldflags "$(LDFLAGS)" -o bin/neutron .

install:
	go install -ldflags "$(LDFLAGS)" .

test:
	go test -race ./...

lint:
	go vet ./...
	@which golangci-lint > /dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed, skipping"

clean:
	rm -rf bin/ dist/

snapshot:
	goreleaser release --snapshot --clean
