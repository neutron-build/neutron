.PHONY: test-cli test-nucleus test-rust test-python test-go test-ts test-all \
        build-nucleus build-cli build-studio \
        release-cli release-nucleus

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test-cli:
	cd cli && go test ./...

test-nucleus:
	cd nucleus && cargo test --lib

test-rust:
	cd rust && cargo test

test-python:
	cd python && pytest tests/

test-go:
	cd go && go test ./...

test-ts:
	cd typescript && pnpm test

test-all: test-cli test-nucleus test-rust test-python test-go test-ts

# ---------------------------------------------------------------------------
# Builds
# ---------------------------------------------------------------------------

build-nucleus:
	cd nucleus && cargo build --release

build-cli:
	cd cli && go build -o bin/neutron .

build-studio:
	cd studio && npm run build

# ---------------------------------------------------------------------------
# Releases (require VERSION= argument)
# ---------------------------------------------------------------------------

release-cli:
ifndef VERSION
	$(error VERSION is required -- usage: make release-cli VERSION=0.2.0)
endif
	git tag "cli/v$(VERSION)"
	git push origin "cli/v$(VERSION)"

release-nucleus:
ifndef VERSION
	$(error VERSION is required -- usage: make release-nucleus VERSION=0.2.0)
endif
	git tag "nucleus/v$(VERSION)"
	git push origin "nucleus/v$(VERSION)"
