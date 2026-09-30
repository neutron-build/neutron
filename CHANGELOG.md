# Changelog

All notable changes to the Neutron ecosystem will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [2026-09-30]

### Changed

- TypeScript core and CLI 0.3.0 require atomic shared-cache publication and
  revision-fenced session stores. `create-neutron` 0.1.7 and the universal CLI
  generate projects pinned to this pair.
- Go SDK 0.3.0 requires Go 1.26. Session middleware requires a versioned atomic
  store. Complete WebAuthn ceremonies use go-webauthn 0.18.2; legacy unverified
  credentials require authenticated recovery and re-enrollment.

### Fixed

- Nucleus 1.2.0 refuses failed durable engine opens, propagates columnar WAL
  failures, fences uncertain persistence outcomes and preserves other tables'
  acknowledged buffered rows during columnar replacement.
- Derived FTS/vector/zone-map images use generation coordination and conservative
  transaction fallbacks. Conditional deletes validate the visible row revision
  before publication, preventing competing stale transaction replacements.
- Migration adoption preflights every checksum and snapshots file inputs before
  waiting for ownership. Nucleus metadata DDL limitations remain explicit.

### Breaking

- Nucleus retired the insecure encrypted-index modes. Their constructor and SQL
  surfaces fail closed; legacy base rows remain readable. This does not retire
  page-level encryption at rest.

See [TypeScript release notes](typescript/CHANGELOG.md),
[authentication migration](go/neutronauth/README.md) and
[model boundaries](nucleus/docs/MODEL_SEMANTICS.md) for requirements and limits.

## [0.1.0] - 2026-02-25

### Added

#### Neutron RS (Rust Web Framework)
- HTTP/1.1, HTTP/2, and HTTP/3 support
- Convention-driven middleware and routing
- WebSocket support
- 18 crates covering core, CLI, GraphQL, gRPC, jobs, Redis, Postgres, OAuth, SMTP, storage, cache, OpenTelemetry, Stripe, and WebAuthn
- 600+ tests passing

#### Neutron TS (TypeScript UI Framework)
- SSR with file-based routing
- Preact with signals-based reactivity
- View transitions, shallow routing, control flow components
- Tag-based cache invalidation, CSP, incremental prefetch
- Server Islands, build adapters, fonts API
- 177 tests passing

#### Neutron Mojo (ML Library)
- Typed tensors with SIMD kernels
- Full inference pipeline with tokenizer, quantization, KV cache, attention, and transformer
- GGUF and SafeTensors model loading
- Speculative decoding, LoRA, mixture of experts
- Continuous batching and request scheduling
- 110+ test suites

#### Nucleus (Database Engine)
- Multi-model database with SQL, KV, Vector, Timeseries, Document, Graph, FTS, Geo, and Pub/Sub
- PostgreSQL wire protocol compatibility
- MVCC snapshot isolation and WAL crash recovery
- Columnar storage engine with filter pushdown
- Encryption at rest and LZ4 compression
- Connection pooling and embedded API
- 2161 tests passing
