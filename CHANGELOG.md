# Changelog

All notable changes to the Neutron ecosystem will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
