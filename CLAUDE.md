# Neutron Framework

Multi-language meta framework with a unified multi-model database (Nucleus). Light at the core, totally modular — each component is independent. Rivals Astro, Next.js, and PostgreSQL combined.

## Ecosystem Map

```
FRAMEWORKS              DATABASE        SDKS              PLATFORMS         VERIFICATION
typescript/ (TypeScript) nucleus/ (Rust) go/    (Go)       native/  (Mobile) lean4/ (Proofs)
rust/       (Rust)       studio/  (UI)   python/ (Python)  desktop/ (Tauri)  quint/ (TLA+)
mojo/       (Mojo/GPU)                   zig/    (12KB)    mobile-preview/   verus/ (SMT)
                                         julia/  (Science) cli/     (Go)
                                         elixir/ (OTP)     site/    (Astro)
```

## Key Architecture

**Nucleus** is the database — 14 data models (SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, PubSub, Columnar, Datalog, CDC) accessible over PostgreSQL wire protocol (pgwire). Any Postgres client works.

**Framework SDKs** each implement FRAMEWORK_CONTRACT.md — same wire protocol, same error format (RFC 7807), same middleware order, same health checks. Different languages, identical behavior.

**Philosophy:** Import only what you need. SQL ships by default, every other model is an optional import. Unused code strips at compile time.

## Directory Breakdown

| Dir | Language | What it is |
|-----|----------|------------|
| `typescript/` | TypeScript | Web framework — file-based routing, Preact SSR, islands, loaders/actions |
| `rust/` | Rust | High-perf web framework on Hyper — trie routing, 15+ middleware, HTTP/1-3 |
| `mojo/` | Mojo | GPU kernels, ML training/inference, scientific compute (awaiting Mojo 1.0) |
| `nucleus/` | Rust | Multi-model database engine (has own CLAUDE.md) |
| `studio/` | TypeScript | Visual DB management UI for all 14 models |
| `go/` | Go | Backend framework + Nucleus client, struct-tag ORM |
| `python/` | Python | AI app framework (Starlette + Pydantic + Nucleus), MCP integration |
| `zig/` | Zig | Embedded SDK — 12KB binary, zero heap, comptime SQL validation |
| `julia/` | Julia | Scientific computing — DifferentialEquations, ModelingToolkit, CUDA |
| `elixir/` | Elixir | Distributed framework — OTP supervisors, hot reload, clustering |
| `modelica/` | Python+Julia | Physics simulation, FMI 3.0 interop |
| `native/` | TypeScript | Mobile framework — Preact components render to native iOS/Android views |
| `desktop/` | Rust+TypeScript | Desktop apps via Tauri 2.0, ~10MB bundles, embedded Nucleus |
| `mobile-preview/` | Go | Live preview app for mobile dev (like Expo Go but not SDK-versioned) |
| `cli/` | Go | Universal CLI — `neutron new`, `neutron db`, `neutron migrate`, `neutron studio` |
| `site/` | TypeScript | Marketing/docs site (Astro) |
| `archive/` | Docs | Cross-language architecture docs and planning |
| `lean4/` | Lean 4 | Machine-checked correctness proofs (MVCC, B-tree, WAL, Raft) |
| `quint/` | Quint | Protocol verification (Multi-Raft, resharding, distributed tx) |
| `verus/` | Verus | Rust code verification via SMT solver (deferred) |

## Key Files

- `FRAMEWORK_CONTRACT.md` — Wire-level API spec all SDKs implement (2,500 LOC)
- `llms.txt` — AI-readable ecosystem reference
- `docs/phases/PHASE4-INDEX.md` — DevOps/deployment documentation index
- `docs/` — Phase reports, benchmarks, research, operations guides
- `archive/ARCHITECTURE.md` — Pillar system and language boundaries

## Build Commands by Language

```bash
# Rust (nucleus, rust, desktop backend)
cargo build
cargo test
cargo clippy

# TypeScript (typescript, native, studio, site)
pnpm install        # typescript/ uses pnpm
npm install         # site/, studio/ use npm
pnpm dev / npm run dev

# Go (cli, go, mobile-preview)
go build ./...
go test ./...

# Python
pip install -e ".[dev]"
pytest

# Julia
julia --project=. -e 'import Pkg; Pkg.test()'

# Zig
zig build test
```

## Cross-Language Integration

All SDKs connect to Nucleus via pgwire (PostgreSQL wire protocol). The connection pattern is always:

1. Standard PostgreSQL connection string (`DATABASE_URL`)
2. Feature detection query to check if connected to Nucleus or plain Postgres
3. SQL functions for non-relational models (`KV_GET`, `VECTOR_DISTANCE`, `GRAPH_SHORTEST_PATH`, etc.)

## Conventions

- Each SDK ships SQL-only by default; other models are optional imports
- Error responses follow RFC 7807 across all languages
- Middleware order is standardized: Request ID → Logging → Recovery → CORS → Compression → RateLimit → Auth → Timeout → OpenTelemetry
- Health endpoint: `GET /health` returns `{ status, nucleus, version }`
- Graceful shutdown on SIGTERM/SIGINT in all frameworks
- Nucleus has its own CLAUDE.md — defer to that for database-specific context
