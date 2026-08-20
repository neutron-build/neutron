# Neutron

**One framework ecosystem across 8 languages, backed by Nucleus — a single database engine with 14 data models. Ship web, mobile, desktop, and AI from one mental model.**

[![npm](https://img.shields.io/npm/v/@neutron-build/core.svg?label=%40neutron-build%2Fcore)](https://www.npmjs.com/package/@neutron-build/core)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](#license)
[![Website](https://img.shields.io/badge/neutron.build-00E5A0)](https://neutron.build)

**[Website](https://neutron.build)** · **[Documentation](https://neutron.build/docs)** · **[llms.txt](https://neutron.build/llms.txt)** · **[Contributing](./AGENTS.md)**

Neutron is a modular, multi-language full-stack framework. Every component is independent and connected by one thing — the PostgreSQL wire protocol that Nucleus speaks. Import only what you need: SQL ships by default, and every other data model is an optional import that strips out at compile time.

## Quick start

```bash
npm create neutron@latest
```

Scaffolds a TypeScript app with file-based routing, SSR on Preact, and a typed Nucleus client. Then:

```bash
cd my-app && npm run dev
```

Full guide: **[neutron.build/docs](https://neutron.build/docs)**.

## Why Neutron

- **8 languages, one contract.** TypeScript, Rust, Go, Python, Elixir, Zig, Julia, and Mojo. The six web-framework SDKs (TypeScript, Rust, Go, Python, Elixir, Zig) implement the same [framework contract](./FRAMEWORK_CONTRACT.md) — RFC 7807 errors, a standard middleware order, `GET /health`, graceful shutdown — while the code stays idiomatic in each language. A language-agnostic conformance matrix in [`conformance/`](./conformance) verifies this live: **all five booted SDKs pass 12/12 dimensions** (Go, Rust, Python, TypeScript, Elixir — measured 2026-08-20). Julia (scientific computing) and Mojo (ML) are client libraries, not web SDKs.
- **14 data models, one database.** Nucleus stands in for Postgres + Redis + a vector store + a search index + a graph DB: SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, and PubSub — all over the Postgres wire protocol, so any Postgres client connects. 311,526 lines of Rust, 4,802 declared tests, MVCC + WAL crash recovery.
- **Fast.** On the TypeScript benchmark harness in [`typescript/benchmarks/`](./typescript/benchmarks), Neutron beat Next.js on all 8 scenarios in every recorded run (2026-02 to 2026-08) and Astro on 6–8 of 8 depending on the run. Absolute throughput varies several-fold across machines — recorded 8-scenario averages span ~3.3k–28.6k req/s — so run the harness on your own hardware before quoting a number.
- **Honest about its limits.** [`docs/RESIDUAL_RISKS.md`](./docs/RESIDUAL_RISKS.md) is a standing register of what is *not* hardened — cross-model transactions, two index paths that read the whole table, what "formally verified" does and does not mean here — kept separate from the release notes on purpose, with each entry naming the test or measurement behind it.
- **Agent-native.** Ships [`llms.txt`](./llms.txt) + [`llms-full.txt`](https://neutron.build/llms-full.txt), a first-party MCP server (`neutron mcp` — 17 Nucleus tools plus `search_docs`/`get_doc`), and an `AGENTS.md` in every scaffolded app — so AI coding agents can build with Neutron from day one.

## What's inside

### Web & app frameworks
| Directory | Language | Description |
|-----------|----------|-------------|
| [`typescript/`](./typescript) | TypeScript | UI framework — SSR, file-based routing, Preact, islands, signals |
| [`rust/`](./rust) | Rust | Web framework — Hyper, trie routing, HTTP/1–2 (+HTTP/3 behind the `http3` feature), 19 composable crates |
| [`go/`](./go) | Go | Backend framework — generics, OpenAPI 3.1, OAuth2, WebAuthn, jobs |
| [`python/`](./python) | Python | AI app framework — Starlette + Pydantic, RAG, MCP integration |
| [`elixir/`](./elixir) | Elixir | OTP fault-tolerant backend — Plug + Bandit, channels, presence |

### Database
| Directory | Language | Description |
|-----------|----------|-------------|
| [`nucleus/`](./nucleus) | Rust | Multi-model engine — SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, PubSub |
| [`studio/`](./studio) | TypeScript | Visual database manager — all 14 data models in one UI |

### Clients & libraries
| Directory | Language | Description |
|-----------|----------|-------------|
| [`zig/`](./zig) | Zig | Systems/embedded Nucleus client — comptime-typed queries, fixed-capacity connection pool |
| [`julia/`](./julia) | Julia | Scientific-computing client — DataFrames, DiffEq, Flux, CUDA, Makie bridges |
| [`mojo/`](./mojo) | Mojo | ML library — tensors, quantization, inference (preview; built on Mojo 1.0) |

### Platforms & tooling
| Directory | Language | Description |
|-----------|----------|-------------|
| [`native/`](./native) | TypeScript | Mobile — React-compatible components to native iOS/Android views (RN Fabric); Preact on web via compat alias |
| [`desktop/`](./desktop) | Rust + TS | Desktop apps — Tauri 2.0 + Preact, 12 plugin crates, Nucleus embedded mode (bundle-size targets, not yet measured) |
| [`cli/`](./cli) | Go | Universal CLI — `neutron new/dev/studio/generate/migrate`, plus `neutron mcp` |

### Verification
| Directory | Language | Description |
|-----------|----------|-------------|
| [`lean4/`](./lean4) | Lean 4 | Machine-checked proofs of models of the core algorithms (MVCC, B-tree, WAL, Raft) |
| [`quint/`](./quint) | Quint | Bounded model-checking of the distributed protocols (Multi-Raft, resharding, distributed tx) |

## The ORM

Each language ships an idiomatic Nucleus client covering all 14 data models — SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, PubSub — not just SQL like Drizzle or Prisma.

- **TypeScript** — Drizzle-style, schema in code, no codegen
- **Rust** — Typed model handles via `NucleusClient`
- **Go** — Typed generics, struct tags
- **Python** — Pydantic models, async
- **Elixir** — Postgrex pool, OTP patterns
- **Zig** — Comptime, zero overhead
- **Julia** — Multiple dispatch, ecosystem bridges

See [`studio/`](./studio) for the visual ORM and [`llms.txt`](./llms.txt) for an AI-readable reference.

## Working on the monorepo

```bash
# Rust  (nucleus/, rust/)
cargo build && cargo test

# TypeScript  (typescript/, native/ use pnpm; studio/ uses npm)
pnpm install && pnpm test

# Go  (cli/, go/)
go build ./... && go test ./...

# Python
pip install -e ".[dev]" && pytest
```

See [AGENTS.md](./AGENTS.md) for contributor and AI-agent guidance, and [CLAUDE.md](./CLAUDE.md) for the full ecosystem map. Each package directory has its own README.

## CI

Path-filtered per directory — a change to `rust/` only runs `rust.yml`, and so on for `typescript.yml`, `nucleus.yml`, `mojo-validation.yml`, `cli.yml`, and `desktop.yml`.

## License

- **MIT** — all framework projects (`typescript/`, `rust/`, `go/`, `python/`, `elixir/`, `zig/`, `julia/`, `mojo/`, `studio/`, `native/`, `desktop/`, `cli/`, `lean4/`, `quint/`)
- **BSL 1.1** — Nucleus database engine (`nucleus/`), converts to MIT on 2046-01-01
