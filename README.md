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

- **8 languages, one contract.** TypeScript, Rust, Go, Python, Elixir, Zig, Julia, and Mojo. Every SDK implements the same [framework contract](./FRAMEWORK_CONTRACT.md) — RFC 7807 errors, a standard middleware order, `GET /health`, graceful shutdown — so behavior is identical while the code stays idiomatic in each language.
- **14 data models, one database.** Nucleus stands in for Postgres + Redis + a vector store + a search index + a graph DB: SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, and PubSub — all over the Postgres wire protocol, so any Postgres client connects. 304,584 lines of Rust, 4,688 declared tests, MVCC + WAL crash recovery.
- **Fast.** The TypeScript framework averages ~18,500 req/s across 8 scenarios — roughly 2.7x Next.js and 1.7x Astro on the same hardware. These are representative numbers; run the reproducible harness in [`typescript/benchmarks/`](./typescript/benchmarks) on your own machine.
- **Agent-native.** Ships [`llms.txt`](./llms.txt) + [`llms-full.txt`](https://neutron.build/llms-full.txt), a first-party MCP server (`neutron mcp` — 17 Nucleus tools plus `search_docs`/`get_doc`), and an `AGENTS.md` in every scaffolded app — so AI coding agents can build with Neutron from day one.

## What's inside

### Web & app frameworks
| Directory | Language | Description |
|-----------|----------|-------------|
| [`typescript/`](./typescript) | TypeScript | UI framework — SSR, file-based routing, Preact, islands, signals |
| [`rust/`](./rust) | Rust | Web framework — Hyper, trie routing, HTTP/1–3, 19 composable crates |
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
| [`zig/`](./zig) | Zig | Systems/embedded Nucleus client — comptime SQL validation, zero-alloc |
| [`julia/`](./julia) | Julia | Scientific-computing client — DataFrames, DiffEq, Flux, CUDA, Makie bridges |
| [`mojo/`](./mojo) | Mojo | ML library — tensors, quantization, inference (preview, awaiting Mojo 1.0) |

### Platforms & tooling
| Directory | Language | Description |
|-----------|----------|-------------|
| [`native/`](./native) | TypeScript | Mobile — Preact components rendering to native iOS/Android views |
| [`desktop/`](./desktop) | Rust + TS | Desktop apps — Tauri 2.0 + Preact, ~10MB bundles, Nucleus embedded |
| [`cli/`](./cli) | Go | Universal CLI — `neutron new/dev/build/studio`, plus `neutron mcp` |

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
