# Neutron

A full-stack framework ecosystem. One mental model across web, mobile, desktop, and AI — backed by Nucleus, a multi-model database engine built in Rust.

## Projects

### Frameworks
| Directory | Language | Description |
|-----------|----------|-------------|
| [`rs/`](./rs) | Rust | Web framework — HTTP/1.1, HTTP/2, HTTP/3, middleware, WebSockets |
| [`ts/`](./ts) | TypeScript | UI framework — SSR, file-based routing, Preact, signals |
| [`mojo/`](./mojo) | Mojo | ML library — tensors, quantization, inference pipeline |

### Database
| Directory | Language | Description |
|-----------|----------|-------------|
| [`nucleus/`](./nucleus) | Rust | Multi-model database — SQL, KV, Vector, Timeseries, Document, Graph, FTS, Geo, Pub/Sub |
| [`studio/`](./studio) | TypeScript | Visual database management — all 14 data models in one UI |

### Platforms
| Directory | Language | Description |
|-----------|----------|-------------|
| [`native/`](./native) | TypeScript | Mobile framework — Preact components rendering to native iOS/Android views via Expo Go |
| [`desktop/`](./desktop) | Rust + TS | Desktop apps — Tauri 2.0 + Preact, ~10MB bundles |

### Language SDKs
| Directory | Language | Description |
|-----------|----------|-------------|
| [`go/`](./go) | Go | Full backend framework + Nucleus client |
| [`elixir/`](./elixir) | Elixir | BEAM fault-tolerant framework — OTP supervisors, distributed, hot code reload |
| [`zig/`](./zig) | Zig | 4-layer systems library — zero allocations, comptime SQL |
| [`python/`](./python) | Python | AI application framework — Starlette + Pydantic + Nucleus client |

## Quick Start

```bash
# Rust web framework
cd rs && cargo build && cargo test

# TypeScript framework
cd ts && pnpm install && pnpm dev

# Mojo ML library
cd mojo/neutron-mojo && pixi run mojo build -I src test/test_tensor.mojo -o /tmp/test_tensor

# Nucleus database
cd nucleus && cargo build && cargo test --lib
```

## The ORM

Each language has an idiomatic Nucleus client covering all 14 data models — SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, PubSub. Unlike Drizzle or Prisma which only cover SQL.

- **TypeScript** — Drizzle-style, schema in code, no codegen
- **Rust** — Typed model handles via NucleusClient
- **Go** — Typed generics, struct tags
- **Python** — Pydantic models, async
- **Elixir** — Postgrex pool, OTP patterns
- **Zig** — Comptime, zero overhead
- **Julia** — Multiple dispatch, ecosystem bridges

See [`studio/`](./studio) for the full ORM API and [`llms.txt`](./llms.txt) for a quick AI-readable reference.

## CI

| Workflow | Trigger |
|----------|---------|
| `rs.yml` | Changes to `rs/` |
| `ts.yml` | Changes to `ts/` |
| `nucleus.yml` | Changes to `nucleus/` |
| `mojo-validation.yml` | Changes to `mojo/` |

## License

- **MIT** — all framework projects (`rs/`, `ts/`, `mojo/`, `studio/`, `go/`, `elixir/`, `zig/`, `python/`, `julia/`, `native/`, `desktop/`)
- **BSL 1.1** — Nucleus database engine (`nucleus/`), converts to MIT on 2046-01-01
