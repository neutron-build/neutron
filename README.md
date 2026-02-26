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
| [`studio/`](./studio) | TypeScript | Visual database management — all 9 data models in one UI |

### Platforms
| Directory | Language | Description |
|-----------|----------|-------------|
| [`native/`](./native) | TypeScript | Mobile framework — Preact components rendering to native iOS/Android views |
| [`desktop/`](./desktop) | Rust + TS | Desktop apps — Tauri 2.0 + Preact, ~10MB bundles |
| [`mobile-preview/`](./mobile-preview) | Go | On-device preview app — scan QR code, see native app instantly (Expo Go equivalent) |

### Language SDKs
| Directory | Language | Description |
|-----------|----------|-------------|
| [`go/`](./go) | Go | Nucleus client + Neutron bindings for Go |
| [`zig/`](./zig) | Zig | Nucleus embedded client — 12KB, zero allocations, comptime SQL |
| [`python/`](./python) | Python | Python interop via Mojo + Nucleus Python client |

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

Each language has an idiomatic Nucleus ORM covering all 9 data models — SQL, KV, Vector, Timeseries, Document, and more. Unlike Drizzle or Prisma which only cover SQL.

- **TypeScript** — Drizzle-style, schema in code, no codegen
- **Rust** — Proc macros, compile-time checked
- **Zig** — Comptime, zero overhead
- **Python** — Pydantic-style class definitions
- **Go** — Struct tags

See [`studio/`](./studio) for the full ORM API and [`llms.txt`](./llms.txt) for a quick AI-readable reference.

## CI

| Workflow | Trigger |
|----------|---------|
| `rs.yml` | Changes to `rs/` |
| `ts.yml` | Changes to `ts/` |
| `nucleus.yml` | Changes to `nucleus/` |
| `mojo-validation.yml` | Changes to `mojo/` |

## License

- **MIT** — all framework projects (`rs/`, `ts/`, `mojo/`, `studio/`, `go/`, `zig/`, `python/`, `native/`, `desktop/`, `mobile-preview/`)
- **BSL 1.1** — Nucleus database engine (`nucleus/`), converts to MIT on 2046-01-01
