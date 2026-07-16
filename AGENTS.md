# Contributing to Neutron (AGENTS.md)

Guidance for AI coding agents (Claude Code, Codex, Cursor, Zed, …) and humans
working **on the Neutron monorepo itself**. Neutron is a young framework and is
**not well represented in model training data** — do not assume Next.js, Astro,
or Prisma conventions. Verify against the code in this repo.

> Building an app *with* Neutron? That's a different guide — every scaffolded
> project ships its own `AGENTS.md` (`create-neutron` templates). This file is
> for changing the framework, not for using it.

## What Neutron is

A multi-language full-stack framework ecosystem backed by **Nucleus**, a
multi-model database engine (SQL, KV, Vector, TimeSeries, Document, Graph, FTS,
Geo, Blob, Streams, Columnar, Datalog, CDC, PubSub) that speaks the PostgreSQL
wire protocol. Each language SDK implements the same `FRAMEWORK_CONTRACT.md`:
feature detection, RFC 7807 errors, a standardized middleware order, `GET
/health`, and graceful shutdown. Components are independent — connected by the
wire protocol, not by shared code.

## Repository layout (real directory names)

| Dir | Language | What it is |
|-----|----------|-----------|
| `nucleus/` | Rust | Multi-model database engine (has its own `CLAUDE.md`) |
| `rust/` | Rust | Web framework — Hyper, trie routing, HTTP/1–3 |
| `typescript/` | TypeScript | UI framework (Preact SSR) + `apps/site` (neutron.build) |
| `go/` | Go | Backend framework + Nucleus client |
| `python/` | Python | AI app framework (Starlette + Pydantic) + MCP |
| `elixir/` · `zig/` · `julia/` · `mojo/` · `modelica/` | — | Language SDKs |
| `native/` · `desktop/` | TS / Rust+TS | Mobile (RN Fabric) / Tauri desktop |
| `cli/` | Go | Universal CLI — `neutron new/dev/build/studio/mcp` |
| `studio/` | TypeScript | Visual DB manager (14 models) |
| `lean4/` · `quint/` · `verus/` | — | Formal verification |

There is **no `rs/` or `ts/` directory** — those are `rust/` and `typescript/`.

## Build & test, per language

```bash
# Rust  (nucleus/, rust/, desktop backend)
cargo build && cargo test && cargo clippy

# TypeScript  (typescript/ incl. apps/site, native/ use pnpm; studio/ uses npm)
pnpm install && pnpm test        # pnpm dev / pnpm build

# Go  (cli/, go/)
go build ./... && go test ./...

# Python
pip install -e ".[dev]" && pytest

# Zig        zig build test
# Julia      julia --project=. -e 'import Pkg; Pkg.test()'
```

Run the tests for any component you change. CI is path-filtered per directory.

## Conventions that matter

- **TypeScript framework is Preact, not React.** `jsxImportSource: "preact"`.
  Import framework APIs from `@neutron-build/core` (never `from "neutron"`).
- **Import only what you need.** Every SDK ships SQL-only by default; other
  data models are optional imports that strip at compile time. Don't add a model
  dependency to the default path.
- **Errors are RFC 7807** across every language. **Middleware order is fixed:**
  Request ID → Logging → Recovery → CORS → Compression → RateLimit → Auth →
  Timeout → OpenTelemetry. **Health:** `GET /health` → `{ status, nucleus,
  version }`. **Shutdown:** graceful on SIGTERM/SIGINT.
- **Nucleus has its own `CLAUDE.md`** — defer to it for database work.
- Match the surrounding code's style; don't introduce a new formatter, package
  manager, or lockfile. Use whichever lockfile the directory already has.

## Accuracy discipline (read before writing docs, READMEs, or site copy)

Neutron's docs and marketing have a history of drifting ahead of the code
(inflated test counts, fabricated benchmarks, function names that don't exist).
**Every factual claim must trace to the source.**

- Canonical metrics: `sh nucleus/scripts/metrics.sh` (LOC, tests, modules).
  Benchmarks: the harness in `typescript/benchmarks/` — never hand-write RPS.
- Verify a function/export exists (`grep` the real source) before documenting it.
- State capabilities at their true maturity. If something is a model, prototype,
  or planned, say so. "Impressive and true" beats "more impressive and false" —
  a reader who catches one wrong number distrusts every number.
- Keep numbers in as few places as possible; prefer generating them.

## Docs & the site

The docs site (`typescript/apps/site/`) dogfoods Neutron's own TS framework and
deploys as a static build. Docs are MDX in `src/content/docs/` (sidebar order in
`src/routes/docs/_layout.tsx`). Machine-readable surfaces for agents: `/llms.txt`,
per-page `.md` sources, `sitemap.xml`, and the `neutron mcp` server (`cli/`).
When you change docs, keep these in sync.

## Deeper context

- `CLAUDE.md` — full ecosystem map and per-language build detail.
- `FRAMEWORK_CONTRACT.md` — the wire-level API spec every SDK implements.
- `nucleus/CLAUDE.md` — database engine internals.
- `llms.txt` — AI-readable ecosystem index (regenerate if you change structure).
