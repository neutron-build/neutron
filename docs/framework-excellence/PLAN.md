# Framework Excellence Program — Master Plan

**Goal:** bring every Neutron language framework to a genuine **10/10, best-in-class** standing — each must beat or match the category leader *in its own language ecosystem*, while preserving Neutron's dual identity: one shared contract (multi-language sameness) **and** independently importable layers (modularity).

**Owner:** Tyler. **Driver:** Claude. **Status:** ACTIVE. Created 2026-06-05.

> This is the single source of truth. Refer to it at the start of every work session in this program and update the Working Log at the bottom. Per-framework deep scaffolds live in sibling files (`go.md`, `python.md`, `rust.md`, `ts.md`, …) as they are produced.

---

## Scoreboard

| Framework | Lang | Category leader (the bar) | Current | Target | Scaffold |
|---|---|---|---|---|---|
| Rust (`rust/`) | Rust | Axum (+ Actix) | **7.5** | 10 | `rust.md` ✅ |
| Go (`go/`) | Go | Gin / Echo / Chi | **6.5** | 10 | `go.md` ✅ |
| Python (`python/`) | Python | FastAPI (+ Litestar) | **6.5** | 10 | `python.md` ✅ |
| TypeScript (`typescript/`) | TS | Astro / Next.js / Remix | **6.5** | 10 | `ts.md` ✅ |
| Elixir (`elixir/`) | Elixir | Phoenix | — | 10 | Phase 4 |
| Zig (`zig/`) | Zig | (http.zig / std) | — | 10 | Phase 4 |
| Julia (`julia/`) | Julia | Genie / Oxygen | — | 10 | Phase 4 |
| Mojo (`mojo/`) | Mojo | (nascent ecosystem) | — | 10 | Phase 4 |
| Modelica (`modelica/`) | Py+Julia | OpenModelica / FMI tools | — | 10 | Phase 4 |

---

## Program phases

- **Phase 0 — Audits (in progress).** Deep, skeptical, evidence-based quality audits per framework. Done: Go, Python, Rust (digests below). In progress: TS score.
- **Phase 1 — Research-backed scaffolds (in progress).** For each of Go/Python/Rust/TS: research the category leader's architecture & best practices, map every audit finding to a concrete fix design, produce a phased, file-level implementation scaffold to reach 10/10. Each scaffold is adversarially reviewed for gaps/over-engineering before finalizing. Output: `go.md`, `python.md`, `rust.md`, `ts.md`.
- **Phase 2 — Implementation.** Execute each scaffold (own branch per framework; tests + benchmarks gate each step). Re-audit to confirm 10/10.
- **Phase 3 — Cross-SDK contract conformance (the meta-framework keystone).** Build a shared conformance suite that fires identical requests at every SDK and asserts identical RFC 7807 bodies, enforced middleware order, and `/health` shape. This is what makes "identical behavior across languages" real instead of aspirational.
- **Phase 4 — Remaining languages.** Repeat Phase 0→2 for Elixir, Zig, Julia, Mojo, Modelica.

---

## Cross-cutting systemic findings (apply to ALL SDKs — highest leverage)

These repeat across languages, so fixing the pattern once per language closes the meta-framework's biggest gaps:

1. **10-layer middleware order is documented but enforced NOWHERE.** Go, Python, and Rust all leave order to the user, and the canonical example in *every* language wires it in the *wrong* order. → Each SDK needs a `standardMiddleware()`/default-stack that applies the contract order, plus a conformance test. (Phase 3.)
2. **Same bug ported across SDKs:** KV collection reads (`LRANGE`/`HGETALL`/`SMEMBERS`) split server output on `,`, corrupting values containing commas — identical in Go (`nucleus/kv.go`) and Python (`nucleus/kv.py`). Audit Rust + others for the same. → Needs a wire-format fix in the protocol, not per-SDK string-splitting.
3. **Plain-Postgres path is the weak spot everywhere** (undercuts "any Postgres client works"): Go scans every column as a string (loses BYTEA/JSONB/UUID/NUMERIC fidelity); Python pubsub `NOTIFY {chan}, $1` is broken (NOTIFY takes no bind params). The Nucleus path works; the generic-PG path is under-tested.
4. **DB SQL correctness is unverified by tests** in Go and Python (both mock at the connection level). → Need a real Nucleus/Postgres integration test tier (testcontainers/docker).
5. **`/health` shape diverges:** Go & Python return the contract `{status, nucleus, version}`; Rust ships only `/healthz`+`/readyz` with a different schema.

---

## Audit digests (Phase 0)

### Rust — 7.5/10 — "credible Axum-class lite"
- **Real:** matchit radix router (Axum's engine), Axum-style extractors (0–12 args), HTTP/1+2 (hyper-util) & **HTTP/3 (quinn+h3, which Axum lacks)**, WS/SSE/TLS, RFC 7807, OpenAPI, graceful shutdown, thread-per-core+SO_REUSEPORT. 646 core tests pass, clippy clean, **zero `todo!`/`unimplemented!`**, only 2 (unnecessary) `unsafe` impls. Cargo-feature modularity is real and more granular than Axum.
- **Gaps to 10:** (1) **not Tower-native** — `tower_compat` buffers bodies & drops request extensions/state; (2) **no streaming request bodies** — always buffers (`app.rs:676`); (3) middleware order not enforced + example wrong; (4) `/health` doesn't match contract; (5) delete the 2 needless `unsafe` in `handler.rs:531`; full-workspace test/clippy + honest Axum/Actix benchmarks.

### Go — 6.5/10 — "architecturally ahead of Gin/Echo, but has correctness bugs"
- **Real & ahead:** typed generic handlers + auto request-binding + auto **OpenAPI 3.1** (Huma/Encore tier, above Gin/Echo); built-in RFC 7807, RBAC, OAuth2+PKCE, WebAuthn, CSRF, tiered cache, persistent job queue (`FOR UPDATE SKIP LOCKED`). Lean deps. Build/test/`-race` pass. Real modularity (web pkg pulls zero DB deps).
- **HIGH bugs:** (1) response-writer wrappers don't forward `Flusher`/`Hijacker` → **SSE & WebSocket silently break behind the framework's own logging/compress middleware** (`middleware.go:255,266`, `sse.go:12`); (2) **send-on-closed-channel race** in realtime hub (`hub.go:63,113`); (3) plain-text 404/405 violates its own RFC 7807 contract (`router.go`).
- **MEDIUM:** SQL `scanRow` scans every column as string, losing PG type fidelity (`nucleus/sql.go:145`); KV comma-split (`kv.go`); "cron" can't parse standard cron (`cron.go`); middleware order not provided/enforced + example wrong (`examples/crud-api/main.go:68`).

### Python — 6.5/10 — "credible AI-first framework, a tier below FastAPI on fundamentals"
- **Real & differentiating:** Starlette+Pydantic v2+asyncpg; sophisticated handler introspection/DI; strong auth (JWT alg-confusion guard, CSRF, OAuth2+PKCE, argon2); **AI/MCP/agent stack none of FastAPI/Litestar/Django ship**. 443 tests pass; async correctness verified (sync handlers offloaded via `to_thread`, correct asyncpg pool use). Real modularity (optional extras).
- **HIGH bug:** plain-Postgres pubsub broken — `NOTIFY {chan}, $1` takes no bind params (`nucleus/pubsub.py:58`).
- **MEDIUM:** workflow `BaseException` unpack crash (`ai/workflow.py:131`); **mypy has 131 errors despite strict config** (typing oversold); websocket set-mutation-during-iteration (`realtime/websocket.py:105`); middleware order not enforced.
- **Gaps vs FastAPI:** DI lacks `yield`/teardown + test overrides; OpenAPI scalar params lose typing/enums/constraints; no `orjson` fast path; raw-SQL `migrate` only.

---

### TypeScript — 6.5/10 — "data layer rivals the leaders; gated by render-path fragmentation, regex islands, contract gaps"
- **Real & strong:** 22.9K LOC, 223 tests pass. Data/caching layer is competitive-to-leading — parallel loaders (4.8x, action-before-loaders, Response-throw escape), credential-aware app-response cache + single-flight dedup + ETag revalidation (refuses Set-Cookie/private/CORS), SPA partial-data refetch via `X-Neutron-Routes` route-diffing with abort guards + FOUC prevention, hydration-failure keeps SSR HTML. Clean trie router. Server-only stripping via real Babel AST.
- **Gaps to 10:** (1) **three divergent render paths** (dev Vite-middleware vs runtime-Vite `createServer` vs generated `entry.node.ts`) that don't share streaming/head/error — production entry blocks on `renderToString` (no streaming SSR in the deployable); (2) **island JSX transform is regex-based** (breaks on multiline/`>`/`}`/expression props) and there are **two competing island APIs** (directive vs `<Island>`); (3) **contract non-conformance** — no `/health`, middleware order unenforced (`createServer` wires compression before CORS), `startServer` calls `process.exit(0)` (no 30s drain) and runs a full Vite dev server in production; (4) KV/Document comma-split bug (same cross-SDK bug — `kv/index.ts:274`, `document/index.ts:104`); (5) DB SQL unverified (MockTransport only); typed routes ship OFF by default.
- See `ts.md` for the full phased scaffold.

> Note: the cross-cutting systemic findings above are now confirmed in **4/4** audited SDKs — the KV comma-split bug and the unenforced-middleware-order both recur in TypeScript too.

## What "10/10 best-in-class" requires (acceptance bar)
A framework is 10/10 only when ALL hold:
1. **Zero correctness bugs** in the audited surface; all HIGH/MEDIUM issues fixed.
2. **Beats or matches the category leader** on the fundamentals (routing, extraction/binding, middleware, streaming, errors, OpenAPI, testing ergonomics) — with published benchmarks where performance is claimed.
3. **Idiomatic** to the language (passes the language's strict linters/type-checkers clean: `clippy`/`go vet`+`-race`/`mypy --strict`).
4. **Contract-conformant AND covered by the cross-SDK conformance suite** (Phase 3).
5. **Modularity verified** (import-only-what-you-need proven by dependency/feature analysis).
6. **Real integration tests** against Nucleus/Postgres, not just mocks.
7. **Docs + examples that are correct** (no example that violates the contract).

---

## Phase 1 — Scaffolds (COMPLETE 2026-06-05)

Each scaffold is a standalone phased engineering plan (P0 correctness → P1 fundamentals to match the leader → P2 differentiation to beat it), research-backed and adversarially reviewed. Files: `go.md` (1142 lines), `python.md` (492), `rust.md` (785), `ts.md` (206).

**Go (`go.md`)** — P0: 4 correctness bugs (streaming-writer Flusher/Hijacker + lazy Content-Encoding; hub send-on-closed race; problem+json 404/405; jobs status errors). P1: 7 fundamentals (enforced DefaultStack; bindplan+Resolver+ErrorHandler; validation→OpenAPI with `$ref`/unions; native pgx scanning; KV array encoding + integration tier; test harness + health). P2: 6 differentiators (real cron via robfig/cron; sharded rate limiter; content negotiation CBOR/msgpack; typed SSE; generated typed clients; scaffold gen). Thesis: beat **Huma** (schema/negotiation) + **Encore** (generated clients) — not "we have a DB."

**Python (`python.md`)** — P0: 8 bugs (incl. broken `NOTIFY` bind, workflow BaseException, websocket set-iteration, per-request signal-handler removal). P1: 9 fundamentals (the `HandlerPlan` spine to kill 3-pass divergence; DI with **yield/teardown + overrides + caching**; orjson default + `response_model`; OpenAPI via `TypeAdapter().json_schema()`; enforced contract middleware stack; `BackgroundTasks`). P2: 6 (mypy-clean after HandlerPlan, migrations, etc.). 35-row issue→phase traceability.

**Rust (`rust.md`)** — **P0.0 = TLS: the DB connects with `NoTls` (`pool.rs:182`) — plaintext-DB defect that falsifies the "any Postgres client works" thesis (NEW, security).** P0 also: remove needless unsafe, no-panic resolve, 405 Allow, `/health` shape, KV decode, example order. P1: 10 (Router-as-Tower-`Service`; collapse the parallel non-Tower middleware into one Tower model; **async extractors → streaming request bodies**; `Router<S>`/`FromRef`/derive; lossless Tower bridge; default_stack; debug_handler diagnostics). P2: 8 (errors-through-AppError, OpenAPI derive, validation, conformance suite, testcontainers, benchmarks).

**TypeScript (`ts.md`)** — 7 phases: collapse the 3 render paths into one `render-route.ts` (+ stream in the production entry via `renderToReadableStream`); `default-stack.ts` (enforced order) + `health.ts` + 30s drain; **AST-based island transform** (replace regex) unified on `<Island>`; wire-layer comma-corruption fix + testcontainers tier (real PG + real Nucleus); typed routes on by default; structured head over JSON wire; Suspense/await streaming (generically fixes the rss.xml gap).

### Cross-framework patterns the scaffolds converge on (build once, apply everywhere)
- A single **canonical request-handling spine** per framework (Go bindplan, Python HandlerPlan, TS render-route, Rust Router-as-Service) — every audit found divergent duplicate paths that drift.
- An **enforced default middleware stack** in contract order + a test that reads the order from `FRAMEWORK_CONTRACT.md` (all four).
- **OpenAPI fidelity** upgrade (real `$ref`/enums/unions/constraints) in Go, Python, Rust.
- **Wire-layer fix** for the KV/Document comma-corruption (don't string-split per SDK) + a **testcontainers integration tier against real Postgres AND real Nucleus** with a comma-value regression test (all four).
- These roll up into **Phase 3** (cross-SDK conformance suite).

## Phase 2 — Implementation (IN PROGRESS — Rust first, branch `framework/rust-excellence`)

### Rust P0 status
- ✅ **P0.1** remove dead `unsafe impl Send/Sync` (handler.rs) + compile-time assert
- ✅ **P0.2** `Router::resolve()` returns `RouteError::NotBuilt` instead of panicking
- ✅ **P0.3** 405 carries RFC 7231 `Allow` header (`MethodNotAllowed { allow }`)
- ✅ **P0.4** `HealthCheck::contract()` — `GET /health` = `{status, nucleus, version}`
- ✅ **P0.6** example middleware order fixed (RequestID before Logging)
- ✅ **P0.0** TLS-capable Nucleus connections — rustls (aws-lc-rs) + OS trust store + `sslmode` (disable/prefer/require/verify-full), default-on `tls` feature, cached connector, `NucleusError::Tls`. 85 nucleus tests green; builds tls-on/off/all-features. (commit `928021c`)
- ⏸️ **P0.5** KV collection decode (jsonb, no comma-split) — **DEFERRED (sound reason).** The engine emits comma-joined TEXT from `KV_LRANGE`/`KV_SMEMBERS`/`KV_HGETALL`/`KV_ZRANGE` (`nucleus/src/executor/scalar_fns.rs:2785+`). A client-only change can't fix the corruption (it'd still split); the real fix needs engine item **N-1** to emit `jsonb`, then the client decodes `jsonb`. The engine is under **heavy active concurrent development** (ebad523/4bfc536/af78d19…), so changing its hot-path scalar fns from this stale framework branch would conflict and risk breaking the DB. → Land P0.5 + N-1 together on the nucleus mainline (engine emits jsonb for the 5 collection fns; client `decode_collection`/`decode_hash` helpers parse jsonb, no `split`), verified by the P2.5 testcontainers comma/`=`/newline/unicode round-trip tests.
- Tests: 651 neutron + 85 neutron-nucleus tests green; clippy `--lib` clean. Pre-existing lints noted for the quality-gates sweep: `extract.rs` (approx_constant PI + dead fields), `handler.rs:23` unused `BufMut`/`BytesMut` under `--no-default-features` (feature-gating gap).

**P0 status: 6/7 implemented & committed; P0.5 deferred to the nucleus mainline (engine-coupled). The framework P0 surface is shippable.**

### Repo-hygiene fixes made along the way (both stale-rename artifacts from rs→rust)
- `/rust/` ignore → `/rust/target/` — brought the whole framework under version control (`cb67b4f`).
- `!rs/Cargo.lock` exception → `!rust/Cargo.lock` — the framework lockfile was silently untracked; now tracked for reproducible builds (folded into `928021c`).

> ⚠️ **Critical repo finding (fixed):** the entire Rust framework (`rust/`, 18 crates) was **gitignored and untracked** — `/rust/` in `.gitignore`, added under the stale belief the source lived in `rs/` (renamed to `rust/` in 6079213). The flagship framework existed only on local disk. Fixed: `.gitignore` now ignores only `rust/target/`; framework brought under version control (commit `cb67b4f`, 185 files). The audit/scaffold/P0 work had all been on untracked code.

## Working Log
- **2026-06-05** — Phase 2 begun (Rust). Discovered + fixed the Rust framework being untracked/gitignored (commit `cb67b4f`). Implemented & tested P0.1–P0.4 + P0.6 (651 tests green, clippy `--lib` clean). Remaining P0: **P0.0 TLS** (adds rustls client deps — crypto-backend decision; planned defaults: `tokio-postgres-rustls` + rustls `ring` provider + `rustls-native-certs` OS trust store, `tls` feature default-on) and **P0.5 KV decode** (client jsonb decode is coupled to engine item N-1 emitting jsonb — do them together or sequence N-1 first). Then P1 (Router→Service, async extractors, Router<S> — the invasive keystone the scaffold says to land as its own PR).
- **2026-06-05** — Phase 0 audits complete: Go 6.5, Python 6.5, Rust 7.5, TS 6.5. Cross-cutting systemic findings captured (now confirmed 4/4 SDKs). Plan note created.
- **2026-06-05** — Phase 1 COMPLETE. Orchestrated mini-team per framework (research → scaffold → adversarial review → finalize) via background workflow (13 agents, ~824k tokens). All four scaffolds written to `docs/framework-excellence/`. New finding beyond the audits: Rust DB uses `NoTls` (plaintext) → P0.0. Next: choose first framework to implement, or start Phase 3 conformance harness which several P1 items feed into.
