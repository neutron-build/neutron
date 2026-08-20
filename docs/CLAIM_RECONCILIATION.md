# Claim Reconciliation (S97)

Audit date: 2026-08-19/20. Tree audited: commit `05011eaa` (working tree as
found; line numbers below refer to the pre-edit files at that commit).
Scope: root and per-directory READMEs, `llms.txt`, `FRAMEWORK_CONTRACT.md`,
`typescript/apps/site/src/**`, and user-facing `docs/**`.

Method: every claim was traced to its three evidence legs — **source**
(the implementation exists), **test** (something exercises it), **runner**
(a workflow executes the test). A claim is SUPPORTED only if the legs that
make it true exist. Machine-synced figures (LOC, declared test counts) were
verified with `sh nucleus/scripts/metrics.sh --check` and were not edited.

Verdicts: **SUPPORTED** (evidence found), **NEEDS-CONSTRAINT** (true only
with a qualifier that was missing), **UNSUPPORTED** (no evidence, or evidence
contradicts).

Counts: **93 claims checked — 44 SUPPORTED, 10 NEEDS-CONSTRAINT, 38
UNSUPPORTED** (plus one file listed in scope that does not exist). UNSUPPORTED
includes stale numbers, stale status markers, and four READMEs that described
designs instead of the shipped code.

## README.md (root)

| Line | Claim | Evidence | Verdict | Action |
|---|---|---|---|---|
| 3, 29 | 8 languages | All 8 dirs have source, tests, CI (`typescript.yml`, `rust.yml`, `go.yml`, `python.yml`, `elixir.yml`, `zig.yml`, `julia.yml`, `mojo-validation.yml`) | SUPPORTED | none |
| 29 | "Every SDK implements the same framework contract … behavior is identical" | Contract header listed 5 SDKs; live conformance matrix (`conformance/README.md`) covers Go 12/12, **Rust 10/12** (fails `health.types`, `validation.format`), Python 12/12; Julia/Mojo are libraries, not web SDKs | UNSUPPORTED | Rewritten: six framework SDKs, matrix scores stated, Julia/Mojo labeled libraries |
| 30 | 14 data models over pgwire | All 14 model modules in `nucleus/src/`, SQL functions in `executor/scalar_fns.rs`, exercised by `executor/tests/test_multimodel.rs`, `test_cross_model.rs`; `nucleus.yml` runs tests | SUPPORTED | none |
| 30 | "310,261 lines of Rust, 4,783 declared tests" | `metrics.sh --check` OK (machine-synced from ground truth) | SUPPORTED | untouched (gated) |
| 30 | "MVCC + WAL crash recovery" | `DURABILITY.md` (durable-file inventory, fsync modes), `docs/MODEL_SEMANTICS.md` (write → kill -9 → restart → read-back), crash-injection probes in `src/bin/` (M3 evidence) | SUPPORTED | none |
| 31 | "~18,500 req/s across 8 scenarios — roughly 2.7x Next.js and 1.7x Astro" | Traces to a **single 5s/1-run measurement** (`typescript/benchmarks/results/run-2026-07-15T06-51-39-117Z.json`, avg 18,510). Corpus of 10 full runs spans 3,283–28,599 req/s; latest (2026-08-02) is 7,806. Runner-to-runner swing ±40% documented in `bench-gate-thresholds.json` | UNSUPPORTED as representative | Replaced with corpus-scoped claim: beat Next.js 8/8 scenarios in every recorded run, Astro on 6–8 of 8; run the harness yourself |
| 32 | "17 Nucleus tools plus `search_docs`/`get_doc`" | `cli/internal/mcp/tools.go` defines exactly 17 + 2; `cli.yml` runs `go test ./...` incl. `mcp/tools_test.go` | SUPPORTED | none |
| 32 | "`AGENTS.md` in every scaffolded app" | 5/5 `create-neutron/templates/*/AGENTS.md` | SUPPORTED | none |
| 40 | Rust "HTTP/1–3" | HTTP/1.1+2 default; HTTP/3 exists but only behind non-default `http3` feature (`rust/crates/neutron/Cargo.toml:134`), exercised in `tests/integration.rs` | NEEDS-CONSTRAINT | Cell now says HTTP/3 behind the `http3` feature |
| 40 | "19 composable crates" | 19 dirs under `rust/crates/` | SUPPORTED | none |
| 41 | Go "generics, OpenAPI 3.1, OAuth2, WebAuthn, jobs" | `go/neutron/handler.go` (generic `Get[In,Out]`), `go/neutron/openapi.go` (3.1), `go/neutronauth/oauth.go` + `webauthn.go`, `go/neutronjobs/`; `go.yml` runs 447 test functions | SUPPORTED | none |
| 42 | Python "Starlette + Pydantic, RAG, MCP integration" | `python/neutron/ai/`, `nucleus/`, 565 collected tests; `python.yml` | SUPPORTED | none |
| 43 | Elixir "Plug + Bandit, channels, presence" | `elixir/lib/neutron/realtime/` (channel/presence/socket), 496 tests; `elixir.yml` | SUPPORTED | none |
| 49 | Studio "all 14 data models in one UI" | 14 model modules under `studio/src/modules/`; `cli.yml` builds/tests `studio/**` | SUPPORTED | none (studio's own README said 9 — see below) |
| 54 | Zig "comptime SQL validation, zero-alloc" | No `QueryType`/SQL-string validation in shipped code (only comptime struct decoding, `zig/src/nucleus/sql.zig`); client is allocator-backed with a heap connection pool (`client.zig:86`) | UNSUPPORTED | Now "comptime-typed queries, fixed-capacity connection pool" |
| 55 | Julia "DataFrames, DiffEq, Flux, CUDA, Makie bridges" | 7 package extensions in `julia/ext/`, examples in `julia/examples/`; `julia.yml` | SUPPORTED | none |
| 56 | Mojo "(preview, awaiting Mojo 1.0)" | Mojo 1.0 shipped 2026-08-11; HEAD commit migrated the SDK (`mojoproject.toml`: `mojo >= 1.0`), validation 125/125 on 1.0.0 | UNSUPPORTED (stale) | Now "preview; built on Mojo 1.0" |
| 61 | Native "Preact components rendering to native iOS/Android views" | Shipped code has **no custom renderer**: `render.ts` — "React Native's built-in Fabric renderer handles all native view creation"; Preact only on web via `preact/compat` alias | NEEDS-CONSTRAINT | Cell rewritten; native/README replaced (see below) |
| 62 | Desktop "~10MB bundles, Nucleus embedded" | `nucleus-embedded` feature real + lifecycle test in `desktop.yml`; **no bundle-size measurement anywhere in-repo** | NEEDS-CONSTRAINT | Sizes labeled design targets |
| 63 | CLI "`neutron new/dev/build/studio`" | No top-level `build` command in `cli/cmd/` (build exists only as `desktop build` subcommand) | UNSUPPORTED | Cell now `new/dev/studio/generate/migrate` |
| 68 | Lean 4 "machine-checked proofs of models" | 26 files / 92 theorems / 0 sorry / 3 axioms (`metrics.sh`), `lean4.yml` runs canary + axiom audit; page-level "models, not the binary" caveat lives on the site | SUPPORTED | none |
| 69 | Quint "bounded model-checking" | 15 spec + 14 test `.qnt` files; `quint.yml` | SUPPORTED | none |
| 105 | CI path-filtered per directory | All named workflows exist (`rust.yml`, `typescript.yml`, `nucleus.yml`, `mojo-validation.yml`, `cli.yml`, `desktop.yml`, + others) | SUPPORTED | none |
| 109–110 | MIT (frameworks) / BSL 1.1 → MIT 2046-01-01 (Nucleus) | LICENSE files; `nucleus/LICENSE` Change Date `2046-01-01` | SUPPORTED | none |

## llms.txt

| Line | Claim | Evidence | Verdict | Action |
|---|---|---|---|---|
| 3 | 14 models, 8 languages, pgwire | as above | SUPPORTED | none |
| 29, 141 | Nucleus "production-grade persistence" | `nucleus/README.md` self-describes as **developer preview**; Datalog has no durable store; distributed mode unsupported | UNSUPPORTED | Replaced with preview + per-model-durability pointer |
| 32 | Key SQL functions list | Spot-checked in `nucleus/src/executor/scalar_fns.rs` + specialty-surface tests | SUPPORTED | none |
| 37 | Rust "HTTP/1.1, HTTP/2, HTTP/3" | HTTP/3 behind `http3` feature only | NEEDS-CONSTRAINT | Constraint added |
| 44 | `@neutron-build/core` "v0.1.4" | `typescript/packages/neutron/package.json` = 0.2.0 | UNSUPPORTED (stale) | 0.2.0 |
| 46–47 | TS authoring rules / key exports | Verified against `packages/neutron/src/index.ts`, `vite/index.ts` (`neutronPlugin`), route conventions | SUPPORTED | none |
| 56 | Python "22 modules" | Count matches nothing (58 `.py` files, 17 top-level modules/packages) | UNSUPPORTED (stale) | Count dropped |
| 68 | Mojo "tracks the Mojo nightly toolchain, awaiting Mojo 1.0" | SDK on Mojo 1.0 since 2026-08-19 | UNSUPPORTED (stale) | Rewritten |
| 74 | Julia bridges incl. MTK | `julia/ext/NeutronJuliaMTKExt.jl` exists | SUPPORTED | none |
| 80 | Zig "zero heap allocations … Targets ARM, RISC-V, AVR, ESP32" | Allocator-backed pool; no embedded HALs, no cross-compilation builds in CI (`zig.yml` matrix is layer combos, native only) | UNSUPPORTED | Replaced with what ships; Zig ≥ 0.15 requirement added |
| 82 | Zig "(295 tests)" | 295 stale; 313 `test` declarations in source, 309 verified passing on Zig 0.15.2 (commit `23b9011c`, "verified here … rather than taken on report") | UNSUPPORTED (stale) | 309, with verification provenance |
| 90 | Native "700+ design tokens" | `ALL_TOKENS` union of 9 generated Tailwind-style maps (colors alone ≈ 450) | SUPPORTED | none |
| 95 | Desktop "~10MB bundles vs Electron's 100MB+" | No measurement artifact | NEEDS-CONSTRAINT | Labeled target |
| 100 | Studio "MCP server with 17 tools" | No MCP code in `cli/internal/studio/`; the MCP server is `neutron mcp` (`cli/internal/mcp`), already described in the CLI section | UNSUPPORTED | Removed from studio section |
| 105 | CLI "~21MB binary" | Local `go build` = 23.0 MB (darwin/arm64); platform-dependent | UNSUPPORTED (unreproducible number) | Number dropped |
| 105 | CLI commands incl. `build`, `run` | Neither exists as a top-level command | UNSUPPORTED | Actual command list |
| 133 | Lean4 "70 theorems" | `metrics.sh` counts 92 | UNSUPPORTED (stale) | 92; CI gate noted |
| 134 | Quint "27 spec files, 12 test files" | 15 and 14 | UNSUPPORTED (stale) | Corrected |
| 135 | Verus "Rust verification for nucleus…" | `verus/VERIFIED.md`: annotations written behind commented cfg blocks, "ready for Verus compilation when the tool is installed"; **no workflow references verus** | NEEDS-CONSTRAINT | Labeled annotations-only, not run in CI |
| 139 | "Preact (3KB) not React (42KB)" | Third-party library sizes (Preact's own claim) | SUPPORTED | none |

## FRAMEWORK_CONTRACT.md

| Line | Claim | Evidence | Verdict | Action |
|---|---|---|---|---|
| 3 | Contract frameworks "(Go, Python, Zig, TypeScript, Rust)" | Elixir implements the pipeline (`elixir/lib/neutron/middleware.ex` + tests) but was omitted | SUPPORTED (incomplete list) | Elixir added |
| §1–2 | Feature detection via `VERSION()`; RFC 7807 | Implemented in Go/Zig/Julia/Python clients (feature structs, problem-details modules) | SUPPORTED | none |
| §3 | SQL function tables | Spot-checked `KV_CEXPIRE`, `DATALOG_IMPORT_GRAPH`, `BLOB_DEDUP_RATIO`, `TIME_BUCKET`, `FTS_FUZZY_SEARCH` in `scalar_fns.rs`; exercised by executor tests | SUPPORTED | none |
| §3.14 | Retry reference impls + "each ships a 55P03 attempted-once test" | `go/nucleus/retry.go`+test, `python/neutron/nucleus/retry.py`+`tests/test_nucleus_retry.py`, `typescript/.../retry.ts`+`retry.test.ts` — all three exist with the 55P03 case | SUPPORTED | none |
| §3.14 | Observability metric names | Present in `nucleus/src/metrics/` | SUPPORTED | none |
| §7 | `/health` → `"nucleus": "connected"\|…` | **Drift:** Elixir returns `"nucleus": true` (boolean) | NEEDS-CONSTRAINT | **Reported, not edited** (fixing code is out of scope) |

## Per-directory READMEs

| File | Claim | Evidence | Verdict | Action |
|---|---|---|---|---|
| `nucleus/README.md` | Whole file (support tiers, durability caveats, cross-model WAL non-atomicity, client-compat list) | Cross-checked against `DURABILITY.md`, `MODEL_SEMANTICS.md`, `compat/` | SUPPORTED | none — this is the honesty benchmark the other files were brought up to |
| `typescript/README.md` | Package names `neutron`, `@neutron/auth`, … | Actual names are `@neutron-build/core`, `@neutron-build/auth`, … (package.json files) | UNSUPPORTED (stale) | Table corrected |
| `typescript/README.md` | Perf table (7 scenarios) | Traces 7/7 to one run (`run-2026-02-13T16-57-49-705Z.json`); Neutron's mutate row was bolded despite losing to Astro (776 vs 838) | NEEDS-CONSTRAINT | Provenance footnote added; mutate row un-bolded; corpus summary added |
| `typescript/README.md` | Two modes, streaming SSR, islands, caching, adapters | Adapters exist for static/node/docker/cloudflare/vercel/netlify; `cache`/`revalidateTag` exported; island tests in repo | SUPPORTED | none |
| `rust/README.md` | Feature table incl. `io_uring` (Monoio) | **No `io_uring` feature or monoio dependency exists anywhere in `rust/`** | UNSUPPORTED | Section + table row deleted |
| `rust/README.md` | Feature `h3` | Actual feature is `http3` | UNSUPPORTED (stale name) | Corrected |
| `rust/README.md` | Criterion microbench table | `benches/pipeline.rs` + `benches/router.rs` exist with run commands; numbers not re-run in this audit | SUPPORTED (harness verified) | none |
| `rust/README.md` | Features: middleware/JWT/sessions/WS/SSE/OpenAPI/TLS/HTTP-2 | Verified in crate source + `rust.yml` runs `cargo test` | SUPPORTED | none |
| `python/README.md` | "533 tests" | `pytest --collect-only` = 565 | UNSUPPORTED (stale) | 565 |
| `python/README.md` | "scores 12/12 on its conformance matrix" | `conformance/README.md` matrix: python 12/12 | SUPPORTED | none |
| `go/README.md` | Entire body (module paths `nucleus-go/kv…`, `ParseConfig`/`kv.New` API, "9 data models", "Status: Planned — not yet implemented") | Real SDK: single module `github.com/neutron-dev/neutron-go`, `nucleus.Connect` + typed model accessors, 14 models, 447 test functions, `go.yml`. The documented API/layout never shipped | UNSUPPORTED | Replaced with accurate README + replacement footnote (pattern set by `python/README.md`, S101) |
| `zig/README.md` | Entire body (`client.zig`/`comptime/`/`hal/` layout, `QueryType` comptime SQL validation, zero-alloc, 12KB binary, STM32F4 benchmark table, "Status: Planned") | Real library: 4 layers + `nucleus/` (14 models), 309 verified tests; no `QueryType`, no HALs, no size/bench artifacts. The CI-version contradiction found during the audit (workflow pinned 0.14.0 vs manifest ≥ 0.15.0) was fixed concurrently by `23b9011c` (everything on 0.15.2); the replacement README preserves that commit's Toolchain section | UNSUPPORTED | Replaced with accurate README + footnote; Toolchain section from `23b9011c` preserved |
| `native/README.md` | "preact-reconciler → Fabric (JSI)" bridge, RN 0.82+/Hermes V1, ten `@neutron-build/native-*` packages, `neutron release native` | Shipped: RN 0.76, no custom renderer (`render.ts` states this), device APIs inside core package, CLI has new/dev/run/build only, `native.yml` tests the real packages | UNSUPPORTED | Replaced with accurate README + footnote |
| `desktop/README.md` | "~10MB bundles, ~30MB idle" | No measurement artifact; `desktop.yml`/`desktop-release.yml` exist and run tests | NEEDS-CONSTRAINT | Labeled targets |
| `desktop/README.md` | Per-module JS packages `@neutron/desktop-*` | Single JS package `@neutron/desktop` (`packages/desktop`) | UNSUPPORTED (stale) | Table fixed |
| `desktop/README.md` | File tree `apps/example/`, `packages/neutron-shared/` | Actual: `examples/starter/`, `packages/desktop/` | UNSUPPORTED (stale) | Tree fixed |
| `desktop/README.md` | "Status: Planned — not yet implemented" | 13 crates, `cargo test --workspace` incl. embedded-Nucleus lifecycle test in `desktop.yml` | UNSUPPORTED (stale) | Status rewritten |
| `studio/README.md` | "all 9 data models", Rust backend, Tauri/AG Grid/D3/Cytoscape/keyring stack, "Status: Planned" | 14 model modules in `studio/src/modules/`; backend is Go in `cli/internal/studio`; deps are preact/signals/tanstack/maplibre/observable/codemirror; tests run via `cli.yml` | UNSUPPORTED | Replaced with accurate README + footnote |
| `cli/README.md` | Tools table `geo_radius`, `datalog_eval` | Actual tool names: `geo_distance`, `datalog_query` (`cli/internal/mcp/tools.go`) | UNSUPPORTED (stale names) | Corrected |
| `cli/README.md` | "~25 MB from a local `go build` on this platform" | Measured 23.0 MB here; wording is platform-scoped | SUPPORTED | none |
| `mojo/README.md` | "Mojo has not reached 1.0… tracks the `max` nightly (`max >= 25.1`)… validation 2026-02-20 on nightly" | Mojo 1.0 shipped 2026-08-11; SDK migrated 2026-08-19; `mojoproject.toml` mojo>=1.0/max>=26.5; `reports/core-validation-latest.md` 2026-08-20: 125/125 on 1.0.0; `mojo-validation.yml` runs it | UNSUPPORTED (stale) | Status section rewritten; `MIGRATION_GAPS.md` residuals referenced |
| `julia/README.md` | Model handles, extensions, pool, feature detection | File tree matches (`src/models/` 14 files, `ext/` 7, examples); `julia.yml` | SUPPORTED | none |
| `elixir/README.md` | "481 tests" | 496 `test "` declarations in `elixir/test` | UNSUPPORTED (stale undercount) | 496 |
| `modelica/README.md` | (listed in audit scope) | File does not exist — no action possible | N/A | none |

## Site (`typescript/apps/site/src`)

| File:line | Claim | Evidence | Verdict | Action |
|---|---|---|---|---|
| `routes/index.tsx:107` | "4,783 Declared tests" | `metrics.sh --check` asserts this figure positively | SUPPORTED | untouched (gated) |
| `routes/index.tsx:230` | Rust "1,210 tests across 19 crates" | 1,233 `#[test]`/`#[tokio::test]` attrs in `rust/crates` (1 ignored) | UNSUPPORTED (stale) | 1,233 |
| `routes/index.tsx:249–250` | Mojo "Awaiting Mojo 1.0" | As above | UNSUPPORTED (stale) | "Preview, on Mojo 1.0" |
| `routes/index.tsx:333` | Zig "40+ target architectures" | No cross-compilation evidence anywhere (CI builds native + layer combos only) | UNSUPPORTED | Replaced with layer-wise build claim |
| `routes/go.tsx:22` | Module `github.com/neutron-build/neutron-go` | `go/go.mod` = `github.com/neutron-dev/neutron-go` | UNSUPPORTED | Corrected |
| `routes/mojo.tsx` | "APIs may change with Mojo before 1.0"; "Tracks the current MAX nightly channel" | Post-migration stale | UNSUPPORTED (stale) | Status + toolchain facts updated |
| `routes/lean.tsx:9,32,70,93` | Lean page: 26 files/92 theorems/0 sorry/3 axioms, models-not-binary caveats, axiom-audit script | Matches `metrics.sh` output; `lean4.yml` runs canary + axiom audit; the two-limits section states the caveats verbatim | SUPPORTED | none — this page is the model for how to state proof claims |
| `routes/lean.tsx:98` | "Raft runs replication" | `nucleus/README.md:22`: "Distributed/Raft mode — **Incomplete and unsupported**" | UNSUPPORTED | Rewritten: Raft is the modeled design, shipping mode incomplete |
| `routes/nucleus.tsx:20` | "…and replication modules" | Modules exist in source but are the unsupported cluster path | NEEDS-CONSTRAINT | Constraint added in-line |
| `routes/quint.tsx` | "27 Quint specs" / "12 Invariant Tests" | 15 spec files, 14 test files | UNSUPPORTED (stale) | Corrected |
| `routes/native.tsx`, `desktop.tsx`, `studio.tsx`, `cli.tsx`, `workflow.tsx`, `ai.tsx`, `agents.tsx`, `client.tsx`, `orm.tsx`, `web.tsx`, `zig.tsx`, `julia.tsx`, `elixir.tsx`, `typescript.tsx`, `rust.tsx` | Product facts + "under active development" notes | Spot-checked against packages; notes present and accurate | SUPPORTED | none |
| `content/docs/verification/overview.mdx` | "Neutron ships with two verification tools as cargo dependencies" (Kani, Shuttle) | Zero references to kani/shuttle in `rust/` (no deps, harnesses, or workflows); page's own Getting Started tells the reader to add them — self-contradicting | UNSUPPORTED | Reframed as third-party tools you can apply; scope table qualified |
| `content/docs/verification/kani.mdx`, `shuttle.mdx` | Third-party tool guides | Framed as external tools with links; no false shipping claims once overview fixed | SUPPORTED | none |
| `content/docs/verification/verus.mdx` | "Specs and proof structures are written. Inline annotations are deferred…" | Matches `verus/VERIFIED.md` | SUPPORTED | none |
| `content/docs/verification/lean4.mdx` | "proof about the models, not about the compiled Rust binary itself" | Accurate | SUPPORTED | none |
| `content/docs/nucleus/*.mdx` | Per-model docs | No unsupported performance claims found in sweep (only a definitional "2.5x" dedup-ratio example) | SUPPORTED | none |
| `routes/blog/*` | Launch-post figures | Dated posts; `metrics.sh` sweep exempts `at launch (2026-02-15)` figures by design | SUPPORTED (policy) | none |

## docs/

| File | Claim | Evidence | Verdict | Action |
|---|---|---|---|---|
| `docs/benchmarks/*.md` (6 files) | Nucleus-vs-Postgres/Redis numbers | Already banner-warned 2026-08-15: in-RAM MVCC, failed ops timed as successes; points to `nucleus/docs/BENCH_VS_POSTGRES.md` as the careful document | SUPPORTED (warnings present) | none |
| `docs/README.md` | Self-describes as working material, not user docs | True | SUPPORTED | none |

## Evidence-chain findings (reported, not edited)

These are cases where the **runner leg** is questionable. Editing them means
changing code or CI config, which is outside a documentation audit — each
needs a human decision.

1. **`zig.yml` pinned Zig 0.14.0 while `build.zig.zon` required ≥ 0.15.0**
   (found at commit `05011eaa`). **Resolved during this audit** by concurrent
   commit `23b9011c` ("settle on one compiler version instead of three"):
   workflow, manifest, and README now all name 0.15.2, with `zig build test`
   309/309 verified locally on that version. No action remains; recorded here
   because the audit's zig rows were drafted before the fix landed.
2. **Elixir `/health` shape drift.** `FRAMEWORK_CONTRACT.md` §7 specifies
   `"nucleus": "connected"|"disconnected"|"unconfigured"`; the Elixir README
   documents `{ "status": "ok", "nucleus": true, … }` (boolean). One of them
   needs to change.
3. **Rust conformance failures.** `conformance/README.md` records Rust failing
   `health.types` and `validation.format` (10/12). Nothing in the Rust README
   mentions this; root README now does.
4. **Verus has no runner.** No workflow references `verus/`; its own
   `VERIFIED.md` says annotations await toolchain installation. Any future
   claim that Verus "verifies" anything is currently false.

## Verification commands run during this audit

- `sh nucleus/scripts/metrics.sh --check` — `Canonical plan metrics are current.` (exit 0) at audit start. **Note:** re-run at the end, it fails on `Source LOC: 310456` / unit tests 4382 — caused by concurrent uncommitted nucleus work in the working tree (`kv_wal.rs`, `collections_wal.rs`, `crashpoint.rs`, +96 net lines, +2 tests; commits `23b9011c` and `14e1d512` landed during this session and the tree has further in-flight changes). None of those files were touched by this audit; per repo policy the author of that change updates the gated numbers — not this audit, and not by hand from in-flight state.
- `pnpm --filter neutron-site build` (in `typescript/`) — `Build complete!` / `Rendered 271 pages, skipped 0.`
- `python -m pytest --collect-only -q` (in `python/`) — `565 tests collected`.
- `go build` (in `cli/`) — binary 23.0 MB (darwin/arm64).
- `zig build test` (in `zig/`) — fails on the local Zig 0.16.0 (27 compile
  errors), exactly as `build.zig.zon` documents; superseded by the 309/309
  verification on 0.15.2 recorded in `23b9011c`.
