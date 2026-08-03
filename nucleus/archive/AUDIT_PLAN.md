# Nucleus — Audit & Hardening Plan

> Working document for a dedicated session focused **only** on `nucleus/`.
> Goal: (1) get build + test + clippy green and unblock the `nucleus/v0.1.0`
> release, then (2) systematically audit and harden the entire engine —
> correctness, safety, security, performance, quality — fixing or improving
> everything that should be.

---

## 0. Orientation — read before touching code

1. **`nucleus/CLAUDE.md`** — engine-specific context (defer to it).
2. `nucleus/Cargo.toml` — edition, features, `[[bin]] nucleus` (line ~102), lib target, deps. **Check the Rust edition** (2021 vs 2024) — this likely explains the `E0133` errors below.
3. Workspace layout: `src/` modules seen so far include `simd/`, `fts/`, `compliance/`, `executor/`, `binary_wire/`, `memory/` (has `Pressurable`). Map the rest.
4. **Verification assets are oracles** — `lean4/` (proofs: MVCC, B-tree, WAL, Raft, HMAC, Bloom, LRU, SlidingWindow), `quint/` (Multi-Raft, resharding, distributed tx), `verus/`. Where a proof exists, the code must match the proven algorithm.
5. **Hard constraints:**
   - **pgwire compatibility is the contract** — real PostgreSQL clients must keep working. Do not change wire behavior to make a test pass.
   - Error format / RFC 7807 parity where applicable.
   - BSL 1.1 license headers stay intact.
   - 14 data models: SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, PubSub.

---

## Phase 1 — Establish a GREEN baseline (do this first)

This both unblocks the release and gives a safety net (a passing suite) before the broader audit. Reproduce, then fix root causes. **Never weaken a test to make it pass.**

### 1a. Build (known-good — confirm it still is)
```bash
cd nucleus
cargo build --bin nucleus            # passed in CI as of 2026-05-31
cargo check --bin nucleus            # had 1 warning: unused `use nucleus::memory::Pressurable;`
```

### 1b. Tests — the release blocker
```bash
cargo test --lib                     # FAILED in CI — reproduce and capture full output
cargo test                           # full suite (lib test skips slow stress tests — run everything)
```
Known from CI (verify against current toolchain — date/Rust may have moved):
- **`E0133` (unsafe-block required)** with locations in `src/simd/aggregates.rs` (many lines: 186–293), `src/fts/mod.rs:262/275`, `src/compliance/mod.rs:1017`, `src/executor/types.rs:77`, `src/executor/dml.rs:1531/1550`, `src/binary_wire/tests/error_tests.rs:19`. **Hypothesis:** Rust 2024 edition / newer stable made things like `std::env::set_var`, `unsafe` attributes (`unsafe(#[no_mangle])`), or calls to `unsafe fn` require explicit `unsafe { }`. Fix by wrapping in `unsafe { }` (or marking the fn `unsafe`) — but for each, **confirm the safety invariant actually holds** and add a `// SAFETY:` comment. Don't blanket-`#[allow]`.
- **Failing test `integration_tests::tests::test_view_with_subquery_and_aggregation`** (exit 101). Trace the view + subquery + aggregation path in the SQL `executor/` (planner + `dml.rs`). Determine whether it's a real correctness bug or a stale expectation; fix the **root cause**. Add/keep a regression test.

### 1c. Clippy — 38 findings at `-D warnings`
```bash
cargo clippy --bin nucleus -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings   # broader
```
Histogram captured locally (fix all; the dead-code ones need judgment — remove vs wire up vs `#[allow]` with a written reason):
- 11× collapsible `if`
- 5× very complex type (factor into `type` aliases)
- 3× needless range loop (`for k in 0..` indexing)
- 2× missing `Default` impl (`PubSubRegistry`, `GeoSet`)
- 2× `clamp`-like pattern → use `.clamp()`
- 2× `unwrap()` after `is_some()` check (`czm.min`/`czm.max`) → use `if let`/`?`
- 1× `&mut Vec` should be `&mut [_]`
- 1× `or_insert_with` for a `Default` → `or_default`
- 1× private type in public API (`fts::DocInfo` leaked via `FtsUndoOp::RemovedDoc::info`) — **also a real API-encapsulation issue**
- 1× function too many args (9/7)
- 1× `GeoSet` has `len` but no `is_empty`
- 1× redundant closure
- 1× manual `is_multiple_of`
- dead code: fns `coerce_value`, `coerce_rows_to_schema` never used; fields `table_name`, `column_name`, `col_idx`, `index` never read — investigate intent before deleting.

### 1d. Re-couple the release gate
`.github/workflows/nucleus-release.yml` currently **skips clippy on release tags** and dropped it from `build.needs` (done during the 0.1.0 push to avoid blocking on the 38 lints). Once 1a–1c are green, **revert that**: clippy `if:` guard + `build.needs: [test, clippy]` back to gating.

### 1e. Gate → release
When `cargo build`, `cargo test` (full), and `cargo clippy --all-targets -- -D warnings` are all green and committed, the `nucleus/v0.1.0` release is unblocked. Tag `nucleus/v0.1.0` (the 4-platform release workflow is ready; the TS framework already shipped 0.1.0 to npm). Coordinate the tag push (see `release-and-publishing` memory: push to `origin`; tag force-updates may need a direct `git push github`).

---

## Phase 2 — Safety audit (`unsafe`)

```bash
rg -n "unsafe" --type rust nucleus/src
```
For **every** `unsafe` block/fn:
- Write/verify a `// SAFETY:` comment stating the invariant and why it holds.
- Hotspots: `simd/` (alignment, lane counts, scalar-fallback equivalence), `memory/` (allocators, `Pressurable`, any arena/mmap), `binary_wire/` (raw buffer/length parsing), zero-copy/blob paths, any `transmute`/raw-pointer arithmetic/FFI.
- Prefer rewriting to safe code where it costs nothing; keep `unsafe` only where justified and documented.
- If a nightly toolchain is available: `cargo +nightly miri test` on the miri-compatible subset to catch UB. Extend `nucleus/fuzz/` targets for the wire/parse paths.

---

## Phase 3 — Correctness audit (per-model + shared engine)

Go module-by-module. For each: read the public API → the core data structure → the mutation paths → the recovery/error paths. Hunt for: off-by-one, integer overflow/underflow, `unwrap`/`expect` on external/wire input, reachable panics, lost updates, isolation violations, resource/handle leaks, incorrect error propagation.

- **Shared engine (highest risk):** MVCC (snapshot isolation correctness), WAL (durability, fsync discipline, replay/recovery, partial-write handling), B-tree (split/merge, concurrent access), transaction manager, SQL planner + `executor/`.
- **The 14 models:** SQL · KV · Vector (distance fns + index recall) · TimeSeries · Document · Graph (traversal, shortest-path) · FTS (tokenization, ranking, the `DocInfo` leak from 1c) · Geo · Blob · Streams · Columnar · Datalog (recursion/fixpoint termination) · CDC · PubSub.
- **Cross-check against `lean4/` proofs and `quint/` specs** — where an algorithm is proven, diff the implementation against it; flag divergence.
- Every confirmed bug gets a **regression test** before the fix is considered done.

---

## Phase 4 — Concurrency & durability

- Lock ordering (deadlock potential), atomics ordering correctness, data races.
- Crash consistency: kill-during-write, WAL replay, fsync points, torn pages.
- Stress: `cargo test -- --include-ignored`, high thread counts; if feasible add `loom` model-checking for the lock-heavy structures.
- Raft/replication correctness vs the `quint/` Multi-Raft + resharding specs.

---

## Phase 5 — Wire protocol & security

- **pgwire**: exercise with real Postgres clients (`psql`, drivers) — startup, auth, extended-query protocol, error responses, the SQL functions for non-relational models (`KV_GET`, `VECTOR_DISTANCE`, `GRAPH_SHORTEST_PATH`, …).
- **Robustness/DoS**: malformed/oversized packets, attacker-controlled length fields, slow-loris, unbounded allocations. Verify input limits, query timeouts, and memory backpressure (`memory::Pressurable`).
- Fuzz `binary_wire/` parsing (where the `E0133` was) via `nucleus/fuzz/`.
- AuthN/AuthZ, TLS, secrets handling, SQL-injection surface in the SQL layer.

---

## Phase 6 — Performance (only after correctness)

- Use the existing competitive benchmark suite; **measure before optimizing.**
- SIMD: verify `simd/aggregates.rs` scalar fallback produces identical results to the SIMD path across widths; check runtime feature detection.
- Allocation churn, needless copies, index/scan efficiency, hot-path profiling.

---

## Phase 7 — Quality & maintainability

- `cargo clippy --all-targets --all-features -- -D warnings` clean.
- Dead code resolved (remove or wire up, with rationale).
- Consistent error handling (one error enum, no `unwrap` on fallible paths reachable from the wire).
- `cargo doc` builds; public API documented. `cargo fmt --check` clean.
- Fill test-coverage gaps surfaced during the audit.

---

## Methodology & guardrails

- **Small, verifiable commits**; run `cargo test` (or at least the affected module's tests) after each. Keep the suite green.
- **Root-cause only** — never weaken/delete a test to get green.
- **`unsafe`: prove or remove** — justify with `// SAFETY:` or rewrite safe.
- **Don't break pgwire** — it's the shared contract for every language SDK.
- **Specs are truth** — match `lean4`/`quint` proven algorithms; flag divergences loudly.
- **Loop-until-dry** per phase: keep finding+fixing until a clean pass surfaces nothing new.
- **Triage by severity**: correctness > memory-safety > security > durability > performance > quality. Keep a running findings log (a `nucleus/AUDIT_FINDINGS.md` you append to).
- This is a **large, multi-pass effort** (~154K LOC). For broad sweeps (per-model reads, `unsafe` enumeration, clippy fix-out), consider fanning out with the multi-agent workflow tool; for correctness fixes, go careful and serial with tests.

## Definition of done

- `cargo build --bin nucleus`, full `cargo test`, and `cargo clippy --all-targets -- -D warnings` all green; `cargo fmt --check` clean.
- Every `unsafe` documented and verified (or removed).
- Per-model audit notes + a findings log, each fixed bug covered by a regression test.
- Release gate re-coupled to clippy; **`nucleus/v0.1.0` tagged and released.**
