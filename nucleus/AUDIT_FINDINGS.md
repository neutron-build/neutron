# Nucleus — Audit Findings Log

Running log for the audit/hardening effort described in `AUDIT_PLAN.md`.
Severity order: correctness > memory-safety > security > durability > performance > quality.

## RESUME HERE (for a compacted or fresh session)

- **Branch:** `audit/nucleus-correctness-phase2-3` (pushed to origin). Keep working here.
- **Read first:** this file (fixed = F-005..F-028; "Deferred / verified-real" = the
  remaining work with rationale) + `AUDIT_FINDINGS_RAW.json` (full detail for all 76
  findings incl. the 24 medium + 8 low, plus the unsafe inventory).
- **State:** build + 3603 lib tests + `clippy --all-targets -D warnings` + `cargo fmt`
  all green. Verify each finding against real code before fixing — the audit agents had
  false positives and one inverted finding (F-009). Add a regression test per fix. Stage
  `nucleus/` only (the repo has unrelated `typescript/` changes; don't commit those).
- **Suggested order:** (1) low-risk batch — the 24 medium + 8 low + `unsafe` SAFETY
  comments; (2) the high deferred items that need signature changes (ZRANGE i64);
  (3) **MVCC GC** — diff against `lean4/Nucleus/Nucleus/Proofs/MvccProofs.lean` BEFORE
  touching; (4) feature-sized: RLS-to-authenticated-user wiring, WAL header-CRC format change.
- **AFTER the engine is fully done:** audit/fix the surrounding tooling too — `studio/`,
  the per-language ORMs/SDKs (`go/`, `ts/` neutron-data, `python/`, `rust/`, `zig/`,
  `elixir/`, `julia/`), and anything depending on engine semantics changed here (new error
  variants, DISTINCT ON error propagation, ON CONFLICT enforcement, etc.). Requested by Tyler.

## Phase 2+3 audit — summary (in progress)

A multi-agent discovery pass (per-subsystem correctness review + adversarial
verification, plus an `unsafe` enumeration) surfaced **130 raised → 76 confirmed**
correctness findings (4 critical, 40 high, 24 medium, 8 low) and **53 `unsafe`
blocks (0 unsound, 23 undocumented)**.

Every finding below was **re-verified against the actual code by hand** before
acting — the agents had real false positives and at least one *inverted* finding
(the WAL "off-by-4" pointed at the correct line and called it the bug; see F-009).

**Fixed + verified so far (build/test/clippy/fmt all green, 3602 lib tests pass):**
all 4 criticals (F-005..F-008) and 18 high-severity findings (F-009..F-026),
with regression tests on the criticals and the WAL fix.

**Not yet fixed:** the remaining high/medium/low findings and the `unsafe`
documentation pass are catalogued under "Deferred / verified-real" below, each with
the reason it wasn't changed in this pass (ripple risk, design decision, needs
cross-check against a lean4 proof, or part of an unfinished subsystem). This engine
is ~202K LOC; this is an honest checkpoint, not a claim of completeness.

---

## Critical — FIXED

- **F-005 `ABS(i32::MIN)`/`ABS(i64::MIN)` panic** (`executor/scalar_fns.rs`). Unchecked
  `.abs()` panics on MIN. → `checked_abs()` + "integer out of range". Regression test.
- **F-006 INCR/INCRBY silent overflow** (`kv/mod.rs`). `current + amount` wraps/panics at
  the i64 boundary. → `checked_add` + new `KvError::Overflow` (Redis-compatible). Test.
- **F-007 columnar `concat_columns` ragged-column corruption** (`columnar/mod.rs`). A
  type-mismatch fell to `_ => a.clone()`, dropping `b`'s rows and leaving the column
  shorter than its siblings (misaligned batch). → fail loud on the schema-invariant
  violation. `#[should_panic]` test.
- **F-008 silent shard orphaning** (`sharding/mod.rs`). A shard with no geo-valid alive
  node was dropped from all assignments. → added `RebalancePlan::unplaceable` so it is
  surfaced; geo-invalid-but-alive shards stay on their current node. Test.

## High — FIXED

- **F-009 WAL replay off-by-4 on corrupt control records** (`storage/wal.rs`). The
  control-record CRC-skip advanced `pos += 4 + record_len`, but `record_len` already
  includes its own 4-byte prefix (`RECORD_HEADER_SIZE`), so it over-shot and silently
  dropped every record after a corrupt one. → advance by `record_len`. Also fixed
  `bytes_written` over-counting by 4/record. Regression test (corrupt-then-recover).
  *(The agent's finding was inverted — it would have broken working replay.)*
- **F-010 B-tree corrupt-page panics** (`storage/btree.rs`, 3 readers). Slicing with a
  page-read `key_len` could exceed `PAGE_SIZE` and panic. → bounds-check + stop/log.
- **F-011 LPAD/RPAD byte slicing** (`executor/scalar_fns.rs`). `s[..target_len]` panics on
  a non-char-boundary and breaks Unicode. → operate on characters.
- **F-012 GCD/LCM/DECODE panics+overflow** (`executor/scalar_fns.rs`). `.abs()` on i64::MIN,
  `a/ga*b` overflow, and `DECODE(hex)` slicing `encoded[i..i+2]` (panics on odd length,
  silently dropped bad digits). → checked ops + validated hex decode.
- **F-013 ON CONFLICT DO UPDATE skipped constraints & conflict re-check** (`executor/dml.rs`).
  → wired `enforce_constraints(..., skip_row_idx=Some(pos), all checks)` before the update.
- **F-014 INSERT…SELECT column-count not validated** (`executor/dml.rs`). Silently filled
  missing columns with defaults. → error on arity mismatch.
- **F-015 dead/illogical DEFAULT validation** (`executor/dml.rs`). Simplified to correct
  Postgres semantics (count mismatch → error). *(Agent's suggested fix was wrong.)*
- **F-016 DISTINCT ON dropped eval errors** (`executor/query.rs`). `filter_map(...ok())`
  yielded incomplete keys. → propagate errors.
- **F-017 LIMIT+OFFSET overflow** (`executor/query.rs`, 2 sites). → `saturating_add` for
  pushdown, `checked_add` for top-K.
- **F-018 FTS stem() tautology** (`fts/mod.rs`). A tautological predicate stripped a
  consonant after every `-ing` ("eating"→"ea"). → only undo a real doubled consonant.
- **F-019 IVFFlat dimension `assert_eq!` panic** (`vector/mod.rs`). → log + return empty.
- **F-020 JSON number overflow** (`document/mod.rs`). `f64 as i64` wraps for values > i64. →
  range-check before the integer cast.
- **F-021 timeseries stale `last_value` after full retention purge** (`timeseries/mod.rs`).
  → drop the cache entry when a series is emptied.
- **F-022 datalog worker-join panics crash the server** (`datalog/mod.rs`, 2 sites). →
  `join()` handled gracefully + logged (full Result propagation noted as ideal).
- **F-023 disk scan silently drops undeserializable tuples** (`storage/disk_engine.rs`). → log.
- **F-024 graph WAL replay silently stops on unknown/corrupt tag** (`graph/wal.rs`). → log.
- **F-025 CDC WAL append failures silently ignored** (`executor/mod.rs`, 2 sites). → log
  (CDC advertises durability).
- **F-026 vector SIMD unsafe soundness** (`vector/mod.rs`). Raw-pointer distance loops relied
  on a debug-only equal-length invariant (UB in release if violated). → clamp
  `n = a.len().min(b.len())` (unconditionally in-bounds) + `// SAFETY:` docs.
- **F-027 KV SETBIT unbounded allocation** (`resp/handler.rs`, DoS). A huge offset resized the
  bitmap arbitrarily. → reject offsets ≥ 2^32 at the handler (Redis cap). Regression test.
- **F-028 datalog unbound head var → empty-string key** (`datalog/mod.rs`). An ungroundable
  head variable was substituted with `""`, corrupting the aggregate grouping key. → skip the
  binding (`continue`) so only ground facts are produced.

## Medium — FIXED

- **F-029 MOD by zero / `MIN % -1` panic** (`executor/scalar_fns.rs`). `%` panics on
  `i32::MIN % -1`; mod-by-zero gave a wrong message. → explicit "division by zero" error +
  `checked_rem` (MIN%-1 = 0). Regression test. *(The agent's "return NULL" was wrong —
  Postgres errors on mod-by-zero.)*
- **F-030 GENERATE_SERIES step overflow** (`executor/scalar_fns.rs`). `current += step`
  overflows near i64 bounds (panic / infinite loop). → `checked_add` stops the series.
- **F-031 BlobStore::get_range offset+length overflow** (`blob/mod.rs`). → `saturating_add`.
- **F-032 B-tree find_child corrupt-page panic** (`storage/btree.rs`). Unchecked `key_len`
  slice. → bounds-check + stop/log (completes the F-010 family).
- **F-033 page iter_tuples corrupt-slot panic** (`storage/page.rs`). Slice from a corrupt
  slot offset/len. → bounds-check + skip/log. (`insert_tuple`'s u16 cast is bounded by
  PAGE_SIZE < 65535 — non-issue.)
- **F-034 Streams WAL snapshot wipes data before validating** (`pubsub/streams_wal.rs`). A
  corrupt SNAPSHOT cleared all recovered state, then failed. → parse into a temp map, swap
  in only on success.
- **F-035 Dijkstra silently wrong on negative weights** (`graph/mod.rs`). → clamp negatives
  to 0 + warn (Dijkstra requires non-negative; Bellman-Ford needed for negatives).
- **F-036 FTS merge_segments silent posting loss** (`fts/mod.rs`). `dedup_by_key` dropped
  duplicate-doc_id postings. → `debug_assert` to catch the upstream re-indexing bug early;
  dedup kept as a release safety-net.
- **F-037 COPY FROM STDIN unbounded allocation** (`wire/mod.rs`, DoS). → cap the accumulated
  buffer at 512 MB and error (program_limit_exceeded).
- **F-038 MemoryBudget::try_allocate check-then-act race** (`allocator/mod.rs`). Concurrent
  callers could both pass the check and over-allocate past the limit. → CAS loop that checks
  and reserves atomically (also `saturating_add` against overflow).

### False positives confirmed (no change)

- **MOD divisor-zero "should return NULL"** — Postgres errors; returning an error is correct
  (only the message was improved, see F-029).
- **`BlobIndex::find_chunk` "missing bounds check"** — already handles empty and returns a
  valid index; the caller (`get_range`) range-checks. Not a bug.
- **INSERT DEFAULT-validation "always errors"** (F-015) and **WAL "off-by-4 at line 429"**
  (F-009) — both were misdiagnosed by the agent; the suggested fixes would have *introduced*
  bugs. Fixed the real issues instead.

## Deferred / verified-real (not changed this pass — with rationale)

These are confirmed real but were left for a focused follow-up because a blind change
is risky or the fix is a larger feature. Triaged honestly rather than rushed.

- **MVCC GC watermark** (`storage/mvcc.rs`, high). Possible removal of still-visible
  versions. Must be diffed against the proven algorithm in
  `lean4/Nucleus/Nucleus/Proofs/MvccProofs.lean` before touching — too risky to change
  blind. **Highest-priority follow-up.**
- **WAL page-write CRC covers only the page, not the header** (`storage/wal.rs:210`, high).
  Header corruption (page_id/txn_id) goes undetected. Fix is a WAL-format change (extend
  CRC over header+page on both write and read) — do deliberately at a version boundary.
- **RESP ZRANGE negative indices** (`resp/handler.rs`, high). `i64 as usize` turns `-1` into a
  huge index. Needs `col_zrange` to take `i64` + relative-index handling (signature ripple).
- **Security: SessionContext never set to the authenticated user** (`executor/session.rs`,
  high). RLS/masking always evaluate as user "nucleus"; the engines exist but are not wired
  into scan paths against the real connection identity. This is a **feature-sized** gap, not
  a one-liner — prominent follow-up.
- **document empty-JSONB containment returns nothing** (`document/mod.rs`, high) — should match
  all (GIN `query_contains` empty-query special case).
- **vector HNSW filtered early-exit** (`vector/mod.rs`, high) — may return < k under a selective
  filter; needs careful recall analysis, not a blind reorder.
- **reactive CDC consumer offsets lost on checkpoint** (`reactive/cdc_wal.rs`, high) — needs a
  CdcLog API to expose consumer positions.
- **fts merge_segments dedup data loss** (`fts/mod.rs`, high) — add `debug_assert` / merge
  positions; rare path.
- **graph Dijkstra negative-weight handling** (`graph/mod.rs`, high) — validate non-negative or
  fall back to Bellman-Ford (design decision).
- **binary protocol param substitution** (`binary_wire/query_handler.rs`, high) — real
  `replace("?", …)` bug, but the binary protocol is an unimplemented stub (its tests are
  `#[ignore] "awaiting Phase 1 binary protocol implementation"`); fix when the protocol is built.
- **executor-agg AVG int→float precision**, **graph float-to-int index consistency**,
  **kv silent WAL on SET** — lower impact; batch with the relevant subsystem.
- **24 medium + 8 low** correctness findings — full detail (description, evidence, suggested
  fix, refutation) preserved in `AUDIT_FINDINGS_RAW.json`; to be triaged subsystem-by-subsystem.

## `unsafe` audit (Phase 2)

53 `unsafe` blocks; the audit found **0 unsound**. 23 are "undocumented" — i.e. they need a
`// SAFETY:` comment, not a code fix. Highest-value one (`vector/mod.rs` distance loops) was
both **hardened** (unconditional bounds via `min`) and documented (F-026). Remaining work is
mechanical SAFETY-comment coverage across `simd/`, `storage/`, `executor/session.rs`, and the
`env::set_var` calls in `config/`'s tests (edition-2024 `unsafe`, test-only) — no UB to fix.

---

## Phase 1 — Green baseline (DONE)

Toolchain at audit time: rustc 1.93.1, edition 2024.

| Gate | Before | After |
|------|--------|-------|
| `cargo build --bin nucleus` | green (4 warnings) | green, 0 warnings |
| `cargo test --lib` | 1 flaky failure (see F-001) | **3597 passed, 0 failed**, deterministic |
| `cargo clippy --bin nucleus -- -D warnings` | 38 findings | **clean** |
| `cargo clippy --all-targets -- -D warnings` | ~65 findings | **clean** |
| `cargo fmt --check` | drift | **clean** |

The `E0133` unsafe-block errors the plan anticipated did not reproduce on rustc
1.93.1 — they were already resolved by the current toolchain/edition-2024 setup.

Release gate re-coupled (`.github/workflows/nucleus-release.yml`): clippy is a gate
again (no tag skip), upgraded to `--all-targets`, and `build.needs: [test, clippy]`.

> The CI `test` job still runs `cargo test --lib` (not full `cargo test`) because
> of the pre-existing integration failures in **F-004**. Switch it to full once
> F-004 is resolved.

---

## F-001 — Flaky/incorrect scalar subquery over a VIEW  ·  CORRECTNESS  ·  FIXED

**Symptom.** `integration_tests::tests::test_view_with_subquery_and_aggregation`
failed ~50% of runs (`r.len() == 0`, expected `1`). Stable within a process,
flipped between processes — the signature of HashMap-seed-dependent behavior, not
a thread race.

**Query.** `SELECT region, total_sales FROM regional_sales
WHERE total_sales > (SELECT AVG(total_sales) FROM regional_sales) ORDER BY region`,
where `regional_sales` is a `GROUP BY` view. The per-row scalar subquery returned a
corrupted value, so the `WHERE` filtered every row.

**Root cause.** The plan-execution path threaded the plan-cache key through a
process-global single slot, `Executor::plan_cache_key_hint: Mutex<Option<String>>`.
Under reentrant execution (outer view scan → per-row scalar subquery → re-expansion
of the same view), a nested `execute()` overwrote the slot mid-flight and the wrong
`execute_query` consumed it, caching/reusing a plan under the wrong key → wrong
columns/values. The engine even carried a comment at `query.rs` warning of this
exact "catastrophic cross-contamination." `SET plan_execution = off` made the bug
disappear deterministically, confirming the plan path.

**Fix.** Removed the shared slot as a correctness dependency by threading the key
explicitly (`src/executor/query.rs`, `src/executor/mod.rs`):
- `execute_query(query)` is now a thin wrapper that always recomputes its own key
  (used by all nested/subquery/CTE/view callers) — it never reads the global hint.
- `execute_query_planned(query, plan_cache_key)` takes the key as a parameter.
- Only the top-level statement dispatcher (`execute_single_statement`) passes the
  precomputed key, consumed via `take_plan_cache_key_hint()`.

Verified: 16/16 isolated runs pass; full `cargo test --lib` 3597/0.

**Follow-up (defense in depth, not yet done).** The plan `SeqScan` path has no VIEW
handling at all (`query.rs` `plan_table_scan`/`SeqScan` arm) — views only work via
the AST path. Consider making `query_eligible_for_plan` return false when any
referenced relation is a view, so a view query can never silently take the plan
path. Tracked here for Phase 3.

---

## F-002 — GIN index built but never read  ·  QUALITY/COMPLETENESS  ·  DEFERRED (Phase 3)

`Executor::gin_indexes` is populated at `CREATE INDEX` time (`executor/ddl.rs`) but
the query planner never consults it to accelerate `@>` containment scans — the map
is write-only, so `GinIndexEntry`'s fields are dead. Marked `#[allow(dead_code)]`
with a note in `src/executor/types.rs` rather than deleted (the index-build code is
real and intended). Phase 3: wire up the GIN read path.

---

## F-003 — `coerce_rows_to_schema` not wired into columnar INSERT  ·  CORRECTNESS-RISK  ·  DEFERRED (Phase 3)

`coerce_rows_to_schema`/`coerce_value` (`src/executor/dml.rs`) were written to
type-coerce extended-protocol text params before MergeTree ingest, but are never
called — extended-protocol params can reach `columnar_engine::rows_to_batch`
untyped. Marked `#[allow(dead_code)]` with a note. Phase 3: confirm whether MergeTree
columns store text for extended-protocol inserts and wire coercion if so.
(Related to F-004.)

---

## F-004 — Extended-protocol params for BIGINT columns rejected by strict clients  ·  WIRE/TYPE-INFERENCE  ·  OPEN (Phase 5)

**Pre-existing**, fails on a clean tree (independent of the Phase-1 work).

`tests/otlp_ingest_visibility.rs` (3 tests: `otlp_style_insert_visible_same_connection`,
`..._to_separate_connection`, `..._concurrent_inserts_all_visible`) fail with
`WrongType { postgres: Int8, rust: "alloc::string::String" }`. The test binds Rust
`String` values to `BIGINT` columns (`start_time`/`end_time`/`duration_ms`) via
`$3,$4,$5`, mirroring the neutron-go OTLP handler that sends text. The error is
raised **client-side** by tokio-postgres because Nucleus's `ParameterDescription`
declares those inferred params as `Int8`, so a strict typed client refuses to send a
`String`.

**Decision needed (do not change wire behavior to make a test pass):** either
(a) the test should bind typed `i64`, accepting that text-for-int params are
unsupported by Postgres semantics, or (b) Nucleus should declare column-inferred
`$N` params as unknown/text-coercible (oid 0) and coerce server-side, to support
drivers that send text (the documented neutron-go flow). Resolve in Phase 5 and then
flip the CI `test` job to full `cargo test`.

---

## Note — `clippy::approx_constant` in test fixtures

~25 sites use `3.14`/`3.14159` as arbitrary test floats (the float analog of `42`/
`"hello"`), which clippy reads as imprecise π. They are **not** π, and several are
value-sensitive (`ROUND(3.14159,2)≈3.14`, `CAST('3.14'…)`, a pg_compat tolerance) —
so both rewriting the literal and substituting `std::f64::consts::PI` would change
results (PI is 0.00159 from 3.14, which breaks the tolerance asserts). Resolved with
scoped `#[allow(clippy::approx_constant)]` on the affected test modules, each with a
comment. Production code is unaffected (future real π misuse is still caught).
