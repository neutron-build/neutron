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
- **Dogfood-driven cluster (D-1..D-10), see "teploy-observe 2026-06" section below:**
  product-blocking findings from a real consumer over pgwire — **D-1 (UPDATE no-op breaks
  API-key revocation) is security-critical**. Source brief:
  `_internal/NUCLEUS_AUDIT_FROM_TEPLOY_OBSERVE_2026-06.md`. These are verify-first (confirm
  current engine semantics, some may already be fixed) then fix; each owes observe a one-line verdict.
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

## Phase A verification sweep — outcome (2026-06)

A 13-agent read-only sweep re-triaged the 24 medium + 8 low against current code and
statically answered the 10 D-cluster questions. **Med/low triage: 8 already-fixed (by
F-005..F-038), 14 false-positive (refuted with evidence — MOD-by-zero, insert_tuple u16
cast, COUNT SIMD, find_chunk, etc.), 10 still-real** (mostly low — Phase C). **D-cluster:
D-1/D-2/D-3/D-5(tag)/D-10#24/#26 confirmed real bugs; D-6/D-7/D-8 partial; D-9 OK; D-10#28
not reproducible (already correct).** Full evidence in the workflow result; still-real items
tracked below and in the Phase C/D batches.

### Phase B1 — FIXED (security + silent-wrong correctness)

- **D-1 UPDATE/DELETE physical-position bug (security)** — replacing/aggregating MergeTree
  without a single-col PK constraint fed *deduped* scan positions to `update()`/`delete()`
  (which index physical batches) → wrong row or silent no-op (the API-key-revocation no-op).
  → added `StorageEngine::scan_physical` (default = `scan`; columnar override returns physical
  batches, like `scan_where_eq_positions`); UPDATE/DELETE non-PK path now uses it
  (`storage/mod.rs`, `storage/columnar_engine.rs`, `executor/dml.rs`). Regression tests for the
  no-PK UPDATE and DELETE paths.
- **D-3 aggregate dedup** — `fast_sum_f64(_filtered)`/`fast_min_f64`/`fast_max_f64`/`fast_group_by`
  summed superseded versions (COUNT deduped, SUM didn't). → added `select_batches()` helper;
  all five route through it (dedup when `replacing_config` present). Test (COUNT/SUM/MIN/MAX agree).
- **D-2 TEXT version column** — read as constant 0 → newest-wins collapsed to scan-order. → added
  `Value::Text`/`ColumnData::Text` parse arms to `value_to_version_i64`/`version_value_at`
  (`columnar/mod.rs`); DDL now `warn!`s on a non-numeric version column (`executor/ddl.rs`). Test.
- **D-5 doubled command tag** — fast-path point INSERT/UPDATE/DELETE embedded the count in the tag
  while the wire layer also appends `rows_affected` → `DELETE 1 1`. → fast-path now emits bare tags
  matching the general path (`executor/mod.rs`). Test asserts bare tags. (Retention DELETE coercion
  itself was already correct.)
- **D-6 COPY FROM cache staleness** — `COPY … FROM STDIN` inserts rows but isn't a `Statement::Insert`,
  so it skipped result-cache invalidation → stale SELECT for the cache TTL. → `execute_copy_from`
  now calls `query_cache_invalidate_all()` on success (`executor/copy.rs`). (COPY is wire-protocol-only,
  so no executor-level test; covered by the same invalidation path the DML dispatcher uses.)

### Phase B2 — FIXED (aggregate detection / evaluation)

- **D-10 #24 `CAST(<aggregate> AS …)`** returned N NULL rows (aggregate not detected under the
  cast → per-row projection). → `collect_aggregates_from_expr` now recurses into Cast/UnaryOp;
  a cast wrapping an aggregate is made plan-ineligible (`expr_has_unsupported`) so it routes to
  the AST aggregate path, which substitutes the aggregate then casts. Returns one row `"3"`. Test.
- **D-10 #26 `COALESCE(MAX(x), 0)`** (and any scalar fn wrapping an aggregate) errored "aggregate
  outside context". → `contains_aggregate` now recurses into function arguments; added
  `eval_scalar_over_aggregates` + `substitute_aggregates_inplace` to compute the aggregate
  sub-exprs and evaluate the wrapping scalar/cast/CASE with the full row evaluator. Test (incl.
  empty-table → one row `0`).
- **D-10 #28 `GROUP BY … HAVING COUNT(*) >= N`** with COUNT **not** in the SELECT list dropped
  every group. **This was real** — the original audit's "not reproducible" verdict used
  non-discriminating data (no group met the threshold). Root cause: `agg_funcs` was collected
  from the projection only, so a HAVING-only aggregate was never computed. → also collect
  aggregates from the HAVING clause. Test (count in/out of projection + threshold).

### Phase B3a — FIXED (ALTER COLUMN TYPE)

- **D-7 ALTER COLUMN TYPE silent divergence** — `SetDataType` mutated only the catalog; physical
  storage keeps the value's original variant, so a columnar/MergeTree table read back the stale
  type while the catalog claimed the new one. → on a type change, rewrite the stored column by
  casting each value (`scan_physical` + `Value::cast` + `update`); a value that can't cast aborts
  the ALTER with a clear error (catalog untouched — verified). `TEXT '10' → BIGINT` now reads back
  `Int64(10)` and `SUM` works. Tests: successful rewrite + atomic rejection on an uncastable value.

**Still open (Phase B3b — features, not bugs):** D-4/D-8 — `argMax`/`argMin`, `percentile_cont/disc`
(p50/p95/p99), `SummingMergeTree`, and read-time collapse for `AggregatingMergeTree`. Blessed path
per the brief is "implement or tell observe to use pattern Y". Plan: implement the high-value
standard-SQL ones (percentile_cont/disc, argMax/argMin); for SummingMergeTree + aggregating
read-time collapse, decide implement-vs-document. D-9 is doc-only (visibility caveats to observe).
**Deferred to Phase G:** one-time `metrics.sh` doc-header sync (STATUS/AUDIT-REPORT/ROADMAP/etc.) —
pre-existing drift across the whole branch; sync once when test counts stabilize.

## Dogfood-driven findings — teploy-observe 2026-06 (verify-then-fix)

Source brief: `_internal/NUCLEUS_AUDIT_FROM_TEPLOY_OBSERVE_2026-06.md` (cross-ref:
`teploy-observe/_internal/AUDIT_FINDINGS_2026-06.md`, memory note `nucleus_dogfood_findings.md`).
These came from a real consumer (teploy-observe/dash dogfooding Nucleus over pgwire/pgx), so
they are **product-blocking, not theoretical** — until they're settled, observe's analytics,
retention, and **security controls (API-key revocation, password change)** are untrustworthy.
Each needs a one-line verdict back to observe: *supported as-is* / *fixed in commit X* /
*not supported — use pattern Y*. Several are verify-first (confirm current semantics) before any
code change; some may already be fixed (dogfood #10/#29 marked RESOLVED upstream — re-confirm).

Priority order (from the brief):
- **D-1 UPDATE semantics** (security-critical). `UPDATE` on plain/ReplacingMergeTree tables is
  "best-effort" → `RevokeAPIKey` / `ChangePassword` / rate-limit updates are **silent no-ops**;
  revoked keys keep authenticating. Maps to dogfood #32 (OPEN): UPDATE/DELETE on a
  `replacing_mergetree` without a single-column PK uses `Executor::scan` which now returns
  *deduped* rows, so row positions don't map to physical rows. → Define+document exactly which
  engines/PK-shapes support UPDATE and the visibility timing; either make it reliable for small
  config tables or bless the revoke-as-insert (newest-wins) pattern.
- **D-2 ReplacingMergeTree version-column typing.** Version columns declared `TEXT DEFAULT '0'`
  appear to be treated as 0 → newest-wins dedup silently broken everywhere. → Confirm numeric
  comparison; decide whether DDL should **reject/warn** on a non-numeric version column.
- **D-3 read-time dedup under AGGREGATES** (`SELECT SUM(x) FROM replacing_table`). Row-level
  SELECT dedup (#10 fix) apparently doesn't extend to aggregates → dashboards double/triple-count.
- **D-4 `argMax(value, version)` / `COUNT(DISTINCT)` / HLL** support — confirm which exist; the
  cleanest dedup-then-aggregate path depends on it.
- **D-5 retention DELETE coercion** — BIGINT column vs quoted TEXT literal matched nothing → TTLs
  inert, storage unbounded. Maps to dogfood #29 (RESOLVED, `0b35f54`) — **re-confirm** an int64
  `$1` cutoff actually deletes rows on the current build; also #31 command-tag (`DELETE 1 1`) pgx parse.
- **D-6 query-result cache invalidation on DELETE/UPDATE** (dogfood #30, OPEN) — stale row set
  served after a delete; compounds D-1/D-5.
- **D-7 ALTER COLUMN TYPE (TEXT→BIGINT) on a populated MergeTree** — does it rewrite already-merged
  parts or only new writes? If not a full rewrite, document the rebuild/backfill path observe must use.
- **D-8 SummingMergeTree / AggregatingMergeTree + percentile aggregate-state** — needed for RED
  trace metrics (p50/p95/p99); confirm if implemented or tell observe to compute from raw spans.
- **D-9 cross-connection write-visibility lag** (dogfood #2, OPEN, ~1s) — document the real window
  (gates how fast revocation/password/rate-limit take effect across pooled conns).
- **D-10 open projection/aggregate eval bugs** — dogfood **#24** `CAST(<aggregate> AS TEXT)` returns
  3 empty rows not one `"3"`; **#26** `COALESCE(MAX(...), 0)` rejected; **#28** `GROUP BY … HAVING
  COUNT(*) >= N` returns zero rows (projection-vs-HAVING ordering). All hit observe's query layer.

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
