# Nucleus Index-Maintenance — Work Handoff

**Living note. Update as work progresses.** Purpose: let a fresh session pick up any
remaining item with standalone context.

- **Worktree:** `~/Documents/Code Projects/Neutron-RLS/` · **Branch:** `fix/nucleus-value-type-consistency`
- **Companion memory:** `nucleus-index-maintenance-decision` (auto-loaded; architecture + rationale)
- **This file is a working note — not committed by default.** Delete or gitignore when done.
- **Note:** this note was lost when the worktree dir vanished (2026-07-16) and recreated from the
  git history + memory; all *committed* work was safe (branch head `b78a91c`).

---

## Current state — index-maintenance stream COMPLETE, green

`git log --oneline` tail on this branch:

```
b78a91c test(nucleus): close the oracle blind spots that hid the encrypted bug
5619b73 fix(nucleus): encrypted-index insert hook keys on true scan position
c399066 test(nucleus): incremental HNSW UPDATE moves row without stale duplicate
d2f42f7 perf(nucleus): pk->node registry — incremental HNSW UPDATE + compaction
723e587 feat(nucleus): recovery-safe PK-keying — durable incremental HNSW DELETE
f35cb93 fix(nucleus): gate PK-keyed HNSW postings to non-durable indexes
55d8487 perf(nucleus): incremental HNSW maintenance on DELETE (PK-keyed postings)
340510f test(nucleus): vector recall-regression harness
fc097c3 test(nucleus): soak harness for the durable engine (T1.4)
cb22766 test(nucleus): coherence oracle probe for derived indexes
a63e305 fix(nucleus): derived-index coherence across DDL/DML/txn + create_index idempotency
```

**Green bar:** `cargo test --lib --features server` → 3775 pass, 0 fail. `cargo clippy` clean
(lib + bins). Three probes green: `probe_index_coherence` (sensitive+calibrated), `probe_vector_recall`
(incl. delete-heavy compaction phase), `probe_soak`.

**What works today:**
- Derived-index coherence across DDL/DML/txn; `create_index` is idempotent.
- HNSW indexes (in-memory AND durable): DELETE **and UPDATE** are incremental via a pk->node
  registry (`VectorIndexEntry.registry`) — fresh node per insert/update, tombstone on delete, no
  in-place overwrite. Recovery-safe: pk_column persisted in the `index_meta.json` sidecar; the
  registry is not persisted, so after a reopen resolve brute-forces (correct) until the first
  ineligible DML rebuilds and repopulates it. Deferred compaction rebuilds once tombstones exceed
  the live set (pgvector-VACUUM equivalent) — now gated by a delete-heavy recall phase.
- IvfFlat and encrypted indexes: still full rebuild on delete/update (see #2 note on IvfFlat, and
  #4). The encrypted INSERT-append path is now also correct (was a silent wrong-position bug — #4,
  fixed in `5619b73`).

---

## Validation harnesses (run these to gate any change here)

```sh
cd nucleus
cargo build --release --features server --bin probe_index_coherence --bin probe_vector_recall --bin probe_soak

# Coherence: randomized DML vs brute-force model, all engines. btree/PK/encrypted checked EXACTLY.
# Codes are grouped (c{id%8}) so the encrypted index sees DUPLICATE values, and postings over all
# live codes must be a permutation of {0..N-1} (catches collision + gap). Omits the encrypted index
# ~1/3 of iterations so the in-memory HNSW fast path is exercised.
./target/release/probe_index_coherence --iterations 250 --ops 60

# Recall: indexed KNN vs brute-force recall@k — fresh, after churn, AND a delete-heavy phase (HNSW)
# that drives tombstones past the compaction heuristic. CATCHES silent recall loss.
./target/release/probe_vector_recall --queries 40

# Soak: concurrent mixed workload on durable WAL MVCC; leak (Linux /proc), coherence, reopen durability.
./target/release/probe_soak --duration-secs 20 --concurrency 8

cargo test --lib --features server   # full suite, must stay green
cargo clippy                         # must stay clean (lib + bins)
```

Key recovery test: `durable_mvcc_hnsw_pk_vector_search_survives_reopen` (in `src/embedded.rs`) — PK
10/20/30 so pk≠position. `test_hnsw_pk_keyed_recovery_after_fastpath_delete` (in
`src/executor/tests/test_specialty_persistence.rs`).
Encrypted regression: `test_encrypted_index_insert_hook_positions_duplicates` (in `test_index.rs`).

---

## REMAINING WORK — index-maintenance stream is done; these are the only items left

### 1. Recovery-safe pk-keying → durable incremental DELETE  ✅ DONE (`723e587`)
### 2. pk→node registry → incremental UPDATE + compaction  ✅ DONE (`d2f42f7`, `c399066`)

**IvfFlat intentionally NOT made incremental.** It trains centroids at build; an incremental
(rebuild-skipped) index never retrains → centroid drift → recall decay. Full rebuild (retrains every
DML) is *better* for IvfFlat. Stays full-rebuild by design.

### 3. HNSW tombstone compaction  ✅ DONE (`d2f42f7`) + gated (`b78a91c`)

`vector_index_needs_compaction` (mod.rs) triggers a full rebuild from the DELETE/UPDATE fast path
once tombstones are material (>= 64) and exceed the live set. `probe_vector_recall` now has a
delete-heavy phase (HNSW only) that deletes down to a small survivor set — sustained pure deletes are
the only workload that drives tombstones past the heuristic (mixed churn never reaches the ratio), so
compaction now fires repeatedly and the phase gates that recall survives the rebuild
(hnsw ~0.79-0.81, floor 0.55).

### 4. Encrypted-index incremental  ✅ RESOLVED (`5619b73`) — bug fixed, PK-keying declined

Surfaced a **live correctness bug**, not just a missed optimization: the inline INSERT hook
(`update_encrypted_indexes_on_insert`) used `EncryptedIndex::len()` (distinct-ciphertext count) as
the appended row's id, while the rebuild path uses the scan position (`.enumerate()`). Two
consecutive duplicate values made the next row collide → `ENCRYPTED_LOOKUP` returned a wrong
position. Masked until plain INSERT stopped rebuilding (`a63e305` gated the rebuild to ON CONFLICT).
Fixed by tracking a running posting count (`num_postings`) and using it as the append id; between
rebuilds the ids are always exactly {0..N-1}, so the posting count is the correct next position.
Regression `test_encrypted_index_insert_hook_positions_duplicates`.

**PK-keying (the original "incremental" plan) DECLINED — deliberate.** It would (a) change
`ENCRYPTED_LOOKUP`'s observable output from scan positions to PK values, breaking the semantics tests
assert on, and (b) only buy DELETE-fast-path eligibility for encrypted tables, whose rebuild is a
cheap BTreeMap over base rows. Low value + user-facing output change = not worth it.

**Oracle gap CLOSED (`b78a91c`).** `probe_index_coherence` now groups codes and asserts the
permutation-of-{0..N-1} invariant under duplicate values. Validated sensitive (reintroducing the bug
trips it: 114 divergences) + calibrated (0 divergences / 45k mutations).

### 5. Zone-map maintenance decision  ✅ BANKED — no change (workload tradeoff, not correctness)

`rebuild_zone_map` full-scans on every non-eligible DML. It's a **pure optimization with a safety
net**: `apply_zone_map_pruning` (`src/executor/query.rs` ~8744) only prunes when granule row-counts
match the scan, else falls back to the row filter — so a stale zone map is never wrong, just
unoptimized. Keeping the per-DML rebuild is the correct *safe default*: it keeps pruning effective.
Switching to clear-on-DML is a workload-dependent perf tradeoff to **measure, not guess**. The DELETE
fast path already clears it O(1). Revisit only with a write-heavy benchmark in hand.

---

## Not mine, but flagged (do at merge time, not mid-stream)

- `scripts/metrics.sh --check` fails on stale `DATABASE_COMPLETION.md` counts (Source LOC / Rust
  files / declared unit + integration tests). Branch-wide drift — the counts move as tests/probes
  land, so a single authoritative sync at merge time is correct, not a mid-stream churn. Per
  `nucleus/CLAUDE.md` a test-count change also touches STATUS.md / AUDIT-REPORT.md / TODO-NEXT.md /
  COMPETITOR-GAPS.md / NUCLEUS-ROADMAP.md.
- Broader branch scope: this branch also carries the Nucleus long-running-DB roadmap (T1.2 spill /
  operator-gating, T2.1 backup/PITR, **T2.2 RLS — needs Tyler's enforce-vs-disclaim decision**, T2.4
  CI gates, Tier 3). Index maintenance is done; the branch is not merge-ready in that larger sense.

## Landmines discovered (read before touching this area)

- **Durable recovery drops constraints** (`constraints: Vec::new()` in embedded build) — any feature
  that keys on PK after reopen must not rely on the recovered catalog's constraints. Root of the #1 bug.
- **`create_index` was non-idempotent** — double-registered names in `table_idx_names` → duplicate
  rows. Fixed (`a63e305`); keep it idempotent.
- **HNSW `insert` must clear the tombstone** for a re-inserted id (`src/vector/mod.rs`), or revived
  rows vanish from search.
- **Encrypted positional ids assume no gaps.** `num_postings` == next position only because
  delete/update on encrypted tables always full-rebuild (they're not fast-path eligible). If that
  eligibility ever changes, the positional scheme breaks — re-key on PK instead.
- **The coherence oracle now checks the permutation invariant, but only when the encrypted index is
  present.** Always run `probe_vector_recall` too when touching HNSW maintenance; it's the only thing
  that catches silent recall loss.
