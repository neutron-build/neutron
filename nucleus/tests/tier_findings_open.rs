//! Status of the Tier 1/2 probe-harness findings. Each was a confirmed real bug;
//! this file tracks which are fixed vs still open. Fixed ones have dedicated
//! regression tests; their harnesses are in scripts/probe.sh's gating list.
//!
//! FIXED (with regressions, all green; probe_concurrency_threads clean over
//! 15000+ rounds; probe_fts_rank passes):
//!   1.  [CRITICAL] Concurrent read-modify-write lost updates (RR + SERIALIZABLE).
//!       Four root causes: index_version_lookup returning a false-empty (skipping
//!       the CAS conflict check); index_lookup_sync returning a stale cached row
//!       inside a txn; non-atomic id-assignment vs snapshot capture in
//!       TransactionManager::begin; non-atomic active→committed handoff in
//!       commit(). See concurrent_lost_update_regression.rs +
//!       concurrent_rmw_regression.rs.
//!   1b/2.[HIGH] SERIALIZABLE write-skew was missed when the second txn read/wrote
//!       AFTER the first committed. Fixed: deferred SSI cleanup (a committed txn's
//!       SIREAD/write sets + edges are retained until every concurrent peer
//!       finishes), SIREAD now recorded on the eq/point read path, and a
//!       concurrency guard so edges only form between overlapping txns. See
//!       ssi_write_skew_regression.rs. NOTE: a point SELECT currently runs as a
//!       full scan, so its SIREAD is table-wide ("predicate lock", as PostgreSQL
//!       does for seq scans) — disjoint access can over-abort on the first try.
//!       That is SAFE (no anomaly) and resolved by retry; tuple-granular SIREAD
//!       (index-point reads) is a future precision optimization, not a bug.
//!   1c. [HIGH] Multi-row UPDATE/DELETE WHERE indexed_col=X inside a txn hit the
//!       WRONG row. Fixed by making the MVCC engine's positions be stable version
//!       indices end-to-end (scan_where_eq_positions / scan_physical /
//!       index_version_lookup return (version_idx, row); update()/delete() mutate
//!       that exact version). See txn_multirow_mutation_regression.rs.
//!   3.  [MEDIUM] Executor txn_state desync when a SERIALIZABLE COMMIT fails SSI —
//!       commit_txn now always clears the session txn and discards its dirty/
//!       savepoint state on the error path.
//!   5.  [HIGH] Disk-mode tables vanished from SQL after reopen — the on-disk
//!       directory now persists column names and the embedded builder repopulates
//!       the catalog from DiskEngine::recovered_schemas(). See
//!       disk_recovery_regression.rs.
//!   5b. [HIGH] Disk-mode recovery persisted an ABANDONED transaction's writes
//!       (BEGIN + writes, dropped with no COMMIT) — they survived a reopen. The
//!       engine applied txn writes to its in-memory directory immediately and
//!       flushed that uncommitted directory on Drop. Fixed: Drop rolls back any
//!       open txn's in-memory state before flushing
//!       (DiskEngine::rollback_open_txn_in_memory). probe_recover_engines now
//!       clean (gated); see disk_recovery_dml_regression.rs.
//!   4.  [LOW] READ COMMITTED now takes a fresh snapshot per statement
//!       (execute_statement → refresh_statement_snapshot), so a statement sees
//!       rows committed by other transactions since the previous statement, while
//!       SNAPSHOT/SERIALIZABLE keep a fixed snapshot. See read_committed_regression.rs.
//!   6.  [MEDIUM] FTS_RANK was TF-only and could rank inversely to FTS_SEARCH's
//!       BM25 — now uses BM25 tf-saturation. See fts_rank_regression.rs.
//!
//! ── Tier 3 (adversarial DML+DDL+constraint concurrency harness) ───────────
//!   7.  [HIGH] FIXED. PRIMARY KEY / UNIQUE were not enforced under concurrency —
//!       concurrent transactions inserting (or updating to) the same key all
//!       succeeded (up to 4 duplicate rows for one PK). check_unique_constraints
//!       did a snapshot scan then inserted, not atomic across txns. Fixed with
//!       atomic MVCC-aware enforcement in the engine: insert_unique/update_unique
//!       check a committed-live chain AND an in-flight reservation map (keys held
//!       by uncommitted inserts, released on commit/abort) under one lock, so two
//!       racing transactions cannot both take the same key; NULL keys are distinct
//!       (SQL). See concurrency_schema_constraints_probe (now passing) +
//!       concurrent_unique_constraint_regression.rs.
//!
//! ── Tier 1/2 (all fixed and gated) ────────────────────────────────────────
//! ALL Tier 1/2 findings are fixed and gated. SERIALIZABLE SIREAD is now
//! tuple-granular for point access too: point reads/writes record SIREAD on only
//! the matched row, single-table unqualified predicates push down so point
//! SELECTs use the fast scan, and internal maintenance scans (zone-map rebuild)
//! are SIREAD-free — so disjoint SERIALIZABLE access no longer over-aborts. See
//! ssi_write_skew_regression.rs (serializable_disjoint_both_commit).
#![cfg(feature = "server")]
// The module doc above is a human-readable findings log using custom
// enumerators (1b/2., 1c., 5b.) that aren't valid Markdown list markers, which
// trips clippy's lazy-continuation heuristic. Cosmetic; keep the log as written.
#![allow(clippy::doc_lazy_continuation)]
