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
//!       disk_recovery_regression.rs. (Catalog-visibility part of #5; the disk WAL
//!       data-correctness part is tracked as 5b below.)
//!   6.  [MEDIUM] FTS_RANK was TF-only and could rank inversely to FTS_SEARCH's
//!       BM25 — now uses BM25 tf-saturation. See fts_rank_regression.rs.
//!
//! STILL OPEN:
//!   4.  [LOW] READ COMMITTED doesn't take a fresh per-statement snapshot, so it
//!       behaves as Snapshot/Repeatable Read (stricter than spec — safe, not a
//!       correctness hazard, but not standards-compliant).
//!   5b. [HIGH] Disk-mode WAL recovery is not data-correct under update/delete:
//!       probe_recover_engines shows the recovered row set diverging from the
//!       pre-shutdown set (extra rows — likely WAL replay / page-chain handling
//!       re-materializing deleted/updated rows). Distinct from #5 (catalog
//!       visibility, fixed). Disk mode is opt-in; the default MvccStorageAdapter
//!       is unaffected. Keep probe_recover_engines out of the gating list until
//!       fixed.
#![cfg(feature = "server")]
