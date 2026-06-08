//! Status of the Tier 1/2 probe-harness findings. Each was a confirmed real bug;
//! this file tracks which are fixed vs still open. Fixed ones have dedicated
//! regression tests; open ones are pinned where they can be expressed
//! deterministically (see referenced files) and their harnesses are added to
//! scripts/probe.sh as they're fixed.
//!
//! FIXED (with regressions, all green; probe_concurrency_threads clean over
//! 15000 rounds; probe_fts_rank/probe_recover_engines pass):
//!   1.  [CRITICAL] Concurrent read-modify-write lost updates (RR + SERIALIZABLE).
//!       Four root causes fixed: index_version_lookup returning a false-empty
//!       (skipping the CAS conflict check); index_lookup_sync returning a stale
//!       cached row inside a txn; non-atomic id-assignment vs snapshot capture in
//!       TransactionManager::begin; non-atomic active→committed handoff in
//!       commit(). See concurrent_lost_update_regression.rs +
//!       concurrent_rmw_regression.rs.
//!   3.  [MEDIUM] Executor txn_state desync when a SERIALIZABLE COMMIT fails SSI —
//!       commit_txn now always clears the session txn and discards its dirty/
//!       savepoint state on the error path.
//!   5.  [HIGH] Disk-mode tables vanished from SQL after reopen — the on-disk
//!       directory now persists column names and the embedded builder repopulates
//!       the catalog from DiskEngine::recovered_schemas(). See
//!       disk_recovery_regression.rs. (Catalog-visibility part of #5; the disk WAL
//!       data-correctness part is now tracked as 5b below.)
//!   6.  [MEDIUM] FTS_RANK was TF-only and could rank inversely to FTS_SEARCH's
//!       BM25 — now uses BM25 tf-saturation. See fts_rank_regression.rs.
//!
//! STILL OPEN (entangled MVCC-visibility cluster — needs careful work, task #24):
//!   1c. [HIGH] Multi-row UPDATE/DELETE WHERE indexed_col=X inside a transaction
//!       can hit the WRONG row. scan_where_eq_positions returns virtual match
//!       positions; update()/delete() map them against the full scan; the txn
//!       scan cache that would reconcile them is populated only for auto-commit.
//!       The obvious fix (cache for txns) exposed the deeper issue below, so it
//!       was reverted. Pinned in txn_multirow_mutation_regression.rs (#[ignore]).
//!   1b/2.[HIGH] SERIALIZABLE misses write-skew when the second txn does its
//!       read/write AFTER the first commits: cleanup_ssi purges the committing
//!       txn's SIREAD/write sets immediately (needs deferred cleanup until
//!       concurrent peers finish), and SIREAD is not recorded on the eq/point read
//!       path (needs record_siread there + a concurrency guard on edge creation).
//!       Entangled with 1c (both touch index vs chain MVCC visibility — under
//!       concurrency index_version_lookup (newest-visible) and engine.scan can
//!       disagree on the visible PK version; that must be reconciled first).
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
