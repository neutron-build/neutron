//! OPEN findings from the Tier 1/2 probe harnesses — confirmed real bugs not yet
//! fixed. Each is reproduced live by its harness (src/bin/probe_*.rs); a focused
//! #[ignore]'d unit test is added where it can be expressed deterministically.
//! Un-ignore (and add the harness to scripts/probe.sh) as each is fixed.
//!
//! Open findings:
//!   1. [CRITICAL] Concurrent read-modify-write lost updates under RR AND
//!      SERIALIZABLE (probe_concurrency_threads).
//!      PARTIALLY FIXED: the dominant mechanism — a PK/eq UPDATE/DELETE matching
//!      zero rows (and so skipping its CAS write-conflict check) because the index
//!      was rebuilt to the latest snapshot and dropped the version a concurrent
//!      snapshot still needs — is fixed (index_version_lookup now defers to the
//!      chain scan; see concurrent_lost_update_regression.rs). Loss magnitude
//!      dropped from ~50% to a few %. A RESIDUAL timing race remains (both RR and
//!      SERIALIZABLE still lose a small number under true OS-thread contention);
//!      root cause not yet isolated — needs runtime instrumentation, not static
//!      analysis. Plus the SSI-specific gap below.
//!   1b.[HIGH] SERIALIZABLE misses write-skew when the second txn does its
//!      read/write AFTER the first commits: cleanup_ssi purges the committing
//!      txn's SIREAD/write sets immediately, so no rw-conflict edge forms; also
//!      SIREAD is not recorded on the eq/point read path. Needs deferred SSI
//!      cleanup (retain until concurrent peers finish) + record_siread on eq
//!      reads + a concurrency guard on edge creation. (Entangled with 1c.)
//!   1c.[HIGH] Multi-row UPDATE/DELETE WHERE indexed_col=X inside a transaction
//!      can hit the WRONG row — scan_where_eq_positions returns virtual match
//!      positions but update()/delete() map them against the full scan, and the
//!      txn scan cache that would reconcile them is populated only for
//!      auto-commit. The obvious fix (cache for txns) exposed a deeper issue:
//!      index_version_lookup (newest-visible) and engine.scan disagree on the
//!      visible PK version under concurrency (>1 version visible to one
//!      snapshot). Must reconcile index vs chain MVCC visibility first. Pinned in
//!      txn_multirow_mutation_regression.rs (#[ignore]). (task #24)
//!   2. [HIGH] SIREAD locks not recorded on point/equality read paths
//!      (fast_scan_where_eq / scan_where_eq_positions / fast_scan_where_eq_topk).
//!   3. [MEDIUM] Executor txn_state desync when a SERIALIZABLE COMMIT fails SSI
//!      (commit error path doesn't clear/rollback txn_state).
//!   4. [LOW] READ COMMITTED doesn't take a fresh snapshot per statement.
//!   5. [HIGH] Disk-mode persistence: tables vanish from SQL after reopen — the
//!      catalog isn't repopulated from the DiskEngine's restored directory
//!      (embedded.rs build(): recovered_schemas only set in the DurableMvcc arm).
//!      Data is physically durable but inaccessible. (probe_recover_engines)
//!   6. [MEDIUM] FTS_RANK computes TF-only but is documented/used as BM25, so it
//!      inverts rankings vs FTS_SEARCH (Okapi BM25). (probe_fts_rank)
#![cfg(feature = "server")]
use nucleus::embedded::Database;
use nucleus::executor::ExecResult;

/// Finding 5: disk-mode tables must survive a reopen.
#[tokio::test]
#[ignore = "OPEN BUG: disk-mode catalog not repopulated on reopen (tables vanish)"]
async fn disk_mode_tables_survive_reopen() {
    let dir = std::env::temp_dir().join("nucleus_tier_disk_reopen");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    {
        let db = Database::builder().disk(&dir).build().unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await.unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20)").await.unwrap();
        let _ = db.sync();
    }
    let db = Database::builder().disk(&dir).build().unwrap();
    let n = match db.execute("SELECT id, v FROM t ORDER BY id").await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => rows.len(),
        o => panic!("{o:?}"),
    };
    assert_eq!(n, 2, "disk-mode table should survive reopen");
    let _ = std::fs::remove_dir_all(&dir);
}
