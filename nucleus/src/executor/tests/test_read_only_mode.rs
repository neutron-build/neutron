//! M10 — degraded read-only mode enforcement, end to end through the executor.
//!
//! `src/ops/disk.rs` proves the watermark state machine in isolation. These
//! tests prove the part that actually protects the database: with the shared
//! [`ServiceState`] degraded, real statements are refused with the actionable
//! SQLSTATE, reads keep working, and the recovery path (VACUUM, transaction
//! control) stays available. Each rejection case has a mirror case proving the
//! same statement succeeds when the server is healthy.

use super::*;
use crate::ops::{DegradeReason, ServiceState};
use crate::wire::error_codec::{ErrorCodec, PgWireErrorCodec};

/// Executor whose service state we can flip, mirroring how the server-level
/// disk guard and the executor share one gate.
fn degradable_executor() -> (Executor, Arc<ServiceState>) {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    let mut ex = Executor::new(catalog, storage);
    let service = Arc::new(ServiceState::new());
    ex.set_service_state(service.clone());
    (ex, service)
}

fn sqlstate(err: &ExecError) -> String {
    let codec = PgWireErrorCodec;
    codec.code_to_string(codec.encode(err).code)
}

async fn seed(ex: &Executor) {
    exec(ex, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    exec(ex, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;
}

#[tokio::test]
async fn healthy_server_admits_every_write() {
    let (ex, service) = degradable_executor();
    seed(&ex).await;
    assert!(!service.is_read_only());
    for sql in [
        "INSERT INTO t VALUES (3, 'c')",
        "UPDATE t SET v = 'z' WHERE id = 1",
        "DELETE FROM t WHERE id = 2",
        "CREATE TABLE t2 (a INT)",
        "DROP TABLE t2",
    ] {
        assert!(
            ex.execute(sql).await.is_ok(),
            "{sql} should succeed on a healthy server"
        );
    }
    assert_eq!(service.rejected_writes(), 0);
}

#[tokio::test]
async fn disk_degraded_server_refuses_writes_with_sqlstate_53100() {
    let (ex, service) = degradable_executor();
    seed(&ex).await;
    service.enter_read_only(DegradeReason::DiskWatermark, "only 1.20% free on /data");

    for sql in [
        "INSERT INTO t VALUES (3, 'c')",
        "UPDATE t SET v = 'z' WHERE id = 1",
        "DELETE FROM t WHERE id = 1",
        "CREATE TABLE t2 (a INT)",
        "DROP TABLE t",
        "ALTER TABLE t ADD COLUMN w INT",
        "CREATE INDEX idx_t_v ON t (v)",
        "TRUNCATE TABLE t",
    ] {
        let err = ex
            .execute(sql)
            .await
            .expect_err(&format!("{sql} must be refused while degraded"));
        assert!(
            matches!(err, ExecError::DiskFull(_)),
            "{sql}: expected DiskFull, got {err:?}"
        );
        assert_eq!(sqlstate(&err), "53100", "{sql}");
        // The message must tell the operator what to actually do.
        let msg = err.to_string();
        assert!(msg.contains("read-only"), "{msg}");
        assert!(msg.contains("Free space"), "{msg}");
        assert!(msg.contains("1.20% free on /data"), "{msg}");
    }
    assert_eq!(service.rejected_writes(), 8);
}

#[tokio::test]
async fn reads_still_work_while_degraded() {
    let (ex, service) = degradable_executor();
    seed(&ex).await;
    service.enter_read_only(DegradeReason::DiskWatermark, "low disk");

    let r = ex.execute("SELECT id, v FROM t ORDER BY id").await.unwrap();
    assert_eq!(rows(&r[0]).len(), 2, "reads must survive degradation");
    assert!(ex.execute("SELECT COUNT(*) FROM t").await.is_ok());
    assert!(ex.execute("EXPLAIN SELECT * FROM t").await.is_ok());
    assert!(ex.execute("SET search_path = public").await.is_ok());
}

#[tokio::test]
async fn transaction_control_and_vacuum_stay_available_while_degraded() {
    let (ex, service) = degradable_executor();
    seed(&ex).await;
    service.enter_read_only(DegradeReason::DiskWatermark, "low disk");

    // An open transaction must still be able to finish or roll back, and
    // VACUUM is the in-SQL recovery path out of a disk watermark.
    assert!(ex.execute("BEGIN").await.is_ok());
    assert!(
        ex.execute("INSERT INTO t VALUES (9, 'x')").await.is_err(),
        "writes inside a transaction must still be refused"
    );
    assert!(ex.execute("ROLLBACK").await.is_ok());
    assert!(
        ex.execute("VACUUM").await.is_ok(),
        "VACUUM must stay available or the degraded state is unrecoverable from SQL"
    );
}

#[tokio::test]
async fn operator_read_only_uses_sqlstate_25006() {
    let (ex, service) = degradable_executor();
    seed(&ex).await;
    service.enter_read_only(DegradeReason::Operator, "maintenance window");
    let err = ex
        .execute("INSERT INTO t VALUES (3, 'c')")
        .await
        .unwrap_err();
    assert!(matches!(err, ExecError::ReadOnly(_)), "got {err:?}");
    assert_eq!(sqlstate(&err), "25006");
}

#[tokio::test]
async fn writes_resume_after_the_degraded_state_clears() {
    let (ex, service) = degradable_executor();
    seed(&ex).await;
    service.enter_read_only(DegradeReason::DiskWatermark, "low disk");
    assert!(ex.execute("INSERT INTO t VALUES (3, 'c')").await.is_err());

    service.resume_if(DegradeReason::DiskWatermark);
    assert!(
        ex.execute("INSERT INTO t VALUES (3, 'c')").await.is_ok(),
        "writes must resume once the watermark clears"
    );
    let r = ex.execute("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(scalar(&r[0]), &Value::Int64(3));
}

#[tokio::test]
async fn no_partial_write_survives_a_refused_statement() {
    let (ex, service) = degradable_executor();
    seed(&ex).await;
    service.enter_read_only(DegradeReason::DiskWatermark, "low disk");
    let _ = ex.execute("INSERT INTO t VALUES (3, 'c'), (4, 'd')").await;
    let _ = ex.execute("DELETE FROM t").await;
    service.resume();
    let r = ex.execute("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(
        scalar(&r[0]),
        &Value::Int64(2),
        "a refused statement must not have touched storage"
    );
}

/// Specialty-store writes arrive as `SELECT kv_set(...)`, which the
/// statement-level gate cannot classify. They must still be refused.
#[tokio::test]
async fn specialty_store_writes_are_refused_while_degraded() {
    let (ex, service) = degradable_executor();

    // Mirror case first: healthy server accepts them.
    assert!(ex.execute("SELECT kv_set('k', 'v')").await.is_ok());
    assert!(ex.execute("SELECT ts_insert('m', 1000, 1.5)").await.is_ok());

    service.enter_read_only(DegradeReason::DiskWatermark, "low disk");

    for sql in [
        "SELECT kv_set('k2', 'v2')",
        "SELECT kv_del('k')",
        "SELECT kv_incr('counter')",
        "SELECT ts_insert('m', 2000, 2.5)",
    ] {
        let err = ex
            .execute(sql)
            .await
            .expect_err(&format!("{sql} must be refused while degraded"));
        assert!(
            matches!(err, ExecError::DiskFull(_)),
            "{sql}: expected DiskFull, got {err:?}"
        );
        assert_eq!(sqlstate(&err), "53100", "{sql}");
    }

    // Specialty *reads* stay available.
    let r = ex.execute("SELECT kv_get('k')").await.unwrap();
    assert_eq!(scalar(&r[0]), &Value::Text("v".to_string()));
}

/// The OLTP fast path bypasses `execute_statement`; it needs its own gate.
#[tokio::test]
async fn sql_oltp_fast_path_is_gated_too() {
    use crate::wire::kv_fast_path::try_parse_sql_fast_path;

    let (ex, service) = degradable_executor();
    // The fast path only takes constraint-free tables (it writes straight to
    // storage), so use one it will actually accept.
    exec(&ex, "CREATE TABLE fp (a INT, b TEXT)").await;
    exec(&ex, "INSERT INTO fp VALUES (1, 'x')").await;

    let insert = try_parse_sql_fast_path("INSERT INTO fp VALUES (7, 'g')")
        .expect("fast path should recognise a simple INSERT");
    assert!(
        ex.execute_sql_fast_path(0, &insert)
            .await
            .expect("healthy server should take the fast path")
            .is_ok(),
        "healthy server should accept the fast-path INSERT"
    );

    service.enter_read_only(DegradeReason::DiskWatermark, "low disk");
    let err = ex
        .execute_sql_fast_path(0, &insert)
        .await
        .expect("fast path should still handle the statement")
        .expect_err("fast-path INSERT must be refused while degraded");
    assert_eq!(sqlstate(&err), "53100");

    // A fast-path point SELECT must still be served.
    let select = try_parse_sql_fast_path("SELECT * FROM fp WHERE a = 1")
        .expect("fast path should recognise a point SELECT");
    assert!(
        ex.execute_sql_fast_path(0, &select)
            .await
            .expect("point SELECT should still take the fast path")
            .is_ok()
    );
}

/// Wiring the guard to the executor's state is what makes the watermark
/// actually protect the database, so assert the whole chain once.
#[tokio::test]
async fn disk_guard_degradation_reaches_real_sql() {
    use crate::ops::{DiskGuard, DiskWatermarks, SpaceInfo, SpaceProbe};
    use std::path::Path;

    #[derive(Debug)]
    struct Probe(parking_lot::Mutex<u64>);
    impl SpaceProbe for Probe {
        fn probe(&self, _p: &Path) -> std::io::Result<SpaceInfo> {
            Ok(SpaceInfo {
                total_bytes: 100_000,
                available_bytes: *self.0.lock(),
            })
        }
    }

    let (ex, service) = degradable_executor();
    seed(&ex).await;
    let probe = Arc::new(Probe(parking_lot::Mutex::new(50_000)));
    let guard = DiskGuard::new(
        "/tmp/nucleus-test",
        probe.clone(),
        DiskWatermarks {
            warn_free_pct: 10.0,
            readonly_free_pct: 3.0,
            min_free_bytes: 0,
            resume_free_pct: 6.0,
        },
        service.clone(),
    );

    guard.evaluate();
    assert!(ex.execute("INSERT INTO t VALUES (3, 'c')").await.is_ok());

    // Disk fills past the critical watermark.
    *probe.0.lock() = 2_000; // 2% free
    guard.evaluate();
    let err = ex
        .execute("INSERT INTO t VALUES (4, 'd')")
        .await
        .unwrap_err();
    assert_eq!(sqlstate(&err), "53100");

    // Disk recovers past the resume watermark.
    *probe.0.lock() = 20_000; // 20% free
    guard.evaluate();
    assert!(ex.execute("INSERT INTO t VALUES (4, 'd')").await.is_ok());
}

// ======================================================================
// NU-216: the degraded gate is only as complete as its list
//
// `SELECT kv_set(...)` parses as a Query, so the statement-level gate — which
// is otherwise fail-closed — cannot see it. A second list, `MUTATING_SCALAR_FNS`,
// carries the specialty-store mutators, and a third registry in `scalar_fns.rs`
// (`side_effecting_return_type`) independently declares which functions WRITE
// so pgwire's Describe never probe-executes one.
//
// Diffing the two on 2026-08-17 found six functions the side-effect registry
// declares as writing that the admission list did not gate: NEXTVAL, SETVAL,
// RETENTION_SET, STREAM_XREADGROUP, SUBSCRIBE, UNSUBSCRIBE. Every one could
// allocate durable state, advance an identifier, or claim stream entries on a
// server that had just refused an INSERT for want of disk.
//
// `mutating_registries_agree` is the part that matters: it derives the answer
// from the second registry instead of from a human noticing, so the next
// mutator added in one place fails here rather than in production.
// ======================================================================

#[tokio::test]
async fn degraded_server_refuses_sequence_and_stream_mutators() {
    let (ex, service) = degradable_executor();
    exec(&ex, "CREATE SEQUENCE s216").await;
    exec(&ex, "SELECT STREAM_XADD('s216_stream', 'k', 'v')").await;
    exec(&ex, "SELECT STREAM_XGROUP_CREATE('s216_stream', 'g', 0)").await;
    service.enter_read_only(DegradeReason::DiskWatermark, "only 1.20% free on /data");

    for sql in [
        // Allocates a durable identifier that can never be handed out again.
        "SELECT NEXTVAL('s216')",
        "SELECT SETVAL('s216', 100)",
        // Advances a consumer group's cursor and records pending entries:
        // a read in name only.
        "SELECT STREAM_XREADGROUP('s216_stream', 'g', 'c', 10)",
        // Installs durable retention policy.
        "SELECT RETENTION_SET('t', 3600)",
    ] {
        let err = ex
            .execute(sql)
            .await
            .expect_err(&format!("{sql} must be refused while degraded"));
        assert!(
            matches!(err, ExecError::DiskFull(_)),
            "{sql}: expected DiskFull, got {err:?}"
        );
        assert_eq!(sqlstate(&err), "53100", "{sql}");
    }
}

/// A degraded server must still answer reads, including reads of the very
/// models whose mutators are refused above. Without this the test above could
/// be satisfied by refusing everything.
#[tokio::test]
async fn degraded_server_still_serves_specialty_reads() {
    let (ex, service) = degradable_executor();
    exec(&ex, "SELECT KV_SET('k216', 'v')").await;
    exec(&ex, "SELECT STREAM_XADD('s216_read', 'k', 'v')").await;
    service.enter_read_only(DegradeReason::DiskWatermark, "only 1.20% free on /data");

    for sql in [
        "SELECT KV_GET('k216')",
        "SELECT KV_EXISTS('k216')",
        "SELECT STREAM_XLEN('s216_read')",
        "SELECT STREAM_XRANGE('s216_read', 0, 9999999999999, 10)",
        "SELECT CURRVAL('s216_missing')",
    ] {
        let r = ex.execute(sql).await;
        if let Err(e) = &r {
            assert!(
                !matches!(e, ExecError::DiskFull(_)),
                "{sql} is a read and must not be refused as a write"
            );
        }
    }
}

/// The registries answer the same question — "does this function write?" — and
/// had drifted apart in both directions. Both directions are checked here.
#[test]
fn mutating_registries_agree() {
    use crate::executor::admission::{
        MUTATING_SCALAR_FNS, MUTATING_SCALAR_FNS_EXTRA, scalar_fn_mutates,
    };
    use crate::executor::scalar_fns::{SIDE_EFFECTING_FN_NAMES, side_effecting_return_type};

    assert!(
        SIDE_EFFECTING_FN_NAMES.windows(2).all(|w| w[0] < w[1]),
        "the list is binary-searched, so it must stay sorted and unique"
    );

    for name in SIDE_EFFECTING_FN_NAMES.iter().copied() {
        // Describe must know it writes, or pgwire probe-executes it — the
        // defect that made KV_SETNX run twice per client Execute.
        assert!(
            side_effecting_return_type(name).is_some(),
            "{name} writes but Describe does not know its type, so Describe \
             will probe-execute it"
        );
        // A degraded server must refuse it.
        assert!(
            scalar_fn_mutates(name),
            "{name} writes durable state but is not gated by read-only \
             admission — a degraded server would still execute it"
        );
    }

    // The reverse direction: nothing the old hand-maintained arrays gate may
    // be missing from the authority, or the authority is not one.
    for name in MUTATING_SCALAR_FNS
        .iter()
        .chain(MUTATING_SCALAR_FNS_EXTRA.iter())
        .copied()
    {
        // VECTOR_INSERT / VECTOR_DELETE are gated but do not exist as
        // functions at all (recorded in MODEL_SEMANTICS.md); gating a
        // phantom is harmless, so they are excused rather than added.
        if matches!(name, "VECTOR_INSERT" | "VECTOR_DELETE") {
            continue;
        }
        assert!(
            SIDE_EFFECTING_FN_NAMES.binary_search(&name).is_ok(),
            "{name} is refused while degraded but missing from \
             SIDE_EFFECTING_FN_NAMES, so Describe may probe-execute it"
        );
    }
}
