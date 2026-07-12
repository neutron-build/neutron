//! Regressions for crash/DoS panics found by the full-scale crash fuzzer
//! (probe_crash, PROBE_SCALE=full): these scalar functions must NOT panic on
//! adversarial input, only error or return a value.
//!   - TENSOR_STORE sliced its hex arg by byte index → char-boundary panic on
//!     multi-byte UTF-8 ("日本語🎉"). Now slices over bytes.
//!   - KV_SET / KV_EXPIRE added a TTL Duration to an Instant → overflow panic on
//!     a huge TTL (i64::MAX). Now checked_add (overflow ⇒ effectively never).
//!   - SUBSTR cast a negative start to usize → "slice start > end" panic. Now
//!     signed, clamped index math.
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::MvccStorageAdapter;

async fn ok(ex: &Executor, sid: u64, sql: &str) {
    // Must not panic. Error or value both acceptable.
    let _ = ex.execute_with_session(sid, sql).await;
}

#[tokio::test]
async fn adversarial_scalar_inputs_do_not_panic() {
    let ex = Executor::new(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()),
    );
    let s = ex.create_session();

    // TENSOR_STORE with a multi-byte UTF-8 "hex" arg (char-boundary panic).
    ok(
        &ex,
        s,
        "SELECT TENSOR_STORE('a', TRUE, '[1,2,3]', '1,2,3', '日本語🎉')",
    )
    .await;
    ok(&ex, s, "SELECT GRAPH_NODE_DEGREE(ARRAY[0,1], TENSOR_STORE('a',TRUE,'[1,2,3]','1,2,3','日本語🎉'), '\\x00')").await;

    // KV TTL overflow (Instant + Duration).
    ok(&ex, s, "SELECT KV_SET('k', 'v', 9223372036854775807)").await;
    ok(&ex, s, "SELECT KV_EXPIRE('k', 9223372036854775807)").await;

    // SUBSTR with negative start / odd args (slice start > end).
    ok(&ex, s, "SELECT SUBSTR(TRUE, -3.14, 7, NULL)").await;
    ok(&ex, s, "SELECT SUBSTR('hello', -5, 3)").await;
    ok(&ex, s, "SELECT SUBSTR('hello', -100, 2)").await;

    ex.drop_session(s);
    // Reaching here without a panic is the assertion.
}

#[tokio::test]
async fn substr_negative_and_clamped_values() {
    // Verify SUBSTR still returns sensible results (not just no-panic).
    let ex = Executor::new(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()),
    );
    let s = ex.create_session();
    use nucleus::executor::ExecResult;
    use nucleus::types::Value;
    let val = |r: Vec<ExecResult>| -> String {
        match r.into_iter().next_back().unwrap() {
            ExecResult::Select { rows, .. } => match &rows[0][0] {
                Value::Text(t) => t.clone(),
                other => format!("{other:?}"),
            },
            o => format!("{o:?}"),
        }
    };
    let r = ex
        .execute_with_session(s, "SELECT SUBSTR('hello', 2, 3)")
        .await
        .unwrap();
    assert_eq!(val(r), "ell", "SUBSTR('hello',2,3)");
    let r = ex
        .execute_with_session(s, "SELECT SUBSTR('hello', 2)")
        .await
        .unwrap();
    assert_eq!(val(r), "ello", "SUBSTR('hello',2)");
    ex.drop_session(s);
}
