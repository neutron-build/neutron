//! Aggregate overflow behavior must be uniform: the AST path errors
//! ("integer out of range") on SUM overflow, and every optimized aggregate
//! path must do the same — the plan-path scalar fallback used unchecked i64
//! addition (debug panic / release wrap), and the columnar fast AVG answered
//! from an f64 sum with no precision guard at all.

use super::*;

/// Two BIGINT values whose sum overflows i64, and whose f64 projections
/// round (2^62 + 1 is not representable in f64): both the checked-integer
/// and the precision-loss failure modes in one input.
const A: i64 = 4_611_686_018_427_387_905; // 2^62 + 1
const B: i64 = 4_611_686_018_427_387_905;

#[tokio::test]
async fn sum_overflow_errors_on_every_path() {
    // The plan path (plan_execution defaults on; the simple-aggregate shape
    // is plan-eligible) must surface the AST path's clean error, not panic.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE big (id INT PRIMARY KEY, v BIGINT)").await;
    exec(&ex, &format!("INSERT INTO big VALUES (1, {A})")).await;
    exec(&ex, &format!("INSERT INTO big VALUES (2, {B})")).await;

    let served_before = ex.metrics().plan_path_served.get();
    let res = ex.execute("SELECT SUM(v) FROM big").await;
    let served = ex.metrics().plan_path_served.get() > served_before;

    match res {
        Err(e) => assert!(
            e.to_string().contains("out of range"),
            "expected the AST path's out-of-range error, got: {e}"
        ),
        Ok(v) => panic!("SUM overflow must error, got: {v:?} (plan path served: {served})"),
    }
}

#[tokio::test]
async fn avg_overflow_on_columnar_declines_to_exact_path() {
    // The columnar fast aggregate answers AVG(int) from an unchecked f64 sum;
    // with near-i64::MAX values the f64 sum loses the integer exactly and
    // the average is garbage. It must decline (like its own SUM arm does at
    // 2^53) and let the exact path answer — which errors on true overflow.
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE cbig (id INT PRIMARY KEY, v BIGINT) WITH (engine='columnar')",
    )
    .await;
    exec(&ex, &format!("INSERT INTO cbig VALUES (1, {A})")).await;
    exec(&ex, &format!("INSERT INTO cbig VALUES (2, {B})")).await;

    let res = ex.execute("SELECT AVG(v) FROM cbig").await;
    match res {
        Err(e) => assert!(
            e.to_string().contains("out of range"),
            "expected out-of-range error, got: {e}"
        ),
        Ok(v) => {
            // If it answered, it must at least be the exact answer, not an
            // f64-mangled one. A + B overflows i64, so the exact path must
            // have errored — reaching here means the fast path answered.
            panic!("AVG(int) overflow must error, got: {v:?}");
        }
    }
}

#[tokio::test]
async fn sum_overflow_on_columnar_still_errors() {
    // Control: SUM(int) on the columnar engine — the SUM arm declines past
    // 2^53 and the exact path must error rather than wrap.
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE cbig2 (id INT PRIMARY KEY, v BIGINT) WITH (engine='columnar')",
    )
    .await;
    exec(&ex, &format!("INSERT INTO cbig2 VALUES (1, {A})")).await;
    exec(&ex, &format!("INSERT INTO cbig2 VALUES (2, {B})")).await;
    let res = ex.execute("SELECT SUM(v) FROM cbig2").await;
    match res {
        Err(e) => assert!(
            e.to_string().contains("out of range"),
            "expected out-of-range error, got: {e}"
        ),
        Ok(v) => panic!("SUM overflow on columnar must error, got: {v:?}"),
    }
}
