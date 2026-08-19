//! A range predicate on a TIMESTAMP or DATE column must use the index.
//!
//! It did not. Measured 2026-07-28 on 600 all-distinct rows with an index on
//! the key column and a range matching 5 of them:
//!
//! | column type | matched | scanned |
//! |---|---|---|
//! | `BIGINT` | 5 | **5** — a real range scan |
//! | `TIMESTAMP` | 5 | **600** — full scan |
//! | `DATE` | 5 | **600** — full scan |
//! | `TIMESTAMPTZ` | **0** | 0 — returned nothing at all |
//!
//! `EXPLAIN` claimed `Index Scan` in every case: the planner picked the index
//! and the executor silently fell back, because
//! `if let Ok(Some(rows)) = index_lookup_range(..)` drops through to a
//! sequential scan on `Ok(None)`/`Err` with no log line. Results stayed
//! correct; only the pruning was lost — which is why nothing caught it, and
//! why the gate is a COST test rather than a result test.
//!
//! This file is that gate. It is written to fail on the behaviour above.

use super::*;
use crate::bench_hooks;
use crate::storage::buffered_engine::BufferedDiskEngine;
use crate::storage::disk_engine::DiskEngine;

const ROWS: i32 = 600;
/// A range matching 5 rows may touch a few index and heap pages. It may not
/// touch a number that grows with the table.
const BOUND: u64 = 64;

/// Each case: table, column type, how to build the i-th value, and the range
/// predicate that selects rows 100..105.
const CASES: [(&str, &str); 4] = [
    ("b", "BIGINT"),
    ("ts", "TIMESTAMP"),
    ("d", "DATE"),
    ("tz", "TIMESTAMPTZ"),
];

fn value_for(kind: &str, i: i32) -> String {
    match kind {
        "BIGINT" => format!("{}", 1_700_000_000_000i64 + i as i64 * 1000),
        "TIMESTAMP" => format!("TIMESTAMP '2026-01-01 00:00:00' + INTERVAL '{i} seconds'"),
        "DATE" => format!("DATE '2020-01-01' + {i}"),
        _ => format!("TIMESTAMPTZ '2026-01-01 00:00:00+00' + INTERVAL '{i} seconds'"),
    }
}

fn range_for(kind: &str) -> String {
    match kind {
        "BIGINT" => "k >= 1700000100000 AND k < 1700000105000".into(),
        "TIMESTAMP" => {
            "k >= TIMESTAMP '2026-01-01 00:01:40' AND k < TIMESTAMP '2026-01-01 00:01:45'".into()
        }
        "DATE" => "k >= DATE '2020-04-10' AND k < DATE '2020-04-15'".into(),
        _ => {
            "k >= TIMESTAMPTZ '2026-01-01 00:01:40+00' AND k < TIMESTAMPTZ '2026-01-01 00:01:45+00'"
                .into()
        }
    }
}

async fn seeded() -> Arc<Executor> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let engine = Arc::new(DiskEngine::open(&dir.path().join("t.db"), catalog.clone()).unwrap());
    let buffered = Arc::new(BufferedDiskEngine::new(engine));
    let ex = Arc::new(Executor::new(
        catalog,
        buffered as Arc<dyn crate::storage::StorageEngine>,
    ));
    for (table, kind) in CASES {
        exec(
            &ex,
            // `plain` carries no index: it is the control column, and without
            // one a bound of "examined 0" would pass on a counter that had
            // simply stopped counting.
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, k {kind}, plain INT)"),
        )
        .await;
        for chunk in (1..=ROWS).collect::<Vec<_>>().chunks(200) {
            let values: Vec<String> = chunk
                .iter()
                .map(|i| format!("({i}, {}, {i})", value_for(kind, *i)))
                .collect();
            exec(
                &ex,
                &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
            )
            .await;
        }
        exec(&ex, &format!("CREATE INDEX {table}_k ON {table} (k)")).await;
    }
    ex
}

async fn measure(ex: &Executor, sql: &str) -> (usize, u64) {
    bench_hooks::reset_tuples_examined();
    let res = exec(ex, sql).await;
    let examined = bench_hooks::tuples_examined();
    (rows(&res[0]).len(), examined)
}

/// The gate: a temporal range prunes, and an unindexed predicate on the same
/// table does not — so the bound cannot be met by a counter that broke.
#[tokio::test]
async fn a_temporal_range_predicate_prunes() {
    let ex = seeded().await;
    for (table, kind) in CASES {
        let (matched, examined) = measure(
            &ex,
            &format!("SELECT id FROM {table} WHERE {}", range_for(kind)),
        )
        .await;
        assert_eq!(
            matched, 5,
            "{kind}: the range must still return its 5 rows (it returned {matched})"
        );
        assert!(
            examined <= BOUND,
            "{kind}: a range matching 5 rows examined {examined} tuples of {ROWS} — \
             the planner picks the index and the executor is falling back to a scan"
        );

        let (ctl_matched, ctl_examined) =
            measure(&ex, &format!("SELECT id FROM {table} WHERE plain > 10")).await;
        assert_eq!(ctl_matched, (ROWS - 10) as usize);
        assert!(
            ctl_examined >= ROWS as u64,
            "{kind}: the unindexed control examined only {ctl_examined} of {ROWS}; \
             the counter is not measuring this query shape, so the bound above \
             proves nothing"
        );
    }
}

/// The same predicate under an aggregate. `COUNT(*)` can be answered from the
/// index alone, so this asserts the aggregate path prunes as well — it is a
/// different code path in `query.rs`, and the 2026-07-28 measurement covered
/// both.
#[tokio::test]
async fn a_temporal_range_prunes_under_an_aggregate() {
    let ex = seeded().await;
    for (table, kind) in CASES {
        bench_hooks::reset_tuples_examined();
        let res = exec(
            &ex,
            &format!("SELECT COUNT(*) FROM {table} WHERE {}", range_for(kind)),
        )
        .await;
        let examined = bench_hooks::tuples_examined();
        assert_eq!(scalar(&res[0]), &Value::Int64(5), "{kind}: wrong count");
        assert!(
            examined <= BOUND,
            "{kind}: COUNT over a range examined {examined} tuples of {ROWS}"
        );
    }
}
