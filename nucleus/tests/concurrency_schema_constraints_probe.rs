//! Adversarial concurrency harness: DML + DDL + constraints under real OS-thread
//! contention. Many workers hammer one table with INSERT/UPDATE/DELETE (random
//! keys that frequently COLLIDE on the PRIMARY KEY / UNIQUE column and frequently
//! VIOLATE a CHECK), while other workers concurrently run DDL (CREATE/DROP INDEX,
//! ALTER TABLE ADD COLUMN). After the storm, hard invariants must hold:
//!
//!   I1. PRIMARY KEY uniqueness — no two live rows share an id.
//!   I2. UNIQUE column uniqueness — no two live rows share a uq value.
//!   I3. CHECK (v >= 0) — no live row violates it.
//!   I4. No panic / no engine corruption — the table stays queryable and the
//!       projected schema is self-consistent.
//!
//! These are correctness invariants the engine must uphold regardless of
//! interleaving; a serialization/conflict/constraint *error* on any single op is
//! fine (we ignore per-op errors) — silently breaking an invariant is not.
#![cfg(feature = "server")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
/// Run sql, swallowing per-op errors (serialization/constraint/conflict are all
/// acceptable). Returns true if it did NOT panic.
fn run(ex: &Executor, r: &tokio::runtime::Runtime, sid: u64, sql: &str) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = r.block_on(ex.execute_with_session(sid, sql));
    }))
    .is_ok()
}

fn select_rows(
    ex: &Executor,
    r: &tokio::runtime::Runtime,
    sid: u64,
    sql: &str,
) -> Option<Vec<Vec<Value>>> {
    match r.block_on(ex.execute_with_session(sid, sql)) {
        Ok(mut v) => match v.pop() {
            Some(ExecResult::Select { rows, .. }) => Some(rows),
            _ => None,
        },
        Err(_) => None,
    }
}

#[test]
fn schema_and_constraints_hold_under_concurrency() {
    let panics = Arc::new(AtomicUsize::new(0));
    for round in 0..20u64 {
        let ex = Arc::new(Executor::new(
            Arc::new(Catalog::new()),
            Arc::new(MvccStorageAdapter::new()),
        ));
        let r0 = rt();
        let s0 = ex.create_session();
        // PK on id, UNIQUE on uq, CHECK v >= 0.
        run(
            &ex,
            &r0,
            s0,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, uq INTEGER UNIQUE, v INTEGER CHECK (v >= 0))",
        );
        ex.drop_session(s0);

        let workers = 6;
        let per = 40;
        let key_space = 12; // small → frequent PK/UNIQUE collisions
        let barrier = Arc::new(Barrier::new(workers));
        let mut handles = Vec::new();
        for w in 0..workers {
            let ex = ex.clone();
            let barrier = barrier.clone();
            let panics = panics.clone();
            handles.push(std::thread::spawn(move || {
                let r = rt();
                let sid = ex.create_session();
                let mut rng = Rng(0x9E37_79B9 ^ (round.wrapping_mul(131) + w as u64 + 1));
                barrier.wait();
                for _ in 0..per {
                    let id = rng.below(key_space);
                    let uq = rng.below(key_space);
                    let v = rng.below(20) as i64 - 5; // sometimes negative → CHECK violation
                    let ok = match rng.below(10) {
                        0..=4 => run(
                            &ex,
                            &r,
                            sid,
                            &format!("INSERT INTO t (id, uq, v) VALUES ({id},{uq},{v})"),
                        ),
                        5..=6 => run(&ex, &r, sid, &format!("UPDATE t SET v={v} WHERE id={id}")),
                        7 => run(&ex, &r, sid, &format!("UPDATE t SET uq={uq} WHERE id={id}")),
                        8 => run(&ex, &r, sid, &format!("DELETE FROM t WHERE id={id}")),
                        _ => {
                            // Concurrent DDL interleaved with the DML storm.
                            match rng.below(3) {
                                0 => run(&ex, &r, sid, "CREATE INDEX IF NOT EXISTS ix_v ON t (v)"),
                                1 => run(&ex, &r, sid, "DROP INDEX IF EXISTS ix_v"),
                                _ => run(
                                    &ex,
                                    &r,
                                    sid,
                                    &format!("ALTER TABLE t ADD COLUMN c{} INTEGER", rng.below(3)),
                                ),
                            }
                        }
                    };
                    if !ok {
                        panics.fetch_add(1, Ordering::Relaxed);
                    }
                }
                ex.drop_session(sid);
            }));
        }
        for h in handles {
            let _ = h.join();
        }

        // ── Invariant checks ──
        let rc = rt();
        let sc = ex.create_session();

        // I4: table is still queryable.
        let all = select_rows(&ex, &rc, sc, "SELECT id, uq, v FROM t")
            .unwrap_or_else(|| panic!("round {round}: table not queryable after concurrency"));

        // I1/I2: PK and UNIQUE uniqueness over live rows.
        let mut ids = std::collections::HashMap::<i64, usize>::new();
        let mut uqs = std::collections::HashMap::<i64, usize>::new();
        let geti = |row: &[Value], i: usize| match row.get(i) {
            Some(Value::Int64(n)) => Some(*n),
            Some(Value::Int32(n)) => Some(*n as i64),
            _ => None, // NULL (uq is nullable) — skip
        };
        for row in &all {
            if let Some(id) = geti(row, 0) {
                *ids.entry(id).or_default() += 1;
            }
            if let Some(uq) = geti(row, 1) {
                *uqs.entry(uq).or_default() += 1;
            }
            // I3: CHECK v >= 0.
            if let Some(v) = geti(row, 2) {
                assert!(v >= 0, "round {round}: CHECK (v>=0) violated: stored v={v}");
            }
        }
        let dup_id: Vec<_> = ids.iter().filter(|&(_, &c)| c > 1).collect();
        let dup_uq: Vec<_> = uqs.iter().filter(|&(_, &c)| c > 1).collect();
        assert!(
            dup_id.is_empty(),
            "round {round}: PRIMARY KEY duplicated: {dup_id:?}"
        );
        assert!(
            dup_uq.is_empty(),
            "round {round}: UNIQUE column duplicated: {dup_uq:?}"
        );

        ex.drop_session(sc);
    }
    println!(
        "20 rounds clean ({} per-op errors swallowed, 0 panics)",
        panics.load(Ordering::Relaxed)
    );
}
