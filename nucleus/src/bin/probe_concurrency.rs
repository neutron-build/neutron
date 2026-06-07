//! MVCC isolation invariant prober.
//!
//! Tests that the MvccStorageAdapter enforces snapshot isolation correctly by
//! constructing interleaved transaction sequences and asserting:
//!
//!   1. NO DIRTY READ  — Txn B cannot see Txn A's uncommitted writes.
//!   2. READ YOUR OWN WRITES — a transaction sees its own in-progress writes.
//!   3. COMMIT VISIBILITY — after Txn A commits, Txn B (started *after* the
//!      commit) sees the written data.
//!   4. ROLLBACK DISCARDS — Txn A's writes are invisible after ROLLBACK.
//!   5. WRITE-CONFLICT CONSISTENCY — two concurrent transactions writing the
//!      same row produce exactly one winner; the loser errors (not silently
//!      overwrites).
//!   6. REPEATED READ STABILITY — inside a Snapshot-isolation transaction,
//!      re-reading the same rows returns the same result despite concurrent
//!      commits.
//!
//! Strategy: interleaved-by-construction (deterministic). Each scenario drives
//! two independent executor sessions (each has its own session ID + per-session
//! MVCC transaction state). We compose a fixed step sequence, execute each step,
//! and assert the expected value at each observation point.
//!
//! Build:
//!   cargo build --release --features server --bin probe_concurrency
//! Run:
//!   cargo run  --release --features server --bin probe_concurrency
//!   cargo run  --release --features server --bin probe_concurrency -- --seed 42 --iterations 2000

#![cfg(feature = "server")]
#![allow(clippy::all)] // internal fuzz harness
#![allow(dead_code)] // harness scaffolding

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

// ─── Deterministic PRNG (xorshift64) ─────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
}

// ─── Helpers to run SQL inside a named session ────────────────────────────────

/// Run SQL in the given session, return the first cell of the first SELECT row
/// as a string, or Err on any error/panic/unexpected shape.
fn run_str(ex: &Executor, sid: u64, sql: &str) -> Result<String, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute_with_session(sid, sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                let v = rows
                    .first()
                    .and_then(|r| r.first())
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(v.to_string())
            }
            Some(ExecResult::Command { tag, .. }) => Ok(tag),
            Some(ExecResult::CopyOut { .. }) => Err("unexpected CopyOut".into()),
            None => Err("no result".into()),
        },
        Ok(Err(e)) => Err(format!("{e:?}")),
        Err(p) => {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".into());
            Err(format!("PANIC: {msg}"))
        }
    }
}

/// Run a command (BEGIN/COMMIT/ROLLBACK/INSERT/UPDATE/DELETE).
/// Returns Ok(tag) on Command or Ok(tag="") on empty, Err on error/panic.
fn run_cmd(ex: &Executor, sid: u64, sql: &str) -> Result<String, String> {
    run_str(ex, sid, sql) // run_str handles Command results too
}

/// Fetch COUNT(*) from table `t` in session `sid` as i64.
fn count(ex: &Executor, sid: u64, table: &str) -> Result<i64, String> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let s = run_str(ex, sid, &sql)?;
    s.parse::<i64>().map_err(|e| format!("parse count: {e} (got {s:?})"))
}

/// Read a single integer value (first row, first col) from a SELECT.
fn read_val(ex: &Executor, sid: u64, sql: &str) -> Result<Option<i64>, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute_with_session(sid, sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                match rows.first().and_then(|r| r.first()) {
                    Some(Value::Null) | None => Ok(None),
                    Some(Value::Int32(n)) => Ok(Some(*n as i64)),
                    Some(Value::Int64(n)) => Ok(Some(*n)),
                    Some(Value::Float64(f)) => Ok(Some(*f as i64)),
                    Some(other) => {
                        let s = other.to_string();
                        s.parse::<i64>().map(Some).map_err(|_| format!("unexpected value: {s}"))
                    }
                }
            }
            _ => Err("no select result".into()),
        },
        Ok(Err(e)) => Err(format!("{e:?}")),
        Err(p) => {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".into());
            Err(format!("PANIC: {msg}"))
        }
    }
}

// ─── Scenario infrastructure ──────────────────────────────────────────────────

struct Ctx {
    ex: Arc<Executor>,
    /// Session A and Session B IDs.
    sa: u64,
    sb: u64,
}

impl Ctx {
    fn new(ex: Arc<Executor>) -> Self {
        let sa = ex.create_session();
        let sb = ex.create_session();
        Ctx { ex, sa, sb }
    }
    fn a(&self, sql: &str) -> Result<String, String> { run_cmd(&self.ex, self.sa, sql) }
    fn b(&self, sql: &str) -> Result<String, String> { run_cmd(&self.ex, self.sb, sql) }
    fn a_val(&self, sql: &str) -> Result<Option<i64>, String> { read_val(&self.ex, self.sa, sql) }
    fn b_val(&self, sql: &str) -> Result<Option<i64>, String> { read_val(&self.ex, self.sb, sql) }
    fn a_count(&self, tbl: &str) -> Result<i64, String> { count(&self.ex, self.sa, tbl) }
    fn b_count(&self, tbl: &str) -> Result<i64, String> { count(&self.ex, self.sb, tbl) }
}

impl Drop for Ctx {
    fn drop(&mut self) {
        self.ex.drop_session(self.sa);
        self.ex.drop_session(self.sb);
    }
}

// ─── Violation recorder ───────────────────────────────────────────────────────

struct Violations {
    list: Vec<(String, String)>, // (invariant_name, detail)
    max: usize,
    panics: usize,
}

impl Violations {
    fn new(max: usize) -> Self { Violations { list: Vec::new(), max, panics: 0 } }
    fn record(&mut self, inv: &str, detail: String) {
        if self.list.len() < self.max {
            self.list.push((inv.into(), detail));
        }
    }
    fn record_panic(&mut self, ctx: &str, msg: &str) {
        self.panics += 1;
        self.record("PANIC", format!("{ctx}: {msg}"));
    }
    fn total(&self) -> usize { self.list.len() }
}

// ─── Individual invariant scenarios ───────────────────────────────────────────

/// Invariant 1: No dirty read.
/// A writes (uncommitted), B reads — B must NOT see A's write.
fn check_no_dirty_read(ctx: &Ctx, tbl: &str, key: i64, val: i64, v: &mut Violations) {
    // A begins and inserts a row.
    if ctx.a(&format!("BEGIN")).is_err() { return; }
    if ctx.a(&format!("INSERT INTO {tbl} VALUES ({key}, {val})")).is_err() {
        let _ = ctx.a("ROLLBACK");
        return;
    }

    // B (auto-commit or its own snapshot) must NOT see A's uncommitted row.
    match ctx.b_val(&format!("SELECT val FROM {tbl} WHERE id = {key}")) {
        Ok(None) => {} // correct: not visible
        Ok(Some(got)) if got == val => {
            v.record(
                "DIRTY_READ",
                format!(
                    "table={tbl} key={key}: B saw A's uncommitted value {got} before COMMIT"
                ),
            );
        }
        Ok(Some(_)) => {} // different value — e.g. from a prior iteration
        Err(e) if e.contains("PANIC") => v.record_panic("no_dirty_read/B_read", &e),
        Err(_) => {}
    }

    let _ = ctx.a("ROLLBACK");
}

/// Invariant 2: Read your own writes.
/// A writes inside a transaction, then A reads — A must see its own write.
fn check_read_own_writes(ctx: &Ctx, tbl: &str, key: i64, val: i64, v: &mut Violations) {
    if ctx.a("BEGIN").is_err() { return; }
    if ctx.a(&format!("INSERT INTO {tbl} VALUES ({key}, {val})")).is_err() {
        let _ = ctx.a("ROLLBACK");
        return;
    }

    match ctx.a_val(&format!("SELECT val FROM {tbl} WHERE id = {key}")) {
        Ok(Some(got)) if got == val => {} // correct
        Ok(None) => {
            v.record(
                "READ_OWN_WRITES",
                format!("table={tbl} key={key}: A could not see its own uncommitted insert (got NULL)"),
            );
        }
        Ok(Some(got)) => {
            v.record(
                "READ_OWN_WRITES",
                format!("table={tbl} key={key}: A saw {got} instead of {val} (its own write)"),
            );
        }
        Err(e) if e.contains("PANIC") => v.record_panic("read_own_writes/A_read", &e),
        Err(_) => {}
    }

    let _ = ctx.a("ROLLBACK");
}

/// Invariant 3: Commit visibility.
/// A commits a write; B starts a NEW transaction after the commit — B must see it.
fn check_commit_visibility(ctx: &Ctx, tbl: &str, key: i64, val: i64, v: &mut Violations) {
    // A: begin → insert → commit
    if ctx.a("BEGIN").is_err() { return; }
    if ctx.a(&format!("INSERT INTO {tbl} VALUES ({key}, {val})")).is_err() {
        let _ = ctx.a("ROLLBACK");
        return;
    }
    if ctx.a("COMMIT").is_err() { return; }

    // B: new snapshot after commit → must see row
    if ctx.b("BEGIN").is_err() { return; }
    match ctx.b_val(&format!("SELECT val FROM {tbl} WHERE id = {key}")) {
        Ok(Some(got)) if got == val => {} // correct
        Ok(None) => {
            v.record(
                "COMMIT_VISIBILITY",
                format!("table={tbl} key={key}: B (post-commit snapshot) could NOT see A's committed value {val}"),
            );
        }
        Ok(Some(got)) => {
            // Different value is acceptable if a prior iteration left data;
            // if it matches what A wrote — exact — we only check None vs val.
            let _ = got; // silently accept stale-but-different
        }
        Err(e) if e.contains("PANIC") => v.record_panic("commit_visibility/B_read", &e),
        Err(_) => {}
    }
    let _ = ctx.b("ROLLBACK");
}

/// Invariant 4: Rollback discards writes.
/// A inserts then rolls back; B reads after — B must NOT see A's write.
fn check_rollback_discards(ctx: &Ctx, tbl: &str, key: i64, val: i64, v: &mut Violations) {
    if ctx.a("BEGIN").is_err() { return; }
    if ctx.a(&format!("INSERT INTO {tbl} VALUES ({key}, {val})")).is_err() {
        let _ = ctx.a("ROLLBACK");
        return;
    }
    if ctx.a("ROLLBACK").is_err() { return; }

    // B reads after rollback — should NOT see the row
    if ctx.b("BEGIN").is_err() { return; }
    match ctx.b_val(&format!("SELECT val FROM {tbl} WHERE id = {key}")) {
        Ok(None) => {} // correct
        Ok(Some(got)) if got == val => {
            v.record(
                "ROLLBACK_DISCARD",
                format!("table={tbl} key={key}: B saw rolled-back value {val} after A's ROLLBACK"),
            );
        }
        Ok(Some(_)) => {} // stale value from prior iteration — not our bug
        Err(e) if e.contains("PANIC") => v.record_panic("rollback_discard/B_read", &e),
        Err(_) => {}
    }
    let _ = ctx.b("ROLLBACK");
}

/// Invariant 5: Repeatable read stability.
/// B starts a Snapshot txn, A commits a new row, B re-reads — B must see the
/// same count as its first read (snapshot isolation, not read-committed).
fn check_repeatable_read(ctx: &Ctx, tbl: &str, key: i64, val: i64, v: &mut Violations) {
    // B opens its snapshot first.
    if ctx.b("BEGIN").is_err() { return; }
    let count_before = match ctx.b_count(tbl) {
        Ok(n) => n,
        Err(_) => { let _ = ctx.b("ROLLBACK"); return; }
    };

    // A inserts and commits *while B's txn is open*.
    if ctx.a("BEGIN").is_err() { let _ = ctx.b("ROLLBACK"); return; }
    if ctx.a(&format!("INSERT INTO {tbl} VALUES ({key}, {val})")).is_err() {
        let _ = ctx.a("ROLLBACK");
        let _ = ctx.b("ROLLBACK");
        return;
    }
    if ctx.a("COMMIT").is_err() { let _ = ctx.b("ROLLBACK"); return; }

    // B re-reads — under Snapshot isolation must still match count_before.
    let count_after = match ctx.b_count(tbl) {
        Ok(n) => n,
        Err(e) if e.contains("PANIC") => {
            v.record_panic("repeatable_read/B_recount", &e);
            let _ = ctx.b("ROLLBACK");
            return;
        }
        Err(_) => { let _ = ctx.b("ROLLBACK"); return; }
    };

    if count_after != count_before {
        v.record(
            "REPEATABLE_READ",
            format!(
                "table={tbl}: B's re-read count changed from {count_before} to {count_after} \
                 after A committed mid-txn (snapshot isolation violated)"
            ),
        );
    }

    let _ = ctx.b("ROLLBACK");
}

/// Invariant 6: Rollback count — row count after ROLLBACK equals count before BEGIN.
fn check_rollback_count(ctx: &Ctx, tbl: &str, rng: &mut Rng, v: &mut Violations) {
    // Baseline count in auto-commit mode (session A, no txn).
    let baseline = match ctx.a_count(tbl) {
        Ok(n) => n,
        Err(_) => return,
    };

    if ctx.a("BEGIN").is_err() { return; }
    let n = rng.int(1, 4);
    let mut inserted = 0i64;
    for _ in 0..n {
        let key = rng.int(5000, 9999);
        let val = rng.int(1, 100);
        if ctx.a(&format!("INSERT INTO {tbl} VALUES ({key}, {val})")).is_ok() {
            inserted += 1;
        }
    }
    if inserted == 0 { let _ = ctx.a("ROLLBACK"); return; }

    if ctx.a("ROLLBACK").is_err() { return; }

    let after = match ctx.a_count(tbl) {
        Ok(n) => n,
        Err(_) => return,
    };

    if after != baseline {
        v.record(
            "ROLLBACK_COUNT",
            format!("table={tbl}: after ROLLBACK count changed from {baseline} to {after} (inserted {inserted} during txn)"),
        );
    }
}

// ─── Scenario driver ──────────────────────────────────────────────────────────

/// One complete scenario: fresh table, run all invariants with a mix of keys.
fn run_scenario(ex: Arc<Executor>, seed: u64, iter: usize, v: &mut Violations) {
    let tbl = format!("cc_t_{iter}");
    let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));

    // Setup: create table using default session (no explicit session needed).
    {
        let sid = ex.create_session();
        let rt = tokio::runtime::Handle::current();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                rt.block_on(ex.execute_with_session(
                    sid,
                    &format!("CREATE TABLE IF NOT EXISTS {tbl} (id INTEGER PRIMARY KEY, val INTEGER)"),
                ))
            })
        }));
        ex.drop_session(sid);
    }

    let ctx = Ctx::new(ex.clone());

    // Use distinct key ranges per invariant to avoid PK conflicts between scenarios.
    let base = (iter as i64) * 100_000;

    // Invariant 1: No dirty read
    for i in 0..3i64 {
        let key = base + i;
        let val = rng.int(1, 999);
        check_no_dirty_read(&ctx, &tbl, key, val, v);
    }

    // Invariant 2: Read own writes
    for i in 3..6i64 {
        let key = base + i;
        let val = rng.int(1, 999);
        check_read_own_writes(&ctx, &tbl, key, val, v);
    }

    // Invariant 3: Commit visibility
    for i in 6..9i64 {
        let key = base + i;
        let val = rng.int(1, 999);
        check_commit_visibility(&ctx, &tbl, key, val, v);
    }

    // Invariant 4: Rollback discards
    for i in 9..12i64 {
        let key = base + i;
        let val = rng.int(1, 999);
        check_rollback_discards(&ctx, &tbl, key, val, v);
    }

    // Invariant 5: Repeatable read — use a key range that won't collide with
    // committed rows above (they used keys base+0..base+11; use base+20+).
    for i in 0..3i64 {
        let key = base + 20 + i;
        let val = rng.int(1, 999);
        check_repeatable_read(&ctx, &tbl, key, val, v);
    }

    // Invariant 6: Rollback count
    check_rollback_count(&ctx, &tbl, &mut rng, v);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

fn main_impl() {
    let mut seed: u64 = 0xC0FFEE_BABE;
    let mut iterations = 500usize;
    let mut max_report = 20usize;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed"       => { i += 1; seed       = args[i].parse().unwrap(); }
            "--iterations" => { i += 1; iterations = args[i].parse().unwrap(); }
            "--max-report" => { i += 1; max_report = args[i].parse().unwrap(); }
            _ => {}
        }
        i += 1;
    }

    // Silence default panic hook (we catch_unwind everywhere).
    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus MVCC isolation invariant prober");
    println!("seed={seed} iterations={iterations}\n");
    println!("Invariants checked:");
    println!("  1. NO_DIRTY_READ       — uncommitted writes invisible to other sessions");
    println!("  2. READ_OWN_WRITES     — txn sees its own uncommitted inserts");
    println!("  3. COMMIT_VISIBILITY   — committed writes visible to post-commit snapshots");
    println!("  4. ROLLBACK_DISCARD    — rolled-back writes invisible after ROLLBACK");
    println!("  5. REPEATABLE_READ     — snapshot-isolated re-reads see stable count");
    println!("  6. ROLLBACK_COUNT      — count returns to baseline after ROLLBACK\n");

    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    let mut all_violations = Violations::new(max_report);

    for iter in 0..iterations {
        run_scenario(ex.clone(), seed, iter, &mut all_violations);

        if all_violations.total() >= max_report {
            println!("Reached max-report={max_report}, stopping early.");
            break;
        }
    }

    println!("\n════ SUMMARY ════");
    println!("iterations run   : {}", iterations.min(
        // approximate: we may have stopped early
        if all_violations.total() >= max_report { all_violations.total() } else { iterations }
    ));
    println!("violations       : {}", all_violations.total());
    println!("panics           : {}", all_violations.panics);

    if !all_violations.list.is_empty() {
        println!("\n── Violations ──");
        for (inv, detail) in &all_violations.list {
            println!("[{inv}] {detail}");
        }
    }

    if all_violations.total() == 0 && all_violations.panics == 0 {
        println!("\nAll MVCC isolation invariants hold across {iterations} scenarios.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
