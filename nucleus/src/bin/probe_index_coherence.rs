//! Coherence oracle for derived/secondary indexes.
//!
//! Hammers a table carrying a btree index (scalar col), a vector index
//! (HNSW/IVFFlat), and an encrypted index with a randomized, interleaved
//! stream of INSERT / UPDATE / DELETE / TRUNCATE, while maintaining a
//! brute-force in-memory reference model. After every op it checks the
//! indexed query paths against ground truth.
//!
//! The whole workload runs against each storage engine, which differ in
//! delete semantics: memory compacts (positions shift), mvcc tombstones,
//! columnar rewrites. Position-addressed index postings (vector/encrypted)
//! are exactly what break under those shifts, so this is the safety net for
//! making index maintenance incremental instead of full-rebuild-per-DML.
//!
//! Checks are chosen to be sensitive to incoherence but robust to the vector
//! indexes' approximate recall: btree equality, PK uniqueness, and encrypted
//! equality are EXACT (the encrypted index shares the vector index's
//! position-addressed maintenance path, so it is the exact detector for the
//! position-staleness bug class). Vector KNN is checked by SOUNDNESS only (all
//! returned ids live, no duplicates) — recall (self-match, ordering, top-k
//! completeness) is deliberately not asserted because HNSW/IVFFlat are
//! approximate and would false-positive.
//!
//! `cargo run --release --features server --bin probe_index_coherence`
#![cfg(feature = "server")]
#![allow(clippy::too_many_arguments, clippy::unusual_byte_groupings)]

use std::collections::BTreeMap;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{
    ColumnarStorageEngine, DiskEngine, LsmStorageEngine, MemoryEngine, MvccStorageAdapter,
    StorageEngine,
};
use nucleus::types::Value;

const DIM: usize = 6;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

#[derive(Clone)]
struct RefRow {
    val: i64,
    vec: Vec<f32>,
}

/// A distinct vector derived from a row id, so self-match KNN is unambiguous.
fn vec_for(id: i64, salt: u64) -> Vec<f32> {
    let mut r = Rng((id as u64)
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add(salt)
        | 1);
    (0..DIM)
        .map(|_| (r.below(2000) as f32) / 100.0 - 10.0)
        .collect()
}

fn vec_lit(v: &[f32]) -> String {
    let body: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
    format!("VECTOR('[{}]')", body.join(","))
}

/// Run a statement; true on success. Panic-safe.
fn exec(ex: &Executor, sql: &str) -> bool {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }))
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

/// Run a query, returning its rows. None on error/panic/non-Select.
fn query(ex: &Executor, sql: &str) -> Option<Vec<Vec<Value>>> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => Some(rows),
            _ => None,
        },
        _ => None,
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int32(n) => Some(*n as i64),
        Value::Int64(n) => Some(*n),
        _ => None,
    }
}

/// First-column integers from a result set.
fn ids_of(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .filter_map(|r| r.first().and_then(as_i64))
        .collect()
}

fn make_engine(name: &str, catalog: &Arc<Catalog>, suffix: &str) -> Arc<dyn StorageEngine> {
    match name {
        "memory" => Arc::new(MemoryEngine::new()),
        "columnar" => Arc::new(ColumnarStorageEngine::new()),
        "lsm" => Arc::new(LsmStorageEngine::new()),
        "disk" => {
            let path = std::env::temp_dir().join(format!("probe_coherence_{suffix}.ndb"));
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(path.with_extension("wal"));
            let _ = std::fs::remove_dir_all(path.with_extension("wal.d"));
            Arc::new(DiskEngine::open(&path, catalog.clone()).expect("disk engine open"))
        }
        _ => Arc::new(MvccStorageAdapter::new()),
    }
}

struct Report {
    divergences: u64,
    max_report: u64,
}
impl Report {
    fn fail(&mut self, engine: &str, iter: usize, log: &[String], msg: String) {
        self.divergences += 1;
        if self.divergences <= self.max_report {
            println!(
                "\n─── DIVERGENCE #{} [{engine} iter {iter}] ───",
                self.divergences
            );
            println!("  {msg}");
            let tail = log.len().saturating_sub(12);
            println!("  last ops:");
            for op in &log[tail..] {
                println!("    {op}");
            }
        }
    }
}

/// Run one table's full lifecycle on `ex`, checking coherence after each op.
fn run_lifecycle(
    ex: &Executor,
    engine: &str,
    iter: usize,
    ops: usize,
    use_hnsw: bool,
    use_encrypted: bool,
    rng: &mut Rng,
    rep: &mut Report,
) {
    let mut log: Vec<String> = Vec::new();
    macro_rules! stmt {
        ($sql:expr) => {{
            let sql = $sql;
            log.push(sql.clone());
            if !exec(ex, &sql) {
                rep.fail(engine, iter, &log, format!("statement failed: {sql}"));
                return;
            }
        }};
    }

    stmt!("CREATE TABLE t (id INT PRIMARY KEY, val INT, code TEXT, v VECTOR(6))".to_string());
    let idx_kind = if use_hnsw { "hnsw" } else { "ivfflat" };
    // Seed a couple of rows so IVFFlat has something to train on at CREATE time.
    let mut model: BTreeMap<i64, RefRow> = BTreeMap::new();
    let mut next_id: i64 = 1;
    let mut recently_deleted: Vec<i64> = Vec::new();
    let salt = iter as u64;

    for _ in 0..3 {
        let id = next_id;
        next_id += 1;
        let val = rng.below(20) as i64;
        let code = format!("c{id}");
        let vec = vec_for(id, salt);
        stmt!(format!(
            "INSERT INTO t VALUES ({id}, {val}, '{code}', {})",
            vec_lit(&vec)
        ));
        model.insert(id, RefRow { val, vec });
    }

    stmt!("CREATE INDEX t_val ON t (val)".to_string());
    stmt!(format!("CREATE INDEX t_v ON t USING {idx_kind} (v)"));
    // Omitting the encrypted index makes an HNSW + integer-PK table eligible for
    // the incremental DELETE fast path, so those iterations exercise it with the
    // exact btree/PK checks below.
    if use_encrypted {
        stmt!("CREATE INDEX t_code ON t USING encrypted (code)".to_string());
    }

    for _ in 0..ops {
        // ---- mutate ----
        let choice = rng.below(100);
        if choice < 42 || model.is_empty() {
            // INSERT
            let id = next_id;
            next_id += 1;
            let val = rng.below(20) as i64;
            let code = format!("c{id}");
            let vec = vec_for(id, salt);
            stmt!(format!(
                "INSERT INTO t VALUES ({id}, {val}, '{code}', {})",
                vec_lit(&vec)
            ));
            model.insert(id, RefRow { val, vec });
        } else if choice < 67 {
            // UPDATE val + vec of a live row
            let ids: Vec<i64> = model.keys().copied().collect();
            let id = ids[rng.below(ids.len())];
            let val = rng.below(20) as i64;
            let vec = vec_for(id.wrapping_add(next_id), salt ^ 0xABCD);
            stmt!(format!(
                "UPDATE t SET val = {val}, v = {} WHERE id = {id}",
                vec_lit(&vec)
            ));
            let row = model.get_mut(&id).unwrap();
            row.val = val;
            row.vec = vec;
        } else if choice < 92 {
            // DELETE a live row
            let ids: Vec<i64> = model.keys().copied().collect();
            let id = ids[rng.below(ids.len())];
            stmt!(format!("DELETE FROM t WHERE id = {id}"));
            model.remove(&id);
            recently_deleted.push(id);
            if recently_deleted.len() > 16 {
                recently_deleted.remove(0);
            }
        } else {
            // TRUNCATE
            stmt!("TRUNCATE TABLE t".to_string());
            for id in model.keys() {
                recently_deleted.push(*id);
            }
            model.clear();
        }

        // ---- check ----
        check_btree(ex, engine, iter, &log, &model, rng, rep);
        check_pk_uniqueness(ex, engine, iter, &log, &model, rng, rep);
        check_vector(ex, engine, iter, &log, &model, rng, rep);
        if use_encrypted {
            check_encrypted(ex, engine, iter, &log, &model, &recently_deleted, rng, rep);
        }
    }

    let _ = exec(ex, "DROP TABLE t");
}

fn check_btree(
    ex: &Executor,
    engine: &str,
    iter: usize,
    log: &[String],
    model: &BTreeMap<i64, RefRow>,
    rng: &mut Rng,
    rep: &mut Report,
) {
    // Equality on the btree-indexed scalar column.
    let target = rng.below(20) as i64;
    let sql = format!("SELECT id FROM t WHERE val = {target} ORDER BY id");
    let Some(rows) = query(ex, &sql) else {
        rep.fail(engine, iter, log, format!("btree query failed: {sql}"));
        return;
    };
    let mut got = ids_of(&rows);
    got.sort_unstable();
    let mut want: Vec<i64> = model
        .iter()
        .filter(|(_, r)| r.val == target)
        .map(|(id, _)| *id)
        .collect();
    want.sort_unstable();
    if got != want {
        rep.fail(
            engine,
            iter,
            log,
            format!("btree val={target}: index returned {got:?}, reference {want:?}"),
        );
    }
}

fn check_pk_uniqueness(
    ex: &Executor,
    engine: &str,
    iter: usize,
    log: &[String],
    model: &BTreeMap<i64, RefRow>,
    rng: &mut Rng,
    rep: &mut Report,
) {
    if model.is_empty() {
        return;
    }
    let ids: Vec<i64> = model.keys().copied().collect();
    let id = ids[rng.below(ids.len())];
    let sql = format!("SELECT id FROM t WHERE id = {id}");
    let Some(rows) = query(ex, &sql) else {
        rep.fail(engine, iter, log, format!("pk query failed: {sql}"));
        return;
    };
    let got = ids_of(&rows);
    if got != vec![id] {
        rep.fail(
            engine,
            iter,
            log,
            format!("pk id={id}: expected exactly [{id}], got {got:?} (duplicate/stale rows)"),
        );
    }
}

fn check_vector(
    ex: &Executor,
    engine: &str,
    iter: usize,
    log: &[String],
    model: &BTreeMap<i64, RefRow>,
    rng: &mut Rng,
    rep: &mut Report,
) {
    if model.is_empty() {
        return;
    }
    let ids: Vec<i64> = model.keys().copied().collect();
    let probe = ids[rng.below(ids.len())];
    let q = model.get(&probe).unwrap().vec.clone();
    let k = model.len().min(8);
    let sql = format!(
        "SELECT id FROM t ORDER BY VECTOR_DISTANCE(v, {}, 'l2') ASC LIMIT {k}",
        vec_lit(&q)
    );
    let Some(rows) = query(ex, &sql) else {
        rep.fail(engine, iter, log, format!("vector query failed: {sql}"));
        return;
    };
    let got = ids_of(&rows);

    // (b) no duplicate ids
    let mut seen = std::collections::HashSet::new();
    for id in &got {
        if !seen.insert(*id) {
            rep.fail(
                engine,
                iter,
                log,
                format!("vector KNN returned duplicate id {id}: {got:?}"),
            );
            return;
        }
    }
    // (a) all returned ids live
    for id in &got {
        if !model.contains_key(id) {
            rep.fail(
                engine,
                iter,
                log,
                format!("vector KNN returned stale/dead id {id} (query self={probe}): {got:?}"),
            );
            return;
        }
    }
    // Vector coherence is checked by SOUNDNESS only (no dups above, all-live
    // above). We deliberately do NOT assert recall (self-match, top-k
    // completeness, or distance ordering): HNSW/IVFFlat are approximate, so a
    // missed or reordered result is expected recall behavior, not incoherence,
    // and IVFFlat legitimately returns nothing on a few-row untrained index.
    // The position-staleness bug class that a full-rebuild-to-incremental
    // refactor could reintroduce is caught EXACTLY by the encrypted-index
    // check, which exercises the identical `rebuild_position_indexes_for_table`
    // maintenance path but with an exact (unique-code) oracle.
    let _ = probe;
}

fn check_encrypted(
    ex: &Executor,
    engine: &str,
    iter: usize,
    log: &[String],
    model: &BTreeMap<i64, RefRow>,
    recently_deleted: &[i64],
    rng: &mut Rng,
    rep: &mut Report,
) {
    if model.is_empty() {
        return;
    }
    // Positive: a live row's unique code resolves to exactly one posting.
    let ids: Vec<i64> = model.keys().copied().collect();
    let id = ids[rng.below(ids.len())];
    let live_count = encrypted_count(ex, &format!("c{id}"));
    if live_count != Some(1) {
        rep.fail(
            engine,
            iter,
            log,
            format!("encrypted lookup c{id} (live): expected 1 posting, got {live_count:?}"),
        );
        return;
    }
    // Negative: a deleted (and not re-live) code resolves to zero postings.
    let candidates: Vec<i64> = recently_deleted
        .iter()
        .copied()
        .filter(|d| !model.contains_key(d))
        .collect();
    if !candidates.is_empty() {
        let d = candidates[rng.below(candidates.len())];
        let dead_count = encrypted_count(ex, &format!("c{d}"));
        if dead_count != Some(0) {
            rep.fail(
                engine,
                iter,
                log,
                format!("encrypted lookup c{d} (deleted): expected 0 postings, got {dead_count:?}"),
            );
        }
    }
}

/// Count postings ENCRYPTED_LOOKUP returns for `code`. None on query failure.
fn encrypted_count(ex: &Executor, code: &str) -> Option<usize> {
    let sql = format!("SELECT ENCRYPTED_LOOKUP('t_code', '{code}') FROM t LIMIT 1");
    let rows = query(ex, &sql)?;
    let cell = rows.first().and_then(|r| r.first())?;
    let text = match cell {
        Value::Text(s) => s.clone(),
        Value::Null => String::new(),
        other => format!("{other:?}"),
    };
    Some(text.split(',').filter(|s| !s.trim().is_empty()).count())
}

fn main_impl() {
    unsafe {
        std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef");
    }
    std::panic::set_hook(Box::new(|_| {}));

    let args: Vec<String> = std::env::args().collect();
    let mut seed: u64 = 0xC0FFEE_1234_5678;
    let mut iterations: usize = 300;
    let mut ops: usize = 40;
    let mut max_report: u64 = 15;
    let mut engines: Vec<String> = vec!["mvcc".into(), "memory".into(), "columnar".into()];
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(seed);
            }
            "--iterations" => {
                i += 1;
                iterations = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(iterations);
            }
            "--ops" => {
                i += 1;
                ops = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(ops);
            }
            "--max-report" => {
                i += 1;
                max_report = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(max_report);
            }
            "--engines" => {
                i += 1;
                if let Some(list) = args.get(i) {
                    engines = list.split(',').map(|s| s.trim().to_string()).collect();
                }
            }
            _ => {}
        }
        i += 1;
    }

    println!(
        "probe_index_coherence: engines={engines:?} iterations={iterations} ops={ops} seed={seed:#x}"
    );
    let mut rep = Report {
        divergences: 0,
        max_report,
    };
    let mut total_ops: u64 = 0;

    for engine in &engines {
        for iter in 0..iterations {
            let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3) | 1);
            let catalog = Arc::new(Catalog::new());
            let storage = make_engine(engine, &catalog, &format!("{engine}_{iter}"));
            let ex = Arc::new(Executor::new(catalog, storage));
            let use_hnsw = iter % 2 == 0;
            // Skip the encrypted index on ~1/3 of iterations so HNSW + integer-PK
            // tables become eligible for the incremental DELETE fast path.
            let use_encrypted = iter % 3 != 0;
            run_lifecycle(
                &ex,
                engine,
                iter,
                ops,
                use_hnsw,
                use_encrypted,
                &mut rng,
                &mut rep,
            );
            total_ops += ops as u64;
        }
    }

    println!("\n════ SUMMARY ════");
    println!("engines            : {engines:?}");
    println!("iterations/engine  : {iterations}");
    println!("mutations exercised: {total_ops}");
    println!("divergences        : {}", rep.divergences);
    if rep.divergences == 0 {
        println!("\nNo index-coherence divergences vs brute-force reference. 🎯");
    } else {
        println!("\nReproduce a run with: --seed {seed} --iterations {iterations} --ops {ops}");
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
