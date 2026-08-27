//! Scale and soak harness (roadmap T1.4, DATABASE_COMPLETION M11).
//!
//! Drives a sustained, concurrent, mixed-model workload against **an explicitly
//! named storage engine** and validates the long-running-DB invariants:
//!   * no unbounded memory growth (leak detection) under a BOUNDED working set
//!     — each worker keeps a fixed-size ring of live rows (insert new, delete
//!     oldest), so table size plateaus and any RSS growth is a leak, not data;
//!   * no crashes / no unexpected error storm under concurrency;
//!   * index coherence survives sustained churn (PK uniqueness, btree and
//!     encrypted equality) — the same bug class the coherence oracle guards;
//!   * durability: after closing and reopening, committed rows survive and
//!     stay coherent.
//!
//! ## Which engine is measured
//!
//! This harness used to open `Database::durable_mvcc`, i.e. `MvccStorageAdapter`
//! — a `RwLock<Vec<MvccRow>>` that keeps every row in RAM. The server runs
//! `BufferedDiskEngine` over the paged `DiskEngine`. Numbers from the RAM engine
//! describe a database nobody deploys, and nothing in the old output said so.
//!
//! The engine is now selected with `--engine` and defaults to `buffered-disk`
//! (what `nucleus serve` runs). The RAM engines remain selectable so they can be
//! measured deliberately as a comparison point. Every number printed carries the
//! engine, machine, and configuration that produced it.
//!
//! ## Reported measurements
//!
//! p50/p95/p99 per operation class, throughput, RSS, disk footprint, WAL bytes
//! and syncs, write amplification (physical bytes per logical row byte), buffer
//! pool hit rate, checkpoint cost, and recovery time.
//!
//! ```text
//! cargo run --release --features server --bin probe_soak -- --duration-secs 30
//! cargo run --release --features server --bin probe_soak -- \
//!     --engine buffered-disk --rows-target 1000000 --duration-secs 60 --json out.json
//! ```
//!
//! Exit code is non-zero on any failed invariant or budget regression, so CI can
//! gate on it.
#![cfg(feature = "server")]
#![allow(clippy::unusual_byte_groupings)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nucleus::metrics::harness::{
    BudgetFile, EngineConfig, EngineKind, HarnessDb, MachineInfo, StorageSnapshot, json_str_field,
    parse_budget_file, rss_bytes,
};
use nucleus::metrics::latency::{LatencyRecorder, LatencySummary};
use nucleus::storage::wal::SyncMode;
use nucleus::types::{DataType, Value};

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

/// Operation classes tracked separately — a mixed p99 hides which statement is
/// slow.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Insert,
    Update,
    Select,
    Kv,
    Delete,
}

impl Op {
    const ALL: [Op; 5] = [Op::Insert, Op::Update, Op::Select, Op::Kv, Op::Delete];
    fn name(self) -> &'static str {
        match self {
            Op::Insert => "insert",
            Op::Update => "update",
            Op::Select => "select",
            Op::Kv => "kv_set",
            Op::Delete => "delete",
        }
    }
    fn index(self) -> usize {
        match self {
            Op::Insert => 0,
            Op::Update => 1,
            Op::Select => 2,
            Op::Kv => 3,
            Op::Delete => 4,
        }
    }
}

struct Shared {
    ops: AtomicU64,
    errors: AtomicU64,
    stop: AtomicBool,
    /// Bytes of logical row payload written, for write amplification.
    logical_bytes: AtomicU64,
    /// Live rows the workers have added minus removed. Cheap steady-state
    /// signal for the leak gate: COUNT(*) every sample tick would be a full
    /// scan and would distort the very workload being measured.
    net_rows: std::sync::atomic::AtomicI64,
    lat: [LatencyRecorder; 5],
    err_samples: parking_lot::Mutex<Vec<String>>,
}

impl Shared {
    fn new() -> Self {
        Self {
            ops: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            logical_bytes: AtomicU64::new(0),
            net_rows: std::sync::atomic::AtomicI64::new(0),
            // Reservoir-bounded: a multi-hour soak must not accumulate samples
            // until the leak gate is measuring the sample buffer.
            lat: std::array::from_fn(|_| LatencyRecorder::with_reservoir(250_000)),
            err_samples: parking_lot::Mutex::new(Vec::new()),
        }
    }

    fn record(&self, op: Op, elapsed: Duration, r: Result<(), String>) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.lat[op.index()].record(elapsed);
        if let Err(e) = r {
            self.errors.fetch_add(1, Ordering::Relaxed);
            let mut s = self.err_samples.lock();
            if s.len() < 25 {
                s.push(e);
            }
        }
    }
}

/// Runs one statement, times it, records it, and reports whether it succeeded.
/// The DB error is mapped to a short string for the sample log.
async fn timed(db: &HarnessDb, shared: &Shared, op: Op, sql: &str) -> bool {
    let t = Instant::now();
    let r = db
        .execute(sql)
        .await
        .map(|_| ())
        .map_err(|e| format!("{sql} -> {e}"));
    let ok = r.is_ok();
    shared.record(op, t.elapsed(), r);
    ok
}

const CAP: usize = 160;
const DIM: usize = 4;

/// Column types of the `soak` table, used to size logical row payload exactly
/// as the storage layer serializes it.
fn soak_col_types() -> Vec<DataType> {
    vec![
        DataType::Int64,
        DataType::Int32,
        DataType::Text,
        DataType::Vector(DIM),
    ]
}

/// Logical bytes one `soak` row occupies, computed with the engine's own tuple
/// serializer — so write amplification is a real ratio, not a guess.
fn logical_row_bytes(id: i64, val: i64, code: &str, vec: &[f32]) -> u64 {
    let row: Vec<Value> = vec![
        Value::Int64(id),
        Value::Int32(val as i32),
        Value::Text(code.to_string()),
        Value::Vector(vec.to_vec()),
    ];
    nucleus::storage::tuple::serialize_row(&row, &soak_col_types()).len() as u64
}

fn vlit(r: &mut Rng) -> (String, Vec<f32>) {
    let vals: Vec<f32> = (0..DIM)
        .map(|_| ((r.below(2000) as f64) / 100.0 - 10.0) as f32)
        .collect();
    let body: Vec<String> = vals.iter().map(|v| format!("{v:.2}")).collect();
    (format!("VECTOR('[{}]')", body.join(",")), vals)
}

async fn worker(
    db: Arc<HarnessDb>,
    shared: Arc<Shared>,
    id: usize,
    seed: u64,
    deadline: Instant,
    preloaded_rows: i64,
) {
    let base: i64 = (id as i64 + 1) * 1_000_000_000;
    let mut counter: i64 = 0;
    let mut live: VecDeque<i64> = VecDeque::new();
    let mut rng = Rng(seed.wrapping_add(id as u64).wrapping_mul(0x100000001b3) | 1);

    while Instant::now() < deadline && !shared.stop.load(Ordering::Relaxed) {
        let choice = rng.below(100);
        if live.len() < CAP / 2 || choice < 42 {
            // INSERT a fresh row with a globally-unique id and code.
            let rid = base + counter;
            counter += 1;
            let val = rng.below(64) as i64;
            let (vsql, vvals) = vlit(&mut rng);
            let code = format!("k{rid}");
            let sql = format!(
                "INSERT INTO soak (id, val, code, v) VALUES ({rid}, {val}, '{code}', {vsql})"
            );
            let t = Instant::now();
            let r = db
                .execute(&sql)
                .await
                .map(|_| ())
                .map_err(|e| format!("{sql} -> {e}"));
            let ok = r.is_ok();
            shared.record(Op::Insert, t.elapsed(), r);
            if ok {
                live.push_back(rid);
                shared.net_rows.fetch_add(1, Ordering::Relaxed);
                shared.logical_bytes.fetch_add(
                    logical_row_bytes(rid, val, &code, &vvals),
                    Ordering::Relaxed,
                );
            }
        } else if choice < 62 {
            // UPDATE a live row's indexed columns.
            let rid = live[rng.below(live.len())];
            let val = rng.below(64) as i64;
            let (vsql, vvals) = vlit(&mut rng);
            let sql = format!("UPDATE soak SET val = {val}, v = {vsql} WHERE id = {rid}");
            timed(&db, &shared, Op::Update, &sql).await;

            shared.logical_bytes.fetch_add(
                logical_row_bytes(rid, val, &format!("k{rid}"), &vvals),
                Ordering::Relaxed,
            );
        } else if choice < 80 {
            // SELECT via the indexes — half PK equality, half secondary btree,
            // and (when a load phase ran) some against the preloaded range so
            // reads touch pages outside this worker's hot ring.
            let rid = if preloaded_rows > 0 && choice.is_multiple_of(3) {
                rng.below(preloaded_rows as usize) as i64
            } else {
                live[rng.below(live.len())]
            };
            let target = rng.below(64) as i64;
            let q = if choice.is_multiple_of(2) {
                format!("SELECT id FROM soak WHERE id = {rid}")
            } else {
                format!("SELECT id FROM soak WHERE val = {target}")
            };
            let t = Instant::now();
            let r = db
                .query(&q)
                .await
                .map(|_| ())
                .map_err(|e| format!("{q} -> {e}"));
            shared.record(Op::Select, t.elapsed(), r);
        } else if choice < 90 {
            // KV op — a different model sharing the same engine.
            let k = base + rng.below(256) as i64;
            let sql = format!("SELECT KV_SET('sk{k}', 'v{counter}')");
            timed(&db, &shared, Op::Kv, &sql).await;
        } else if !live.is_empty() {
            // DELETE the oldest — this is the position-shifting op.
            let rid = live.pop_front().unwrap();
            if timed(
                &db,
                &shared,
                Op::Delete,
                &format!("DELETE FROM soak WHERE id = {rid}"),
            )
            .await
            {
                shared.net_rows.fetch_sub(1, Ordering::Relaxed);
            }
        }

        // Keep the working set bounded so RSS growth means a leak, not data.
        while live.len() > CAP {
            let rid = live.pop_front().unwrap();
            if timed(
                &db,
                &shared,
                Op::Delete,
                &format!("DELETE FROM soak WHERE id = {rid}"),
            )
            .await
            {
                shared.net_rows.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

async fn create_schema(db: &HarnessDb) -> Result<(), String> {
    let stmts = [
        "CREATE TABLE soak (id BIGINT PRIMARY KEY, val INT, code TEXT, v VECTOR(4))",
        "CREATE INDEX soak_val ON soak (val)",
        "CREATE INDEX soak_v ON soak USING hnsw (v)",
        "CREATE INDEX soak_code ON soak USING encrypted (code)",
    ];
    for sql in stmts {
        db.execute(sql)
            .await
            .map(|_| ())
            .map_err(|e| format!("{sql} -> {e}"))?;
    }
    Ok(())
}

/// Bulk-load `rows` rows in multi-row batches, reporting per-batch latency.
/// This is the capacity dimension of the ledger ("1M–100M row scales"); the
/// concurrent phase then runs on top of the loaded set.
async fn load_phase(
    db: &HarnessDb,
    rows: i64,
    batch: i64,
    seed: u64,
    logical_bytes: &AtomicU64,
) -> Result<(LatencySummary, f64, u64), String> {
    let mut rng = Rng(seed | 1);
    let lat = LatencyRecorder::with_reservoir(100_000);
    let mut errors = 0u64;
    let t0 = Instant::now();
    let mut id = 0i64;
    while id < rows {
        let n = batch.min(rows - id);
        let mut sql = String::with_capacity(n as usize * 64);
        sql.push_str("INSERT INTO soak (id, val, code, v) VALUES ");
        let mut batch_bytes = 0u64;
        for j in 0..n {
            let rid = id + j;
            let val = rid % 64;
            let (vsql, vvals) = vlit(&mut rng);
            let code = format!("k{rid}");
            if j > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({rid},{val},'{code}',{vsql})"));
            batch_bytes += logical_row_bytes(rid, val, &code, &vvals);
        }
        let t = Instant::now();
        let r = db.execute(&sql).await;
        lat.record(t.elapsed());
        match r {
            Ok(_) => {
                logical_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
            }
            Err(e) => {
                errors += 1;
                if errors <= 3 {
                    eprintln!("load error at id={id}: {e}");
                }
            }
        }
        id += n;
    }
    let secs = t0.elapsed().as_secs_f64();
    Ok((lat.summary(), rows as f64 / secs.max(1e-9), errors))
}

/// Post-soak / post-recovery invariant checks. Returns the failure list.
async fn coherence_failures(db: &HarnessDb) -> Vec<String> {
    let mut fails = Vec::new();

    // PK uniqueness: no id may appear twice (the create_index dup bug class).
    match db
        .query("SELECT id FROM soak GROUP BY id HAVING COUNT(*) > 1 LIMIT 5")
        .await
    {
        Ok(rows) if !rows.is_empty() => {
            fails.push(format!(
                "{} duplicate PK id(s) present after churn",
                rows.len()
            ));
        }
        Err(e) => fails.push(format!("pk-uniqueness query failed: {e}")),
        _ => {}
    }

    // Sample live rows: PK equality returns exactly one, encrypted code exactly one.
    let sample = db
        .query("SELECT id FROM soak LIMIT 40")
        .await
        .unwrap_or_default();
    for row in &sample {
        let Some(Value::Int64(rid)) = row.first() else {
            continue;
        };
        let rid = *rid;
        match db
            .query(&format!("SELECT id FROM soak WHERE id = {rid}"))
            .await
        {
            Ok(rows) if rows.len() != 1 => {
                // Discriminate the 0-row lookup before reporting it (see
                // _internal/PROBE_SOAK_CI_INVESTIGATION.md §6.2). The point
                // path (index_lookup_inner) rechecks each fetched row's
                // serialized key against the probe; the range path
                // (index_lookup_range_inner) seeks the SAME B-tree and applies
                // the same recheck per entry; `id + 0` defeats the index and
                // reads the heap. So:
                //   point=0 range=0 seq=1  -> entry lost from the B-tree
                //   point=0 range=0 seq=0  -> row itself missing (heap loss)
                //   point=0 range=1, range's row id != rid
                //     -> stale entry surfacing another row (slot recycling)
                //   point=0 range=1, range's row id == rid
                //     -> entry present and resolving, point descent missed it
                let range_row = db
                    .query(&format!(
                        "SELECT id FROM soak WHERE id >= {rid} AND id <= {rid}"
                    ))
                    .await
                    .ok()
                    .and_then(|r| r.first().cloned());
                let range = range_row.is_some().to_string();
                let range_id = range_row
                    .and_then(|r| r.first().cloned())
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".into());
                let seq = db
                    .query(&format!("SELECT id FROM soak WHERE id + 0 = {rid}"))
                    .await
                    .ok()
                    .map(|r| r.len().to_string())
                    .unwrap_or_else(|| "err".into());
                fails.push(format!(
                    "pk id={rid} returned {} rows (expected 1); same-key range-scan rows={range} (row id={range_id}), seq-scan rows={seq}",
                    rows.len()
                ));
            }
            Err(e) => fails.push(format!("pk id={rid} query failed: {e}")),
            _ => {}
        }
        let enc = db
            .query_one(&format!(
                "SELECT ENCRYPTED_LOOKUP('soak_code', 'k{rid}') FROM soak LIMIT 1"
            ))
            .await;
        if let Ok(Some(Value::Text(s))) = enc {
            let n = s.split(',').filter(|p| !p.trim().is_empty()).count();
            if n != 1 {
                fails.push(format!(
                    "encrypted lookup k{rid}: {n} postings (expected 1)"
                ));
            }
        }
        if fails.len() > 10 {
            break;
        }
    }

    // Val-index coherence: the `soak_val` secondary index lives in the dense
    // duplicate-key regime (64 distinct values across every row), the shape
    // where a B-tree duplicate-run defect first appears. For each distinct
    // val, the index path (`WHERE val = v`) and the heap path (`val + 0`
    // defeats the index) must agree exactly.
    let mut val_mismatch = 0usize;
    for v in 0..64i64 {
        let via_index = db
            .query(&format!("SELECT COUNT(*) FROM soak WHERE val = {v}"))
            .await;
        let via_heap = db
            .query(&format!("SELECT COUNT(*) FROM soak WHERE val + 0 = {v}"))
            .await;
        match (via_index, via_heap) {
            (Ok(a), Ok(b)) if !a.is_empty() && !b.is_empty() => {
                if a[0] != b[0] {
                    val_mismatch += 1;
                    if val_mismatch <= 5 {
                        fails.push(format!(
                            "val={v}: index path returned {:?} but heap scan returned {:?}",
                            a[0], b[0]
                        ));
                    }
                }
            }
            (Err(e), _) | (_, Err(e)) => {
                fails.push(format!("val={v} coherence query failed: {e}"));
                break;
            }
            _ => {
                fails.push(format!("val={v} coherence query returned no rows"));
                break;
            }
        }
    }
    if val_mismatch > 5 {
        fails.push(format!(
            "...and {val_mismatch} more val-index disagreements in total"
        ));
    }
    fails
}

async fn row_count(db: &HarnessDb) -> i64 {
    match db.query_one("SELECT COUNT(*) FROM soak").await {
        Ok(Some(Value::Int64(n))) => n,
        Ok(Some(Value::Int32(n))) => n as i64,
        _ => -1,
    }
}

struct Args {
    engine: EngineKind,
    config: EngineConfig,
    seed: u64,
    concurrency: usize,
    duration_secs: u64,
    leak_limit_mb: u64,
    rows_target: i64,
    load_batch: i64,
    dir: Option<String>,
    keep_dir: bool,
    json_out: Option<String>,
    budget_in: Option<String>,
    budget_out: Option<String>,
    budget_slack: f64,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            engine: EngineKind::BufferedDisk,
            config: EngineConfig::default(),
            seed: 0x50AC_BEEF_1234,
            concurrency: 8,
            duration_secs: 20,
            leak_limit_mb: 96,
            rows_target: 0,
            load_batch: 500,
            dir: None,
            keep_dir: false,
            json_out: None,
            budget_in: None,
            budget_out: None,
            budget_slack: 1.5,
        }
    }
}

fn usage() -> String {
    let mut s = String::from(
        "probe_soak — scale/soak harness\n\nOptions:\n  \
         --engine <kind>          storage engine to measure (default buffered-disk)\n",
    );
    for k in EngineKind::ALL {
        s.push_str(&format!("      {:<16} {}\n", k.name(), k.description()));
    }
    s.push_str(
        "  --duration-secs <n>      concurrent-phase duration (default 20)\n  \
         --concurrency <n>        concurrent workers (default 8)\n  \
         --rows-target <n>        bulk-load n rows before the concurrent phase (default 0)\n  \
         --load-batch <n>         rows per multi-row INSERT during load (default 500)\n  \
         --seed <n>               workload seed\n  \
         --leak-limit-mb <n>      post-warmup RSS growth allowed (default 96)\n  \
         --buffer-pool-mb <n>     paged engines only (server default 32)\n  \
         --wal-segment-mb <n>     0 = single-file WAL (server default 64)\n  \
         --sync-mode <m>          fsync | fdatasync | none (server default fsync)\n  \
         --dir <path>             data directory (default a temp dir, removed on exit)\n  \
         --keep-dir               do not delete the data directory on exit\n  \
         --json <path>            write the full result record as JSON\n  \
         --budget <path>          fail if results regress against this budget file\n  \
         --write-budget <path>    write this run's results as a budget file\n  \
         --budget-slack <f>       multiplier applied when writing a budget (default 1.5)\n",
    );
    s
}

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().collect();
    let mut a = Args::default();
    let mut i = 1;
    while i < argv.len() {
        let flag = argv[i].as_str();
        let value = |i: &mut usize| -> Result<String, String> {
            *i += 1;
            argv.get(*i)
                .cloned()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag {
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            "--engine" => {
                let v = value(&mut i)?;
                a.engine = EngineKind::parse(&v)
                    .ok_or_else(|| format!("unknown engine '{v}'\n\n{}", usage()))?;
            }
            "--duration-secs" => {
                a.duration_secs = value(&mut i)?.parse().map_err(|_| "bad --duration-secs")?
            }
            "--concurrency" => {
                a.concurrency = value(&mut i)?.parse().map_err(|_| "bad --concurrency")?
            }
            "--rows-target" => {
                a.rows_target = value(&mut i)?.parse().map_err(|_| "bad --rows-target")?
            }
            "--load-batch" => {
                a.load_batch = value(&mut i)?.parse().map_err(|_| "bad --load-batch")?
            }
            "--seed" => a.seed = value(&mut i)?.parse().map_err(|_| "bad --seed")?,
            "--leak-limit-mb" => {
                a.leak_limit_mb = value(&mut i)?.parse().map_err(|_| "bad --leak-limit-mb")?
            }
            "--buffer-pool-mb" => {
                a.config.buffer_pool_mb =
                    value(&mut i)?.parse().map_err(|_| "bad --buffer-pool-mb")?
            }
            "--wal-segment-mb" => {
                a.config.wal_segment_mb =
                    value(&mut i)?.parse().map_err(|_| "bad --wal-segment-mb")?
            }
            "--sync-mode" => a.config.sync_mode = SyncMode::from_str(&value(&mut i)?),
            "--dir" => a.dir = Some(value(&mut i)?),
            "--keep-dir" => a.keep_dir = true,
            "--json" => a.json_out = Some(value(&mut i)?),
            "--budget" => a.budget_in = Some(value(&mut i)?),
            "--write-budget" => a.budget_out = Some(value(&mut i)?),
            "--budget-slack" => {
                a.budget_slack = value(&mut i)?.parse().map_err(|_| "bad --budget-slack")?
            }
            other => return Err(format!("unknown flag '{other}'\n\n{}", usage())),
        }
        i += 1;
    }
    if a.concurrency == 0 {
        return Err("--concurrency must be at least 1".into());
    }
    Ok(a)
}

fn main() {
    unsafe {
        std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef");
    }
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("probe_soak: {e}");
            std::process::exit(2);
        }
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((args.concurrency + 2).min(16))
        .enable_all()
        .build()
        .unwrap();
    if rt.block_on(run_soak(args)) {
        std::process::exit(1);
    }
}

/// Everything one run measured. Serialized to JSON and compared to budgets.
struct Report {
    machine: MachineInfo,
    engine: EngineKind,
    config: EngineConfig,
    concurrency: usize,
    duration_secs: u64,
    rows_target: i64,
    load: Option<(LatencySummary, f64)>,
    ops: u64,
    errors: u64,
    err_rate: f64,
    ops_per_sec: f64,
    per_op: Vec<(&'static str, LatencySummary, u64)>,
    before: StorageSnapshot,
    after: StorageSnapshot,
    logical_bytes: u64,
    checkpoint: Option<Result<Duration, String>>,
    /// `None` for engines that make no durability claim.
    recovery: Option<Duration>,
    rows_before: i64,
    rows_after: Option<i64>,
    leak: LeakReport,
}

impl Report {
    /// The workload shape, recorded in budgets so an incomparable one is
    /// rejected rather than silently compared.
    fn workload_signature(&self) -> String {
        format!(
            "concurrency={} duration_secs={} rows_target={}",
            self.concurrency, self.duration_secs, self.rows_target
        )
    }

    /// Physical bytes written per byte of logical row payload. `None` when the
    /// engine writes no WAL/data files or nothing was written.
    fn write_amplification(&self) -> Option<f64> {
        let delta = self.after.delta(&self.before);
        let physical = delta.wal_bytes + delta.disk_bytes;
        if self.logical_bytes == 0 || physical == 0 {
            return None;
        }
        Some(physical as f64 / self.logical_bytes as f64)
    }

    /// Metric name → value for budget comparison. Only quantities that are
    /// comparable across runs on the same machine + engine appear here.
    fn budget_metrics(&self) -> Vec<(String, f64)> {
        let mut m = vec![("mixed.ops_per_sec".to_string(), self.ops_per_sec)];
        for (name, s, _) in &self.per_op {
            if s.count == 0 {
                continue;
            }
            m.push((format!("{name}.p50_us"), s.p50_us));
            m.push((format!("{name}.p95_us"), s.p95_us));
            m.push((format!("{name}.p99_us"), s.p99_us));
        }
        if let Some((s, rows_per_sec)) = &self.load {
            m.push(("load.rows_per_sec".to_string(), *rows_per_sec));
            m.push(("load.batch_p99_us".to_string(), s.p99_us));
        }
        if let Some(recovery) = self.recovery {
            m.push(("recovery_ms".to_string(), recovery.as_secs_f64() * 1000.0));
        }
        if let Some(Ok(d)) = &self.checkpoint {
            m.push(("checkpoint_ms".to_string(), d.as_secs_f64() * 1000.0));
        }
        if let Some(wa) = self.write_amplification() {
            m.push(("write_amplification".to_string(), wa));
        }
        m
    }

    fn to_json(&self) -> String {
        let delta = self.after.delta(&self.before);
        let mut per_op = String::new();
        for (name, s, errs) in &self.per_op {
            if !per_op.is_empty() {
                per_op.push(',');
            }
            per_op.push_str(&format!(
                "\n      \"{name}\": {{\"count\": {}, \"errors\": {errs}, \"min_us\": {:.1}, \"mean_us\": {:.1}, \"p50_us\": {:.1}, \"p95_us\": {:.1}, \"p99_us\": {:.1}, \"max_us\": {:.1}}}",
                s.count, s.min_us, s.mean_us, s.p50_us, s.p95_us, s.p99_us, s.max_us
            ));
        }
        let load = match &self.load {
            Some((s, rps)) => format!(
                "{{\"rows\": {}, \"rows_per_sec\": {:.1}, \"batch_p50_us\": {:.1}, \"batch_p95_us\": {:.1}, \"batch_p99_us\": {:.1}}}",
                self.rows_target, rps, s.p50_us, s.p95_us, s.p99_us
            ),
            None => "null".to_string(),
        };
        let checkpoint = match &self.checkpoint {
            Some(Ok(d)) => format!("{:.3}", d.as_secs_f64() * 1000.0),
            Some(Err(_)) | None => "null".to_string(),
        };
        let hit_rate = match self.after.cache_hit_rate() {
            Some(r) => format!("{r:.4}"),
            None => "null".to_string(),
        };
        let write_amp = match self.write_amplification() {
            Some(w) => format!("{w:.3}"),
            None => "null".to_string(),
        };
        format!(
            r#"{{
  "harness": "probe_soak",
  "machine": {{"os": "{os}", "arch": "{arch}", "cpu": "{cpu}", "logical_cpus": {cpus}, "total_memory_bytes": {mem}, "build_profile": "{profile}", "nucleus_version": "{ver}"}},
  "engine": "{engine}",
  "engine_is_server_default": {is_default},
  "config": {{"buffer_pool_mb": {bp}, "wal_segment_mb": {ws}, "sync_mode": "{sync:?}"}},
  "workload": {{"concurrency": {conc}, "duration_secs": {dur}, "rows_target": {rows}}},
  "load": {load},
  "mixed": {{
    "ops": {ops},
    "errors": {errors},
    "error_rate": {err_rate:.6},
    "ops_per_sec": {ops_per_sec:.1},
    "latency_us": {{{per_op}
    }}
  }},
  "resources": {{
    "rss_peak_bytes": {rss_peak},
    "rss_growth_bytes": {rss_growth},
    "leak_gate": {leak_gate},
    "disk_bytes": {disk},
    "wal_bytes_written": {wal_bytes},
    "wal_syncs": {wal_syncs},
    "logical_bytes_written": {logical},
    "write_amplification": {write_amp},
    "cache_hits": {hits},
    "cache_misses": {misses},
    "cache_hit_rate": {hit_rate},
    "checkpoint_ms": {checkpoint},
    "recovery_ms": {recovery}
  }},
  "durability": {{"engine_is_durable": {is_durable}, "rows_before_reopen": {rows_before}, "rows_after_reopen": {rows_after}}}
}}
"#,
            os = self.machine.os,
            arch = self.machine.arch,
            cpu = self.machine.cpu_model,
            cpus = self.machine.logical_cpus,
            mem = self.machine.total_memory_bytes,
            profile = self.machine.build_profile,
            ver = self.machine.nucleus_version,
            engine = self.engine.name(),
            is_default = self.engine.is_server_default(),
            bp = self.config.buffer_pool_mb,
            ws = self.config.wal_segment_mb,
            sync = self.config.sync_mode,
            conc = self.concurrency,
            dur = self.duration_secs,
            rows = self.rows_target,
            load = load,
            ops = self.ops,
            errors = self.errors,
            err_rate = self.err_rate,
            ops_per_sec = self.ops_per_sec,
            per_op = per_op,
            rss_peak = self.leak.peak_mb * 1024 * 1024,
            rss_growth = self.leak.growth_mb * 1024 * 1024,
            leak_gate = match &self.leak.not_evaluated {
                Some(why) => format!("{{\"evaluated\": false, \"reason\": \"{why}\"}}"),
                None => format!(
                    "{{\"evaluated\": true, \"limit_mb\": {}}}",
                    self.leak.limit_mb
                ),
            },
            disk = self.after.disk_bytes,
            wal_bytes = delta.wal_bytes,
            wal_syncs = delta.wal_syncs,
            logical = self.logical_bytes,
            write_amp = write_amp,
            hits = delta.cache_hits,
            misses = delta.cache_misses,
            hit_rate = hit_rate,
            checkpoint = checkpoint,
            recovery = match self.recovery {
                Some(d) => format!("{:.3}", d.as_secs_f64() * 1000.0),
                None => "null".to_string(),
            },
            is_durable = self.engine.is_durable(),
            rows_before = self.rows_before,
            rows_after = match self.rows_after {
                Some(n) => n.to_string(),
                None => "null".to_string(),
            },
        )
    }
}

/// Metrics where higher is better; everything else is a latency/cost where
/// lower is better.
fn higher_is_better(metric: &str) -> bool {
    metric.ends_with("ops_per_sec") || metric.ends_with("rows_per_sec")
}

/// Extra slack multiplier for metrics a single run cannot resolve tightly.
///
/// Measured on this workload, two runs with the *same seed* differ by more than
/// 1.5x at `delete.p99_us`, because a time-bounded concurrent workload does not
/// replay deterministically: thread interleaving decides how many operations of
/// each class run and which of them land behind a checkpoint. The tail and the
/// once-per-run stopwatch readings (`checkpoint_ms`, `recovery_ms`, the latter
/// dominated by page-cache state) therefore get a wider band.
///
/// The consequence is stated rather than hidden: these bounds catch gross
/// regressions, roughly 3x and worse, not small ones. Tightening them needs
/// budgets recorded as the max across several runs, not a single run.
fn noise_slack(metric: &str) -> f64 {
    if matches!(metric, "recovery_ms" | "checkpoint_ms") || metric.ends_with("p99_us") {
        3.0
    } else if metric.ends_with("p95_us") {
        2.0
    } else {
        1.0
    }
}

fn machine_fingerprint(m: &MachineInfo) -> String {
    format!(
        "{}/{}/{}/{}cpu/{}",
        m.os, m.arch, m.cpu_model, m.logical_cpus, m.build_profile
    )
}

/// Write the run's measurements as a budget file, merging with an existing
/// budget for the same engine and machine.
///
/// A single run of a time-bounded concurrent workload is not reproducible
/// enough to be a tight tripwire: measured on this harness, two runs at the
/// same seed differ by ~2.6x at `insert.p95_us` and ~4x at `recovery_ms`. The
/// answer is not to keep widening the multiplier until nothing ever fails —
/// that yields a tripwire that detects nothing. Instead a budget is an
/// *envelope*: run `--write-budget` several times and each bound relaxes to
/// cover the worst value observed so far, with `runs_recorded` stating how many
/// runs back it. A budget with `runs_recorded: 1` should be treated as
/// provisional.
///
/// `source_run_passed` is recorded too. A budget is a performance tripwire, so
/// it stays useful when the source run failed a correctness invariant — but a
/// reader must never have to guess whether the numbers came from a clean run.
fn write_budget(
    path: &str,
    report: &Report,
    slack: f64,
    source_run_passed: bool,
) -> Result<(), String> {
    // Existing envelope for the same engine and machine, if any.
    let fingerprint = machine_fingerprint(&report.machine);
    let workload = report.workload_signature();
    let config = report.config.describe();
    let existing = std::fs::read_to_string(path).ok().and_then(|src| {
        let clean = json_str_field(&src, "all_source_runs_passed_invariants")
            .map(|v| v == "true")
            .unwrap_or(false);
        parse_budget_file(&src).ok().and_then(|b| {
            // Only merge with an envelope describing the same experiment.
            (b.engine == report.engine.name()
                && b.machine == fingerprint
                && b.config == config
                && b.workload == workload)
                .then_some((b, clean))
        })
    });
    let prior: std::collections::HashMap<String, (Option<f64>, Option<f64>)> = existing
        .as_ref()
        .map(|(b, _)| {
            b.bounds
                .iter()
                .map(|x| (x.metric.clone(), (x.max, x.min)))
                .collect()
        })
        .unwrap_or_default();
    let runs_recorded = existing.as_ref().map(|(b, _)| b.runs_recorded).unwrap_or(0) + 1;
    // Once any contributing run failed its invariants, the envelope carries
    // that fact forward.
    let all_runs_passed =
        existing.as_ref().map(|(_, clean)| *clean).unwrap_or(true) && source_run_passed;

    let mut lines = Vec::new();
    for (name, value) in report.budget_metrics() {
        // Noisy metrics get a deliberately wider band; see `noise_slack`.
        let slack = slack.max(1.0) * noise_slack(&name);
        let (prior_max, prior_min) = prior.get(&name).copied().unwrap_or((None, None));
        let bound = if higher_is_better(&name) {
            let min = (value / slack).min(prior_min.unwrap_or(f64::MAX));
            format!("\"min\": {min:.3}")
        } else {
            let max = (value * slack).max(prior_max.unwrap_or(0.0));
            format!("\"max\": {max:.3}")
        };
        lines.push(format!("    \"{name}\": {{{bound}}}"));
    }
    let body = format!(
        r#"{{
  "_comment": "probe_soak regression budget, an envelope over `runs_recorded` runs. Valid only for the engine, machine, and config recorded here; a number without its hardware is not a datum. Each bound is the loosest seen across contributing runs, at recorded_slack for p50/throughput, 2x for p95, 3x for p99/checkpoint_ms/recovery_ms, because a time-bounded concurrent run does not replay deterministically even at a fixed seed. These bounds catch gross regressions, not small ones; runs_recorded=1 is provisional. Re-run --write-budget to widen, delete the file to start over.",
  "engine": "{engine}",
  "machine": "{machine}",
  "config": "{config}",
  "recorded_slack": {slack},
  "runs_recorded": {runs},
  "all_source_runs_passed_invariants": "{passed}",
  "workload": "{workload_sig}",
  "budgets": {{
{lines}
  }}
}}
"#,
        engine = report.engine.name(),
        machine = fingerprint,
        config = config,
        slack = slack,
        runs = runs_recorded,
        passed = all_runs_passed,
        workload_sig = workload,
        lines = lines.join(",\n"),
    );
    std::fs::write(path, body).map_err(|e| format!("write budget {path}: {e}"))
}

/// Returns the list of budget violations, or an explanation of why the budget
/// could not be applied (which is never a pass and never a failure — an
/// incomparable budget is reported, not silently ignored).
fn check_budget(path: &str, report: &Report) -> (Vec<String>, Vec<String>) {
    let src = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => return (vec![format!("budget {path} unreadable: {e}")], Vec::new()),
    };
    let budget: BudgetFile = match parse_budget_file(&src) {
        Ok(b) => b,
        Err(e) => return (vec![format!("budget {path}: {e}")], Vec::new()),
    };
    let mut violations = Vec::new();
    let mut notes = Vec::new();
    if budget.engine != report.engine.name() {
        notes.push(format!(
            "budget was recorded on engine '{}' but this run used '{}' — NOT compared",
            budget.engine,
            report.engine.name()
        ));
        return (violations, notes);
    }
    let fingerprint = machine_fingerprint(&report.machine);
    if budget.machine != fingerprint {
        notes.push(format!(
            "budget was recorded on '{}' but this run is on '{}' — NOT compared",
            budget.machine, fingerprint
        ));
        return (violations, notes);
    }
    // Storage configuration and workload shape both move latency directly, so a
    // budget recorded under different ones is not a baseline for this run.
    let config = report.config.describe();
    if !budget.config.is_empty() && budget.config != config {
        notes.push(format!(
            "budget was recorded with '{}' but this run used '{}' — NOT compared",
            budget.config, config
        ));
        return (violations, notes);
    }
    let workload = report.workload_signature();
    if !budget.workload.is_empty() && budget.workload != workload {
        notes.push(format!(
            "budget was recorded for workload '{}' but this run was '{}' — NOT compared",
            budget.workload, workload
        ));
        return (violations, notes);
    }
    let measured: std::collections::HashMap<String, f64> =
        report.budget_metrics().into_iter().collect();
    for bound in &budget.bounds {
        let name = &bound.metric;
        let (max, min) = (&bound.max, &bound.min);
        let Some(value) = measured.get(name) else {
            notes.push(format!("budget metric '{name}' not measured in this run"));
            continue;
        };
        if let Some(max) = max
            && *value > *max
        {
            violations.push(format!("{name} = {value:.1} exceeds budget max {max:.1}"));
        }
        if let Some(min) = min
            && *value < *min
        {
            violations.push(format!("{name} = {value:.1} below budget min {min:.1}"));
        }
    }
    (violations, notes)
}

async fn run_soak(args: Args) -> bool {
    let machine = MachineInfo::detect();
    let dir = match &args.dir {
        Some(d) => std::path::PathBuf::from(d),
        None => std::env::temp_dir().join(format!("nucleus_soak_{:x}", args.seed)),
    };
    if args.dir.is_none() {
        let _ = std::fs::remove_dir_all(&dir);
    }

    println!("probe_soak");
    println!("  machine : {}", machine.describe());
    println!(
        "  engine  : {} — {}",
        args.engine.name(),
        args.engine.description()
    );
    if !args.engine.is_server_default() {
        println!(
            "  NOTE    : this is NOT the engine `nucleus serve` runs (that is 'buffered-disk')."
        );
    }
    if args.engine.is_ram_resident() {
        println!(
            "  NOTE    : rows are RAM-resident on this engine; capacity numbers describe RAM."
        );
    }
    println!("  config  : {}", args.config.describe());
    println!(
        "  workload: concurrency={} duration={}s rows_target={} dir={}",
        args.concurrency,
        args.duration_secs,
        args.rows_target,
        dir.display()
    );

    let db = match HarnessDb::open(args.engine, &dir, args.config).await {
        Ok(d) => Arc::new(d),
        Err(e) => {
            println!("FAIL: open {}: {e}", args.engine.name());
            return true;
        }
    };
    if let Err(e) = create_schema(&db).await {
        println!("FAIL: schema setup: {e}");
        return true;
    }

    let shared = Arc::new(Shared::new());
    let before = db.snapshot();

    // ---- optional load phase (capacity dimension) ----
    let load = if args.rows_target > 0 {
        println!(
            "\nloading {} rows in batches of {} ...",
            args.rows_target, args.load_batch
        );
        match load_phase(
            &db,
            args.rows_target,
            args.load_batch.max(1),
            args.seed,
            &shared.logical_bytes,
        )
        .await
        {
            Ok((summary, rows_per_sec, errors)) => {
                if errors > 0 {
                    println!("  {errors} load batch(es) failed");
                }
                println!(
                    "  loaded at {rows_per_sec:.0} rows/s; batch latency {}",
                    summary.fmt_ms()
                );
                Some((summary, rows_per_sec))
            }
            Err(e) => {
                println!("FAIL: load phase: {e}");
                return true;
            }
        }
    } else {
        None
    };

    // ---- concurrent mixed phase ----
    // Steady state is tracked by the workers themselves (Shared::net_rows) and
    // read by the sampler, so the leak gate can tell a plateaued working set
    // from a growing one without a COUNT(*) full scan on every tick.
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.duration_secs);

    let sampler_stop = shared.clone();
    let sampler = tokio::spawn(async move {
        let mut series: Vec<Sample> = Vec::new();
        while Instant::now() < deadline && !sampler_stop.stop.load(Ordering::Relaxed) {
            series.push(Sample {
                t: start.elapsed().as_secs_f64(),
                rss: rss_bytes(),
                net_rows: sampler_stop.net_rows.load(Ordering::Relaxed),
            });
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        series
    });

    let mut handles = Vec::new();
    for w in 0..args.concurrency {
        handles.push(tokio::spawn(worker(
            db.clone(),
            shared.clone(),
            w,
            args.seed,
            deadline,
            args.rows_target,
        )));
    }
    for h in handles {
        let _ = h.await;
    }
    shared.stop.store(true, Ordering::Relaxed);
    let series = sampler.await.unwrap_or_default();

    let ops = shared.ops.load(Ordering::Relaxed);
    let errors = shared.errors.load(Ordering::Relaxed);
    let per_op: Vec<(&'static str, LatencySummary, u64)> = Op::ALL
        .iter()
        .map(|op| (op.name(), shared.lat[op.index()].summary(), 0u64))
        .collect();
    let rows_before = row_count(&db).await;

    // ---- checkpoint cost (a ledger quantity with no existing counter) ----
    let checkpoint = db.checkpoint();
    let after = db.snapshot();

    // ---- coherence on the live database, before any close ----
    // This runs for every engine. The reopen checks below only apply to engines
    // that claim durability; failing `memory`/`mvcc` for losing data on close
    // would be testing a promise they never made.
    let mut failed = false;
    let churn_fails = coherence_failures(&db).await;

    // ---- close and reopen: recovery time + durability (durable engines) ----
    let _ = db.sync();
    let db = Arc::try_unwrap(db).ok().expect("workers finished");
    db.close();

    let mut recovery: Option<Duration> = None;
    let mut rows_after: Option<i64> = None;
    let mut recover_fails: Vec<String> = Vec::new();
    if args.engine.is_durable() {
        let db2 = match HarnessDb::open(args.engine, &dir, args.config).await {
            Ok(d) => d,
            Err(e) => {
                println!("FAIL: reopen after close: {e}");
                return true;
            }
        };
        recovery = Some(db2.open_elapsed());
        rows_after = Some(row_count(&db2).await);
        recover_fails = coherence_failures(&db2).await;
        db2.close();
    } else {
        println!(
            "durability/recovery: NOT APPLICABLE — engine '{}' is not durable by design, so it is \
             not checked for surviving a reopen",
            args.engine.name()
        );
    }

    let err_rate = if ops > 0 {
        errors as f64 / ops as f64
    } else {
        0.0
    };
    if err_rate > 0.02 {
        failed = true;
        println!(
            "FAIL: error rate {:.3}% exceeds 2% ({errors}/{ops})",
            err_rate * 100.0
        );
        for s in shared.err_samples.lock().iter().take(8) {
            println!("      err: {s}");
        }
    }

    let leak = analyze_leak(&series, args.duration_secs, args.leak_limit_mb);
    if let Some(msg) = &leak.failure {
        failed = true;
        println!("FAIL: {msg}");
    }
    if let Some(msg) = &leak.not_evaluated {
        println!("LEAK GATE NOT EVALUATED: {msg}");
    }

    if !churn_fails.is_empty() {
        failed = true;
        println!("FAIL: post-soak coherence:");
        for f in &churn_fails {
            println!("      {f}");
        }
    }

    if let Some(rows_after) = rows_after
        && rows_before >= 0
        && rows_after != rows_before
    {
        failed = true;
        println!("FAIL: durability: {rows_before} rows before close, {rows_after} after reopen");
    }
    if !recover_fails.is_empty() {
        failed = true;
        println!("FAIL: post-recovery coherence:");
        for f in &recover_fails {
            println!("      {f}");
        }
    }
    if let Some(Err(e)) = &checkpoint {
        failed = true;
        println!("FAIL: checkpoint: {e}");
    }

    let report = Report {
        machine,
        engine: args.engine,
        config: args.config,
        concurrency: args.concurrency,
        duration_secs: args.duration_secs,
        rows_target: args.rows_target,
        load,
        ops,
        errors,
        err_rate,
        ops_per_sec: ops as f64 / args.duration_secs.max(1) as f64,
        per_op,
        before,
        after,
        logical_bytes: shared.logical_bytes.load(Ordering::Relaxed),
        checkpoint,
        recovery,
        rows_before,
        rows_after,
        leak,
    };

    if !args.keep_dir && args.dir.is_none() {
        let _ = std::fs::remove_dir_all(&dir);
    }

    print_summary(&report);

    if let Some(path) = &args.json_out {
        match std::fs::write(path, report.to_json()) {
            Ok(()) => println!("\nJSON results written to {path}"),
            Err(e) => {
                failed = true;
                println!("\nFAIL: could not write {path}: {e}");
            }
        }
    }
    if let Some(path) = &args.budget_out {
        match write_budget(path, &report, args.budget_slack, !failed) {
            Ok(()) => println!("Budget written to {path} (slack {}x)", args.budget_slack),
            Err(e) => {
                failed = true;
                println!("FAIL: {e}");
            }
        }
    }
    if let Some(path) = &args.budget_in {
        let (violations, notes) = check_budget(path, &report);
        for n in &notes {
            println!("budget note: {n}");
        }
        if !violations.is_empty() {
            failed = true;
            println!("\nFAIL: budget regressions vs {path}:");
            for v in &violations {
                println!("      {v}");
            }
        } else if notes.is_empty() {
            println!("Budget {path}: all bounds satisfied.");
        }
    }

    if failed {
        println!("\nSOAK FAILED");
    } else {
        // Name only the gates that actually ran. A blanket "bounded memory,
        // durable across reopen" after a skipped leak gate or a non-durable
        // engine would be a claim the run did not earn.
        let mut verified = vec!["no error storm", "index coherence under churn"];
        if report.leak.not_evaluated.is_none() {
            verified.push("bounded memory");
        }
        if report.recovery.is_some() {
            verified.push("durable across reopen");
        }
        println!("\nSoak passed. Verified: {}.", verified.join(", "));
        if let Some(why) = &report.leak.not_evaluated {
            println!("Not verified: bounded memory — {why}.");
        }
        if report.recovery.is_none() {
            println!(
                "Not verified: durability — engine '{}' makes no durability claim.",
                report.engine.name()
            );
        }
    }
    failed
}

fn print_summary(r: &Report) {
    let delta = r.after.delta(&r.before);
    println!("\n════ SOAK SUMMARY ════");
    println!("machine         : {}", r.machine.describe());
    println!(
        "engine          : {} ({}){}",
        r.engine.name(),
        r.config.describe(),
        if r.engine.is_server_default() {
            " — server default"
        } else {
            ""
        }
    );
    println!(
        "workload        : concurrency={} duration={}s rows_target={}",
        r.concurrency, r.duration_secs, r.rows_target
    );
    if let Some((s, rps)) = &r.load {
        println!("load            : {rps:.0} rows/s, batch {}", s.fmt_ms());
    }
    println!(
        "ops             : {}   errors: {} ({:.3}%)   {:.0} ops/s",
        r.ops,
        r.errors,
        r.err_rate * 100.0,
        r.ops_per_sec
    );
    println!("\nlatency by operation (microseconds)");
    println!(
        "  {:<8} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "op", "count", "p50", "p95", "p99", "max"
    );
    for (name, s, _) in &r.per_op {
        if s.count == 0 {
            continue;
        }
        println!(
            "  {:<8} {:>10} {:>10.1} {:>10.1} {:>10.1} {:>10.1}",
            name, s.count, s.p50_us, s.p95_us, s.p99_us, s.max_us
        );
    }

    println!("\nresources");
    println!(
        "  RSS start/peak/end   : {} / {} / {} MB (post-warmup growth {} MB, limit {})",
        r.leak.first_mb, r.leak.peak_mb, r.leak.last_mb, r.leak.growth_mb, r.leak.limit_mb
    );
    match &r.leak.not_evaluated {
        Some(why) => println!("  leak gate            : NOT EVALUATED — {why}"),
        None => println!(
            "  leak gate            : evaluated over a plateaued working set (limit {} MB)",
            r.leak.limit_mb
        ),
    }
    if r.engine.is_durable() {
        println!(
            "  disk footprint       : {:.1} MB",
            r.after.disk_bytes as f64 / (1024.0 * 1024.0)
        );
    }
    if r.engine.has_buffer_pool() {
        println!(
            "  WAL written / syncs  : {:.1} MB / {}",
            delta.wal_bytes as f64 / (1024.0 * 1024.0),
            delta.wal_syncs
        );
        match r.write_amplification() {
            Some(wa) => println!(
                "  write amplification  : {wa:.2}x ({:.1} MB physical / {:.1} MB logical)",
                (delta.wal_bytes + delta.disk_bytes) as f64 / (1024.0 * 1024.0),
                r.logical_bytes as f64 / (1024.0 * 1024.0)
            ),
            None => println!("  write amplification  : n/a (no physical writes recorded)"),
        }
        match r.after.cache_hit_rate() {
            Some(hr) => println!(
                "  buffer pool hit rate : {:.2}% ({} hits / {} misses, {} evictions)",
                hr * 100.0,
                delta.cache_hits,
                delta.cache_misses,
                delta.evictions
            ),
            None => println!("  buffer pool hit rate : n/a (no page reads)"),
        }
        match &r.checkpoint {
            Some(Ok(d)) => println!(
                "  checkpoint cost      : {:.1} ms",
                d.as_secs_f64() * 1000.0
            ),
            Some(Err(e)) => println!("  checkpoint cost      : FAILED — {e}"),
            None => {}
        }
    } else {
        println!("  (no buffer pool / WAL page accounting on this engine)");
    }
    match (r.recovery, r.rows_after) {
        (Some(recovery), Some(rows_after)) => {
            println!(
                "  recovery time        : {:.1} ms (reopen of the populated directory)",
                recovery.as_secs_f64() * 1000.0
            );
            println!(
                "  rows before/after reopen: {} / {rows_after}",
                r.rows_before
            );
        }
        _ => println!(
            "  recovery time        : n/a — engine '{}' makes no durability claim",
            r.engine.name()
        ),
    }
}

/// One sample of the resource series.
#[derive(Clone, Copy)]
struct Sample {
    /// Seconds since the mixed phase began.
    t: f64,
    /// Process RSS in bytes; 0 means unreadable.
    rss: u64,
    /// Live rows the workers hold, inserts minus deletes.
    net_rows: i64,
}

struct LeakReport {
    first_mb: u64,
    last_mb: u64,
    peak_mb: u64,
    growth_mb: i64,
    limit_mb: u64,
    failure: Option<String>,
    /// Set when the gate could not legitimately be applied. Never a pass: it is
    /// printed prominently and recorded in the JSON so no reader mistakes the
    /// run for a verified-bounded-memory run.
    not_evaluated: Option<String>,
}

/// Compare RSS in the post-warmup early window against the final window. A
/// bounded workload plateaus, so sustained growth past `limit_mb` is a leak.
///
/// Two conditions make the verdict meaningless, and both are reported rather
/// than silently passed:
///
/// * RSS unreadable — the old harness returned 0 on any platform without
///   `/proc`, turning the gate into a no-op green on macOS. That is now a
///   FAILURE: a gate that cannot run must say so.
/// * The working set still growing across the measurement window — then RSS
///   growth is data, not a leak, and the gate is marked NOT EVALUATED. The
///   comparison is made over the same window as the RSS comparison, using the
///   workers' own insert-minus-delete count, so a run that begins with an empty
///   table but reaches steady state is still evaluated.
fn analyze_leak(series: &[Sample], duration_secs: u64, limit_mb: u64) -> LeakReport {
    let mb = |b: u64| b / (1024 * 1024);
    if series.iter().all(|s| s.rss == 0) {
        return LeakReport {
            first_mb: 0,
            last_mb: 0,
            peak_mb: 0,
            growth_mb: 0,
            limit_mb,
            failure: Some(format!(
                "RSS is unreadable on this platform ({}/{}) — the leak gate cannot run and is \
                 reported as a failure rather than a meaningless pass",
                std::env::consts::OS,
                std::env::consts::ARCH
            )),
            not_evaluated: None,
        };
    }
    let first_mb = series.first().map(|s| mb(s.rss)).unwrap_or(0);
    let last_mb = series.last().map(|s| mb(s.rss)).unwrap_or(0);
    let peak_mb = series.iter().map(|s| mb(s.rss)).max().unwrap_or(0);
    if series.len() < 4 {
        return LeakReport {
            first_mb,
            last_mb,
            peak_mb,
            growth_mb: 0,
            limit_mb,
            failure: None,
            not_evaluated: Some(
                "fewer than 4 resource samples were taken; the run was too short to evaluate \
                 memory growth"
                    .to_string(),
            ),
        };
    }

    let warmup = duration_secs as f64 * 0.25;
    let post: Vec<Sample> = series.iter().copied().filter(|s| s.t >= warmup).collect();
    let post = if post.len() >= 4 {
        post
    } else {
        series.to_vec()
    };
    let q = (post.len() / 4).max(1);
    let early_window = &post[..q];
    let late_window = &post[post.len() - q..];

    let early = early_window.iter().map(|s| s.rss).max().unwrap_or(0);
    let late = late_window.iter().map(|s| s.rss).max().unwrap_or(0);
    let growth_mb = mb(late.saturating_sub(early)) as i64;

    // Steady state: the live row count must not still be climbing across the
    // window, or RSS growth cannot be attributed to a leak.
    let rows_early = early_window.iter().map(|s| s.net_rows).max().unwrap_or(0);
    let rows_late = late_window.iter().map(|s| s.net_rows).max().unwrap_or(0);
    if (rows_late as f64) > (rows_early as f64) * 1.05 + 100.0 {
        return LeakReport {
            first_mb,
            last_mb,
            peak_mb,
            growth_mb,
            limit_mb,
            failure: None,
            not_evaluated: Some(format!(
                "the working set was still growing across the measurement window ({rows_early} -> \
                 {rows_late} live rows), so RSS growth here is data rather than a leak; run longer \
                 so the bounded ring reaches steady state"
            )),
        };
    }

    let failure = if growth_mb > limit_mb as i64 {
        Some(format!(
            "RSS grew {growth_mb} MB post-warmup (early {} MB -> late {} MB) at a steady \
             {rows_late} live rows, exceeding the {limit_mb} MB leak limit",
            mb(early),
            mb(late)
        ))
    } else {
        None
    };
    LeakReport {
        first_mb,
        last_mb,
        peak_mb,
        growth_mb,
        limit_mb,
        failure,
        not_evaluated: None,
    }
}
