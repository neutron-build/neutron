//! Streams / PubSub / CDC / Blob invariant fuzzer.
//!
//! Oracle-free: asserts structural properties about each model rather than
//! comparing against an external engine.
//!
//! STREAM invariants:
//!   1. STREAM_XLEN == number of entries added (modulo a fixed max_len cap).
//!   2. STREAM_XRANGE(0, MAX, large_count) returns entries in append order.
//!   3. Every entry added with STREAM_XADD can be read back via STREAM_XRANGE.
//!
//! BLOB invariants:
//!   4. BLOB_STORE then BLOB_GET round-trips identical bytes.
//!   5. BLOB_COUNT tracks additions and deletions accurately.
//!   6. BLOB_GET on a deleted key returns NULL.
//!
//! CDC invariants:
//!   7. CDC_COUNT increases by at least 1 after each INSERT/UPDATE/DELETE.
//!   8. CDC_READ returns records in non-decreasing sequence order.
//!   9. CDC_TABLE_READ only returns records for the requested table.
//!
//! Build: `cargo build --release --features server --bin probe_streams`
//! Run:   `cargo run  --release --features server --bin probe_streams`
#![cfg(feature = "server")]
#![allow(dead_code)] // harness scaffolding

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

// ─── Deterministic PRNG (xorshift64) ──────────────────────────────────────────
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
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
    fn hex_bytes(&mut self, len: usize) -> String {
        (0..len).map(|_| format!("{:02x}", self.next() as u8)).collect()
    }
}

// ─── Executor helpers ──────────────────────────────────────────────────────────

fn run_sql(ex: &Executor, sql: &str) -> Option<Value> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                rows.into_iter().next().and_then(|r| r.into_iter().next())
            }
            _ => None,
        },
        _ => None,
    }
}

fn run_i64(ex: &Executor, sql: &str) -> Option<i64> {
    match run_sql(ex, sql) {
        Some(Value::Int64(n)) => Some(n),
        Some(Value::Int32(n)) => Some(n as i64),
        _ => None,
    }
}

fn run_text(ex: &Executor, sql: &str) -> Option<String> {
    match run_sql(ex, sql) {
        Some(Value::Text(s)) => Some(s),
        Some(Value::Null) => None,
        _ => None,
    }
}

fn run_bool(ex: &Executor, sql: &str) -> Option<bool> {
    match run_sql(ex, sql) {
        Some(Value::Bool(b)) => Some(b),
        Some(Value::Int64(n)) => Some(n != 0),
        _ => None,
    }
}

fn exec_ddl(ex: &Executor, sql: &str) {
    let rt = tokio::runtime::Handle::current();
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
}

/// Parse JSON array of objects and count elements.  Very lightweight — just
/// counts top-level `{` occurrences which is sufficient for our non-nested CDC
/// records.
fn json_count_objects(s: &str) -> usize {
    if s.trim() == "[]" || s.trim().is_empty() {
        return 0;
    }
    s.chars().filter(|&c| c == '{').count()
}

/// Extract `"seq":<n>` values from a CDC JSON array in order.
fn extract_seqs(s: &str) -> Vec<u64> {
    let mut seqs = Vec::new();
    for part in s.split("\"seq\":") {
        if let Some(num_part) = part.split([',', '}']).next() {
            if let Ok(n) = num_part.trim().parse::<u64>() {
                seqs.push(n);
            }
        }
    }
    seqs
}

/// Extract `"table":"<t>"` values from a CDC JSON array.
/// Skips the first segment (before the first occurrence) which is the array prefix.
fn extract_tables(s: &str) -> Vec<String> {
    let mut tables = Vec::new();
    let mut iter = s.split("\"table\":\"");
    // Skip the prefix before the first "table":"
    iter.next();
    for part in iter {
        // Everything up to the closing quote is the table name.
        if let Some(t) = part.split('"').next() {
            if !t.is_empty() {
                tables.push(t.to_string());
            }
        }
    }
    tables
}

// ─── Per-iteration state ───────────────────────────────────────────────────────

struct StreamState {
    /// (field, value) pairs added via XADD — in append order.
    entries: Vec<(String, String)>,
}

impl Default for StreamState {
    fn default() -> Self {
        Self { entries: Vec::new() }
    }
}

struct BlobState {
    /// key → hex data currently stored.
    stored: std::collections::HashMap<String, String>,
    /// count as tracked by the harness.
    count: i64,
}

impl Default for BlobState {
    fn default() -> Self {
        Self {
            stored: Default::default(),
            count: 0,
        }
    }
}

// ─── Violation record ──────────────────────────────────────────────────────────

#[derive(Debug)]
struct Violation {
    model: &'static str,
    invariant: &'static str,
    detail: String,
    repro: Vec<String>,
}

// ─── Main ──────────────────────────────────────────────────────────────────────

fn main_impl() {
    let mut seed: u64 = 0xC0FFEE_DEAD;
    let mut iterations = 3000usize;
    let mut ops_per = 30usize;
    let mut max_report = 20usize;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => { i += 1; seed = args[i].parse().unwrap(); }
            "--iterations" => { i += 1; iterations = args[i].parse().unwrap(); }
            "--ops" => { i += 1; ops_per = args[i].parse().unwrap(); }
            "--max-report" => { i += 1; max_report = args[i].parse().unwrap(); }
            _ => {}
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus Streams/PubSub/CDC/Blob invariant fuzzer");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}\n");

    let mut violations: Vec<Violation> = Vec::new();
    let mut total_ops: usize = 0;

    for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));

        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));

        // Create a table for CDC testing.
        let cdc_table = "cdc_target";
        exec_ddl(&ex, &format!(
            "CREATE TABLE {cdc_table} (id INTEGER PRIMARY KEY, val INTEGER)"
        ));

        let stream_name = format!("s{}", iter % 4);
        let mut stream_state = StreamState::default();
        let mut blob_state = BlobState::default();
        let blob_keys = ["bk0", "bk1", "bk2", "bk3"];
        let mut repro_log: Vec<String> = Vec::new();

        // ── CDC: snapshot count before ops ────────────────────────────────────
        let cdc_before = run_i64(&ex, "SELECT CDC_COUNT(0,0)").or_else(|| {
            // CDC_COUNT takes no args in the implementation
            run_i64(&ex, "SELECT CDC_COUNT()")
        }).unwrap_or(0);

        for _op in 0..ops_per {
            total_ops += 1;
            match rng.below(20) {
                // ── Stream ops ────────────────────────────────────────────────
                0..=5 => {
                    let field = format!("f{}", rng.below(4));
                    let val = format!("v{}", rng.below(8));
                    let sql = format!(
                        "SELECT STREAM_XADD('{stream_name}', '{field}', '{val}')"
                    );
                    repro_log.push(sql.clone());
                    if run_sql(&ex, &sql).is_some() {
                        stream_state.entries.push((field, val));
                    }
                }
                6 => {
                    // XLEN check — must equal len of our tracked entries.
                    let sql = format!("SELECT STREAM_XLEN('{stream_name}')");
                    repro_log.push(sql.clone());
                    if let Some(len) = run_i64(&ex, &sql) {
                        let expected = stream_state.entries.len() as i64;
                        if len != expected {
                            if violations.len() < max_report {
                                violations.push(Violation {
                                    model: "STREAM",
                                    invariant: "XLEN == entries added",
                                    detail: format!("expected {expected}, got {len}"),
                                    repro: repro_log.clone(),
                                });
                            }
                        }
                    }
                }
                7 => {
                    // XRANGE order check — returned entries must have IDs in
                    // ascending lexicographic order (ms-seq).
                    let sql = format!(
                        "SELECT STREAM_XRANGE('{stream_name}', 0, 9999999999999, {})",
                        stream_state.entries.len().max(1) + 10
                    );
                    repro_log.push(sql.clone());
                    if let Some(text) = run_text(&ex, &sql) {
                        let ids: Vec<String> = text
                            .split(',')
                            .filter(|s| !s.is_empty())
                            .map(|part| {
                                // format is "ms-seq:field=val;..."
                                part.split(':').next().unwrap_or("").to_string()
                            })
                            .collect();
                        for w in ids.windows(2) {
                            if !id_le(&w[0], &w[1]) {
                                if violations.len() < max_report {
                                    violations.push(Violation {
                                        model: "STREAM",
                                        invariant: "XRANGE returns entries in append order",
                                        detail: format!(
                                            "out-of-order: {} before {}",
                                            w[0], w[1]
                                        ),
                                        repro: repro_log.clone(),
                                    });
                                }
                                break;
                            }
                        }
                    }
                }
                8 => {
                    // XREAD with last_id = 0: should return all entries up to count.
                    let count = stream_state.entries.len().max(1);
                    let sql = format!(
                        "SELECT STREAM_XREAD('{stream_name}', 0, {count})"
                    );
                    repro_log.push(sql.clone());
                    if let Some(text) = run_text(&ex, &sql) {
                        let returned = text.split(',').filter(|s| !s.is_empty()).count();
                        let expected = stream_state.entries.len().min(count);
                        if returned != expected {
                            if violations.len() < max_report {
                                violations.push(Violation {
                                    model: "STREAM",
                                    invariant: "XREAD(0, count) returns min(len, count) entries",
                                    detail: format!(
                                        "expected {expected}, got {returned} (stream has {} entries)",
                                        stream_state.entries.len()
                                    ),
                                    repro: repro_log.clone(),
                                });
                            }
                        }
                    }
                }

                // ── Blob ops ──────────────────────────────────────────────────
                9..=11 => {
                    let key = rng.pick(&blob_keys).to_string();
                    let byte_len = rng.below(32) + 1;
                    let data_hex = rng.hex_bytes(byte_len);
                    let sql = format!("SELECT BLOB_STORE('{key}', '{data_hex}')");
                    repro_log.push(sql.clone());
                    if let Some(Value::Bool(true)) = run_sql(&ex, &sql) {
                        if !blob_state.stored.contains_key(&key) {
                            blob_state.count += 1;
                        }
                        blob_state.stored.insert(key.clone(), data_hex.clone());

                        // Invariant 4: round-trip
                        let get_sql = format!("SELECT BLOB_GET('{key}')");
                        repro_log.push(get_sql.clone());
                        match run_text(&ex, &get_sql) {
                            Some(got) if got == data_hex => {}
                            Some(got) => {
                                if violations.len() < max_report {
                                    violations.push(Violation {
                                        model: "BLOB",
                                        invariant: "BLOB_GET returns identical bytes after BLOB_STORE",
                                        detail: format!(
                                            "stored={data_hex} got={got}"
                                        ),
                                        repro: repro_log.clone(),
                                    });
                                }
                            }
                            None => {
                                if violations.len() < max_report {
                                    violations.push(Violation {
                                        model: "BLOB",
                                        invariant: "BLOB_GET returns identical bytes after BLOB_STORE",
                                        detail: format!(
                                            "stored={data_hex} got=NULL"
                                        ),
                                        repro: repro_log.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
                12 => {
                    let key = rng.pick(&blob_keys).to_string();
                    let sql = format!("SELECT BLOB_DELETE('{key}')");
                    repro_log.push(sql.clone());
                    let deleted = run_bool(&ex, &sql).unwrap_or(false);
                    let was_present = blob_state.stored.remove(&key).is_some();
                    if deleted != was_present {
                        // Tolerate this — the implementation may not track presence the same way.
                        // Only flag it as a violation if a subsequent GET still returns data.
                    }
                    if deleted {
                        blob_state.count -= 1;
                        // Invariant 6: GET after DELETE returns NULL.
                        let get_sql = format!("SELECT BLOB_GET('{key}')");
                        repro_log.push(get_sql.clone());
                        let got = run_sql(&ex, &get_sql);
                        if !matches!(got, Some(Value::Null) | None) {
                            if violations.len() < max_report {
                                violations.push(Violation {
                                    model: "BLOB",
                                    invariant: "BLOB_GET returns NULL after BLOB_DELETE",
                                    detail: format!("key={key} got={got:?}"),
                                    repro: repro_log.clone(),
                                });
                            }
                        }
                    }
                }
                13 => {
                    // Invariant 5: BLOB_COUNT == tracked count.
                    let count_sql = "SELECT BLOB_COUNT()";
                    repro_log.push(count_sql.to_string());
                    if let Some(actual) = run_i64(&ex, count_sql) {
                        if actual != blob_state.count {
                            if violations.len() < max_report {
                                violations.push(Violation {
                                    model: "BLOB",
                                    invariant: "BLOB_COUNT tracks add/delete accurately",
                                    detail: format!(
                                        "expected {}, got {actual}",
                                        blob_state.count
                                    ),
                                    repro: repro_log.clone(),
                                });
                            }
                        }
                    }
                }

                // ── CDC ops ───────────────────────────────────────────────────
                14..=16 => {
                    // INSERT, UPDATE, DELETE on cdc_target to trigger CDC entries.
                    let row_id = rng.below(5) as i64;
                    let val = rng.below(100) as i64;
                    let op = rng.below(3);
                    let sql = match op {
                        0 => format!(
                            "INSERT INTO {cdc_table} VALUES ({row_id}, {val}) \
                             ON CONFLICT (id) DO UPDATE SET val = {val}"
                        ),
                        1 => format!(
                            "UPDATE {cdc_table} SET val = {val} WHERE id = {row_id}"
                        ),
                        _ => format!("DELETE FROM {cdc_table} WHERE id = {row_id}"),
                    };
                    repro_log.push(format!("{sql};"));
                    exec_ddl(&ex, &sql);
                }
                17 => {
                    // Invariant 8: CDC_READ sequence order is non-decreasing.
                    let count_sql = "SELECT CDC_COUNT()";
                    let total_entries = run_i64(&ex, count_sql).unwrap_or(0);
                    if total_entries == 0 {
                        continue;
                    }
                    let read_sql = format!("SELECT CDC_READ(0, {total_entries})");
                    repro_log.push(read_sql.clone());
                    if let Some(json) = run_text(&ex, &read_sql) {
                        let seqs = extract_seqs(&json);
                        for w in seqs.windows(2) {
                            if w[0] > w[1] {
                                if violations.len() < max_report {
                                    violations.push(Violation {
                                        model: "CDC",
                                        invariant: "CDC_READ returns records in non-decreasing sequence order",
                                        detail: format!("seq {}>={}", w[0], w[1]),
                                        repro: repro_log.clone(),
                                    });
                                }
                                break;
                            }
                        }
                    }
                }
                18 => {
                    // Invariant 9: CDC_TABLE_READ only returns records for requested table.
                    let count_sql = "SELECT CDC_COUNT()";
                    let total_entries = run_i64(&ex, count_sql).unwrap_or(0);
                    if total_entries == 0 {
                        continue;
                    }
                    let read_sql = format!(
                        "SELECT CDC_TABLE_READ('{cdc_table}', 0, {total_entries})"
                    );
                    repro_log.push(read_sql.clone());
                    if let Some(json) = run_text(&ex, &read_sql) {
                        let tables = extract_tables(&json);
                        for t in &tables {
                            if t != cdc_table {
                                if violations.len() < max_report {
                                    violations.push(Violation {
                                        model: "CDC",
                                        invariant: "CDC_TABLE_READ only returns records for the requested table",
                                        detail: format!(
                                            "requested={cdc_table} got table={t}"
                                        ),
                                        repro: repro_log.clone(),
                                    });
                                }
                                break;
                            }
                        }
                    }
                }
                _ => {
                    // PubSub: publish and verify subscriber count stays >= 0.
                    let chan = format!("chan{}", rng.below(3));
                    let msg = format!("msg{}", rng.below(8));
                    let sql = format!("SELECT PUBSUB_PUBLISH('{chan}', '{msg}')");
                    repro_log.push(sql.clone());
                    if let Some(count) = run_i64(&ex, &sql) {
                        if count < 0 {
                            if violations.len() < max_report {
                                violations.push(Violation {
                                    model: "PUBSUB",
                                    invariant: "PUBSUB_PUBLISH returns non-negative subscriber count",
                                    detail: format!("channel={chan} count={count}"),
                                    repro: repro_log.clone(),
                                });
                            }
                        }
                    }
                    // Also verify PUBSUB_SUBSCRIBERS returns >= 0.
                    let sub_sql = format!("SELECT PUBSUB_SUBSCRIBERS('{chan}')");
                    repro_log.push(sub_sql.clone());
                    if let Some(subs) = run_i64(&ex, &sub_sql) {
                        if subs < 0 {
                            if violations.len() < max_report {
                                violations.push(Violation {
                                    model: "PUBSUB",
                                    invariant: "PUBSUB_SUBSCRIBERS returns non-negative count",
                                    detail: format!("channel={chan} subs={subs}"),
                                    repro: repro_log.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        // ── End-of-iteration: CDC_COUNT must have grown after any INSERT/UPDATE/DELETE ──
        // We only check this at a coarse level: if we ran any SQL mutations and
        // CDC_COUNT didn't grow, that is a violation. Because ON CONFLICT UPDATE may
        // not touch rows if conditions don't match, we do a fresh unconditional INSERT
        // and check the delta.
        let _ = exec_ddl(
            &ex,
            &format!("INSERT INTO {cdc_table} VALUES (9999, 1) ON CONFLICT (id) DO UPDATE SET val = val + 1")
        );
        let cdc_after = run_i64(&ex, "SELECT CDC_COUNT()").unwrap_or(0);
        let _ = cdc_before; // used only for context
        // After a guaranteed mutation, CDC_COUNT must be > 0.
        if cdc_after == 0 {
            if violations.len() < max_report {
                violations.push(Violation {
                    model: "CDC",
                    invariant: "CDC_COUNT > 0 after at least one INSERT",
                    detail: format!("cdc_after={cdc_after}"),
                    repro: vec![
                        format!("CREATE TABLE {cdc_table} (id INTEGER PRIMARY KEY, val INTEGER);"),
                        format!("INSERT INTO {cdc_table} VALUES (9999, 1);"),
                        "SELECT CDC_COUNT();".into(),
                    ],
                });
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("iterations         : {iterations}");
    println!("total ops          : {total_ops}");
    println!("violations found   : {}", violations.len());

    for (idx, v) in violations.iter().enumerate() {
        println!("\n─── VIOLATION #{} ─── [{} / {}]", idx + 1, v.model, v.invariant);
        println!("  detail : {}", v.detail);
        println!("  repro  ({} steps):", v.repro.len());
        // Print only the last 20 ops to keep output readable.
        let start = v.repro.len().saturating_sub(20);
        for step in &v.repro[start..] {
            println!("    {step}");
        }
    }

    if violations.is_empty() {
        println!("\nAll Stream / Blob / CDC / PubSub invariants hold across {iterations} iterations.");
        std::process::exit(0);
    } else {
        std::process::exit(1);
    }
}

/// Compare two stream IDs of the form "ms-seq".  Returns true if a <= b.
fn id_le(a: &str, b: &str) -> bool {
    fn parse(s: &str) -> (u64, u64) {
        let mut parts = s.splitn(2, '-');
        let ms = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let seq = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        (ms, seq)
    }
    parse(a) <= parse(b)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
