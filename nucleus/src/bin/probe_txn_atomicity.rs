//! Is a user transaction atomic across a crash on the paged engine the server runs?
//!
//! `main.rs` builds `BufferedDiskEngine::new(DiskEngine)`. Client writes are
//! buffered in memory for the life of the transaction, and COMMIT calls
//! `apply_buffer`, which replays every buffered op into pages one at a time,
//! and only THEN calls `make_durable`. There is no atomicity marker around the
//! replay: the page WAL is pure physical redo at txn 0 (`log_page_write(0, ..)`
//! at every site) and recovery never reads COMMIT/ABORT, so nothing tells a
//! restart "these page images belong to a transaction that never committed".
//!
//! Whether that is reachable depends on whether any of those pages reach the OS
//! before the crash. `apply_buffer` itself never syncs — but the buffer pool
//! evicts. `BufferPool::get_free_frame` WAL-logs a dirty victim page and calls
//! `disk.write_page` inline, so a transaction that dirties more pages than the
//! pool holds pushes its own uncommitted pages out to the file, mid-apply, with
//! no way to take them back.
//!
//! This probe measures that. A child process updates every row in a table to a
//! new generation inside one transaction, with a pool far smaller than the
//! table, and the parent SIGKILLs it at a random instant. On reopen, the
//! recovery invariant for an atomic engine is:
//!
//!   every surviving row carries the SAME generation
//!
//! A mix of generations is a torn transaction: a COMMIT that was never acked
//! left half its work durable. The parent also checks the durability direction
//! — a generation the child printed as committed must still be there.
//!
//! Run: cargo run --release --features server --bin probe_txn_atomicity
//!      cargo run --release --features server --bin probe_txn_atomicity -- --rounds 20 --rows 20000

#![cfg(feature = "server")]
#![allow(clippy::all)] // internal probe harness
#![allow(dead_code)] // Outcome's payloads are reported through Debug

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{DiskEngine, StorageEngine, buffered_engine::BufferedDiskEngine};
use nucleus::types::Value;

/// Deterministic PRNG so a failing round can be replayed from its seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

/// The open sequence `main.rs` performs, with a caller-chosen pool size:
/// restore the persisted catalog, open the paged engine, re-register the
/// restored tables so the engine knows them, then build a persistence-aware
/// executor over `BufferedDiskEngine`. Skipping the catalog steps makes every
/// table vanish on reopen, which looks exactly like data loss and is not.
async fn open(dir: &Path, pool_frames: usize) -> Executor {
    let catalog = Arc::new(Catalog::new());
    let catalog_path = dir.join("catalog.json");
    let _ = nucleus::storage::persistence::CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await;

    let disk = Arc::new(
        DiskEngine::open_with_pool_size(&dir.join("nucleus.db"), catalog.clone(), pool_frames)
            .expect("open disk engine"),
    );
    for table_name in catalog.table_names().await {
        let _ = disk.create_table(&table_name).await;
    }
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir))
}

const PAD: &str = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

// ─── Child: seeds once, then rewrites the whole table per transaction ─────────

fn child_main(dir: &str, rows: usize, pool_frames: usize, gens: u64) -> ! {
    std::panic::set_hook(Box::new(|_| {}));
    let dir = PathBuf::from(dir);
    let rt = tokio::runtime::Runtime::new().expect("child rt");
    let ex = rt.block_on(open(&dir, pool_frames));

    // Seed: every row at generation 0, committed and durable before any kill
    // window opens. Autocommit, so each batch is its own durable transaction.
    rt.block_on(async {
        ex.execute("CREATE TABLE t (id INT PRIMARY KEY, g INT NOT NULL, pad TEXT NOT NULL)")
            .await
            .expect("create");
        let mut id = 1usize;
        while id <= rows {
            let end = (id + 199).min(rows);
            let mut sql = String::from("INSERT INTO t VALUES ");
            for i in id..=end {
                if i > id {
                    sql.push(',');
                }
                sql.push_str(&format!("({i},0,'{PAD}')"));
            }
            ex.execute(&sql).await.expect("insert");
            id = end + 1;
        }
    });
    println!("SEEDED");
    let _ = std::io::stdout().flush();

    // Each generation is one transaction that rewrites every row. One UPDATE
    // statement, so the whole table is a single buffered op and the entire
    // page rewrite happens inside one `apply_buffer`.
    for gnum in 1..=gens {
        rt.block_on(async {
            ex.execute("BEGIN").await.expect("begin");
            ex.execute(&format!("UPDATE t SET g = {gnum}"))
                .await
                .expect("update");
            ex.execute("COMMIT").await.expect("commit");
        });
        println!("COMMITTED {gnum}");
        let _ = std::io::stdout().flush();
    }
    std::process::exit(0);
}

// ─── Parent ───────────────────────────────────────────────────────────────────

struct TmpDir(PathBuf);
impl TmpDir {
    fn new(tag: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!("nucleus_txnatom_{tag}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("mkdir temp");
        TmpDir(p)
    }
}
impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug)]
enum Outcome {
    /// Every surviving row carries one generation. The atomic answer.
    Clean { gnum: i64, rows: usize },
    /// Two or more generations coexist: a transaction was half-applied.
    Torn {
        counts: BTreeMap<i64, usize>,
        rows: usize,
    },
    /// A generation the child announced as committed is not what came back.
    LostCommitted { printed: i64, recovered: i64 },
    /// Row count changed. An UPDATE must not create or destroy rows.
    RowCount { got: usize, want: usize },
    ReopenError(String),
    ReadError(String),
}

fn verify(dir: &Path, pool_frames: usize, rows: usize, last_printed: i64) -> Outcome {
    let rt = tokio::runtime::Runtime::new().expect("verify rt");
    let ex = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(open(dir, pool_frames))
    }));
    let ex = match ex {
        Ok(e) => e,
        Err(_) => return Outcome::ReopenError("panic reopening engine".into()),
    };
    let res = match rt.block_on(ex.execute("SELECT id, g FROM t ORDER BY id")) {
        Ok(r) => r,
        Err(e) => return Outcome::ReadError(format!("{e:?}")),
    };

    let mut counts: BTreeMap<i64, usize> = BTreeMap::new();
    let mut n = 0usize;
    for r in res {
        if let ExecResult::Select { rows: got, .. } = r {
            for row in got {
                let g = match row[1] {
                    Value::Int64(v) => v,
                    Value::Int32(v) => v as i64,
                    _ => return Outcome::ReadError("g not an integer".into()),
                };
                *counts.entry(g).or_insert(0) += 1;
                n += 1;
            }
        }
    }

    if counts.len() > 1 {
        return Outcome::Torn { counts, rows: n };
    }
    if n != rows {
        return Outcome::RowCount {
            got: n,
            want: rows,
        };
    }
    let gnum = counts.keys().copied().next().unwrap_or(-1);
    if gnum < last_printed {
        return Outcome::LostCommitted {
            printed: last_printed,
            recovered: gnum,
        };
    }
    Outcome::Clean { gnum, rows: n }
}

fn main() {
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() >= 6 && raw[1] == "--child" {
        child_main(
            &raw[2],
            raw[3].parse().unwrap(),
            raw[4].parse().unwrap(),
            raw[5].parse().unwrap(),
        );
    }

    let mut seed = 0x5EED_A701u64;
    let mut rounds = 12usize;
    let mut rows = 12_000usize;
    let mut pool_frames = 32usize;
    let gens = 40u64;
    let mut i = 1;
    while i < raw.len() {
        match raw[i].as_str() {
            "--seed" => {
                i += 1;
                seed = raw[i].parse().unwrap();
            }
            "--rounds" => {
                i += 1;
                rounds = raw[i].parse().unwrap();
            }
            "--rows" => {
                i += 1;
                rows = raw[i].parse().unwrap();
            }
            "--pool" => {
                i += 1;
                pool_frames = raw[i].parse().unwrap();
            }
            other => {
                eprintln!("unknown arg {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    let exe = std::env::current_exe().expect("current exe");
    let mut rng = Rng(seed);
    let mut torn = 0usize;
    let mut clean = 0usize;
    let mut skipped = 0usize;
    let mut other = 0usize;

    // A frame is one PAGE_SIZE page (16 KB). The production default is
    // `buffer_pool_size_mb = 32`, i.e. 2048 frames — pass `--pool 2048` to
    // measure the shipped configuration rather than a deliberately small one.
    let pool_kb = pool_frames * 16;
    println!(
        "probe_txn_atomicity: rounds={rounds} rows={rows} pool={pool_frames} frames ({pool_kb} KB) seed={seed}"
    );
    println!(
        "  table is ~{} KB of rows against a {pool_kb} KB pool, so one whole-table UPDATE",
        rows * 200 / 1024
    );
    println!("  cannot be applied without evicting its own uncommitted pages to disk.\n");

    for round in 0..rounds {
        let tmp = TmpDir::new(&format!("{}_{round}", std::process::id()));
        let mut child = Command::new(&exe)
            .arg("--child")
            .arg(tmp.0.to_str().unwrap())
            .arg(rows.to_string())
            .arg(pool_frames.to_string())
            .arg(gens.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child");

        // Collect the child's progress markers on a reader thread; the parent
        // needs to know the last generation it announced as committed.
        let lines: Arc<Mutex<Vec<(Instant, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&lines);
        let stdout = child.stdout.take().expect("child stdout");
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                sink.lock().unwrap().push((Instant::now(), line));
            }
        });

        let last = |lines: &Arc<Mutex<Vec<(Instant, String)>>>| -> i64 {
            lines
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(_, l)| l.strip_prefix("COMMITTED ")?.parse::<i64>().ok())
                .max()
                .unwrap_or(0)
        };
        let count = |lines: &Arc<Mutex<Vec<(Instant, String)>>>| lines.lock().unwrap().len();

        // Wait for two committed generations, so the cycle time is known, then
        // kill at a uniformly random point inside the next cycle. That is what
        // lands the SIGKILL inside `apply_buffer` rather than always at a
        // quiescent boundary.
        let start = Instant::now();
        while count(&lines) < 3 && start.elapsed() < Duration::from_secs(180) {
            std::thread::sleep(Duration::from_millis(2));
            if let Ok(Some(_)) = child.try_wait() {
                break;
            }
        }
        let cycle_ms = {
            let l = lines.lock().unwrap();
            let commits: Vec<Instant> = l
                .iter()
                .filter(|(_, s)| s.starts_with("COMMITTED"))
                .map(|(t, _)| *t)
                .collect();
            if commits.len() >= 2 {
                commits
                    .last()
                    .unwrap()
                    .duration_since(commits[0])
                    .as_millis() as u64
                    / (commits.len() as u64 - 1)
            } else {
                0
            }
        };
        if cycle_ms == 0 {
            let _ = child.kill();
            let _ = child.wait();
            skipped += 1;
            println!("round {round:>2}: child never committed twice — skipped");
            continue;
        }

        std::thread::sleep(Duration::from_millis(rng.below(cycle_ms.max(1))));
        let printed = last(&lines);
        let _ = child.kill(); // SIGKILL on Unix: no unwinding, no Drop, no flush
        let _ = child.wait();

        match verify(&tmp.0, pool_frames, rows, printed) {
            Outcome::Clean { gnum, rows: n } => {
                clean += 1;
                println!(
                    "round {round:>2}: clean   — all {n} rows at g={gnum} (child had acked g={printed}, cycle {cycle_ms} ms)"
                );
            }
            Outcome::Torn { counts, rows: n } => {
                torn += 1;
                let detail: Vec<String> =
                    counts.iter().map(|(g, c)| format!("g={g}:{c}")).collect();
                println!(
                    "round {round:>2}: TORN    — {n} rows across {} generations [{}] (acked g={printed})",
                    counts.len(),
                    detail.join(" ")
                );
            }
            o => {
                other += 1;
                println!("round {round:>2}: {o:?}");
            }
        }
    }

    println!("\n─── summary ───");
    println!("  clean (atomic)      : {clean}");
    println!("  TORN (partial txn)  : {torn}");
    println!("  other findings      : {other}");
    println!("  skipped             : {skipped}");
    if torn > 0 {
        println!(
            "\nA transaction the server never acknowledged is durable in part. The page\nWAL carries these images at txn 0 with no COMMIT record, so recovery replays\nthem unconditionally — there is nothing in the log that could undo them."
        );
        std::process::exit(1);
    }
}
