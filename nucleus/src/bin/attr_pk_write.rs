//! attr_pk_write — split the PRIMARY KEY insert cost between its two halves.
//!
//! A bulk load into a PK table on the paged engine costs ~50.5 us/row against
//! ~12.6 us/row without the PK. Where the extra ~38 us goes has been guessed
//! at three times (batching the insert loop, cheapening the page-table hash,
//! removing two allocations per row) and all three measured as noise. This
//! measures it instead, by running the same load four ways and subtracting:
//!
//!   full   probe + index maintenance  (what a PK table really does)
//!   noprobe        index maintenance  (uniqueness check deleted)
//!   noidx  probe                      (B-tree maintenance deleted)
//!   none                              (~= a table with no PK)
//!
//!   full - noprobe = the uniqueness probe, against a real B-tree
//!   noprobe - none = B-tree maintenance
//!   and those two sum to full - none, the whole PK cost.
//!
//! `noidx` is the extra datapoint: with maintenance off the tree stays empty,
//! so `noidx - none` is the probe's FIXED overhead (locks, the `col_types`
//! clone, key serialisation) with the tree descent removed. The descent is
//! then the remainder.
//!
//! Arms are interleaved inside one process because this machine drifts far
//! enough between batches to invent a 40% win (see BENCH_VS_POSTGRES.md).
//! Each arm gets a fresh database directory, and per-round order is rotated so
//! no arm always runs on a cold or a hot cache.
//!
//! `--ab` switches to a two-arm interleaved comparison of the B-tree leaf
//! insert itself, old decode-and-rewrite versus in-place. That is the A/B that
//! justified the current implementation; both arms are correct.
//!
//! Build:
//!   cargo run --release --features server --bin attr_pk_write -- [--rows N] [--rounds N] [--no-pk] [--ab]

#![cfg(feature = "server")]

use std::sync::Arc;
use std::time::Instant;

use nucleus::bench_hooks;
use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{DiskEngine, StorageEngine, buffered_engine::BufferedDiskEngine};

#[derive(Clone, Copy, PartialEq)]
enum Arm {
    Full,
    NoProbe,
    NoIdx,
    None,
}

impl Arm {
    fn label(self) -> &'static str {
        match self {
            Arm::Full => "full   (probe + index)",
            Arm::NoProbe => "noprobe(        index)",
            Arm::NoIdx => "noidx  (probe        )",
            Arm::None => "none   (             )",
        }
    }
    fn apply(self) {
        bench_hooks::set_skip_unique_probe(matches!(self, Arm::NoProbe | Arm::None));
        bench_hooks::set_skip_index_insert(matches!(self, Arm::NoIdx | Arm::None));
    }
}

const ARMS: [Arm; 4] = [Arm::Full, Arm::NoProbe, Arm::NoIdx, Arm::None];

fn chunks(rows: usize, chunk: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut id = 1usize;
    while id <= rows {
        let end = (id + chunk - 1).min(rows);
        // Same shape as pg_compare's bulk load, so the numbers are comparable.
        let mut sql = String::from("INSERT INTO bench_orders VALUES ");
        for i in id..=end {
            if i > id {
                sql.push(',');
            }
            let user_id = (i % 1000) + 1;
            let amount = 10.0 + (i % 500) as f64;
            let status = match i % 3 {
                0 => "shipped",
                1 => "pending",
                _ => "cancelled",
            };
            sql.push_str(&format!("({i},{user_id},{amount},'{status}')"));
        }
        out.push(sql);
        id = end + 1;
    }
    out
}

/// One timed bulk load into a fresh on-disk database. Only the INSERTs are
/// timed; opening the engine and CREATE TABLE are not.
async fn run_load(dir: &std::path::Path, statements: &[String], pk: bool) -> u128 {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("create bench dir");

    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(
        DiskEngine::open(&dir.join("bench.db"), catalog.clone()).expect("open disk engine"),
    );
    // Exactly what main.rs builds for a server.
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    let ex = Executor::new(catalog, storage);

    let id_col = if pk { "id INT PRIMARY KEY" } else { "id INT" };
    ex.execute(&format!(
        "CREATE TABLE bench_orders (
             {id_col},
             user_id INT NOT NULL,
             amount  FLOAT NOT NULL,
             status  TEXT NOT NULL
         )"
    ))
    .await
    .expect("create table");

    let t = Instant::now();
    for sql in statements {
        ex.execute(sql).await.expect("insert");
    }
    let elapsed = t.elapsed().as_millis();

    drop(ex);
    let _ = std::fs::remove_dir_all(dir);
    elapsed
}

fn summary(name: &str, samples: &[u128], rows: usize) {
    let mut s: Vec<u128> = samples.to_vec();
    s.sort_unstable();
    let median = s[s.len() / 2];
    let mean = s.iter().sum::<u128>() / s.len() as u128;
    let per_row = (median as f64 * 1000.0) / rows as f64;
    let all: Vec<String> = samples.iter().map(|v| v.to_string()).collect();
    println!(
        "  {name}  median {median:>6} ms  mean {mean:>6} ms  {per_row:>6.1} us/row   [{}]",
        all.join(" ")
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut rows = 50_000usize;
    let mut rounds = 5usize;
    let mut pk = true;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--rows" => {
                i += 1;
                rows = args[i].parse().unwrap();
            }
            "--rounds" => {
                i += 1;
                rounds = args[i].parse().unwrap();
            }
            // Control: a table with no PRIMARY KEY at all. Should land on top
            // of the `none` arm — if it does not, the stubs are not removing
            // everything the missing PK removes.
            "--no-pk" => pk = false,
            _ => {}
        }
        i += 1;
    }

    let statements = chunks(rows, 500);
    let dir = std::env::temp_dir().join(format!("nucleus-attr-{}", std::process::id()));

    // --ab: interleaved A/B of the B-tree leaf insert, old vs in-place, inside
    // one process. Both arms are correct; only speed differs.
    if args.iter().any(|a| a == "--ab") {
        println!(
            "attr_pk_write --ab: {rows} rows, {rounds} rounds, leaf insert legacy vs in-place\n"
        );
        bench_hooks::set_legacy_leaf_ops(true);
        let warm = run_load(&dir, &statements, pk).await;
        println!("  warm-up: {warm} ms (discarded)\n");

        let (mut legacy, mut inplace) = (Vec::new(), Vec::new());
        for round in 0..rounds {
            // Alternate which arm goes first so neither always runs cold.
            let order = if round % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            };
            for on in order {
                bench_hooks::set_legacy_leaf_ops(on);
                let ms = run_load(&dir, &statements, pk).await;
                if on { &mut legacy } else { &mut inplace }.push(ms);
                let name = if on { "legacy " } else { "in-place" };
                println!("  round {round} {name}  {ms:>6} ms");
            }
        }
        bench_hooks::set_legacy_leaf_ops(false);

        println!();
        summary("legacy  ", &legacy, rows);
        summary("in-place", &inplace, rows);
        let med = |v: &Vec<u128>| {
            let mut s = v.clone();
            s.sort_unstable();
            s[s.len() / 2] as f64
        };
        let (l, i) = (med(&legacy), med(&inplace));
        println!(
            "\n  delta {:.0} ms ({:.1} us/row, {:.0}%)",
            l - i,
            ((l - i) * 1000.0) / rows as f64,
            (l - i) / l * 100.0
        );
        return;
    }

    println!(
        "attr_pk_write: {rows} rows, {} statements x 500, {rounds} rounds, engine=buffered-disk, pk={pk}",
        statements.len()
    );

    // Warm-up: first load pays page-cache and allocator start-up costs that
    // would otherwise land entirely on whichever arm ran first.
    Arm::Full.apply();
    let warm = run_load(&dir, &statements, pk).await;
    println!("  warm-up: {warm} ms (discarded)\n");

    let mut samples: Vec<Vec<u128>> = vec![Vec::new(); ARMS.len()];
    for round in 0..rounds {
        // Rotate arm order each round so no arm keeps the same cache position.
        for k in 0..ARMS.len() {
            let ai = (k + round) % ARMS.len();
            let arm = ARMS[ai];
            arm.apply();
            let ms = run_load(&dir, &statements, pk).await;
            samples[ai].push(ms);
            println!("  round {round} {:<24} {ms:>6} ms", arm.label());
        }
    }
    Arm::Full.apply();
    bench_hooks::set_skip_unique_probe(false);
    bench_hooks::set_skip_index_insert(false);

    println!("\nresults ({rows} rows)");
    for (ai, arm) in ARMS.iter().enumerate() {
        summary(arm.label(), &samples[ai], rows);
    }

    let med = |ai: usize| -> f64 {
        let mut s = samples[ai].clone();
        s.sort_unstable();
        s[s.len() / 2] as f64
    };
    let (full, noprobe, noidx, none) = (med(0), med(1), med(2), med(3));
    let us = |ms: f64| (ms * 1000.0) / rows as f64;

    println!("\nattribution (median, us/row)");
    println!("  whole PK cost        full - none    : {:>6.1}", us(full - none));
    println!("  uniqueness probe     full - noprobe : {:>6.1}", us(full - noprobe));
    println!("  B-tree maintenance   noprobe - none : {:>6.1}", us(noprobe - none));
    println!("  probe fixed overhead noidx - none   : {:>6.1}", us(noidx - none));
    println!(
        "  probe tree descent   remainder      : {:>6.1}",
        us((full - noprobe) - (noidx - none))
    );
}
