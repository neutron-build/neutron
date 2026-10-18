//! `e0-bench`: the E0 measurement driver (card X-0 Work 7).
//!
//! Measures the spike's B+tree against SQLite in the same process: bulk
//! insert, random point gets, 100-key range scans, random updates, and
//! 8-thread point gets, over 1M 8-byte big-endian random u64 keys with
//! 100-byte values, in two configurations:
//!
//!  * (i) in memory: `MemStore`, pool ≥ data;
//!  * (ii) constrained: `FileStore`, pool = 1/4 of the data pages. The OS
//!    page cache is warm, so (ii) measures **eviction cost**, not disk.
//!
//! SQLite is the E0 baseline; the C build is confined to the benchmark
//! feature (`sqlite`, off by default — binaries cannot use
//! dev-dependencies). Output: a table of ns/op for both sides plus the
//! ratio (SQLite time / ours: >1 means we are faster), and optional JSON.
//!
//! No panics on any path: failures print to stderr and exit nonzero.

#[cfg(not(feature = "sqlite"))]
fn main() {
    eprintln!(
        "e0-bench: build with --features sqlite (SQLite is the E0 baseline; \
         the C build is confined to the benchmark feature)"
    );
    std::process::exit(2);
}

#[cfg(feature = "sqlite")]
fn main() {
    if let Err(e) = e0_bench::run() {
        eprintln!("e0-bench: {e}");
        std::process::exit(1);
    }
}

#[cfg(feature = "sqlite")]
mod e0_bench {
    use e0_btree::{BTree, FileStore, MemStore, PageStore, StoreError};
    use rusqlite::Connection;
    use std::fmt::Write as _;
    use std::hint::black_box;
    use std::thread;
    use std::time::Instant;

    /// Default key count (card X-0 Work 7: 1M).
    const DEFAULT_KEYS: usize = 1_000_000;
    /// Point gets per run.
    const POINT_OPS: usize = 1_000_000;
    /// Range scans per run.
    const RANGE_OPS: usize = 100_000;
    /// Keys per range scan window (by density).
    const RANGE_KEYS: usize = 100;
    /// Updates per run.
    const UPDATE_OPS: usize = 500_000;
    /// Threads for W-mt.
    const MT_THREADS: usize = 8;
    /// Point gets per thread in W-mt.
    const MT_OPS_PER_THREAD: usize = 500_000;
    /// Value size for every key.
    const VALUE_LEN: usize = 100;

    /// Deterministic splitmix64 stream (no external RNG in the spike).
    struct Sm(u64);

    impl Sm {
        /// New stream from a seed.
        fn new(seed: u64) -> Self {
            Sm(seed)
        }
        /// Next value.
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }
    }

    /// The 100-byte value for a key: deterministic bytes from the key.
    fn value_for(key: [u8; 8], salt: u64) -> Vec<u8> {
        let mut s = Sm::new(u64::from_be_bytes(key) ^ salt);
        (0..VALUE_LEN).map(|_| s.next() as u8).collect()
    }

    /// Per-run measurements of one side. Times are ns/op; `mt_ops` is
    /// aggregate ops/s over 8 threads.
    #[derive(Default, Clone, Copy)]
    struct Work {
        /// ns/op bulk insert (build).
        insert_ns: f64,
        /// ns/op point get.
        point_ns: f64,
        /// ns/op per ~100-key range scan.
        range_ns: f64,
        /// ns/op update.
        update_ns: f64,
        /// ops/s, 8-thread point gets.
        mt_ops: f64,
    }

    fn median3(v: [f64; 3]) -> f64 {
        let mut s = v;
        s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        s[1]
    }

    fn pick<'a>(keys: &'a [[u8; 8]], rng: &mut Sm) -> Result<&'a [u8; 8], String> {
        keys.get((rng.next() as usize) % keys.len())
            .ok_or_else(|| "no keys".to_string())
    }

    fn range_span(keys: usize) -> u64 {
        let density = keys as f64 / (u64::MAX as f64);
        ((RANGE_KEYS as f64 / density).ceil() as u64).max(1)
    }

    // ---- our side -------------------------------------------------------

    /// Fresh tree over `store`, W-insert timed, then the other workloads.
    fn ours_full<S: PageStore>(store: S, capacity: usize, keys: &[[u8; 8]]) -> Result<Work, String> {
        let tree = BTree::new(store, capacity).map_err(|e| e.to_string())?;
        let b0 = Instant::now();
        for k in keys {
            let v = value_for(*k, 1);
            tree.insert(k, &v).map_err(|e| e.to_string())?;
        }
        let insert_ns = b0.elapsed().as_secs_f64() * 1e9 / keys.len() as f64;
        let mut w = ours_workloads(&tree, keys)?;
        w.insert_ns = insert_ns;
        Ok(w)
    }

    fn ours_workloads<S: PageStore>(tree: &BTree<S>, keys: &[[u8; 8]]) -> Result<Work, String> {
        let mut rng = Sm::new(0xBE4C_0001);
        // W-point: 1M random point gets over existing keys.
        let t0 = Instant::now();
        let mut found = 0usize;
        for _ in 0..POINT_OPS {
            let k = pick(keys, &mut rng)?;
            if tree.get(k).map_err(|e| e.to_string())?.is_some() {
                found += 1;
            }
        }
        let point = t0.elapsed();
        if found != POINT_OPS {
            return Err(format!("point gets found only {found}/{POINT_OPS}"));
        }
        // W-range: 100k scans of ~100 keys.
        let span = range_span(keys.len());
        let t0 = Instant::now();
        let mut scanned = 0usize;
        for _ in 0..RANGE_OPS {
            let base = rng.next() % (u64::MAX - span);
            let lo = base.to_be_bytes();
            let hi = base.wrapping_add(span).to_be_bytes();
            let iter = tree.range(&lo, &hi).map_err(|e| e.to_string())?;
            for entry in iter {
                let (k, v) = entry.map_err(|e| e.to_string())?;
                black_box((k, v));
                scanned += 1;
            }
        }
        let range = t0.elapsed();
        if scanned + RANGE_KEYS * 10 < RANGE_OPS * RANGE_KEYS {
            return Err(format!("range scans returned only {scanned} keys"));
        }
        // W-update: 500k upserts with different values.
        let t0 = Instant::now();
        for _ in 0..UPDATE_OPS {
            let k = pick(keys, &mut rng)?;
            let v = value_for(*k, 2);
            tree.insert(k, &v).map_err(|e| e.to_string())?;
        }
        let update = t0.elapsed();
        // W-mt: 8 threads of point gets (scoped: the tree is borrowed).
        let t0 = Instant::now();
        let total = thread::scope(|s| -> Result<usize, String> {
            let mut hs = Vec::new();
            for ti in 0..MT_THREADS {
                hs.push(s.spawn(move || -> Result<usize, String> {
                    let mut rng = Sm::new(0xBE4C_1000 + ti as u64);
                    let mut found = 0usize;
                    for _ in 0..MT_OPS_PER_THREAD {
                        let k = pick(keys, &mut rng)?;
                        if tree.get(k).map_err(|e| e.to_string())?.is_some() {
                            found += 1;
                        }
                    }
                    Ok(found)
                }));
            }
            let mut total = 0usize;
            for h in hs {
                let v = h.join().map_err(|_| "mt thread panicked".to_string())??;
                total += v;
            }
            Ok(total)
        })?;
        let mt = t0.elapsed();
        if total != MT_THREADS * MT_OPS_PER_THREAD {
            return Err(format!("mt gets found only {total}"));
        }
        Ok(Work {
            insert_ns: 0.0,
            point_ns: point.as_secs_f64() * 1e9 / POINT_OPS as f64,
            range_ns: range.as_secs_f64() * 1e9 / RANGE_OPS as f64,
            update_ns: update.as_secs_f64() * 1e9 / UPDATE_OPS as f64,
            mt_ops: (MT_THREADS * MT_OPS_PER_THREAD) as f64 / mt.as_secs_f64(),
        })
    }

    // ---- SQLite side ----------------------------------------------------

    const URI: &str = "file:e0benchmt?mode=memory&cache=shared";

    fn sqlite_pragmas(conn: &Connection) -> Result<(), String> {
        let _ = conn
            .query_row("PRAGMA journal_mode=OFF", [], |_| Ok(()))
            .map_err(|e| e.to_string())?;
        conn.pragma_update(None, "synchronous", "OFF")
            .map_err(|e| e.to_string())?;
        conn.pragma_update(None, "cache_size", -2_000_000)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn sqlite_full(keys: &[[u8; 8]]) -> Result<Work, String> {
        let mut conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
        sqlite_pragmas(&conn)?;
        conn.execute_batch("CREATE TABLE t(k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID")
            .map_err(|e| e.to_string())?;
        // W-insert: one transaction of prepared upserts, same key order.
        let t0 = Instant::now();
        {
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            {
                let mut stmt = tx
                    .prepare(
                        "INSERT INTO t(k, v) VALUES(?1, ?2) \
                         ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                    )
                    .map_err(|e| e.to_string())?;
                for k in keys {
                    let v = value_for(*k, 1);
                    stmt.execute(rusqlite::params![&k[..], &v[..]])
                        .map_err(|e| e.to_string())?;
                }
            }
            tx.commit().map_err(|e| e.to_string())?;
        }
        let insert = t0.elapsed();
        // W-point
        let mut rng = Sm::new(0xBE4C_0001);
        let t0 = Instant::now();
        let mut found = 0usize;
        {
            let mut stmt = conn
                .prepare("SELECT v FROM t WHERE k = ?1")
                .map_err(|e| e.to_string())?;
            for _ in 0..POINT_OPS {
                let k = pick(keys, &mut rng)?;
                let r: Result<Vec<u8>, _> =
                    stmt.query_row(rusqlite::params![&k[..]], |row| row.get(0));
                match r {
                    Ok(v) => {
                        black_box(v);
                        found += 1;
                    }
                    Err(rusqlite::Error::QueryReturnedNoRows) => {}
                    Err(e) => return Err(e.to_string()),
                }
            }
        }
        let point = t0.elapsed();
        if found != POINT_OPS {
            return Err(format!("sqlite point found only {found}"));
        }
        // W-range
        let span = range_span(keys.len());
        let t0 = Instant::now();
        let mut scanned = 0usize;
        {
            let mut stmt = conn
                .prepare("SELECT v FROM t WHERE k >= ?1 AND k < ?2")
                .map_err(|e| e.to_string())?;
            for _ in 0..RANGE_OPS {
                let base = rng.next() % (u64::MAX - span);
                let lo = base.to_be_bytes();
                let hi = base.wrapping_add(span).to_be_bytes();
                let mut rows = stmt
                    .query(rusqlite::params![&lo[..], &hi[..]])
                    .map_err(|e| e.to_string())?;
                while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                    let v: Vec<u8> = row.get(0).map_err(|e| e.to_string())?;
                    black_box(v);
                    scanned += 1;
                }
            }
        }
        let range = t0.elapsed();
        if scanned < RANGE_OPS {
            return Err(format!("sqlite range scans returned only {scanned} keys"));
        }
        // W-update (one transaction, same as ours would be in a txn engine)
        let t0 = Instant::now();
        {
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            {
                let mut stmt = tx
                    .prepare(
                        "INSERT INTO t(k, v) VALUES(?1, ?2) \
                         ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                    )
                    .map_err(|e| e.to_string())?;
                for _ in 0..UPDATE_OPS {
                    let k = pick(keys, &mut rng)?;
                    let v = value_for(*k, 2);
                    stmt.execute(rusqlite::params![&k[..], &v[..]])
                        .map_err(|e| e.to_string())?;
                }
            }
            tx.commit().map_err(|e| e.to_string())?;
        }
        let update = t0.elapsed();
        // W-mt: shared-cache in-memory DB, one connection per thread. The
        // keeper connection pins the database in memory while threads run.
        let mut keeper = Connection::open(URI).map_err(|e| e.to_string())?;
        sqlite_pragmas(&keeper)?;
        keeper
            .execute_batch("CREATE TABLE t(k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID")
            .map_err(|e| e.to_string())?;
        {
            let tx = keeper.transaction().map_err(|e| e.to_string())?;
            {
                let mut stmt = tx
                    .prepare("INSERT INTO t(k, v) VALUES(?1, ?2)")
                    .map_err(|e| e.to_string())?;
                for k in keys {
                    let v = value_for(*k, 2);
                    stmt.execute(rusqlite::params![&k[..], &v[..]])
                        .map_err(|e| e.to_string())?;
                }
            }
            tx.commit().map_err(|e| e.to_string())?;
        }
        let t0 = Instant::now();
        let total = thread::scope(|s| -> Result<usize, String> {
            let mut hs = Vec::new();
            for ti in 0..MT_THREADS {
                hs.push(s.spawn(move || -> Result<usize, String> {
                    let conn = Connection::open(URI).map_err(|e| e.to_string())?;
                    conn.pragma_update(None, "read_uncommitted", 1)
                        .map_err(|e| e.to_string())?;
                    let mut rng = Sm::new(0xBE4C_1000 + ti as u64);
                    let mut stmt = conn
                        .prepare("SELECT v FROM t WHERE k = ?1")
                        .map_err(|e| e.to_string())?;
                    let mut found = 0usize;
                    for _ in 0..MT_OPS_PER_THREAD {
                        let k = pick(keys, &mut rng)?;
                        let r: Result<Vec<u8>, _> =
                            stmt.query_row(rusqlite::params![&k[..]], |row| row.get(0));
                        match r {
                            Ok(v) => {
                                black_box(v);
                                found += 1;
                            }
                            Err(rusqlite::Error::QueryReturnedNoRows) => {}
                            Err(e) => return Err(e.to_string()),
                        }
                    }
                    Ok(found)
                }));
            }
            let mut total = 0usize;
            for h in hs {
                let v = h.join().map_err(|_| "sqlite mt thread panicked".to_string())??;
                total += v;
            }
            Ok(total)
        })?;
        let mt = t0.elapsed();
        drop(keeper);
        if total != MT_THREADS * MT_OPS_PER_THREAD {
            return Err(format!("sqlite mt found only {total}"));
        }
        Ok(Work {
            insert_ns: insert.as_secs_f64() * 1e9 / keys.len() as f64,
            point_ns: point.as_secs_f64() * 1e9 / POINT_OPS as f64,
            range_ns: range.as_secs_f64() * 1e9 / RANGE_OPS as f64,
            update_ns: update.as_secs_f64() * 1e9 / UPDATE_OPS as f64,
            mt_ops: total as f64 / mt.as_secs_f64(),
        })
    }

    // ---- driver ---------------------------------------------------------

    struct Args {
        keys: usize,
        runs: usize,
        out: String,
    }

    fn parse_args() -> Result<Args, String> {
        let mut args = Args {
            keys: DEFAULT_KEYS,
            runs: 3,
            out: String::new(),
        };
        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            match a.as_str() {
                "--keys" => {
                    let v = it.next().ok_or("--keys needs a value")?;
                    args.keys = v.parse().map_err(|_| format!("bad --keys {v}"))?;
                }
                "--runs" => {
                    let v = it.next().ok_or("--runs needs a value")?;
                    args.runs = v.parse().map_err(|_| format!("bad --runs {v}"))?;
                }
                "--out" => {
                    args.out = it.next().ok_or("--out needs a value")?;
                }
                other => return Err(format!("unknown argument {other}")),
            }
        }
        if args.keys < 10_000 {
            return Err("--keys below 10_000 makes range windows meaningless".into());
        }
        Ok(args)
    }

    fn cpu_model() -> String {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("model name"))
                    .map(|l| l.trim().to_owned())
            })
            .unwrap_or_else(|| "model name: unknown".into())
    }

    fn rustc_version() -> String {
        std::process::Command::new("rustc")
            .arg("-V")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_else(|| "rustc ?".into())
    }

    fn json_escape(s: &str) -> String {
        let mut out = String::new();
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                _ => out.push(c),
            }
        }
        out
    }

    /// Entry point (from `main`).
    pub fn run() -> Result<(), String> {
        let args = parse_args()?;
        let mut rng = Sm::new(0xE0_E0_E0_E0);
        let keys: Vec<[u8; 8]> = (0..args.keys).map(|_| rng.next().to_be_bytes()).collect();
        // Calibrate the page count once with an untimed in-memory build so
        // both pools can be sized against the real data size.
        let cap_big = args.keys / 40 + 256;
        let cal = {
            let store = MemStore::new(8192).map_err(|e: StoreError| e.to_string())?;
            let tree = BTree::new(store, cap_big).map_err(|e| e.to_string())?;
            for k in keys.iter() {
                let v = value_for(*k, 1);
                tree.insert(k, &v).map_err(|e| e.to_string())?;
            }
            tree
        };
        let pages = cal.into_store().page_count().unwrap_or(0);
        let cap_mem = pages + pages / 8 + 64; // (i): pool >= data
        let cap_file = (pages / 4).max(64); // (ii): pool = 1/4 of data pages
        println!(
            "keys={} pages={} pool(i)={cap_mem} frames pool(ii)={cap_file} frames",
            args.keys, pages
        );
        let file_path = std::env::temp_dir().join(format!("e0-bench-{}.pgs", std::process::id()));

        let mut mem_ours = [Work::default(); 3];
        let mut file_ours = [Work::default(); 3];
        let mut sql = [Work::default(); 3];
        for r in 0..args.runs {
            let t0 = Instant::now();
            let store = MemStore::new(8192).map_err(|e| e.to_string())?;
            mem_ours[r] = ours_full(store, cap_mem, &keys)?;
            let _ = std::fs::remove_file(&file_path);
            let store = FileStore::open(&file_path, 8192).map_err(|e| e.to_string())?;
            file_ours[r] = ours_full(store, cap_file, &keys)?;
            sql[r] = sqlite_full(&keys)?;
            println!(
                "run {}/{} done in {:.1}s",
                r + 1,
                args.runs,
                t0.elapsed().as_secs_f64()
            );
        }
        let _ = std::fs::remove_file(&file_path);

        let med = |get: fn(&Work) -> f64, a: &Work, b: &Work, c: &Work| median3([get(a), get(b), get(c)]);
        let g_ins = |w: &Work| w.insert_ns;
        let g_pt = |w: &Work| w.point_ns;
        let g_rg = |w: &Work| w.range_ns;
        let g_up = |w: &Work| w.update_ns;
        let g_mt = |w: &Work| w.mt_ops;
        let mo = Work {
            insert_ns: med(g_ins, &mem_ours[0], &mem_ours[1], &mem_ours[2]),
            point_ns: med(g_pt, &mem_ours[0], &mem_ours[1], &mem_ours[2]),
            range_ns: med(g_rg, &mem_ours[0], &mem_ours[1], &mem_ours[2]),
            update_ns: med(g_up, &mem_ours[0], &mem_ours[1], &mem_ours[2]),
            mt_ops: med(g_mt, &mem_ours[0], &mem_ours[1], &mem_ours[2]),
        };
        let fo = Work {
            insert_ns: med(g_ins, &file_ours[0], &file_ours[1], &file_ours[2]),
            point_ns: med(g_pt, &file_ours[0], &file_ours[1], &file_ours[2]),
            range_ns: med(g_rg, &file_ours[0], &file_ours[1], &file_ours[2]),
            update_ns: med(g_up, &file_ours[0], &file_ours[1], &file_ours[2]),
            mt_ops: med(g_mt, &file_ours[0], &file_ours[1], &file_ours[2]),
        };
        let so = Work {
            insert_ns: med(g_ins, &sql[0], &sql[1], &sql[2]),
            point_ns: med(g_pt, &sql[0], &sql[1], &sql[2]),
            range_ns: med(g_rg, &sql[0], &sql[1], &sql[2]),
            update_ns: med(g_up, &sql[0], &sql[1], &sql[2]),
            mt_ops: med(g_mt, &sql[0], &sql[1], &sql[2]),
        };

        let mut tbl = String::new();
        let _ = writeln!(
            tbl,
            "{:<10} {:<10} {:>14} {:>14} {:>8}",
            "workload", "config", "ours ns/op", "sqlite ns/op", "ratio"
        );
        let rows: [(&str, &str, f64, f64); 8] = [
            ("W-insert", "(i) mem", mo.insert_ns, so.insert_ns),
            ("W-point", "(i) mem", mo.point_ns, so.point_ns),
            ("W-range", "(i) mem", mo.range_ns, so.range_ns),
            ("W-update", "(i) mem", mo.update_ns, so.update_ns),
            ("W-insert", "(ii) 1/4", fo.insert_ns, so.insert_ns),
            ("W-point", "(ii) 1/4", fo.point_ns, so.point_ns),
            ("W-range", "(ii) 1/4", fo.range_ns, so.range_ns),
            ("W-update", "(ii) 1/4", fo.update_ns, so.update_ns),
        ];
        for (name, cfg, ours, sqln) in rows {
            let _ = writeln!(
                tbl,
                "{:<10} {:<10} {:>14.2} {:>14.2} {:>8.3}",
                name,
                cfg,
                ours,
                sqln,
                sqln / ours
            );
        }
        let _ = writeln!(
            tbl,
            "{:<10} {:<10} {:>14} {:>14} {:>8.3}",
            "W-mt",
            "(i) mem",
            format!("{:.0} ops/s", mo.mt_ops),
            format!("{:.0} ops/s", so.mt_ops),
            so.mt_ops / mo.mt_ops
        );
        let _ = writeln!(
            tbl,
            "{:<10} {:<10} {:>14} {:>14} {:>8.3}",
            "W-mt",
            "(ii) 1/4",
            format!("{:.0} ops/s", fo.mt_ops),
            format!("{:.0} ops/s", so.mt_ops),
            so.mt_ops / fo.mt_ops
        );
        println!("{tbl}");
        println!(
            "ratio = sqlite time / ours (>1 means we are faster). Decision rule \
             (PLAN-v3 §4 E0): pass if ours >= 0.8x SQLite on W-point and W-range \
             in configuration (i), i.e. both ratios >= 0.8: point={:.3} range={:.3}.",
            so.point_ns / mo.point_ns,
            so.range_ns / mo.range_ns
        );
        println!(
            "(ii) runs with the pool at 1/4 of the data pages over a file store; \
             the OS page cache is warm, so it measures eviction cost, NOT disk."
        );

        if !args.out.is_empty() {
            let mut j = String::new();
            let _ = writeln!(
                j,
                "{{\n  \"keys\": {},\n  \"runs\": {},\n  \"pages\": {},\n  \"cpu\": \"{}\",\n  \"rustc\": \"{}\",",
                args.keys,
                args.runs,
                pages,
                json_escape(&cpu_model()),
                json_escape(&rustc_version())
            );
            let _ = writeln!(
                j,
                "  \"ours_mem\": {{\"insert_ns\": {:.2}, \"point_ns\": {:.2}, \"range_ns\": {:.2}, \"update_ns\": {:.2}, \"mt_ops\": {:.0}}},",
                mo.insert_ns, mo.point_ns, mo.range_ns, mo.update_ns, mo.mt_ops
            );
            let _ = writeln!(
                j,
                "  \"ours_file\": {{\"insert_ns\": {:.2}, \"point_ns\": {:.2}, \"range_ns\": {:.2}, \"update_ns\": {:.2}, \"mt_ops\": {:.0}}},",
                fo.insert_ns, fo.point_ns, fo.range_ns, fo.update_ns, fo.mt_ops
            );
            let _ = writeln!(
                j,
                "  \"sqlite\": {{\"insert_ns\": {:.2}, \"point_ns\": {:.2}, \"range_ns\": {:.2}, \"update_ns\": {:.2}, \"mt_ops\": {:.0}}},",
                so.insert_ns, so.point_ns, so.range_ns, so.update_ns, so.mt_ops
            );
            let _ = writeln!(
                j,
                "  \"ratios_sqlite_over_ours\": {{\"point_i\": {:.3}, \"range_i\": {:.3}, \"mt_i\": {:.3}}}\n}}",
                so.point_ns / mo.point_ns,
                so.range_ns / mo.range_ns,
                so.mt_ops / mo.mt_ops
            );
            std::fs::write(&args.out, j).map_err(|e| e.to_string())?;
            println!("wrote {}", args.out);
        }
        Ok(())
    }
}
