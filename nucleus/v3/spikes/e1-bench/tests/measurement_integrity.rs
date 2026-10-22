//! Deterministic guards of the measurement's integrity (card X-1b gate
//! rework, defect 2, verdict 2026-10-22): nothing here measures an arm —
//! each test pins down a property the measurement *depends on*, so the
//! gate's previously uncaught mutants now fail a named test:
//!
//! - **SEED-PER-ARM** (a W1/W2 rng salted by `type_name::<E>()`, giving
//!   the arms different key streams) → `Sim<0>` and `Sim<1>` are two
//!   distinct types with one implementation; driven through
//!   [`w1::run`](e1_bench::w1::run) and [`w2::run`](e1_bench::w2::run),
//!   their recorded op streams must be identical:
//!   `w1_*_streams_are_identical_across_engine_types`,
//!   `w2_*_streams_are_identical_across_engine_types`.
//! - **POOL-PER-ARM** (`make_c` gets `pool / 4`) → the workload factories
//!   must receive the same pool for every arm (asserted in the stream
//!   tests), and the library factories must forward their pool verbatim:
//!   at pool 1 a shrink reaches 0, and a zero pool is a broken
//!   configuration both arm constructors document:
//!   `factories_forward_their_pool_verbatim`.
//! - **VERIFY-BYPASS** (`arm_c_passed` forced true) → the B-ABORT
//!   decision must be false for a deliberately broken engine:
//!   `verification_fails_a_broken_engine` (plus the `report_passed`
//!   truth table in `e1_bench::verify`'s unit tests).
//! - **W5-ALIVE-OFF** (the reader's `original` flag never set false) →
//!   an engine whose `get` lies must be reported as a dead reader:
//!   `w5_flags_a_reader_that_reads_wrong_values` (and its honest
//!   counterpart proves the test itself can pass).
//! - **CPU-WINDOW-INCLUDES-PRELOAD** (W1's `cpu0` taken before the
//!   engine and preload) → an engine that burns a fixed slice of CPU
//!   per put must show the workload window's burn only:
//!   `w1_cpu_window_excludes_the_preload`.
//
// Tests may unwrap (clippy.toml's allow-unwrap-in-tests covers `#[test]`
// bodies; this file's engines and helpers live outside them, so the
// allow is explicit here — same as tests/w6_head_of_line.rs).
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// `key → ts-ascending version list` (the same shape as the reference
/// engine's version lists).
pub type VersionLists = BTreeMap<Vec<u8>, Vec<(Ts, Option<Vec<u8>>)>>;

use e1_bench::json::{Json, field};
use e1_bench::util;
use e1_bench::verify;
use e1_bench::{Config, engines, w1, w2, w5, workload};
use e1_mvcc::api::{EngineStats, Isolation, SavepointId, Ts, TxnEngine, TxnErr, TxnId, Wait};
use e1_mvcc::common::{LockTable, Sequencer};
use e1_mvcc::reference::RefEngine;

/// The gate's mutants are named per engine type in the wild; two tags
/// give two `type_name`s with one implementation.
mod sim {
    use super::*;

    /// The recorded op stream: content only — no txn ids (their
    /// assignment order is scheduling), no wall times.
    pub type Log = Arc<Mutex<Vec<String>>>;

    /// One open txn of [`Sim`].
    pub struct SimTxn {
        /// The txn's id (lock-table identity).
        pub id: TxnId,
        /// RC refreshes per statement; RR keeps its first snapshot.
        pub snap: Ts,
        /// RC flag.
        pub rc: bool,
        /// Buffered writes (own writes win over the committed state).
        pub ws: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        /// Savepoint snapshots (savepoint id → write set then).
        pub sps: Vec<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
        /// The txn's write-set bytes (for `stats`).
        pub bytes: u64,
    }

    /// A small correct-by-construction MVCC engine for the integrity
    /// tests: full version lists under one mutex, the shared
    /// [`LockTable`]/[`Sequencer`], and three test hooks — the op log,
    /// `lie_on_get` (W5-ALIVE) and `burn_ms` of CPU per put
    /// (CPU-window). `TAG` only exists to give two instantiations
    /// different type names.
    pub struct Sim<const TAG: u8> {
        /// `key → ts-ascending version list`.
        pub data: Mutex<VersionLists>,
        /// Newest commit ts (also the RR/RC snapshot source).
        pub visible: std::sync::atomic::AtomicU64,
        /// Commit ts source.
        pub next_ts: std::sync::atomic::AtomicU64,
        /// Txn id source.
        pub next_txn: std::sync::atomic::AtomicU64,
        /// The shared lock table (real parking, like the arms).
        pub locks: LockTable,
        /// The shared sequencer (returned to the trait; unused by Sim).
        pub seq: Sequencer,
        /// Open txns' write-set bytes.
        pub ws_bytes: Mutex<std::collections::HashMap<TxnId, u64>>,
        /// Cumulative applied bytes.
        pub bytes_applied: std::sync::atomic::AtomicU64,
        /// When set, every `get` of an existing value returns garbage.
        pub lie_on_get: bool,
        /// CPU (not wall) burned inside every `put`.
        pub burn_ms: u64,
        /// The op stream (None: not recording).
        pub log: Option<Log>,
    }

    impl<const TAG: u8> Sim<TAG> {
        /// A fresh engine with the given hooks.
        pub fn new(lie_on_get: bool, burn_ms: u64, log: Option<Log>) -> Self {
            Self {
                data: Mutex::new(VersionLists::new()),
                visible: std::sync::atomic::AtomicU64::new(0),
                next_ts: std::sync::atomic::AtomicU64::new(0),
                next_txn: std::sync::atomic::AtomicU64::new(1),
                locks: LockTable::new(),
                seq: Sequencer::new(),
                ws_bytes: Mutex::new(std::collections::HashMap::new()),
                bytes_applied: std::sync::atomic::AtomicU64::new(0),
                lie_on_get,
                burn_ms,
                log,
            }
        }

        fn record(&self, line: String) {
            if let Some(l) = &self.log
                && let Ok(mut v) = l.lock()
            {
                v.push(line);
            }
        }

        /// The value of `key` at `snap` (None = absent or deleted).
        fn at(&self, key: &[u8], snap: Ts) -> Option<Option<Vec<u8>>> {
            let data = self.data.lock().unwrap();
            data.get(key)
                .and_then(|vs| vs.iter().rev().find(|(t, _)| *t <= snap))
                .map(|(_, v)| v.clone())
        }

        /// The newest committed value of `key` (the write-path's EPQ
        /// base).
        fn newest(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
            let data = self.data.lock().unwrap();
            data.get(key)
                .and_then(|vs| vs.last())
                .map(|(_, v)| v.clone())
        }
    }

    impl<const TAG: u8> TxnEngine for Sim<TAG> {
        type Txn = SimTxn;

        fn name(&self) -> &'static str {
            "sim"
        }

        fn begin(&self, iso: Isolation) -> SimTxn {
            SimTxn {
                id: self
                    .next_txn
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                snap: self.visible.load(std::sync::atomic::Ordering::Acquire),
                rc: matches!(iso, Isolation::ReadCommitted),
                ws: BTreeMap::new(),
                sps: Vec::new(),
                bytes: 0,
            }
        }

        fn next_statement(&self, t: &mut SimTxn) {
            if t.rc {
                t.snap = self.visible.load(std::sync::atomic::Ordering::Acquire);
            }
        }

        fn get(&self, t: &mut SimTxn, key: &[u8]) -> Result<Option<Vec<u8>>, TxnErr> {
            let got = match t.ws.get(key) {
                Some(v) => v.clone(),
                None => self.at(key, t.snap).flatten(),
            };
            let out = match got {
                Some(v) if self.lie_on_get => Some(vec![0xEE; v.len() + 1]),
                other => other,
            };
            self.record(format!("get {}", util::key_hex(key)));
            Ok(out)
        }

        fn scan(
            &self,
            t: &mut SimTxn,
            lo: &[u8],
            hi: &[u8],
            limit: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnErr> {
            if lo >= hi {
                return Ok(Vec::new());
            }
            let data = self.data.lock().unwrap();
            let mut keys: std::collections::BTreeSet<Vec<u8>> = data
                .range(lo.to_vec()..hi.to_vec())
                .map(|(k, _)| k.clone())
                .collect();
            keys.extend(t.ws.range(lo.to_vec()..hi.to_vec()).map(|(k, _)| k.clone()));
            let mut out = Vec::new();
            for k in keys {
                let v = match t.ws.get(&k) {
                    Some(v) => v.clone(),
                    None => self.at(&k, t.snap).flatten(),
                };
                if let Some(v) = v {
                    out.push((k, v));
                    if out.len() >= limit {
                        break;
                    }
                }
            }
            Ok(out)
        }

        fn update_with(
            &self,
            t: &mut SimTxn,
            key: &[u8],
            wait: Wait,
            f: &mut dyn FnMut(Option<&[u8]>) -> Option<Vec<u8>>,
        ) -> Result<(), TxnErr> {
            self.locks.acquire(key, t.id, wait)?;
            let cur = self.newest(key).unwrap_or_default();
            let new = f(cur.as_deref());
            t.bytes = t.bytes.saturating_sub(ws_entry_bytes(&t.ws, key));
            t.bytes += key.len() as u64 + new.as_ref().map_or(0, |v| v.len()) as u64;
            t.ws.insert(key.to_vec(), new);
            Ok(())
        }

        fn put(&self, t: &mut SimTxn, key: &[u8], value: &[u8], wait: Wait) -> Result<(), TxnErr> {
            self.locks.acquire(key, t.id, wait)?;
            if self.burn_ms > 0 {
                burn_cpu(Duration::from_millis(self.burn_ms));
            }
            self.record(format!(
                "put {}={}",
                util::key_hex(key),
                value_digest(value)
            ));
            t.bytes = t.bytes.saturating_sub(ws_entry_bytes(&t.ws, key));
            t.bytes += key.len() as u64 + value.len() as u64;
            t.ws.insert(key.to_vec(), Some(value.to_vec()));
            self.ws_bytes.lock().unwrap().insert(t.id, t.bytes);
            Ok(())
        }

        fn insert_unique(
            &self,
            t: &mut SimTxn,
            key: &[u8],
            value: &[u8],
            wait: Wait,
        ) -> Result<(), TxnErr> {
            self.locks.acquire(key, t.id, wait)?;
            if matches!(self.newest(key), Some(Some(_))) {
                return Err(TxnErr::UniqueViolation);
            }
            t.bytes = t.bytes.saturating_sub(ws_entry_bytes(&t.ws, key));
            t.bytes += key.len() as u64 + value.len() as u64;
            t.ws.insert(key.to_vec(), Some(value.to_vec()));
            Ok(())
        }

        fn delete(&self, t: &mut SimTxn, key: &[u8], wait: Wait) -> Result<bool, TxnErr> {
            self.locks.acquire(key, t.id, wait)?;
            let committed_live = matches!(self.newest(key), Some(Some(_)));
            let own_live = matches!(t.ws.get(key), Some(Some(_)));
            t.bytes = t.bytes.saturating_sub(ws_entry_bytes(&t.ws, key));
            t.bytes += key.len() as u64;
            t.ws.insert(key.to_vec(), None);
            Ok(committed_live || own_live)
        }

        fn savepoint(&self, t: &mut SimTxn) -> SavepointId {
            t.sps.push(t.ws.clone());
            SavepointId(t.sps.len() as u32)
        }

        fn rollback_to(&self, t: &mut SimTxn, sp: SavepointId) {
            let idx = (sp.0 as usize).min(t.sps.len());
            if let Some(restore) = t.sps.get(idx - 1).cloned() {
                t.bytes = restore
                    .iter()
                    .map(|(k, v)| k.len() as u64 + v.as_ref().map_or(0, |x| x.len()) as u64)
                    .sum();
                t.ws = restore;
                t.sps.truncate(idx - 1);
            }
        }

        fn commit(&self, t: SimTxn) -> Result<Ts, TxnErr> {
            let ts = self
                .next_ts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            {
                let mut data = self.data.lock().unwrap();
                for (k, v) in &t.ws {
                    data.entry(k.clone()).or_default().push((ts, v.clone()));
                }
            }
            self.bytes_applied
                .fetch_add(t.bytes, std::sync::atomic::Ordering::Relaxed);
            self.ws_bytes.lock().unwrap().remove(&t.id);
            self.locks.release_all(t.id);
            self.visible
                .fetch_max(ts, std::sync::atomic::Ordering::Release);
            self.record("commit".to_string());
            Ok(ts)
        }

        fn abort(&self, t: SimTxn) {
            self.ws_bytes.lock().unwrap().remove(&t.id);
            self.locks.release_all(t.id);
            self.record("abort".to_string());
        }

        fn gc(&self) -> u64 {
            0 // Sim keeps every version; nothing here depends on GC.
        }

        fn stats(&self) -> EngineStats {
            let data = self.data.lock().unwrap();
            let versions: u64 = data.values().map(|vs| vs.len() as u64).sum();
            let logical: u64 = data
                .iter()
                .map(|(k, vs)| {
                    vs.last()
                        .and_then(|(_, v)| v.as_ref())
                        .map_or(0, |v| k.len() as u64 + v.len() as u64)
                })
                .sum();
            let ws: u64 = self.ws_bytes.lock().unwrap().values().sum();
            EngineStats {
                tree_bytes: 0,
                logical_bytes: logical,
                versions,
                undo_bytes: 0,
                undo_spilled_bytes: 0,
                lock_entries: self.locks.entries(),
                write_set_bytes: ws,
                bytes_applied: self
                    .bytes_applied
                    .load(std::sync::atomic::Ordering::Relaxed),
            }
        }

        fn lock_table(&self) -> &LockTable {
            &self.locks
        }

        fn sequencer(&self) -> &Sequencer {
            &self.seq
        }
    }

    /// The write-set bytes the entry for `key` currently occupies.
    fn ws_entry_bytes(ws: &BTreeMap<Vec<u8>, Option<Vec<u8>>>, key: &[u8]) -> u64 {
        ws.get(key).map_or(0, |v| {
            key.len() as u64 + v.as_ref().map_or(0, |x| x.len()) as u64
        })
    }

    /// Burn `d` of CPU (spin, not sleep — sleeping would not show up in
    /// the process CPU the W1 window measures).
    pub fn burn_cpu(d: Duration) {
        let t0 = Instant::now();
        while t0.elapsed() < d {
            std::hint::spin_loop();
        }
    }

    /// A short stable identity of a value for the op stream (the first
    /// bytes differ for every salt `value_of` uses).
    pub fn value_digest(v: &[u8]) -> String {
        let mut h = 0xcbf29ce484222325u64;
        for b in v {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x100000001b3);
        }
        format!("{}:{}", h, v.len())
    }
}

use sim::{Log, Sim};

/// The engines' undo-budget and pool forwarding tests plus the CPU-window
/// test burn real CPU; cargo test runs this binary's tests in parallel
/// threads of one process, and W1's CPU metric is whole-process — so the
/// CPU-heavy tests serialize against each other (the light stream tests
/// stay parallel; their CPU is microseconds).
fn heavy_gate() -> std::sync::MutexGuard<'static, ()> {
    static M: OnceLock<Mutex<()>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(())).lock().unwrap()
}

/// A tiny full-fidelity configuration for the stream tests (the shape of
/// `Config::quick` with counts shrunk further; the seeds and streams are
/// the production code paths).
fn tiny_cfg() -> Config {
    let mut c = Config::quick();
    c.runs = 1;
    c.preload_keys = 32;
    c.preload_batch = 8;
    c.value_len = 16;
    c.w1_threads = vec![3];
    c.w1_txns_per_thread = 20;
    c.w2_readers = 1;
    c.w2_writer_counts = vec![0];
    c.w2_hot_keys = 16;
    c.w2_duration = Duration::from_millis(80);
    c.w5_writers = 2;
    c.w5_updates = 24;
    c.w5_sample_keys = 24;
    c.sampler_interval = Duration::from_millis(1);
    c.write_wait = Duration::from_secs(5);
    c
}

/// A factory that records the pool it was called with, then builds `Sim`.
fn recording_factory<const TAG: u8>(
    lie: bool,
    burn_ms: u64,
    log: Log,
    pools: Arc<Mutex<Vec<usize>>>,
) -> impl Fn(usize) -> Result<Sim<TAG>, e1_bench::BenchError> {
    move |pool| {
        pools.lock().unwrap().push(pool);
        Ok(Sim::<TAG>::new(lie, burn_ms, Some(Arc::clone(&log))))
    }
}

/// SEED-PER-ARM on W1: two distinct engine types driven through the real
/// `w1::run` must issue the identical multiset of puts (keys *and*
/// values) — a per-type rng salt desynchronizes the streams and shows up
/// here. Both factories must also be called with the workload's one pool
/// (`cfg.pool_frames`), identically for both types.
#[test]
fn w1_put_streams_are_identical_across_engine_types() {
    let cfg = tiny_cfg();
    let log_a: Log = Arc::new(Mutex::new(Vec::new()));
    let log_b: Log = Arc::new(Mutex::new(Vec::new()));
    let pools_a = Arc::new(Mutex::new(Vec::new()));
    let pools_b = Arc::new(Mutex::new(Vec::new()));

    let make_a = recording_factory::<0>(false, 0, Arc::clone(&log_a), Arc::clone(&pools_a));
    let make_b = recording_factory::<1>(false, 0, Arc::clone(&log_b), Arc::clone(&pools_b));
    let ra = w1::run(&make_a, &cfg, 0, 3).unwrap();
    let rb = w1::run(&make_b, &cfg, 0, 3).unwrap();

    // The workloads completed and agree on their own accounting.
    assert_eq!(field(&ra, "txns"), field(&rb, "txns"));

    let mut a = log_a.lock().unwrap().clone();
    let mut b = log_b.lock().unwrap().clone();
    a.sort();
    b.sort();
    assert_eq!(a, b, "W1 issued different op streams for two engine types");
    assert_eq!(
        pools_a.lock().unwrap().as_slice(),
        &[cfg.pool_frames],
        "the first arm's factory got the wrong pool"
    );
    assert_eq!(
        pools_b.lock().unwrap().as_slice(),
        &[cfg.pool_frames],
        "the second arm's factory got a different pool"
    );
}

/// SEED-PER-ARM on W2: with one reader (and separately one writer) the
/// op stream is a single rng stream, so the shorter of the two engines'
/// logs must be a prefix of the other — window-length noise cannot break
/// the comparison, and any stream desynchronization does.
#[test]
fn w2_reader_and_writer_streams_are_identical_across_engine_types() {
    let cfg = tiny_cfg();

    let log_a: Log = Arc::new(Mutex::new(Vec::new()));
    let log_b: Log = Arc::new(Mutex::new(Vec::new()));
    let fa = recording_factory::<0>(
        false,
        0,
        Arc::clone(&log_a),
        Arc::new(Mutex::new(Vec::new())),
    );
    let fb = recording_factory::<1>(
        false,
        0,
        Arc::clone(&log_b),
        Arc::new(Mutex::new(Vec::new())),
    );
    w2::run(&fa, &cfg, 0, 0).unwrap(); // readers = 1, writers = 0
    w2::run(&fb, &cfg, 0, 0).unwrap();
    prefix_or_die(&log_a, &log_b, "W2 reader stream");

    let log_a: Log = Arc::new(Mutex::new(Vec::new()));
    let log_b: Log = Arc::new(Mutex::new(Vec::new()));
    let mut wcfg = cfg.clone();
    wcfg.w2_readers = 0;
    wcfg.w2_duration = Duration::from_millis(60);
    let fa = recording_factory::<2>(
        false,
        0,
        Arc::clone(&log_a),
        Arc::new(Mutex::new(Vec::new())),
    );
    let fb = recording_factory::<3>(
        false,
        0,
        Arc::clone(&log_b),
        Arc::new(Mutex::new(Vec::new())),
    );
    w2::run(&fa, &wcfg, 0, 1).unwrap(); // readers = 0, writers = 1
    w2::run(&fb, &wcfg, 0, 1).unwrap();
    prefix_or_die(&log_a, &log_b, "W2 writer stream");
}

/// Assert that the shorter recorded stream is a prefix of the longer one
/// (identical streams up to the window's length noise).
fn prefix_or_die(a: &Log, b: &Log, what: &str) {
    let a = a.lock().unwrap().clone();
    let b = b.lock().unwrap().clone();
    let n = a.len().min(b.len());
    assert!(n > 0, "{what}: nothing was recorded");
    assert_eq!(
        &a[..n],
        &b[..n],
        "{what} desynchronized for two engine types (first difference at or before {n})"
    );
}

/// POOL-PER-ARM: the library factories must forward their pool argument
/// verbatim. Pool 1 is a legal (minimum) pool on which a one-key txn
/// works; a factory that shrinks its argument (`pool / 4`) turns 1 into
/// 0, and a zero pool is a broken configuration both arm constructors
/// document — every later call returns `TxnErr::Engine`.
#[test]
fn factories_forward_their_pool_verbatim() {
    let cfg = Config::quick();
    let root = scratch("pool-forward");
    for key in [b"a" as &[u8], b"b"] {
        let b = engines::make_b(&cfg)(1).unwrap();
        let mut t = b.begin(Isolation::ReadCommitted);
        b.put(&mut t, key, &[1, 2, 3], Wait::NoWait).unwrap();
        b.commit(t).unwrap();

        let c = engines::make_c(&cfg, &root)(1).unwrap();
        let mut t = c.begin(Isolation::ReadCommitted);
        c.put(&mut t, key, &[1, 2, 3], Wait::NoWait).unwrap();
        c.commit(t).unwrap();
    }
}

/// POOL-PER-ARM's budget sibling: `make_c` must hand arm (c) exactly
/// `cfg.undo_budget_bytes`. An engine that writes well over a quarter of
/// the configured budget of undo — but less than the budget — must show
/// `undo_spilled_bytes == 0`; a factory that shrank the budget (the /4
/// mutant) would spill here.
#[test]
fn arm_c_factory_forwards_the_undo_budget() {
    let mut cfg = Config::quick();
    cfg.undo_budget_bytes = 256 * 1024;
    let root = scratch("undo-forward");
    let make = engines::make_c(&cfg, &root);
    let engine = make(4096).unwrap();

    workload::preload(&engine, 64, 8, 100, 0).unwrap();
    // 16 rounds over 64 keys = 1024 replaced versions ≈ 140 KiB of undo
    // records: above a quarter of the budget (64 KiB), below the budget.
    for round in 1..=16u64 {
        workload::preload(&engine, 64, 8, 100, round).unwrap();
    }
    let st = engine.stats();
    assert_eq!(
        st.undo_spilled_bytes, 0,
        "undo spilled below the configured 256 KiB budget (undo_bytes {})",
        st.undo_bytes
    );
    assert!(
        st.undo_bytes > cfg.undo_budget_bytes / 4,
        "the test wrote {} undo bytes, never crossing a quarter of the budget — \
         it cannot see a budget-shrinking factory",
        st.undo_bytes
    );
}

/// VERIFY-BYPASS: a deliberately broken engine (every commit fails; the
/// reference engine underneath keeps locking and snapshots well-behaved)
/// must fail `check_arm`, and the B-ABORT decision over its report must
/// be false. A passing reference engine gives the decision's other
/// branch, proving the test can distinguish.
#[test]
fn verification_fails_a_broken_engine() {
    let _heavy = heavy_gate();

    let broken = check_of_broken();
    assert!(
        !broken.passed,
        "the broken engine passed the behaviour suite: {}",
        broken.failures
    );
    assert!(!broken.failures.is_empty());

    let good = verify::check_arm(&|| RefEngine::new());
    assert!(
        good.passed,
        "the reference engine failed: {}",
        good.failures
    );

    let report = verify::report(broken.clone(), good.clone());
    assert!(!verify::report_passed(&report));
    let report = verify::report(good.clone(), broken);
    assert!(!verify::report_passed(&report));
    assert!(verify::report_passed(&verify::report(good.clone(), good)));
}

/// `check_arm` on the broken engine (one indirection so the engine type
/// stays local to this test).
fn check_of_broken() -> verify::ArmCheck {
    verify::check_arm(&|| Broken(RefEngine::new()))
}

/// The reference engine with a commit that always fails (and correctly
/// releases the txn's locks on the way out, so nothing parks forever).
struct Broken(RefEngine);

impl TxnEngine for Broken {
    type Txn = e1_mvcc::reference::RefTxn;

    fn name(&self) -> &'static str {
        "broken"
    }

    fn begin(&self, iso: Isolation) -> Self::Txn {
        self.0.begin(iso)
    }

    fn next_statement(&self, t: &mut Self::Txn) {
        self.0.next_statement(t);
    }

    fn get(&self, t: &mut Self::Txn, key: &[u8]) -> Result<Option<Vec<u8>>, TxnErr> {
        self.0.get(t, key)
    }

    fn scan(
        &self,
        t: &mut Self::Txn,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnErr> {
        self.0.scan(t, lo, hi, limit)
    }

    fn update_with(
        &self,
        t: &mut Self::Txn,
        key: &[u8],
        wait: Wait,
        f: &mut dyn FnMut(Option<&[u8]>) -> Option<Vec<u8>>,
    ) -> Result<(), TxnErr> {
        self.0.update_with(t, key, wait, f)
    }

    fn put(&self, t: &mut Self::Txn, key: &[u8], value: &[u8], wait: Wait) -> Result<(), TxnErr> {
        self.0.put(t, key, value, wait)
    }

    fn insert_unique(
        &self,
        t: &mut Self::Txn,
        key: &[u8],
        value: &[u8],
        wait: Wait,
    ) -> Result<(), TxnErr> {
        self.0.insert_unique(t, key, value, wait)
    }

    fn delete(&self, t: &mut Self::Txn, key: &[u8], wait: Wait) -> Result<bool, TxnErr> {
        self.0.delete(t, key, wait)
    }

    fn savepoint(&self, t: &mut Self::Txn) -> SavepointId {
        self.0.savepoint(t)
    }

    fn rollback_to(&self, t: &mut Self::Txn, sp: SavepointId) {
        self.0.rollback_to(t, sp);
    }

    fn commit(&self, t: Self::Txn) -> Result<Ts, TxnErr> {
        self.0.abort(t);
        Err(TxnErr::Engine("commit broken by test".into()))
    }

    fn abort(&self, t: Self::Txn) {
        self.0.abort(t);
    }

    fn gc(&self) -> u64 {
        self.0.gc()
    }

    fn stats(&self) -> EngineStats {
        self.0.stats()
    }

    fn lock_table(&self) -> &LockTable {
        self.0.lock_table()
    }

    fn sequencer(&self) -> &Sequencer {
        self.0.sequencer()
    }
}

/// W5-ALIVE: an engine whose `get` returns wrong values after the writers
/// ran must be reported as a reader that does not read its originals —
/// never as `true` (the W5-ALIVE-OFF mutant froze the flag at true). The
/// honest engine gives the flag's true branch, proving the workload can
/// still report a live reader.
#[test]
fn w5_flags_a_reader_that_reads_wrong_values() {
    let mut cfg = tiny_cfg();
    cfg.w5_sample_keys = cfg.preload_keys;

    let lying = {
        let make = |_: usize| Ok(Sim::<4>::new(true, 0, None));
        let r = w5::run(&make, &cfg, 0).unwrap();
        field(&r, "reader_reads_original_values")
    };
    let honest = {
        let make = |_: usize| Ok(Sim::<5>::new(false, 0, None));
        let r = w5::run(&make, &cfg, 0).unwrap();
        field(&r, "reader_reads_original_values")
    };
    assert_eq!(lying, Some(Json::Bool(false)));
    assert_eq!(honest, Some(Json::Bool(true)));
}

/// CPU-WINDOW-INCLUDES-PRELOAD: with an engine that burns a fixed slice
/// of CPU per put, W1's reported CPU per txn must cover the workload
/// window only (12 puts), not the preload (20 more puts). Window
/// arithmetic at 40 ms/put: honest ≈ 40,000 µs/txn; a window that starts
/// before the preload ≈ 106,000 — the assert sits between with margin.
#[test]
fn w1_cpu_window_excludes_the_preload() {
    let _heavy = heavy_gate();

    let mut cfg = tiny_cfg();
    cfg.preload_keys = 20;
    cfg.preload_batch = 5;
    cfg.w1_threads = vec![1];
    cfg.w1_txns_per_thread = 12;

    let make = |_: usize| Ok(Sim::<6>::new(false, 40, None));
    let r = w1::run(&make, &cfg, 0, 1).unwrap();
    let cpu = match field(&r, "cpu_us_per_txn") {
        Some(Json::Num(v)) => v,
        other => panic!("cpu_us_per_txn missing or non-numeric: {other:?}"),
    };
    assert!(
        cpu < 70_000.0,
        "cpu_us_per_txn {cpu} includes work outside the worker window \
         (expected ≈ 40,000 for 12 burned puts; ≈ 106,000 if the 20 puts \
         of the preload leaked in)"
    );
}

/// A per-test scratch directory (arm (c) removes its spill files on
/// drop; the directories stay and are the test's own).
fn scratch(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("e1-bench-integrity-{tag}-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&d);
    d
}
