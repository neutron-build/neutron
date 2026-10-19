//! C-DIAG1: root-cause the one-shot `unexpected error:
//! Corrupt("intent value: bad magic")` in
//! `arbiter_lock_restart_still_hits_lock_timeout`
//! (`--test c_t2c_deadline`, full acceptance, 2026-10-17, under heavy load;
//! not reproduced in 30 solo runs or 300 runs at 12-way parallel).
//!
//! ## The diagnosis (evidence below; classified: RIG, not engine)
//!
//! The panic is the rig's own `ok()` (`tests/c_t2c_support/mod.rs`),
//! reached from `assert_count_exact`: its scan is
//! `(Unbounded, Unbounded)`, so it walks the whole KV **including
//! `/sys/`**, and for every key `parse_key` reports as `Entry::Intent` it
//! decodes the value with `decode_intent` — with no `/sys/` exclusion.
//!
//! `parse_key` (`src/encoding.rs:70-86`) splits raw keys **from the right**:
//! any key whose last byte is `0x00` parses as an intent key. A
//! `/sys/ts_clock/{ts}` sample whose `ts` is a multiple of 256 ends in
//! `0x00` (be64 low byte). The commit pipeline writes such samples itself
//! (`src/commit.rs:712-718`): at most one per wall-clock second, at the ts
//! of the first commit that observes the new second, value
//! `be64(unix_seconds)` — never `NTXI`. So when a sample lands on a
//! multiple of 256, the rig's unbounded scan hands `decode_intent` a
//! wall-clock value, `src/encoding.rs:294-295` returns
//! `Corrupt("intent value: bad magic")`, and `ok()` panics with exactly
//! the observed message. Nothing deletes `ts_clock` samples inside a test
//! (only the GC job, which this rig never starts).
//!
//! Every engine-side decoder is safe by construction: the write/read paths
//! only decode under `[intent_key(L), end_key(L))` ranges of user logical
//! keys (`/sys/` sorts before `/t/`, `/u/`, `/i/`), and the engine's two
//! unbounded scanners skip `SYS_PREFIX` before parsing
//! (`src/jobs/sweep.rs:116`, `src/gc/job.rs:206`; pinned by
//! `b1_sweep::sweep_never_decodes_non_catalog_sys_keys`). The rig's own
//! `count_intents` (`tests/c_t2c_support/mod.rs`) applies the same
//! exclusion and documents the hazard ("rework item 3") —
//! `assert_count_exact` was left without it.
//!
//! ## The fix (C-DIAG1fix, this branch)
//!
//! `assert_count_exact` now applies the same `SYS_PREFIX` skip as
//! `count_intents`. The deterministic reproduction below — which on the
//! C-DIAG1 branch asserted the rig panics — is now an always-run
//! regression test: it drives the ts-256 sample through the real pipeline,
//! pins that the offending key is present in the KV, and asserts the real
//! `assert_count_exact` runs clean over it. With the skip removed (the
//! C-DIAG1 rig), it fails with the exact acceptance message.
//!
//! ## Why rare, and why "under load"
//!
//! The failing test cycles ~1 commit per park for its 300 ms
//! `lock_timeout` (hundreds to thousands of commits, ts 1..N dense from 1,
//! `StatusTable::begin`/`process_group`). A bad key needs a sample to land
//! on `ts ≡ 0 (mod 256)` at a wall-second boundary — a coincidence whose
//! probability is ~1/256 per boundary sample and depends on where the
//! boundary cuts the commit stream. Load stretches the run over more
//! wall-clock seconds (more samples) and shifts the commit rate (which ts
//! each boundary hits); it rerolls the dice, it does not cause a new
//! failure mode. `/sys/txn/{n}` records (`n ≡ 0 mod 256` has the same key
//! shape) are excluded: the resolver's truncation pass deletes every
//! committed record in this rig at the `resolve()` that follows its commit
//! (`src/resolver.rs:103-110`, `min_view_counter() == u64::MAX` when no
//! view is open), so none is ever present at the scan — asserted below.
//! `/sys/log` is never written (`commit_submit` passes `log: None`,
//! `src/commit.rs:441`).
//!
//! ## What runs
//!
//! - Two `#[test]`s pin the mechanism's encoding facts (fast,
//!   deterministic, always run).
//! - `diag1_deterministic_repro_bad_magic_through_real_rig_scan`
//!   (always run since C-DIAG1fix): the smallest deterministic
//!   reproduction — drives 256 real commits through the real pipeline with
//!   a stepping `Clock` so the ts-256 sample lands on a multiple of 256,
//!   pins that the offending `/sys/` key is present, then calls the rig's
//!   real (fixed) `assert_count_exact` and asserts it does not panic.
//! - `diag1_natural_soak_under_load` (`#[ignore]`): the C-DIAG1 load
//!   loop — CPU hogs in threads plus a fresh real-`SystemClock` world per
//!   iteration whose commit stream mimics the failing test's parks,
//!   recording every natural capture (key bytes, value bytes) without
//!   panicking. Retained from the diagnosis for reruns; the fixed rig no
//!   longer decodes what it captures.
//! - `diag1_cpu_hogs` (`#[ignore]`): stand-alone load for looping the real
//!   `c_t2c_deadline` binary externally (no system tools).

mod c_t2c_support;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use c_t2c_support::{ok, u64v, Fixed, TestRowLocks};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{Clock, CommitPipeline, SyncCommit};
use nucleus_txn::encoding::{
    decode_intent, parse_key, sys_ts_clock_key, sys_txn_prefix, Entry, SYS_PREFIX,
};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::write::{RowOp, StmtCtx, UniqueRule};
use nucleus_txn::{Ts, TxnError};

// ---- Environment knobs (all optional) --------------------------------------

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---- Shared drivers ---------------------------------------------------------

/// One preload-style insert commit: begin, insert, submit, process, ack.
/// The `Rig`/`ArbiterCycles::preload` shape (`tests/c_t2c_support/mod.rs`).
fn commit_insert(
    core: &Arc<Core<MemKv>>,
    pipeline: &mut CommitPipeline<MemKv>,
    key: &[u8],
    value: &[u8],
) {
    let t = core.begin(Isolation::ReadCommitted);
    let seq = ok(t.next_seq());
    ok(core.insert_key(
        &t,
        key,
        None,
        value.to_vec(),
        StmtCtx::new(core.visible_ts(), seq, seq),
        UniqueRule::Unique { same_row: None },
    ));
    let ticket = ok(core.commit_submit(t, SyncCommit::On));
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    ok(ticket.try_ack().expect("acked after process_group"));
}

/// One blocker-style update commit of `row`: the exact write shape of
/// `ArbiterCycles::advance` (`tests/c_t2c_support/mod.rs:216-229`), one
/// txn, one update, one commit, then drain the resolver.
fn commit_update_row(
    core: &Arc<Core<MemKv>>,
    pipeline: &mut CommitPipeline<MemKv>,
    row: &[u8],
    value: u64,
) {
    let t = core.begin(Isolation::ReadCommitted);
    let seq = ok(t.next_seq());
    let op = RowOp::Update {
        value: u64v(value),
        key_cols_changed: false,
    };
    ok(core.row_op(
        &t,
        row,
        None,
        op.clone(),
        StmtCtx::new(core.visible_ts(), seq, seq),
        &mut Fixed(op),
    ));
    let ticket = ok(core.commit_submit(t, SyncCommit::On));
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    ok(ticket.try_ack().expect("acked after process_group"));
    resolve_all(core);
}

/// Drains the resolver until idle (`ArbiterCycles::resolve`).
fn resolve_all(core: &Arc<Core<MemKv>>) {
    while ok(Resolver::run_once(core)) > 0 {}
}

/// Every `/sys/` key currently in the KV, with its raw value.
fn sys_span(core: &Arc<Core<MemKv>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let view = core.open_view();
    let mut out = Vec::new();
    for row in view.scan(
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        false,
    ) {
        let (k, v) = ok(row);
        if k.starts_with(SYS_PREFIX) {
            out.push((k, v));
        }
    }
    out
}

/// The pre-fix `assert_count_exact` scan shape, unbounded and with no
/// `/sys/` exclusion, reporting every key that parses as `Entry::Intent`
/// whose value fails `decode_intent` — the natural-capture recorder. Never
/// panics; a catch is data.
fn scan_bad_magic_captures(core: &Arc<Core<MemKv>>) -> Vec<(Vec<u8>, Vec<u8>, TxnError)> {
    let view = core.open_view();
    let mut out = Vec::new();
    for row in view.scan(
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        false,
    ) {
        let (k, v) = ok(row);
        if let Some((_, Entry::Intent)) = parse_key(&k) {
            if let Err(e) = decode_intent(&v) {
                out.push((k, v, e));
            }
        }
    }
    out
}

/// A clock that never repeats a second: every `process_group` observes a
/// "new" wall second, so every commit writes a `/sys/ts_clock/{ts}` sample
/// (`src/commit.rs:712-718`). The engine's own seam
/// (`CommitPipeline::set_clock`), not an engine edit.
struct StepClock(AtomicU64);

impl Clock for StepClock {
    fn now_secs(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst).wrapping_add(1)
    }
}

// ---- Mechanism pins (deterministic, always run) -----------------------------

/// `parse_key` splits from the right (`src/encoding.rs:70-86`): a
/// `/sys/ts_clock/{ts}` key whose ts is a multiple of 256 ends in `0x00`,
/// so it parses as an intent key of a "logical key" that is actually the
/// sample key minus its last byte. Neighbouring ts values do not.
#[test]
fn sys_ts_clock_keys_at_multiples_of_256_parse_as_intent_keys() {
    for ts in [256u64, 512, 768, 65_536] {
        let key = sys_ts_clock_key(Ts(ts));
        assert!(
            matches!(parse_key(&key), Some((_, Entry::Intent))),
            "{ts} multiple of 256: key {key:?} must parse as an intent key"
        );
    }
    for ts in [1u64, 255, 257, 511, 513] {
        let key = sys_ts_clock_key(Ts(ts));
        assert!(
            parse_key(&key).is_none(),
            "{ts} not a multiple of 256: key {key:?} must not parse at all"
        );
    }
}

/// A `ts_clock` sample's value is `be64(unix_seconds)`
/// (`src/commit.rs:716`): it never starts with the `NTXI` magic, so
/// `decode_intent` refuses it with exactly the observed error.
#[test]
fn ts_clock_sample_values_fail_the_intent_magic() {
    for secs in [0u64, 1, 1_760_000_000] {
        let err = decode_intent(&secs.to_be_bytes())
            .expect_err("a wall-clock value is never an intent value");
        assert!(
            matches!(err, TxnError::Corrupt(ref m) if m == "intent value: bad magic"),
            "secs {secs}: {err:?}"
        );
    }
}

// ---- The deterministic reproduction (C-DIAG1fix regression pin) -------------

/// The smallest deterministic reproduction of the acceptance failure,
/// turned by C-DIAG1fix into the regression test for the fix.
///
/// Drives 256 real commits through the real pipeline over a fresh core
/// (dense ts 1..=256 from `process_group`'s sequencer), with a stepping
/// clock so the ts-256 commit writes its `/sys/ts_clock` sample at exactly
/// ts 256 — a multiple of 256. Then:
///
/// 1. pins the `/sys/` span: the offending sample is there (the only
///    key that parses as an intent and fails `decode_intent`), no
///    `/sys/txn` record survived the resolver's truncation pass, and the
///    fixed helper `count_intents` (which excludes `/sys/`) is immune;
/// 2. calls the rig's real `assert_count_exact` — the exact function the
///    failing test calls through `assert_clean_timeout` — and asserts it
///    runs clean over the ts-256 sample. With the `SYS_PREFIX` skip
///    removed (the C-DIAG1 rig), the spawned thread panics with exactly
///    `unexpected error: Corrupt("intent value: bad magic")`, the
///    one-shot acceptance message, and this test fails on it.
#[test]
fn diag1_deterministic_repro_bad_magic_through_real_rig_scan() {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    core.set_row_locks(TestRowLocks::new());
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    pipeline.set_clock(Arc::new(StepClock(AtomicU64::new(0))));

    // Preload one row (commit ts 1), then blocker-style update commits
    // until the ts-256 sample exists: 255 more commits, ts 2..=256.
    let row = b"/t/r/1";
    commit_insert(&core, &mut pipeline, row, &u64v(0));
    for i in 1..=255u64 {
        commit_update_row(&core, &mut pipeline, row, i);
    }

    // 1a. The offending key exists and is the ONLY `/sys/` key that parses
    //     as an intent: `/sys/ts_clock/` + be64(256), an 8-byte wall-clock
    //     value that is not `NTXI`.
    let bad = scan_bad_magic_captures(&core);
    assert_eq!(bad.len(), 1, "exactly one bad-magic key: {bad:?}");
    let (key, value, err) = &bad[0];
    assert_eq!(key, &sys_ts_clock_key(Ts(256)), "the key bytes (verbatim)");
    assert_eq!(value.len(), 8, "the value is one be64 wall second");
    assert!(
        matches!(err, TxnError::Corrupt(m) if m == "intent value: bad magic"),
        "{err:?}"
    );
    println!("DIAG1 capture key  = {key:02x?}");
    println!("DIAG1 capture value= {value:02x?} (be64 unix seconds)");

    // 1b. No `/sys/txn` record survived: the resolver's truncation pass
    //     (resolver.rs:103-110) deleted every committed record at the
    //     `resolve()` that followed its commit — the parallel suspect
    //     (`n ≡ 0 mod 256` has the same key shape) is excluded.
    let span = sys_span(&core);
    assert!(
        span.iter().all(|(k, _)| !k.starts_with(&sys_txn_prefix())),
        "no /sys/txn record survived to the scan: {span:?}"
    );

    // 1c. The rig's fixed helper (same file, same KV) is immune: it
    //     excludes `/sys/` before parsing ("rework item 3").
    assert_eq!(
        c_t2c_support::count_intents(&core),
        0,
        "count_intents excludes /sys/ and sees no intent"
    );

    // 2. The real rig function, on a spawned thread so a regression (the
    //    skip removed) surfaces the C-DIAG1 panic verbatim instead of
    //    unwinding this test.
    let scan_core = Arc::clone(&core);
    let txn = core.begin(Isolation::ReadCommitted);
    let joined = std::thread::spawn(move || {
        c_t2c_support::assert_count_exact(&scan_core, &txn);
    })
    .join();
    match joined {
        Ok(()) => {
            println!("DIAG1 regression holds: assert_count_exact skipped the ts-256 sample");
        }
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            panic!(
                "C-DIAG1 regression: assert_count_exact decoded the ts-256 \
                 /sys/ts_clock sample: {msg}"
            );
        }
    }
}

// ---- The load reproduction harness (from C-DIAG1, still #[ignore]d) ---------

/// CPU hogs for the load loops: `n` threads of plain arithmetic spinning
/// until `stop` (the card's "busy loops in threads in your harness, not
/// system tools").
fn spawn_hogs(n: usize) -> (Arc<AtomicBool>, Vec<std::thread::JoinHandle<()>>) {
    let stop = Arc::new(AtomicBool::new(false));
    let handles = (0..n)
        .map(|_| {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut x = 0u64;
                let mut tick = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    x = x.wrapping_add(1);
                    tick += 1;
                    if tick >= (1 << 20) {
                        tick = 0;
                        std::hint::black_box(x);
                    }
                }
            })
        })
        .collect();
    (stop, handles)
}

/// Stand-alone load generator for looping the real `c_t2c_deadline` binary
/// externally: `NUCLEUS_DIAG1_HOG_N` (default 12) threads for
/// `NUCLEUS_DIAG1_HOG_SECS` (default 60) seconds.
#[test]
#[ignore = "C-DIAG1 load generator: run explicitly alongside the real test loop"]
fn diag1_cpu_hogs() {
    let n = env_usize("NUCLEUS_DIAG1_HOG_N", 12);
    let secs = env_usize("NUCLEUS_DIAG1_HOG_SECS", 60);
    println!("DIAG1 hogs: {n} threads for {secs}s");
    let (stop, handles) = spawn_hogs(n);
    std::thread::sleep(Duration::from_secs(secs as u64));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
        }
    }
}

/// The natural-rate reproduction loop from the diagnosis. Each iteration
/// builds a fresh world with the REAL `SystemClock` pipeline and streams
/// the failing test's write pattern (one blocker-style update commit +
/// full resolve per cycle, `ArbiterCycles::advance`) for
/// `NUCLEUS_DIAG1_SOAK_WINDOW_MS` (default 1000) ms under
/// `NUCLEUS_DIAG1_HOG_N` (default 8) hog threads, then scans with the
/// pre-fix `assert_count_exact` shape and records every capture — key
/// bytes, value bytes — without panicking. A window of at least one second
/// always crosses a wall-second boundary, so each window rolls the
/// ~1/256-per-sample dice at least once, at the machine's real commit
/// rate. Since C-DIAG1fix the rig itself no longer decodes what this
/// records; kept for rerunning the natural-rate measurement.
#[test]
#[ignore = "C-DIAG1 load soak: minutes-long; run explicitly with the env knobs"]
fn diag1_natural_soak_under_load() {
    let iters = env_usize("NUCLEUS_DIAG1_SOAK_ITERS", 300);
    let window_ms = env_usize("NUCLEUS_DIAG1_SOAK_WINDOW_MS", 1000);
    let hogs = env_usize("NUCLEUS_DIAG1_HOG_N", 8);
    let (stop, hog_handles) = spawn_hogs(hogs);
    let window = Duration::from_millis(window_ms as u64);
    let row = b"/t/r/1".to_vec();
    let mut commits_total = 0u64;
    let mut captures: Vec<(usize, Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..iters {
        let core = Arc::new(ok(Core::open(MemKv::new())));
        core.set_row_locks(TestRowLocks::new());
        let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
        commit_insert(&core, &mut pipeline, &row, &u64v(0));
        let start = Instant::now();
        let mut commits = 0u64;
        let mut v = 0u64;
        while start.elapsed() < window {
            v += 1;
            commit_update_row(&core, &mut pipeline, &row, v);
            commits += 1;
        }
        commits_total += commits;
        for (key, value, _err) in scan_bad_magic_captures(&core) {
            println!("DIAG1 natural capture at iter {i}: key={key:02x?} value={value:02x?}");
            captures.push((i, key, value));
        }
    }
    stop.store(true, Ordering::Relaxed);
    for h in hog_handles {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
        }
    }
    let expected = (iters * window_ms.div_ceil(1000)) as f64 / 256.0;
    println!(
        "DIAG1 soak: {iters} iterations, {commits_total} commits, {} natural captures \
         (expected ≈ {expected:.1} at 1/256 per wall-second sample)",
        captures.len()
    );
    for (i, key, value) in &captures {
        // Decode the capture for the record: a `/sys/ts_clock/` key whose
        // ts is a multiple of 256, value one be64 unix second.
        let ts = key
            .strip_prefix(b"/sys/ts_clock/")
            .and_then(|t| t.try_into().ok())
            .map(u64::from_be_bytes);
        let secs = value.as_slice().try_into().ok().map(u64::from_be_bytes);
        println!(
            "DIAG1 capture decoded: iter {i} ts={ts:?} (ts%256={:?}) unix_secs={secs:?}",
            ts.map(|t| t % 256)
        );
    }
}
