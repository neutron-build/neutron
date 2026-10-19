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
//! ## C-DIAG1fix2 (this branch): the generic twin + the step-1c flake
//!
//! The C-DIAG1fix gate returned two follow-ups; this card does both.
//!
//! **M-2 — the generic twin.** `tests/common/mod.rs::assert_count_exact<K>`
//! (consumers: `write_path`, `write_savepoint`, `t6t1_delete_entry`,
//! `b1_sweep`) carried the identical pre-fix hole: an unbounded scan, no
//! `/sys/` exclusion, `ok(decode_intent(&v))` on every key that parses as
//! `Entry::Intent`. It now applies the byte-for-byte same skip, before
//! `parse_key`, gated on `nucleus_txn::encoding::SYS_PREFIX`. The
//! regression pin below extends the deterministic plant to the twin (step 3
//! of the repro): the same ts-256 sample through the real
//! `common::assert_count_exact`. With the twin's skip removed, that step
//! fails with the same one-shot
//! `unexpected error: Corrupt("intent value: bad magic")`.
//!
//! **M-1 — the step-1c residue.** The gate observed the new pin's step 1c
//! fail **once** in ~1400 runs (`count_intents excludes /sys/ and sees no
//! intent, left: 1, right: 0`): one **non-`/sys/`** key parsed as
//! `Entry::Intent` after the 256-commit drain. Not reproduced since
//! (8 + 30 + 40 cargo runs and 1200 raw-binary runs under CPU hogs, gate
//! instrumentation never fired). Single-threaded sequencing says the
//! residue is impossible, so either the engine very rarely leaks a
//! committed txn's intent past a resolver drain, or the one observation was
//! environmental. `diag1_step1c_residue_hunt` below re-runs the exact
//! step-1c scene in a bounded loop with the residue evidence path built in
//! (surviving key/value bytes, three extra `Resolver::run_once` rounds with
//! a re-scan after each, the `/sys/txn` span, and the status-table entries
//! of the txns the residue names). Tally (this card, this machine,
//! CARGO_TARGET_DIR build, debug profile):
//!
//! - in-process hunt: 2600 scenes clean — 2000 plain (~27 ms/scene) +
//!   600 under 6 CPU-hog threads (~75 ms/scene; per-run bound 30 s, never
//!   approached);
//! - external binary loop: 1000 runs of the real always-run pin (fresh
//!   process each time: 500 plain + 500 under the `diag1_cpu_hogs` load),
//!   all green.
//!
//! **Unreproduced-in-bounds → M-1 closed as such.** The pin keeps its
//! determinism note and step 1c stays as written; if the residue ever
//! reappears, the hunt prints the evidence bundle and fails — that is an
//! engine intent-leak escalation, not a rig fix.
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
//!   Since C-DIAG1fix2 step 3 extends the same plant to the generic twin
//!   `common::assert_count_exact` (M-2).
//! - `diag1_natural_soak_under_load` (`#[ignore]`): the C-DIAG1 load
//!   loop — CPU hogs in threads plus a fresh real-`SystemClock` world per
//!   iteration whose commit stream mimics the failing test's parks,
//!   recording every natural capture (key bytes, value bytes) without
//!   panicking. Retained from the diagnosis for reruns; the fixed rig no
//!   longer decodes what it captures.
//! - `diag1_step1c_residue_hunt` (`#[ignore]`, C-DIAG1fix2): the M-1
//!   flake hunt — the exact step-1c scene in a bounded loop (default 2000
//!   iterations, 30 s per-run bound via a scene thread + `recv_timeout`,
//!   optional CPU hogs), each scene checked for a surviving non-`/sys/`
//!   intent; on the first residue it prints the evidence bundle and fails.
//!   See the C-DIAG1fix2 section above for the tally.
//! - `diag1_cpu_hogs` (`#[ignore]`): stand-alone load for looping the real
//!   `c_t2c_deadline` binary externally (no system tools).

mod c_t2c_support;
mod common;

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

/// Runs one rig's `assert_count_exact` on a spawned thread (a regression
/// must surface the rig's own panic verbatim instead of unwinding this
/// test) and, if it panics, fails with `what` plus that message.
fn expect_clean_rig_scan(what: &str, scan: impl FnOnce() + Send + 'static) {
    match std::thread::spawn(scan).join() {
        Ok(()) => {}
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            panic!("{what}: {msg}");
        }
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
///    one-shot acceptance message, and this test fails on it;
/// 3. (C-DIAG1fix2, M-2) does the same through the generic twin
///    `common::assert_count_exact` (`tests/common/mod.rs`), whose four
///    consumer binaries (`write_path`, `write_savepoint`,
///    `t6t1_delete_entry`, `b1_sweep`) inherit the same protection. With
///    the twin's skip removed, this step fails with the same one-shot
///    message.
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
    expect_clean_rig_scan(
        "C-DIAG1 regression: assert_count_exact decoded the ts-256 \
         /sys/ts_clock sample",
        move || {
            c_t2c_support::assert_count_exact(&scan_core, &txn);
            println!("DIAG1 regression holds: assert_count_exact skipped the ts-256 sample");
        },
    );

    // 3. C-DIAG1fix2 (M-2): the generic twin `common::assert_count_exact`
    //    over the same ts-256 sample — the deterministic plant extended to
    //    a consumer rig. Same spawned-thread contract: with the twin's
    //    skip removed this fails with the same one-shot message.
    let twin_core = Arc::clone(&core);
    let twin_txn = core.begin(Isolation::ReadCommitted);
    expect_clean_rig_scan(
        "C-DIAG1fix2 regression: common::assert_count_exact decoded the \
         ts-256 /sys/ts_clock sample",
        move || {
            common::assert_count_exact(&twin_core, &twin_txn);
            println!(
                "DIAG1fix2 twin holds: common::assert_count_exact skipped \
                 the ts-256 sample"
            );
        },
    );
}

// ---- The M-1 residue hunt (C-DIAG1fix2) --------------------------------------

/// `c_t2c_support::count_intents`'s exact scan shape (the step-1c
/// assertion), but capturing the survivors instead of counting: every
/// **non-`/sys/`** key that parses as `Entry::Intent`, with its raw value.
/// Never decodes — a capture is data.
fn live_intents(core: &Arc<Core<MemKv>>) -> Vec<Pair> {
    let view = core.open_view();
    let mut out = Vec::new();
    for row in view.scan(
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        false,
    ) {
        let (k, v) = ok(row);
        if k.starts_with(SYS_PREFIX) {
            continue;
        }
        if parse_key(&k).is_some_and(|(_, e)| matches!(e, Entry::Intent)) {
            out.push((k, v));
        }
    }
    out
}

/// One captured KV pair: raw key bytes, raw value bytes.
type Pair = (Vec<u8>, Vec<u8>);

/// One survivor of a drain: key, value, and `decode_intent`'s owner id
/// (or the decode error — itself diagnostic).
type Survivor = (
    Vec<u8>,
    Vec<u8>,
    std::result::Result<nucleus_txn::TxnId, TxnError>,
);

/// One extra `Resolver::run_once` round: how much it resolved, and what
/// still survives after it.
type ExtraRound = (usize, Vec<Pair>);

/// The M-1 evidence bundle: everything needed to classify a non-`/sys/`
/// intent that survives a full resolver drain
/// (`diag1_step1c_residue_hunt`).
#[derive(Debug)]
struct ResidueEvidence {
    /// Each survivor: key bytes, raw value bytes, and `decode_intent`'s
    /// owner id (or the decode error — itself diagnostic: a user-span
    /// key that fails decode is a different bug than a leaked intent).
    survivors: Vec<Survivor>,
    /// Three extra `Resolver::run_once` rounds: how much each resolved,
    /// and what still survives after it. A survivor cleared by an extra
    /// round means the drain contract (`while run_once() > 0`) returned
    /// early; one that survives all three is an intent-leak invariant
    /// failure — ESCALATE either way.
    extra_rounds: Vec<ExtraRound>,
    /// The `/sys/txn` records still present at capture (the resolver's
    /// own truncation state).
    sys_txn_span: Vec<Pair>,
    /// The status-table entries of the txns the survivors name:
    /// (id, status, gen, released, intent_count, last_removal_counter).
    status: Vec<(
        nucleus_txn::TxnId,
        Option<nucleus_txn::TxnStatus>,
        u64,
        bool,
        i64,
        u64,
    )>,
}

/// Runs one exact step-1c scene (the repro above: fresh core, real
/// pipeline under `StepClock`, insert + 255 blocker-style update commits,
/// each followed by the full `resolve_all` drain) and returns the M-1
/// evidence if any non-`/sys/` intent survives the drain, `None` if the
/// scene is clean.
fn hunt_one(row: &[u8]) -> Option<ResidueEvidence> {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    core.set_row_locks(TestRowLocks::new());
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    pipeline.set_clock(Arc::new(StepClock(AtomicU64::new(0))));
    commit_insert(&core, &mut pipeline, row, &u64v(0));
    for i in 1..=255u64 {
        commit_update_row(&core, &mut pipeline, row, i);
    }

    let survivors = live_intents(&core);
    if survivors.is_empty() {
        return None;
    }
    // The M-1 residue: capture everything before touching anything else.
    let decoded: Vec<Survivor> = survivors
        .iter()
        .map(|(k, v)| (k.clone(), v.clone(), decode_intent(v).map(|i| i.txn)))
        .collect();
    let mut extra_rounds = Vec::with_capacity(3);
    for _ in 0..3 {
        let resolved = ok(Resolver::run_once(&core));
        extra_rounds.push((resolved, live_intents(&core)));
    }
    let sys_txn_span = sys_span(&core)
        .into_iter()
        .filter(|(k, _)| k.starts_with(&sys_txn_prefix()))
        .collect();
    let mut ids: Vec<nucleus_txn::TxnId> = Vec::new();
    for (_, _, id) in decoded.iter() {
        if let Ok(id) = id {
            if !ids.contains(id) {
                ids.push(*id);
            }
        }
    }
    let status = ids
        .into_iter()
        .map(|id| {
            let e = core.status.entry(id);
            (
                id,
                e.map(|e| e.status),
                e.map_or(0, |e| e.gen),
                e.is_some_and(|e| e.released),
                e.map_or(0, |e| e.intent_count),
                e.map_or(0, |e| e.last_removal_counter),
            )
        })
        .collect();
    Some(ResidueEvidence {
        survivors: decoded,
        extra_rounds,
        sys_txn_span,
        status,
    })
}

/// The M-1 flake hunt (C-DIAG1fix2): the exact step-1c scene in a bounded
/// loop, on the lookout for the gate's one-in-~1400 observation — one
/// **non-`/sys/`** intent surviving the resolver drain
/// (`count_intents == 1` after 256 commits). Each scene runs on its own
/// thread under a `recv_timeout` per-run bound, so a stuck scene trips the
/// bound instead of hanging the loop. On the first residue the hunt prints
/// the [`ResidueEvidence`] bundle and fails: that is an engine escalation
/// (intent leak past a drain, or a too-weak drain contract), not a rig
/// fix. If the loop completes clean, the tally is the deliverable and M-1
/// closes as unreproduced-in-bounds.
///
/// Knobs: `NUCLEUS_DIAG1_HUNT_ITERS` (default 2000),
/// `NUCLEUS_DIAG1_HUNT_TIMEOUT_MS` (default 30 000),
/// `NUCLEUS_DIAG1_HUNT_HOGS` (default 0; CPU-hog threads held for the
/// whole loop, "under load" for cheap).
#[test]
#[ignore = "C-DIAG1fix2 flake hunt: bounded loop, minutes; run explicitly with the env knobs"]
fn diag1_step1c_residue_hunt() {
    let iters = env_usize("NUCLEUS_DIAG1_HUNT_ITERS", 2000);
    let timeout_ms = env_usize("NUCLEUS_DIAG1_HUNT_TIMEOUT_MS", 30_000);
    let hogs = env_usize("NUCLEUS_DIAG1_HUNT_HOGS", 0);
    let per_run = Duration::from_millis(timeout_ms as u64);
    let (stop, hog_handles) = spawn_hogs(hogs);
    let row = b"/t/r/1".to_vec();
    println!(
        "DIAG1fix2 hunt: {iters} iterations, {timeout_ms} ms per-run bound, \
         {hogs} hog threads"
    );
    let start = Instant::now();
    for i in 0..iters {
        let (tx, rx) = std::sync::mpsc::channel();
        let row = row.clone();
        let scene = std::thread::spawn(move || {
            let _ = tx.send(hunt_one(&row));
        });
        match rx.recv_timeout(per_run) {
            Ok(None) => {}
            Ok(Some(evidence)) => {
                println!("M-1 RESIDUE at iteration {i}:");
                for (k, v, owner) in &evidence.survivors {
                    println!(
                        "  survivor key={k:02x?} value={v:02x?} \
                         decode_intent->txn: {owner:?}"
                    );
                }
                for (n, (resolved, still)) in evidence.extra_rounds.iter().enumerate() {
                    println!(
                        "  extra round {}: resolved {resolved}, still \
                         alive: {}",
                        n + 1,
                        still.len()
                    );
                    for (k, v) in still {
                        println!(
                            "    alive after round {}: key={k:02x?} \
                             value={v:02x?}",
                            n + 1
                        );
                    }
                }
                println!("  /sys/txn span at capture: {:02x?}", evidence.sys_txn_span);
                println!("  status entries: {:?}", evidence.status);
                stop.store(true, Ordering::Relaxed);
                for h in hog_handles {
                    if let Err(e) = h.join() {
                        std::panic::resume_unwind(e);
                    }
                }
                panic!(
                    "M-1 reproduced at iteration {i} of {iters}: a non-/sys/ \
                     intent survived the resolver drain — engine escalation, \
                     evidence above"
                );
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                stop.store(true, Ordering::Relaxed);
                println!(
                    "M-1 hunt: iteration {i} blew the {timeout_ms} ms per-run \
                     bound (scene thread stuck; evidence unavailable — the \
                     bound is the finding)"
                );
                panic!("M-1 hunt: per-run bound exceeded at iteration {i}");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                stop.store(true, Ordering::Relaxed);
                // The scene thread died before reporting: surface its panic.
                if let Err(p) = scene.join() {
                    std::panic::resume_unwind(p);
                }
                panic!("M-1 hunt: iteration {i} ended without a result");
            }
        }
    }
    let elapsed = start.elapsed();
    println!(
        "DIAG1fix2 hunt: {iters} clean scenes in {elapsed:.1?} ({}/iter, \
         {hogs} hogs) — M-1 unreproduced in bounds",
        elapsed.as_millis() / (iters as u128).max(1)
    );
    stop.store(true, Ordering::Relaxed);
    for h in hog_handles {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
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
