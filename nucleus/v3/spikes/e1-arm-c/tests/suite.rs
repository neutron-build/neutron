//! Card X-3 Work 10: the behaviour suite on arm (c).
//!
//! Four runs, separate `#[test]`s in one binary — safe since X-1a-F3
//! made the suite's hooks per-engine, which `tests/hooks.rs` of e1-mvcc
//! proves for engines racing in one binary:
//! - the suite with the default 64 MiB undo budget (no spill);
//! - the suite again with **`undo_budget_bytes = 4096`**, so every
//!   scenario that supersedes versions (S2, S4, S5, S8, S11b, S12, S13)
//!   runs with spilled undo and reads through the spill file;
//! - the S13 differential with a pool of **64 frames**, so the tree
//!   itself runs through buffer-pool eviction;
//! - the S13 differential again with the small pool **and** an undo
//!   budget of **0** — every record spills on append (verdict X-3,
//!   non-blocking note) — so the mid-schedule `gc()` of its `Gc` ops
//!   runs over spilled records, closing the gap where an over-trim of
//!   the spill index alone was caught only by S12 in the low-budget
//!   `run_all` (per-seed S13 engines never cross a 4096-byte budget).
//!
//! A fifth, focused test pins non-vacuity: with the small budget the
//! log really spills, readers at old and mid snapshots read spilled
//! records correctly, and `gc()` reclaims them once no snapshot needs
//! them (the SPILL-LOSES catcher of the card's evidence list).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use e1_arm_c::ArmC;
use e1_mvcc::api::{Isolation, TxnEngine, Wait};
use e1_mvcc::suite;

/// A fresh scratch directory per test (no new dependencies: `std` only).
/// The engines remove their own spill files on drop; the test removes
/// the directory.
fn tmp_dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "e1-arm-c-suite-{tag}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::create_dir_all(&d);
    d
}

/// Work 10, run 1: `run_all` with the default 64 MiB undo budget.
#[test]
fn arm_c_passes_the_suite() {
    let dir = tmp_dir("default");
    let failures = suite::run_all(&|| ArmC::new(4096, 8192, 64 << 20, &dir));
    assert!(failures.is_empty(), "failures: {failures:#?}");
    let _ = std::fs::remove_dir_all(dir);
}

/// Work 10, run 2: the same suite with `undo_budget_bytes = 4096`, so
/// spill is exercised end to end.
#[test]
fn arm_c_passes_the_suite_with_spill() {
    let dir = tmp_dir("spill");
    let failures = suite::run_all(&|| ArmC::new(4096, 8192, 4096, &dir));
    assert!(failures.is_empty(), "failures: {failures:#?}");
    let _ = std::fs::remove_dir_all(dir);
}

/// Work 10, run 3: the S13 differential with a pool of 64 frames.
#[test]
fn arm_c_s13_differential_with_a_small_pool() {
    let dir = tmp_dir("s13");
    let failures = suite::s13_differential(&|| ArmC::new(64, 8192, 64 << 20, &dir));
    assert!(failures.is_empty(), "failures: {failures:#?}");
    let _ = std::fs::remove_dir_all(dir);
}

/// Work 10, run 4 (verdict X-3, non-blocking note): the S13 differential
/// with the 64-frame pool and an undo budget of **0** — every undo
/// record spills to the file the moment it is appended — so the
/// mid-schedule `gc()` of the `Gc` ops runs over spilled records: any
/// trim that over-reaches the spill index while a snapshot still needs
/// a spilled record surfaces as an engine-vs-reference mismatch (the
/// differential's oracle closes the gap the low-budget `run_all` alone
/// left: an index-only over-trim used to be caught by S12 only).
#[test]
fn arm_c_s13_differential_with_a_small_pool_and_spill() {
    let dir = tmp_dir("s13-spill");
    let failures = suite::s13_differential(&|| ArmC::new(64, 8192, 0, &dir));
    assert!(failures.is_empty(), "failures: {failures:#?}");
    let _ = std::fs::remove_dir_all(dir);
}

/// Non-vacuity of the spill path (and the card's SPILL-LOSES catcher):
/// with a 4096-byte budget, 200 updates of one key spill most of their
/// undo records to the file; a reader at the seed snapshot and a reader
/// that began mid-history both read **spilled** records and must see
/// exactly their snapshot's value; a fresh reader sees the newest value;
/// and once every reader ends, `gc()` reclaims all of it (bytes > 0,
/// `versions` back to 1, undo bytes zero).
#[test]
fn undo_spill_is_exercised_and_reads_are_correct() {
    const UPDATES: u64 = 200;
    let dir = tmp_dir("spill-check");
    let e = ArmC::new(256, 8192, 4096, &dir);
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        e.put(&mut seed, b"k", b"original", Wait::NoWait).unwrap();
        e.commit(seed).unwrap();
    }
    // The long reader pins snapshot ts 1 (the seed value).
    let mut long_reader = e.begin(Isolation::RepeatableRead);
    let mut mid_reader = Option::None;
    for i in 1..=UPDATES {
        // Half-way through, a second reader pins the mid snapshot; its
        // value (be(100)'s successor chain) lives in the spill file.
        if i == UPDATES / 2 + 1 {
            mid_reader = Some(e.begin(Isolation::RepeatableRead));
        }
        let mut t = e.begin(Isolation::ReadCommitted);
        e.put(&mut t, b"k", &i.to_be_bytes(), Wait::NoWait).unwrap();
        e.commit(t).unwrap();
    }
    let stats = e.stats();
    assert!(
        stats.undo_spilled_bytes > 0,
        "the small budget must have spilled undo records (got {stats:?})"
    );
    assert_eq!(
        e.get(&mut long_reader, b"k").unwrap(),
        Some(b"original".to_vec()),
        "the seed-snapshot reader reads spilled undo records"
    );
    let mut mid = mid_reader.expect("opened half-way");
    let want_mid = (UPDATES / 2).to_be_bytes();
    assert_eq!(
        e.get(&mut mid, b"k").unwrap(),
        Some(want_mid.to_vec()),
        "the mid-snapshot reader reads a spilled record"
    );
    e.abort(mid);
    e.abort(long_reader);
    let reclaimed = e.gc();
    assert!(
        reclaimed > 0,
        "gc after the readers ended reclaims the undo"
    );
    let stats = e.stats();
    assert_eq!(stats.versions, 1, "only the tree value remains");
    assert_eq!(stats.undo_spilled_bytes, 0, "the spill file is empty");
    assert_eq!(stats.undo_bytes, 0, "the memory log is empty");
    let mut fresh = e.begin(Isolation::ReadCommitted);
    assert_eq!(
        e.get(&mut fresh, b"k").unwrap(),
        Some(UPDATES.to_be_bytes().to_vec()),
        "the newest value survived the gc"
    );
    e.abort(fresh);
    let _ = std::fs::remove_dir_all(dir);
}
