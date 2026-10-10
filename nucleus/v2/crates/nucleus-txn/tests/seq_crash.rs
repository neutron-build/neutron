//! C-T7 crash matrix (card Work 5) over `Fault<MemKv>` (§7.2 crash model,
//! §10 Sequences): for each crash point — (a) before the `H+B` sync,
//! (b) after the sync but before any value of the new block is returned,
//! (c) mid-block after some values were returned — and under both fault
//! modes (`sync_wal` honored before the crash / the unsynced suffix
//! dropped), reopening must never re-hand-out a value: every value
//! recorded before the crash is strictly less than the first value
//! returned after reboot. Values may skip; repeats are the bug.
//!
//! The I-ACK-style ordering is proven from both sides: values from a
//! block whose sync was dropped cannot exist (the sync precedes handout —
//! scenario `crash_before_reservation_*`), and a sync that completed is
//! never lost (`crash_after_reservation_*` keeps `H` even with every
//! unsynced write dropped).

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nucleus_kv::fault::Fault;
use nucleus_kv::{Batch, Durability, GcFilter, Key, KvError, MemKv, OrderedKv, Result, Value};
use nucleus_txn::boot::Core;
use nucleus_txn::seq::sys_seq_key;

fn ok<T, E: std::fmt::Debug>(r: std::result::Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// An `OrderedKv` wrapper sharing one `Fault<MemKv>` with the test. It
/// counts writes and can crash at a chosen write: `Before` fires the
/// crash instead of the write (the write never applies); `After` applies
/// the write, crashes, and then fails the write (the caller sees an error
/// although the write — here always a synced one — is durable). The
/// trigger is one-shot.
struct CrashKv {
    inner: Arc<Fault<MemKv>>,
    writes: Arc<AtomicU64>,
    arm: Arc<Mutex<Option<Armed>>>,
}

#[derive(Clone, Copy)]
enum Mode {
    Before,
    After,
}

#[derive(Clone, Copy)]
struct Armed {
    at: u64,
    keep: usize,
    mode: Mode,
}

impl Clone for CrashKv {
    fn clone(&self) -> Self {
        CrashKv {
            inner: Arc::clone(&self.inner),
            writes: Arc::clone(&self.writes),
            arm: Arc::clone(&self.arm),
        }
    }
}

impl CrashKv {
    fn new() -> CrashKv {
        CrashKv {
            inner: Arc::new(Fault::new(MemKv::new(), || Ok(MemKv::new()))),
            writes: Arc::new(AtomicU64::new(0)),
            arm: Arc::new(Mutex::new(None)),
        }
    }

    /// The next write will be the `at`-th since construction.
    fn arm(&self, at: u64, keep: usize, mode: Mode) {
        *self.arm.lock().expect("arm") = Some(Armed { at, keep, mode });
    }

    fn crash_now(&self, keep: usize) -> usize {
        ok(self.inner.crash(keep))
    }

    fn writes(&self) -> u64 {
        self.writes.load(Ordering::SeqCst)
    }

    fn hwm(&self, id: u64) -> Option<u64> {
        ok(self.inner.get_latest(&sys_seq_key(id))).map(|v| {
            let mut b = [0u8; 8];
            b.copy_from_slice(&v);
            u64::from_be_bytes(b)
        })
    }
}

impl OrderedKv for CrashKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        let n = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        let armed = *self.arm.lock().expect("arm");
        if let Some(a) = armed {
            if n == a.at {
                *self.arm.lock().expect("arm") = None; // one-shot
                return match a.mode {
                    Mode::Before => {
                        self.inner.crash(a.keep)?;
                        Err(KvError::Backend("crashed before the write".into()))
                    }
                    Mode::After => {
                        self.inner.write(batch, sync)?;
                        self.inner.crash(a.keep)?;
                        Err(KvError::Backend("crashed after the write".into()))
                    }
                };
            }
        }
        self.inner.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.inner.sync_wal()
    }

    fn snapshot(&self) -> Self::Snap {
        self.inner.snapshot()
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        self.inner.get_latest(key)
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        self.inner.ingest_sorted(entries)
    }

    fn checkpoint(&self, dir: &std::path::Path) -> Result<()> {
        self.inner.checkpoint(dir)
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        self.inner.set_gc_filter(filter);
    }

    fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
        self.inner.set_gc_watermark(watermark)
    }
}

/// Unsynced data writes around the sequence ops, so the unsynced queue is
/// non-empty and `keep` actually chooses what survives.
fn noise(core: &Core<CrashKv>, n: u64) {
    for i in 0..n {
        let mut batch = Batch::default();
        batch.put(format!("/t/9/noise-{i}").into_bytes(), b"n".to_vec());
        ok(core.write(batch, Durability::No));
    }
}

fn open(kv: &CrashKv, block: u64) -> Core<CrashKv> {
    let core = ok(Core::open(kv.clone()));
    core.seq_set_block_for_tests(block);
    core
}

/// Records `v` for `id`; a repeat inside one process fails at once.
fn record(recorded: &mut HashMap<u64, BTreeSet<i64>>, id: u64, v: i64) {
    assert!(
        recorded.entry(id).or_default().insert(v),
        "id {id}: value {v} handed out twice in one process"
    );
}

/// The card's core assertion: every value ever handed out for an id is
/// strictly less than the first value handed out after the reboot.
fn assert_no_repeat(recorded: &HashMap<u64, BTreeSet<i64>>, first: &HashMap<u64, i64>) {
    for (id, f) in first {
        if let Some(values) = recorded.get(id) {
            for v in values {
                assert!(
                    v < f,
                    "id {id}: value {v} from before the crash is not strictly less \
                     than the first value after reboot ({f}) — a value repeated"
                );
            }
        }
    }
}

// ---- (a) before the H+B sync ----------------------------------------------

/// Crash point (a), first use: the crash fires *before the creation
/// write* of a fresh sequence. Nothing exists afterwards; the sequence is
/// created from scratch after reboot and starts at 1.
#[test]
fn crash_before_creation_write() {
    for keep in [0, 2] {
        let kv = CrashKv::new();
        let core = open(&kv, 4);
        noise(&core, 2); // writes 2, 3 (write 1 = boot epoch)
        assert_eq!(kv.writes(), 3);

        kv.arm(4, keep, Mode::Before); // the creation write of id 0
        assert!(core.seq_next(0).is_err(), "the crash propagates");
        drop(core);

        assert_eq!(kv.hwm(0), None, "the sequence never came into existence");
        let core2 = open(&kv, 4);
        let first = ok(core2.seq_next(0));
        assert_eq!(first, 1, "a fresh sequence starts at 1 (keep={keep})");
        // Nothing was ever handed out before the crash; the empty era
        // still asserts against the first value after reboot.
        let firsts = HashMap::from([(0u64, first)]);
        assert_no_repeat(&HashMap::new(), &firsts);
    }
}

/// Crash point (a), first use: the creation write (H = 0, synced) is
/// applied, the crash fires before the first block's reservation. This
/// proves the card's creation ordering: the sequence exists durably at
/// H = 0 although no value was ever handed out.
#[test]
fn crash_before_first_reservation_sync() {
    for keep in [0, 2] {
        let kv = CrashKv::new();
        let core = open(&kv, 4);
        noise(&core, 2);
        assert_eq!(kv.writes(), 3);

        kv.arm(5, keep, Mode::Before); // creation applied (write 4), reservation crashes
        assert!(core.seq_next(0).is_err(), "no value is handed out");
        drop(core);

        assert_eq!(
            kv.hwm(0),
            Some(0),
            "the synced creation write survived (keep={keep})"
        );
        let core2 = open(&kv, 4);
        let first = ok(core2.seq_next(0));
        assert_eq!(
            first, 1,
            "the never-handed block is reserved anew (keep={keep})"
        );
        let firsts = HashMap::from([(0u64, first)]);
        assert_no_repeat(&HashMap::new(), &firsts);
    }
}

/// Crash point (a), mid-life: a full block was consumed, the crash fires
/// before the next block's reservation sync. No value of the new block
/// can exist, so after reboot the block is reserved again from the old H.
#[test]
fn crash_before_reservation_sync_mid_life() {
    for keep in [0, 3] {
        let kv = CrashKv::new();
        let core = open(&kv, 4);
        noise(&core, 3); // writes 2..4
        let mut recorded = HashMap::new();
        for expected in 1..=4i64 {
            let v = ok(core.seq_next(0));
            assert_eq!(v, expected);
            record(&mut recorded, 0, v);
        }
        assert_eq!(kv.writes(), 6); // epoch, 3 noise, creation, reservation

        kv.arm(7, keep, Mode::Before); // the reservation of H = 8
        assert!(core.seq_next(0).is_err(), "value 5 was never handed out");
        drop(core);

        assert_eq!(kv.hwm(0), Some(4), "H+B was never applied (keep={keep})");
        let core2 = open(&kv, 4);
        let first = ok(core2.seq_next(0));
        assert_eq!(
            first, 5,
            "the block (4, 8] is reserved anew; its old values were never handed out"
        );
        let firsts = HashMap::from([(0u64, first)]);
        assert_no_repeat(&recorded, &firsts);
        record(&mut recorded, 0, first);
    }
}

// ---- (b) after the sync, before any value of the new block is returned ----

/// Crash point (b): the reservation write applied and is durable, the
/// process dies before returning the block's first value. Even with every
/// unsynced write dropped the block survives (I-ACK-style), and none of
/// its values was handed out — after reboot the whole reserved block is
/// skipped (allowed), never re-handed.
#[test]
fn crash_after_reservation_sync_before_return() {
    for keep in [0, 3] {
        let kv = CrashKv::new();
        let core = open(&kv, 4);
        noise(&core, 3);
        let mut recorded = HashMap::new();
        for expected in 1..=4i64 {
            let v = ok(core.seq_next(0));
            assert_eq!(v, expected);
            record(&mut recorded, 0, v);
        }
        assert_eq!(kv.writes(), 6);

        kv.arm(7, keep, Mode::After); // H = 8 applies synced, crash, error
        assert!(
            core.seq_next(0).is_err(),
            "the value of the new block was never returned"
        );
        drop(core);

        assert_eq!(
            kv.hwm(0),
            Some(8),
            "the synced reservation survived even with keep=0 (keep={keep})"
        );
        let core2 = open(&kv, 4);
        let first = ok(core2.seq_next(0));
        assert_eq!(
            first, 9,
            "the reserved block (4, 8] is skipped, not re-handed"
        );
        for v in 5..=8 {
            assert!(
                !recorded[&0].contains(&v),
                "value {v} of the dropped-at-crash block existed before the crash"
            );
        }
        let firsts = HashMap::from([(0u64, first)]);
        assert_no_repeat(&recorded, &firsts);
        record(&mut recorded, 0, first);
    }
}

/// Crash point (b), first use: the creation write and the first block's
/// synced reservation both applied; the crash fires before value 1 is
/// returned. Values 1..=B never existed; after reboot the sequence starts
/// at B+1.
#[test]
fn crash_after_first_reservation_sync_before_return() {
    for keep in [0, 2] {
        let kv = CrashKv::new();
        let core = open(&kv, 4);
        noise(&core, 2);
        assert_eq!(kv.writes(), 3);

        kv.arm(5, keep, Mode::After); // creation + reservation applied, crash, error
        assert!(core.seq_next(0).is_err());
        drop(core);

        assert_eq!(
            kv.hwm(0),
            Some(4),
            "the first block is durable (keep={keep})"
        );
        let core2 = open(&kv, 4);
        let first = ok(core2.seq_next(0));
        assert_eq!(first, 5, "values 1..=4 never existed; they are skipped");
        let firsts = HashMap::from([(0u64, first)]);
        assert_no_repeat(&HashMap::new(), &firsts);
    }
}

// ---- (c) mid-block after some values were returned -------------------------

/// Crash point (c): some values of the current block were handed out;
/// the process crashes. Every keep of the unsynced queue (and the
/// `sync_wal`-honored mode) must reopen with every recorded value
/// strictly below the first value after reboot. Two ids run interleaved;
/// blocks end mid-air at the crash, so the end-of-block case (every value
/// of the block consumed) is covered by id 1.
#[test]
fn crash_mid_block_all_keeps_both_modes() {
    for sync_first in [false, true] {
        for keep_extra in [0, 1, usize::MAX] {
            let kv = CrashKv::new();
            let core = open(&kv, 8);

            // id 0: three of eight values; id 1: all eight values.
            let mut recorded: HashMap<u64, BTreeSet<i64>> = HashMap::new();
            for expected in 1..=3i64 {
                let v = ok(core.seq_next(0));
                assert_eq!(v, expected);
                record(&mut recorded, 0, v);
            }
            for expected in 1..=8i64 {
                let v = ok(core.seq_next(1));
                assert_eq!(v, expected);
                record(&mut recorded, 1, v);
            }
            // Noise AFTER the last synced write, so it really sits in the
            // unsynced queue the crash may drop (a Durability::Yes write
            // would drain it).
            noise(&core, 3);
            let unsynced = kv.inner.unsynced_len();
            assert_eq!(unsynced, 3, "the noise writes are queued unsynced");

            if sync_first {
                ok(kv.inner.sync_wal()); // fault mode: sync_wal honored
            }
            let keep = if sync_first {
                0
            } else {
                keep_extra.min(unsynced)
            };
            kv.crash_now(keep);
            drop(core);

            assert_eq!(
                kv.hwm(0),
                Some(8),
                "H was synced before any value (sync={sync_first})"
            );
            assert_eq!(kv.hwm(1), Some(8));
            let core2 = open(&kv, 8);
            let firsts =
                HashMap::from([(0u64, ok(core2.seq_next(0))), (1u64, ok(core2.seq_next(1)))]);
            for (id, f) in &firsts {
                assert_eq!(*f, 9, "id {id}: first value after reboot is H+1");
            }
            assert_no_repeat(&recorded, &firsts);
            for (id, f) in &firsts {
                record(&mut recorded, *id, *f);
            }

            // A second crash after another partial block, to chain eras.
            let v = ok(core2.seq_next(0));
            record(&mut recorded, 0, v);
            let v1 = ok(core2.seq_next(1));
            record(&mut recorded, 1, v1);
            noise(&core2, 2);
            let unsynced = kv.inner.unsynced_len();
            let keep2 = if sync_first {
                0
            } else {
                keep_extra.min(unsynced)
            };
            kv.crash_now(keep2);
            drop(core2);

            let core3 = open(&kv, 8);
            let firsts =
                HashMap::from([(0u64, ok(core3.seq_next(0))), (1u64, ok(core3.seq_next(1)))]);
            for (id, f) in &firsts {
                assert_eq!(*f, 17, "id {id}: the second block is skipped after reboot");
            }
            assert_no_repeat(&recorded, &firsts);
        }
    }
}

/// The I-ACK-style drop test, single-minded: values of the newest block
/// were handed out; the crash drops the whole unsynced suffix. Because
/// the block's sync preceded every handout, `H` survives and the first
/// value after reboot is strictly above every handed-out value. (A mutant
/// that hands out before the sync loses `H` here and re-hands values.)
#[test]
fn dropped_unsynced_writes_never_rehand_a_value() {
    for block in [2u64, 8] {
        let kv = CrashKv::new();
        let core = open(&kv, block);

        let mut recorded: HashMap<u64, BTreeSet<i64>> = HashMap::new();
        // Consume into the second block so a handed-out value (block 2's
        // first) is covered by the newest reservation.
        let mut expected = 0;
        for _ in 0..(block as i64 + 2) {
            expected += 1;
            let v = ok(core.seq_next(0));
            assert_eq!(v, expected);
            record(&mut recorded, 0, v);
        }
        // Unsynced writes around the handouts: the crash drops all of
        // them; H must survive regardless.
        noise(&core, 2);
        let unsynced = kv.inner.unsynced_len();
        assert_eq!(unsynced, 2);

        kv.crash_now(0); // drop every unsynced write
        drop(core);

        assert_eq!(
            kv.hwm(0),
            Some(2 * block),
            "the newest block's sync survived although all unsynced writes dropped"
        );
        let core2 = open(&kv, block);
        let first = ok(core2.seq_next(0));
        assert_eq!(first, 2 * block as i64 + 1);
        let firsts = HashMap::from([(0u64, first)]);
        assert_no_repeat(&recorded, &firsts);
        record(&mut recorded, 0, first);
    }
}

/// Crash points (a)+(c) combined with a seeded keep, mirroring the runner
/// style of `crash_matrix.rs`: a few deterministic keep values per point.
#[test]
fn crash_seeded_keeps_across_points() {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }
    }
    let mut rng = Rng(0x5EED_2026_0C07);
    for _ in 0..24u64 {
        let kv = CrashKv::new();
        let core = open(&kv, 4);

        let mut recorded: HashMap<u64, BTreeSet<i64>> = HashMap::new();
        let ids = [0u64, 7];
        // Hand out a seeded number of values per id (crossing block
        // boundaries sometimes), leave some unsynced writes queued, then
        // crash at a seeded keep (or after a sync_wal).
        for id in ids {
            let n = 1 + rng.next() % 9;
            for _ in 0..n {
                let v = ok(core.seq_next(id));
                record(&mut recorded, id, v);
            }
        }
        noise(&core, 2 + rng.next() % 3);
        let unsynced = kv.inner.unsynced_len();
        assert!(unsynced >= 2);
        let keep = if rng.next().is_multiple_of(3) {
            ok(kv.inner.sync_wal());
            0
        } else {
            (rng.next() % (unsynced as u64 + 1)) as usize
        };
        kv.crash_now(keep);
        drop(core);

        let core2 = open(&kv, 4);
        let mut firsts = HashMap::new();
        for id in ids {
            let f = ok(core2.seq_next(id));
            let h = kv.hwm(id).expect("the id exists; its H was synced");
            assert!(
                f > (h - 4) as i64,
                "id {id}: first-after {f} not inside the newest block"
            );
            firsts.insert(id, f);
        }
        assert_no_repeat(&recorded, &firsts);
    }
}
