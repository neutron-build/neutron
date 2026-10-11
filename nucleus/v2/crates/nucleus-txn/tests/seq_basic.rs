//! C-T7 sequences — behaviour: first use, block boundaries, the boot rule,
//! durability of every `/sys/seq` write, and the error paths. Concurrency
//! lives in `seq_concurrent.rs`, crashes in `seq_crash.rs`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use nucleus_kv::{Batch, Durability, GcFilter, Key, KvError, MemKv, Op, OrderedKv, Result, Value};
use nucleus_txn::boot::Core;
use nucleus_txn::seq::{sys_seq_key, BLOCK};
use nucleus_txn::TxnError;

fn ok<T, E: std::fmt::Debug>(r: std::result::Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// Reads `/sys/seq/{id}` straight from the store.
fn hwm_of(kv: &impl OrderedKv, id: u64) -> Option<u64> {
    let v = ok(kv.get_latest(&sys_seq_key(id)));
    v.map(|bytes| {
        let mut b = [0u8; 8];
        b.copy_from_slice(&bytes);
        u64::from_be_bytes(b)
    })
}

/// An `OrderedKv` wrapper sharing one `MemKv` between the test and several
/// sequentially opened cores (reopen pattern). Clones share the store.
#[derive(Clone)]
struct SharedKv {
    inner: Arc<MemKv>,
}

impl SharedKv {
    fn new() -> SharedKv {
        SharedKv {
            inner: Arc::new(MemKv::new()),
        }
    }
}

impl OrderedKv for SharedKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
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

#[test]
fn first_use_creates_sequence_and_values_start_at_one() {
    let core = ok(Core::open(MemKv::new()));
    // The first call creates /sys/seq/7 = 0 synced, then reserves the
    // first block synced, then returns 1.
    assert_eq!(ok(core.seq_next(7)), 1);
    assert_eq!(ok(core.seq_next(7)), 2);
    assert_eq!(core.seq_reservations_for_tests(7).expect("id used"), 1);
    // Values 2..=BLOCK stay inside the first block: no new reservation.
    for expected in 3..=BLOCK as i64 {
        assert_eq!(ok(core.seq_next(7)), expected);
    }
    assert_eq!(core.seq_reservations_for_tests(7).expect("id used"), 1);
    // Value BLOCK+1 opens the second block.
    assert_eq!(ok(core.seq_next(7)), BLOCK as i64 + 1);
    assert_eq!(core.seq_reservations_for_tests(7).expect("id used"), 2);

    let kv = core.into_kv();
    assert_eq!(hwm_of(&kv, 7), Some(2 * BLOCK), "H = 2 blocks");
    // Another id was never touched: its key does not exist.
    assert!(ok(kv.get_latest(&sys_seq_key(8))).is_none());
}

#[test]
fn smaller_block_reserves_exactly_ceil_n_over_b() {
    let core = ok(Core::open(MemKv::new()));
    core.seq_set_block_for_tests(3);
    for expected in 1..=7i64 {
        assert_eq!(ok(core.seq_next(0)), expected);
    }
    assert_eq!(core.seq_reservations_for_tests(0).expect("id used"), 3);
    let kv = core.into_kv();
    assert_eq!(hwm_of(&kv, 0), Some(9));
}

#[test]
fn boot_rule_h_present_cursor_starts_at_h() {
    // Boot: H present → cursor starts at H; the whole reserved block may
    // have been lost (skips allowed). Pre-seed H = 500 directly in the KV.
    let kv = MemKv::new();
    let mut batch = Batch::default();
    batch.put(sys_seq_key(9), 500u64.to_be_bytes().to_vec());
    ok(kv.write(batch, Durability::Yes));
    let core = ok(Core::open(kv));
    assert_eq!(ok(core.seq_next(9)), 501, "first value is H + 1");
    assert_eq!(ok(core.seq_next(9)), 502);
    // A different id on the same store does not exist yet.
    assert_eq!(ok(core.seq_next(1)), 1);
}

#[test]
fn absent_h_creates_the_sequence_synced_at_zero() {
    let kv = SharedKv::new();
    let core = ok(Core::open(kv.clone()));
    assert!(ok(kv.get_latest(&sys_seq_key(42))).is_none());
    assert_eq!(ok(core.seq_next(42)), 1);
    let h = ok(kv.get_latest(&sys_seq_key(42))).expect("created");
    let mut b = [0u8; 8];
    b.copy_from_slice(&h);
    assert_eq!(h.len(), 8, "u64 value");
    assert_eq!(u64::from_be_bytes(b), BLOCK);
}

/// An `OrderedKv` wrapper that records every put with its durability, in
/// call order. Clones share the inner store and the log.
#[derive(Clone)]
struct RecDurKv {
    inner: Arc<MemKv>,
    log: Arc<Mutex<Vec<(Key, Value, Durability)>>>,
}

impl RecDurKv {
    fn new() -> RecDurKv {
        RecDurKv {
            inner: Arc::new(MemKv::new()),
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn puts_of(&self, key: &[u8]) -> Vec<(Value, Durability)> {
        self.log
            .lock()
            .expect("log")
            .iter()
            .filter(|(k, _, _)| k.as_slice() == key)
            .map(|(_, v, d)| (v.clone(), *d))
            .collect()
    }
}

impl OrderedKv for RecDurKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        for op in &batch.ops {
            if let Op::Put(k, v) = op {
                self.log
                    .lock()
                    .expect("log")
                    .push((k.clone(), v.clone(), sync));
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

#[test]
fn every_seq_write_is_synced_and_precedes_handout() {
    let kv = RecDurKv::new();
    let core = ok(Core::open(kv.clone()));
    // By the time seq_next returns 1, both the creation write (H = 0) and
    // the first block's reservation (H = BLOCK) are already logged, each
    // Durability::Yes: the block is durable before any of its values is
    // handed out.
    assert_eq!(ok(core.seq_next(5)), 1);
    let writes = kv.puts_of(&sys_seq_key(5));
    assert_eq!(writes.len(), 2, "creation + first reservation");
    assert_eq!(writes[0].0, 0u64.to_be_bytes().to_vec());
    assert_eq!(writes[0].1, Durability::Yes, "creation write is synced");
    assert_eq!(writes[1].0, BLOCK.to_be_bytes().to_vec());
    assert_eq!(writes[1].1, Durability::Yes, "reservation write is synced");

    // Values 2..=BLOCK come from memory: no further /sys/seq write.
    for _ in 2..=BLOCK as i64 {
        ok(core.seq_next(5));
    }
    assert_eq!(kv.puts_of(&sys_seq_key(5)).len(), 2);

    // Value BLOCK+1 reserves the second block, again synced.
    assert_eq!(ok(core.seq_next(5)), BLOCK as i64 + 1);
    let writes = kv.puts_of(&sys_seq_key(5));
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[2].0, (2 * BLOCK).to_be_bytes().to_vec());
    assert_eq!(writes[2].1, Durability::Yes);
}

#[test]
fn corrupt_hwm_is_reported_not_panicked() {
    let kv = MemKv::new();
    let mut batch = Batch::default();
    batch.put(sys_seq_key(5), b"abc".to_vec());
    ok(kv.write(batch, Durability::Yes));
    let core = ok(Core::open(kv));
    assert!(matches!(core.seq_next(5), Err(TxnError::Corrupt(_))));
    // The error is stable, and no state was created.
    assert!(matches!(core.seq_next(5), Err(TxnError::Corrupt(_))));
}

#[test]
fn u64_exhaustion_errors_without_consuming_or_wrapping() {
    // H + B would overflow u64: no reservation, no value, H unchanged.
    let kv = SharedKv::new();
    let mut batch = Batch::default();
    batch.put(sys_seq_key(11), u64::MAX.to_be_bytes().to_vec());
    ok(kv.write(batch, Durability::Yes));
    let core = ok(Core::open(kv.clone()));
    core.seq_set_block_for_tests(4);
    assert!(matches!(core.seq_next(11), Err(TxnError::Invariant(_))));
    assert!(matches!(core.seq_next(11), Err(TxnError::Invariant(_))));
    assert_eq!(hwm_of(&kv, 11), Some(u64::MAX), "H unchanged");
}

#[test]
fn i64_boundary_seed_errors_per_call() {
    // Seed H = i64::MAX as u64: the cursor boots at H, the reservation
    // fits u64 and is persisted, and the first value of the new block is
    // i64::MAX + 1 — which cannot be returned. The cursor stands still:
    // every call reports the same error instead of wrapping.
    let kv = SharedKv::new();
    let mut batch = Batch::default();
    batch.put(sys_seq_key(13), (i64::MAX as u64).to_be_bytes().to_vec());
    ok(kv.write(batch, Durability::Yes));
    let core = ok(Core::open(kv.clone()));
    core.seq_set_block_for_tests(4);
    assert!(
        matches!(core.seq_next(13), Err(TxnError::Invariant(_))),
        "value i64::MAX + 1 cannot be returned"
    );
    assert_eq!(hwm_of(&kv, 13), Some(i64::MAX as u64 + 4));

    // C-T7r3: more calls than the block size. A mutant that advances the
    // cursor before the i64 check walks the cursor one per erroring call;
    // once it reaches H it reserves again, so H creeps past i64::MAX and
    // the reservation count grows. Pin all three: every call errors, the
    // persisted H never moves after the first error, and the reservation
    // count never grows (the overflowing call consumed nothing).
    for call in 1..=(3 * 4) {
        assert!(
            matches!(core.seq_next(13), Err(TxnError::Invariant(_))),
            "call {call}: the error is stable; the cursor stands still"
        );
        assert_eq!(
            hwm_of(&kv, 13),
            Some(i64::MAX as u64 + 4),
            "call {call}: an overflowing call writes nothing"
        );
        assert_eq!(
            core.seq_reservations_for_tests(13),
            Some(1),
            "call {call}: an overflowing call reserves nothing"
        );
    }
}

/// An `OrderedKv` wrapper whose writes fail once `fail` is set (reads keep
/// working). Clones share the inner store and the flag.
#[derive(Clone)]
struct FailKv {
    inner: Arc<MemKv>,
    fail: Arc<AtomicBool>,
}

impl OrderedKv for FailKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            Err(KvError::Backend("injected failure".into()))
        } else {
            self.inner.write(batch, sync)
        }
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

#[test]
fn kv_failure_propagates_and_never_hands_a_value_out() {
    let kv = FailKv {
        inner: Arc::new(MemKv::new()),
        fail: Arc::new(AtomicBool::new(false)),
    };
    let core = ok(Core::open(kv.clone()));
    core.seq_set_block_for_tests(1);

    // The creation write fails: the call errors, no value is returned.
    kv.fail.store(true, Ordering::SeqCst);
    assert!(matches!(core.seq_next(3), Err(TxnError::Kv(_))));
    kv.fail.store(false, Ordering::SeqCst);
    assert_eq!(ok(core.seq_next(3)), 1);

    // With block 1 every value reserves; a failed reservation write must
    // propagate rather than hand out a value the sync never covered.
    kv.fail.store(true, Ordering::SeqCst);
    assert!(matches!(core.seq_next(3), Err(TxnError::Kv(_))));
    kv.fail.store(false, Ordering::SeqCst);
    assert_eq!(
        ok(core.seq_next(3)),
        2,
        "the failed value is retried, not skipped into"
    );
    assert_eq!(ok(core.seq_next(3)), 3);
}

#[test]
fn second_store_starts_fresh_no_sidecar_aliasing() {
    // Two different stores in one process must not share in-memory seq
    // state, even though the second core may reuse the first core's
    // memory. Values on store B start at 1, and B's own /sys/seq key
    // advances on B.
    let core_a = ok(Core::open(MemKv::new()));
    core_a.seq_set_block_for_tests(4);
    assert_eq!(ok(core_a.seq_next(3)), 1);
    assert_eq!(ok(core_a.seq_next(3)), 2);
    assert_eq!(ok(core_a.seq_next(3)), 3);
    drop(core_a);

    let kv_b = SharedKv::new();
    let core_b = ok(Core::open(kv_b.clone()));
    core_b.seq_set_block_for_tests(4);
    assert_eq!(ok(core_b.seq_next(3)), 1, "store B's sequence starts fresh");
    assert_eq!(ok(core_b.seq_next(3)), 2);
    assert_eq!(hwm_of(&kv_b, 3), Some(4), "B reserved its own block");
}

#[test]
fn many_cores_in_sequence_each_boot_from_h() {
    // The reopen pattern the crash tests use, without any crash: each new
    // core reads H and abandons the unopened remainder of the reserved
    // block (skips allowed), never repeating a value.
    let kv = SharedKv::new();
    let mut seen = std::collections::BTreeSet::new();
    for round in 0..4u64 {
        let core = ok(Core::open(kv.clone()));
        core.seq_set_block_for_tests(2);
        let v = ok(core.seq_next(0));
        assert!(
            seen.insert(v),
            "round {round}: value {v} repeats an earlier round's {seen:?}"
        );
        let v2 = ok(core.seq_next(0));
        assert!(
            seen.insert(v2),
            "round {round}: value {v2} repeats {seen:?}"
        );
    }
    // 4 rounds, block 2: rounds hand out 1,2 then 3,4 then 5,6 then 7,8.
    assert_eq!(
        seen.iter().copied().collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7, 8]
    );
}

#[test]
fn reservation_count_helper_is_none_before_first_use() {
    let core = ok(Core::open(MemKv::new()));
    assert_eq!(core.seq_reservations_for_tests(99), None);
    ok(core.seq_next(99));
    assert_eq!(core.seq_reservations_for_tests(99).expect("id used"), 1);
}
