//! C-SIM work item 3: the simulator's KV. `SimKv` wraps `Fault<MemKv>` (LSM
//! mode) behind an `Arc` and records, in order, every `write` (its ops and
//! durability) and every `sync_wal`, each with a global position. The
//! simulator uses the record for I-WAL-ORDER and I-ACK and the `Arc` to
//! flush/compact and to crash.
//!
//! The record also tracks **eras** (each boot tags its events; ts values
//! restart at every reboot, so WAL-order and history checks group by era)
//! and **what a crash dropped**: durability is a prefix property here (a
//! `Durability::Yes` write or a `sync_wal` flushes everything queued
//! before it, and a crash keeps a prefix of the unsynced batches), so the
//! dropped events form one contiguous range. Checks after a reboot ask
//! [`SimKv::event_live`] whether a commit record survived.

use std::sync::{Arc, Mutex};

use nucleus_kv::fault::Fault;
use nucleus_kv::{Batch, Durability, GcFilter, Key, MemKv, Op, OrderedKv, Result, Value};
use nucleus_txn::encoding::{parse_sys_txn_key, SYS_PREFIX};
use nucleus_txn::{Ts, TxnId};

/// One recorded KV event at a global position.
#[derive(Debug, Clone)]
pub enum RecEvent {
    /// A write batch: its ops and whether it was `Durability::Yes`.
    Write { ops: Vec<Op>, synced: bool },
    /// A `sync_wal`.
    Sync,
}

/// A `/sys/txn/{id}` commit-record write seen by the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecCommit {
    pub id: TxnId,
    pub ts: Ts,
    /// The event position of the record batch.
    pub pos: usize,
    /// The boot era the record was written in.
    pub era: u32,
}

#[derive(Default)]
struct Record {
    events: Vec<RecEvent>,
    /// The boot era of each event position (parallel to `events`).
    eras: Vec<u32>,
    era: u32,
    /// Positions of write batches not yet flushed.
    unsynced: Vec<usize>,
    /// Everything at a position `< durable` is durable.
    durable: usize,
    /// The contiguous ranges of events crashes dropped (cumulative: each
    /// crash adds the range it dropped; a dropped event stays dropped).
    dead: Vec<(usize, usize)>,
    /// Commit records, in write order.
    commits: Vec<RecCommit>,
}

impl Record {
    fn note(&mut self, op: &Op, pos: usize) {
        if let Some((id, ts)) = commit_record_of(op) {
            self.commits.push(RecCommit {
                id,
                ts,
                pos,
                era: self.era,
            });
        }
    }

    fn crash(&mut self, keep: usize) {
        let keep = keep.min(self.unsynced.len());
        let new_durable = if keep == 0 {
            self.durable
        } else {
            self.unsynced[keep - 1] + 1
        };
        self.dead.push((new_durable, self.events.len()));
        self.durable = new_durable;
        // The kept batches are durable now (`Fault::crash` syncs them), so
        // nothing is unsynced: a later crash must count only batches queued
        // after this reboot, as `Fault` does.
        self.unsynced.clear();
    }
}

struct Inner {
    fault: Fault<MemKv>,
    rec: Mutex<Record>,
}

/// The simulator's `OrderedKv`: clones share the `Fault` and the record, so
/// each boot era's `Core` sees the same store across crashes.
#[derive(Clone)]
pub struct SimKv {
    inner: Arc<Inner>,
}

impl SimKv {
    /// A fresh LSM-mode store with an empty crash base.
    pub fn new() -> SimKv {
        SimKv {
            inner: Arc::new(Inner {
                fault: Fault::new(MemKv::lsm(), || Ok(MemKv::lsm())),
                rec: Mutex::new(Record::default()),
            }),
        }
    }

    fn with_rec<R>(&self, f: impl FnOnce(&mut Record) -> R) -> R {
        let mut r = self.inner.rec.lock().expect("simkv record");
        f(&mut r)
    }

    /// Tags subsequent events with `era` (each boot era; the preload runs
    /// in era 1).
    pub fn set_era(&self, era: u32) {
        self.with_rec(|r| r.era = era);
    }

    /// The record so far (a snapshot copy).
    pub fn record(&self) -> Vec<RecEvent> {
        self.with_rec(|r| r.events.clone())
    }

    /// The record's length (the position the next event will get).
    pub fn rec_len(&self) -> usize {
        self.with_rec(|r| r.events.len())
    }

    /// The boot era of the event at `pos`.
    pub fn era_at(&self, pos: usize) -> u32 {
        self.with_rec(|r| r.eras.get(pos).copied().unwrap_or(0))
    }

    /// Every commit record seen, in write order.
    pub fn commits(&self) -> Vec<RecCommit> {
        self.with_rec(|r| r.commits.clone())
    }

    /// Whether the event at `pos` survived every crash so far.
    pub fn event_live(&self, pos: usize) -> bool {
        self.with_rec(|r| !r.dead.iter().any(|&(a, b)| pos >= a && pos < b))
    }

    /// Runs `f` on the live inner `MemKv` (LSM steps: flush, compact).
    pub fn with_live<R>(&self, f: impl FnOnce(&MemKv) -> R) -> R {
        self.inner.fault.with_live(f)
    }

    /// Unsynchronised batches a crash may drop.
    pub fn unsynced_len(&self) -> usize {
        self.inner.fault.unsynced_len()
    }

    /// Simulates the crash: rebuild from the crash base plus the durable log
    /// plus the first `keep` unsynced batches, marking the dropped events
    /// dead in the record. Returns how many batches were kept.
    pub fn crash(&self, keep: usize) -> usize {
        let kept = self
            .inner
            .fault
            .crash(keep)
            .expect("simkv crash rebuild cannot fail on MemKv");
        self.with_rec(|r| r.crash(kept));
        kept
    }
}

/// Extracts `(TxnId, ts)` when `op` puts a `/sys/txn/{id}` commit record.
pub fn commit_record_of(op: &Op) -> Option<(TxnId, Ts)> {
    match op {
        Op::Put(k, v) if k.starts_with(SYS_PREFIX) => {
            let id = parse_sys_txn_key(k)?;
            if v.len() != 8 {
                return None;
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(v);
            Some((id, Ts(u64::from_be_bytes(b))))
        }
        _ => None,
    }
}

/// Whether `op` touches a data (non-`/sys/`) key.
pub fn op_is_data(op: &Op) -> bool {
    let key = match op {
        Op::Put(k, _) | Op::Delete(k) => k,
        Op::DeleteRange { start: lo, .. } => lo,
    };
    !key.starts_with(SYS_PREFIX)
}

impl OrderedKv for SimKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        self.inner.fault.write(batch.clone(), sync)?;
        self.with_rec(|r| {
            let pos = r.events.len();
            r.events.push(RecEvent::Write {
                ops: batch.ops.clone(),
                synced: sync == Durability::Yes,
            });
            r.eras.push(r.era);
            for op in &batch.ops {
                r.note(op, pos);
            }
            if sync == Durability::Yes {
                r.durable = pos + 1;
            } else {
                r.unsynced.push(pos);
            }
        });
        Ok(())
    }

    fn sync_wal(&self) -> Result<()> {
        self.inner.fault.sync_wal()?;
        self.with_rec(|r| {
            let pos = r.events.len();
            r.events.push(RecEvent::Sync);
            r.eras.push(r.era);
            r.durable = pos + 1;
            r.unsynced.clear();
        });
        Ok(())
    }

    fn snapshot(&self) -> Self::Snap {
        self.inner.fault.snapshot()
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        self.inner.fault.get_latest(key)
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        self.inner.fault.ingest_sorted(entries)
    }

    fn checkpoint(&self, dir: &std::path::Path) -> Result<()> {
        self.inner.fault.checkpoint(dir)
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        self.inner.fault.set_gc_filter(filter);
    }

    fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
        self.inner.fault.set_gc_watermark(watermark)
    }
}
