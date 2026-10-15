//! Fault: an `OrderedKv` wrapper that models the WAL and crashes (C-K3).
//!
//! Every write reaches the live inner store at once, so it is visible as in a
//! real engine. A `Durability::No` write is also queued as unsynced. A
//! `Durability::Yes` write or `sync_wal` makes the whole queue durable.
//! `ingest_sorted` is durable on return but is **not** a barrier for earlier
//! unsynced writes: it is appended to the durable log without syncing the
//! queue, faithful to RocksDB's ingest (the ingested file is synced; earlier
//! WAL writes behind it are not), so a test can observe a missing sync
//! (C-T6e1, C-T0 §10). `crash` rebuilds the inner store from its base plus, in
//! original order, every durable op (synced writes and ingests) and a prefix
//! of the unsynced writes — each kept write replayed at its position relative
//! to the ingests (an ingest replaces a same-key entry: newest wins). It drops
//! a suffix of unsynced batches, never a gap and never part of a batch (C-T0
//! I-WAL-ORDER).
//!
//! The registered GC filter and watermark are re-installed on the rebuilt
//! store. Compaction is not logged: data a filter dropped may come back after a
//! crash until the next compaction, which GC tolerates (C-T0 §9).

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::{Batch, Durability, GcFilter, GcStream, Key, OrderedKv, Result, Value};

type Rebuild<K> = Box<dyn Fn() -> Result<K> + Send + Sync>;

pub struct Fault<K: OrderedKv> {
    rebuild: Rebuild<K>,
    st: Mutex<State<K>>,
}

struct State<K> {
    live: K,
    /// Every logged op in call order: writes (marked durable or not) and
    /// ingests (always durable). One log, not a durable list plus a queue,
    /// is what lets `crash` replay each kept unsynced write at its
    /// original position relative to the ingests.
    log: Vec<Logged>,
    gc: Option<Arc<dyn GcFilter>>,
    watermark: Option<u64>,
}

enum Logged {
    /// A write batch. `durable` is set by a `Durability::Yes` write, a
    /// `sync_wal`, or the replay of a crash that kept it.
    Write { batch: Batch, durable: bool },
    /// An ingest: durable on return, never a barrier for the writes above.
    Ingest(Vec<(Key, Value)>),
}

impl<K: OrderedKv> State<K> {
    /// Unsolicited writes a crash may still drop.
    fn unsynced_len(&self) -> usize {
        self.log
            .iter()
            .filter(|e| matches!(e, Logged::Write { durable: false, .. }))
            .count()
    }
}

impl<K: OrderedKv> Fault<K> {
    /// Wraps `initial`. `rebuild` must return a store in the same state as
    /// `initial` was when passed in (the crash base: empty, or a checkpoint).
    pub fn new(initial: K, rebuild: impl Fn() -> Result<K> + Send + Sync + 'static) -> Self {
        Self {
            rebuild: Box::new(rebuild),
            st: Mutex::new(State {
                live: initial,
                log: Vec::new(),
                gc: None,
                watermark: None,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<K>> {
        self.st.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Unsynced batches a crash may drop.
    pub fn unsynced_len(&self) -> usize {
        self.lock().unsynced_len()
    }

    /// Runs `f` on the live inner store (backend-specific ops such as compaction).
    pub fn with_live<R>(&self, f: impl FnOnce(&K) -> R) -> R {
        f(&self.lock().live)
    }

    /// Simulates a crash and restart that keeps the first `keep` unsynced
    /// batches (clamped) and drops the rest. Returns how many were kept.
    /// On error the store is unchanged.
    pub fn crash(&self, keep: usize) -> Result<usize> {
        let mut st = self.lock();
        let keep = keep.min(st.unsynced_len());
        let fresh = (self.rebuild)()?;
        if let Some(f) = &st.gc {
            fresh.set_gc_filter(Box::new(SharedFilter(Arc::clone(f))));
        }
        if let Some(w) = st.watermark {
            fresh.set_gc_watermark(w)?;
        }
        // Replay in original order: every durable op (synced writes and
        // ingests) and, at their original positions relative to the
        // ingests, the first `keep` unsynced writes. Replay into `new_log`
        // first so an error leaves the store (state and live) unchanged.
        let mut new_log: Vec<Logged> = Vec::with_capacity(st.log.len());
        let mut kept = 0usize;
        let mut replay = Ok(());
        for entry in &st.log {
            match entry {
                Logged::Ingest(entries) => {
                    replay = fresh.ingest_sorted(&mut entries.iter().cloned());
                    if replay.is_err() {
                        break;
                    }
                    new_log.push(Logged::Ingest(entries.clone()));
                }
                Logged::Write { batch, durable } => {
                    // A durable write always replays; an unsynced one only
                    // while it is inside the kept prefix.
                    if !*durable && kept >= keep {
                        continue; // a dropped suffix write
                    }
                    replay = fresh.write(batch.clone(), Durability::No);
                    if replay.is_err() {
                        break;
                    }
                    if !*durable {
                        kept += 1;
                    }
                    new_log.push(Logged::Write {
                        batch: batch.clone(),
                        durable: true,
                    });
                }
            }
        }
        replay?;
        fresh.sync_wal()?;
        st.log = new_log;
        st.live = fresh;
        Ok(kept)
    }

    /// `crash` with the kept count drawn deterministically from `seed`,
    /// uniformly over `0..=unsynced_len()`.
    pub fn crash_seeded(&self, seed: u64) -> Result<usize> {
        let n = self.unsynced_len() as u64;
        self.crash((splitmix64(seed) % (n + 1)) as usize)
    }
}

impl<K: OrderedKv> OrderedKv for Fault<K> {
    type Snap = K::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        let mut st = self.lock();
        st.live.write(batch.clone(), sync)?;
        match sync {
            Durability::No => st.log.push(Logged::Write {
                batch,
                durable: false,
            }),
            Durability::Yes => {
                make_durable(&mut st);
                st.log.push(Logged::Write {
                    batch,
                    durable: true,
                });
            }
        }
        Ok(())
    }

    fn sync_wal(&self) -> Result<()> {
        let mut st = self.lock();
        st.live.sync_wal()?;
        make_durable(&mut st);
        Ok(())
    }

    fn snapshot(&self) -> K::Snap {
        self.lock().live.snapshot()
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        self.lock().live.get_latest(key)
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        let entries: Vec<(Key, Value)> = entries.collect();
        let mut st = self.lock();
        st.live.ingest_sorted(&mut entries.iter().cloned())?;
        // Durable on return, but **not** a barrier: the unsynced queue
        // stays unsynced (RocksDB's ingest syncs the file, not the WAL
        // behind it; C-T6e1). Only its own entries become durable.
        st.log.push(Logged::Ingest(entries));
        Ok(())
    }

    fn checkpoint(&self, dir: &Path) -> Result<()> {
        self.lock().live.checkpoint(dir)
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        let shared: Arc<dyn GcFilter> = Arc::from(filter);
        let mut st = self.lock();
        st.live
            .set_gc_filter(Box::new(SharedFilter(Arc::clone(&shared))));
        st.gc = Some(shared);
    }

    fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
        let mut st = self.lock();
        st.live.set_gc_watermark(watermark)?;
        st.watermark = Some(watermark);
        Ok(())
    }
}

fn make_durable<K>(st: &mut State<K>) {
    for entry in &mut st.log {
        if let Logged::Write { durable, .. } = entry {
            *durable = true;
        }
    }
}

struct SharedFilter(Arc<dyn GcFilter>);

impl GcFilter for SharedFilter {
    fn begin(&self) -> Box<dyn GcStream> {
        self.0.begin()
    }
}

fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
