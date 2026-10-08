//! Fault: an `OrderedKv` wrapper that models the WAL and crashes (C-K3).
//!
//! Every write reaches the live inner store at once, so it is visible as in a
//! real engine. A `Durability::No` write is also queued as unsynced. A
//! `Durability::Yes` write, `sync_wal` or `ingest_sorted` (a durability barrier
//! here, like an engine's ingest with its manifest sync) makes the whole queue
//! durable. `crash` rebuilds the inner store from its base plus the durable log
//! plus a prefix of the queue: it drops a suffix of unsynced batches, never a
//! gap and never part of a batch (C-T0 I-WAL-ORDER).
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
    durable: Vec<Logged>,
    unsynced: Vec<Batch>,
    gc: Option<Arc<dyn GcFilter>>,
    watermark: Option<u64>,
}

enum Logged {
    Write(Batch),
    Ingest(Vec<(Key, Value)>),
}

impl<K: OrderedKv> Fault<K> {
    /// Wraps `initial`. `rebuild` must return a store in the same state as
    /// `initial` was when passed in (the crash base: empty, or a checkpoint).
    pub fn new(initial: K, rebuild: impl Fn() -> Result<K> + Send + Sync + 'static) -> Self {
        Self {
            rebuild: Box::new(rebuild),
            st: Mutex::new(State {
                live: initial,
                durable: Vec::new(),
                unsynced: Vec::new(),
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
        self.lock().unsynced.len()
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
        let keep = keep.min(st.unsynced.len());
        let fresh = (self.rebuild)()?;
        if let Some(f) = &st.gc {
            fresh.set_gc_filter(Box::new(SharedFilter(Arc::clone(f))));
        }
        if let Some(w) = st.watermark {
            fresh.set_gc_watermark(w)?;
        }
        for entry in &st.durable {
            match entry {
                Logged::Write(b) => fresh.write(b.clone(), Durability::No)?,
                Logged::Ingest(e) => fresh.ingest_sorted(&mut e.iter().cloned())?,
            }
        }
        for b in &st.unsynced[..keep] {
            fresh.write(b.clone(), Durability::No)?;
        }
        fresh.sync_wal()?;
        let kept: Vec<Batch> = st.unsynced.drain(..).take(keep).collect();
        st.durable.extend(kept.into_iter().map(Logged::Write));
        st.live = fresh;
        Ok(keep)
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
            Durability::No => st.unsynced.push(batch),
            Durability::Yes => {
                make_durable(&mut st);
                st.durable.push(Logged::Write(batch));
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
        make_durable(&mut st);
        st.durable.push(Logged::Ingest(entries));
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
    let queued = std::mem::take(&mut st.unsynced);
    st.durable.extend(queued.into_iter().map(Logged::Write));
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
