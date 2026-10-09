//! C-T0 §2.3/§7.2/§7.4: the in-memory status table, one entry per `TxnId`,
//! plus the boot epoch. Only `Committed` records are persisted (as
//! `/sys/txn/{TxnId}`); `Pending` and `Aborted` are in-memory only (§7).
//!
//! Two lookups with different contracts (§4):
//! - [`lookup_for_intent`][StatusTable::lookup_for_intent] — for the owner of
//!   an intent found in a view: never misses for a current-epoch txn (a miss
//!   is a fatal [`TxnError::Invariant`], I-TRUNC) and a missing older-epoch
//!   entry means `Aborted` (§7.2).
//! - [`lookup_remembered`][StatusTable::lookup_remembered] — for a remembered
//!   id (a wait re-check, a lock holder): a miss means "ended and released"
//!   (§7.4 truncates only released txns).

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use nucleus_kv::{Batch, Durability, OrderedKv};

use crate::encoding::sys_txn_key;
use crate::{Ts, TxnError, TxnId, TxnStatus};

/// One txn's status-table entry (§7.4): status, wake generation (§6), release
/// flag, live intent count (I-COUNT) and the view counter at its last intent
/// removal (§7.3 step 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusEntry {
    pub status: TxnStatus,
    /// Wake generation: bumped on every event that can unblock waiters (§6).
    pub gen: u64,
    /// Abort/commit cleanup has run (§7.4 condition 0).
    pub released: bool,
    /// Number of `k@INTENT` entries this txn owns in the KV (I-COUNT).
    pub intent_count: i64,
    /// `view_counter` at the last removal of one of this txn's intents.
    pub last_removal_counter: u64,
}

/// Result of [`StatusTable::lookup_remembered`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Remembered {
    /// The txn still has a status entry: its status and wake generation.
    Live(TxnStatus, u64),
    /// No entry: the txn ended and its release has run (§4, §7.4).
    Ended,
}

/// The status table. Guarded by one mutex; nested inside the registry mutex
/// by callers that must act atomically w.r.t. view registration (§7.3 step 4,
/// §7.4).
pub struct StatusTable {
    /// The map and the id allocator live under one mutex so `begin` is dense
    /// (no gap, no duplicate) without a second lock.
    st: Mutex<Inner>,
    epoch: u32,
}

#[derive(Default)]
struct Inner {
    map: BTreeMap<TxnId, StatusEntry>,
    next_n: u64,
}

impl StatusTable {
    pub(crate) fn new(epoch: u32) -> Self {
        StatusTable {
            st: Mutex::new(Inner {
                map: BTreeMap::new(),
                next_n: 1,
            }),
            epoch,
        }
    }

    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.st.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Boot (§7.2): load a persisted `Committed(ts)` record. Loaded records
    /// are older-epoch by construction (the epoch was incremented first), and
    /// released by definition.
    pub(crate) fn insert_committed(&self, id: TxnId, ts: Ts) {
        let mut st = self.lock();
        st.map.insert(
            id,
            StatusEntry {
                status: TxnStatus::Committed(ts),
                gen: 0,
                released: true,
                intent_count: 0,
                last_removal_counter: 0,
            },
        );
    }

    /// Allocates the next txn id: dense `n` per epoch (§1).
    pub fn begin(&self) -> TxnId {
        let mut st = self.lock();
        let id = TxnId {
            epoch: self.epoch,
            n: st.next_n,
        };
        st.next_n += 1;
        st.map.insert(
            id,
            StatusEntry {
                status: TxnStatus::Pending,
                gen: 0,
                released: false,
                intent_count: 0,
                last_removal_counter: 0,
            },
        );
        id
    }

    /// Status lookup for the owner of an intent found in a view (§4): a
    /// missing current-epoch entry is a fatal invariant violation (I-TRUNC);
    /// a missing older-epoch entry means `Aborted` (§7.2).
    pub fn lookup_for_intent(&self, id: TxnId) -> Result<TxnStatus, TxnError> {
        let st = self.lock();
        match st.map.get(&id) {
            Some(e) => Ok(e.status),
            None if id.epoch < self.epoch => Ok(TxnStatus::Aborted),
            None => Err(TxnError::Invariant(format!(
                "intent owner {id:?} (epoch {}) has no status entry",
                self.epoch
            ))),
        }
    }

    /// Status lookup for a remembered id (§4): a miss means "ended and
    /// released".
    pub fn lookup_remembered(&self, id: TxnId) -> Remembered {
        let st = self.lock();
        match st.map.get(&id) {
            Some(e) => Remembered::Live(e.status, e.gen),
            None => Remembered::Ended,
        }
    }

    /// Introspection: the whole entry, or `None` if the txn is not registered
    /// (ended and truncated).
    pub fn entry(&self, id: TxnId) -> Option<StatusEntry> {
        self.lock().map.get(&id).copied()
    }

    /// Sets `Committed(ts)` (§3 step 4). Only a current-epoch `Pending` txn
    /// can commit, and `Ts::ZERO` is never a commit ts.
    pub fn set_committed(&self, id: TxnId, ts: Ts) -> Result<(), TxnError> {
        let mut st = self.lock();
        match st.map.get_mut(&id) {
            Some(e) if e.status == TxnStatus::Pending && id.epoch == self.epoch => {
                if ts == Ts::ZERO {
                    return Err(TxnError::Invariant(format!(
                        "commit ts of {id:?} is Ts::ZERO"
                    )));
                }
                e.status = TxnStatus::Committed(ts);
                Ok(())
            }
            Some(e) => Err(TxnError::Invariant(format!(
                "set_committed({id:?}) on {:?} txn",
                e.status
            ))),
            None => Err(TxnError::Invariant(format!(
                "set_committed({id:?}) with no status entry"
            ))),
        }
    }

    /// Sets `Aborted` (§7.1). Only the owning session aborts, and only a
    /// `Pending` txn can abort.
    pub fn set_aborted(&self, id: TxnId) -> Result<(), TxnError> {
        let mut st = self.lock();
        match st.map.get_mut(&id) {
            Some(e) if e.status == TxnStatus::Pending => {
                e.status = TxnStatus::Aborted;
                Ok(())
            }
            Some(e) => Err(TxnError::Invariant(format!(
                "set_aborted({id:?}) on {:?} txn",
                e.status
            ))),
            None => Err(TxnError::Invariant(format!(
                "set_aborted({id:?}) with no status entry"
            ))),
        }
    }

    /// Bumps the wake generation (§6) and returns the new value.
    pub fn bump_gen(&self, id: TxnId) -> Result<u64, TxnError> {
        let mut st = self.lock();
        match st.map.get_mut(&id) {
            Some(e) => {
                e.gen += 1;
                Ok(e.gen)
            }
            None => Err(TxnError::Invariant(format!(
                "bump_gen({id:?}) with no status entry"
            ))),
        }
    }

    /// Marks the txn released (§7.4 condition 0): abort cleanup or commit
    /// step 5 has run.
    pub fn mark_released(&self, id: TxnId) -> Result<(), TxnError> {
        let mut st = self.lock();
        match st.map.get_mut(&id) {
            Some(e) => {
                e.released = true;
                Ok(())
            }
            None => Err(TxnError::Invariant(format!(
                "mark_released({id:?}) with no status entry"
            ))),
        }
    }

    /// `intent_count += 1` (§5.1: the count is raised before the intent
    /// write). Kept next to [`StatusTable::removal_bookkeeping`] so I-COUNT
    /// bookkeeping lives in one place.
    pub fn note_intent_placed(&self, id: TxnId) -> Result<(), TxnError> {
        let mut st = self.lock();
        match st.map.get_mut(&id) {
            Some(e) if id.epoch == self.epoch => {
                e.intent_count += 1;
                Ok(())
            }
            Some(_) => Err(TxnError::Invariant(format!(
                "note_intent_placed({id:?}) on an older-epoch txn"
            ))),
            None => Err(TxnError::Invariant(format!(
                "note_intent_placed({id:?}) with no status entry"
            ))),
        }
    }

    /// §7.3 step 4, the in-memory part: set `last_removal_counter` **first**,
    /// then decrement the count — only for a current-epoch txn (older-epoch
    /// txns have no count). Must be called under the registry mutex, after
    /// the removal batch was written, and only when a removal actually
    /// happened.
    pub fn removal_bookkeeping(&self, id: TxnId, view_counter: u64) -> Result<(), TxnError> {
        let mut st = self.lock();
        match st.map.get_mut(&id) {
            Some(e) => {
                e.last_removal_counter = view_counter;
                if id.epoch == self.epoch {
                    e.intent_count -= 1;
                }
                Ok(())
            }
            None if id.epoch < self.epoch => Ok(()),
            None => Err(TxnError::Invariant(format!(
                "removal bookkeeping for {id:?} with no status entry"
            ))),
        }
    }

    /// §7.4 conditions 0–2, exactly:
    /// 0. `released` (older-epoch txns are released by definition);
    /// 1. `intent_count == 0`; for an older-epoch txn the boot sweep must
    ///    have finished (`sweep_counter` set, §7.4);
    /// 2. every view open at the last removal has closed:
    ///    `min_view_counter > last_removal_counter` (older-epoch:
    ///    `> max(last_removal_counter, sweep_counter)`).
    ///
    /// Must be evaluated under the registry mutex together with the
    /// `min_view_counter`/`sweep_counter` reads.
    pub fn truncate_eligible(
        &self,
        id: TxnId,
        min_view_counter: u64,
        sweep_counter: Option<u64>,
    ) -> bool {
        let st = self.lock();
        let Some(e) = st.map.get(&id) else {
            return false;
        };
        if id.epoch >= self.epoch {
            // Current-epoch txn.
            e.released && e.intent_count == 0 && min_view_counter > e.last_removal_counter
        } else {
            // Older-epoch txn: released by definition, count unknown.
            match sweep_counter {
                None => false,
                Some(sweep) => min_view_counter > e.last_removal_counter.max(sweep),
            }
        }
    }

    /// Removes the entry and writes the `/sys/txn/{TxnId}` delete (§7.4).
    /// The delete is written after the removal batches, so by I-WAL-ORDER a
    /// crash can never keep the delete and lose the resolution. Returns
    /// `true` if an entry was removed.
    pub fn truncate<K: OrderedKv>(&self, id: TxnId, kv: &K) -> Result<bool, TxnError> {
        let removed = {
            let mut st = self.lock();
            st.map.remove(&id)
        };
        match removed {
            Some(e) => {
                if matches!(e.status, TxnStatus::Committed(_)) {
                    // Only Committed records are ever persisted (§2.3).
                    let mut batch = Batch::default();
                    batch.delete(sys_txn_key(id));
                    kv.write(batch, Durability::No).map_err(crate::kv_err)?;
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }
}
