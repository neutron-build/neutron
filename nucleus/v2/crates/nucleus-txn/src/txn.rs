//! C-T0 §1/§5.5: the txn handle. `Txn` carries the id, the isolation level,
//! the per-txn command counter `seq` (never decreases, including across
//! `ROLLBACK TO`), the write-set log `(seq, key)` with one entry per layer
//! pushed or modified (in memory here; spilling is C-T4's problem), and a
//! cancel flag settable from another thread (§6).
//!
//! The C-T2 write path calls [`Txn::log_write`] and
//! [`Core::count_placement`](crate::boot::Core::count_placement) before each
//! placement (§5.1: "log and count both before the write"), so the write-set
//! log names every layer `ROLLBACK TO` must drop and every intent abort
//! cleanup must remove, and `intent_count` never undercounts (I-COUNT).

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use nucleus_kv::Key;

use crate::boot::Core;
use crate::{Seq, TxnId};

/// Isolation levels (§1): RC with PostgreSQL semantics, RR = SI,
/// SERIALIZABLE = SSI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// The cancel flag of one session's txn (§6): cancel requests and timeouts
/// set the flag; the session acts on it at its next check point. Setting the
/// flag also wakes the session if it is parked (lock wait), so a clone of
/// this handle is enough to cancel from another thread.
#[derive(Default)]
pub struct CancelFlag {
    flag: AtomicBool,
    /// The parker the session is currently parked on, if any. Cancel stores
    /// `None` after unparking so a stale parker is never notified twice.
    parked: Mutex<Option<Arc<dyn crate::wait::Parker>>>,
}

impl CancelFlag {
    /// Sets the flag and wakes a parked session (§6).
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
        let parked = {
            let mut p = self.lock_parked();
            p.take()
        };
        if let Some(p) = parked {
            p.unpark();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    fn lock_parked(&self) -> MutexGuard<'_, Option<Arc<dyn crate::wait::Parker>>> {
        self.parked.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A handle to a txn's cancel flag, cloned from [`Txn::cancel_handle`] and
/// usable from another thread.
#[derive(Clone, Default)]
pub struct CancelHandle(Arc<CancelFlag>);

impl CancelHandle {
    pub fn cancel(&self) {
        self.0.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}

/// One in-flight txn (§1). Owned by its session; the cancel handle is the
/// only part other threads touch.
pub struct Txn {
    /// This txn's id (dense per epoch, §1).
    pub id: TxnId,
    pub isolation: Isolation,
    /// Per-txn command counter. Never decreases; `ROLLBACK TO` keeps it.
    seq: AtomicU32,
    /// The write-set log (§5.5): `(seq, key)` per layer pushed or modified,
    /// once per `(seq, key)` (§5.1).
    pub(crate) write_set: Arc<Mutex<Vec<(Seq, Key)>>>,
    pub(crate) cancel: Arc<CancelFlag>,
}

impl Txn {
    pub(crate) fn new(id: TxnId, isolation: Isolation) -> Txn {
        Txn {
            id,
            isolation,
            seq: AtomicU32::new(0),
            write_set: Arc::new(Mutex::new(Vec::new())),
            cancel: Arc::new(CancelFlag::default()),
        }
    }

    /// Allocates the next command seq: strictly above every seq used so far
    /// (§5.5: `ROLLBACK TO` does not give it back).
    pub fn next_seq(&self) -> Seq {
        let s = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        // fetch_add wraps only past u32::MAX; a txn that ran 4G commands is
        // not a wrap bug, it is exhaustion.
        s
    }

    /// The current command seq (the last one handed out).
    pub fn seq(&self) -> Seq {
        self.seq.load(Ordering::SeqCst)
    }

    /// Appends `(seq, key)` to the write-set log, once per `(seq, key)`
    /// (§5.1): a command that modifies an existing layer in place logs the
    /// same pair it logged when the layer was pushed, so the log names every
    /// layer exactly once. C-T2 calls this before each placement write.
    pub fn log_write(&self, seq: Seq, key: &[u8]) {
        let mut ws = self.lock_write_set();
        if !ws.iter().any(|&(s, ref k)| s == seq && k.as_slice() == key) {
            ws.push((seq, key.to_vec()));
        }
    }

    /// The write-set log, copied out.
    pub fn write_set(&self) -> Vec<(Seq, Key)> {
        self.lock_write_set().clone()
    }

    /// The distinct keys of the write-set log, in first-written order: what
    /// abort cleanup and the resolver must touch.
    pub fn write_set_keys(&self) -> Vec<Key> {
        let ws = self.lock_write_set();
        let mut keys: Vec<Key> = Vec::new();
        for (_, k) in ws.iter() {
            if !keys.iter().any(|e| e == k) {
                keys.push(k.clone());
            }
        }
        keys
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// A shareable handle to this txn's cancel flag (§6): setting it also
    /// wakes a parked session.
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle(Arc::clone(&self.cancel))
    }

    pub(crate) fn lock_write_set(&self) -> MutexGuard<'_, Vec<(Seq, Key)>> {
        self.write_set
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl<K: nucleus_kv::OrderedKv> Core<K> {
    /// Begins a txn (§1): allocates its id and returns the session handle.
    pub fn begin(&self, isolation: Isolation) -> Txn {
        Txn::new(self.status.begin(), isolation)
    }

    /// `intent_count(T) += 1` before a placement write (§5.1, I-COUNT). The
    /// C-T2 write path calls this next to [`Txn::log_write`].
    pub fn count_placement(&self, txn: &Txn) -> Result<(), crate::TxnError> {
        self.status.note_intent_placed(txn.id)
    }
}
