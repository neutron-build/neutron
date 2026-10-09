//! C-T0 §1/§5.5: the txn handle. `Txn` carries the id, the isolation level,
//! the per-txn command counter `seq` (never decreases, including across
//! `ROLLBACK TO`), the write-set log `(seq, key, latch_prefix)` with one
//! entry per layer pushed or modified (in memory here; spilling is C-T4's
//! problem), queued end-of-statement / deferred-check / AFTER-trigger
//! events (§5.3, §5.5; opaque here), and a cancel flag settable from
//! another thread (§6).
//!
//! The C-T2 write path calls [`Txn::log_write`] and
//! [`Core::count_placement`](crate::boot::Core::count_placement) before each
//! placement (§5.1: "log and count both before the write"), so the write-set
//! log names every layer `ROLLBACK TO` must drop and every intent abort
//! cleanup must remove, and `intent_count` never undercounts (I-COUNT).
//! Every entry carries its key's latch prefix (§5.0), so every removal path
//! (abort cleanup, the resolver, `ROLLBACK TO`) latches `latch_key(k)`.

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

    /// Publishes the parker the session is about to park on, so
    /// [`CancelFlag::cancel`] can unpark it (§6: setting the flag also wakes
    /// a parked session). Callers re-check [`CancelFlag::is_cancelled`]
    /// after this store: a cancel that ran between the last check and the
    /// store is then still observed before the park.
    pub(crate) fn set_parked(&self, parker: Arc<dyn crate::wait::Parker>) {
        *self.lock_parked() = Some(parker);
    }

    /// Clears the published parker (every return path of a wait).
    pub(crate) fn clear_parked(&self) {
        *self.lock_parked() = None;
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

/// One write-set log entry (§5.5): `(seq, key, latch_prefix)`.
pub(crate) type WriteSetEntry = (Seq, Key, Option<usize>);

/// A queued event (§5.3, §5.5), tagged with the seq that queued it.
pub(crate) type QueuedEvent = (Seq, Vec<u8>);

/// One in-flight txn (§1). Owned by its session; the cancel handle is the
/// only part other threads touch.
pub struct Txn {
    /// This txn's id (dense per epoch, §1).
    pub id: TxnId,
    pub isolation: Isolation,
    /// Per-txn command counter. Never decreases; `ROLLBACK TO` keeps it.
    seq: AtomicU32,
    /// The write-set log (§5.5): `(seq, key, latch_prefix)` per layer pushed
    /// or modified, once per `(seq, key)` (§5.1). The latch prefix (§5.0) is
    /// `Some(n)` when the key is a deferrable `/i/{idx}/{key}{pk}` entry
    /// whose latch key is `key[..n]`, `None` when the key latches itself.
    pub(crate) write_set: Arc<Mutex<Vec<WriteSetEntry>>>,
    /// Queued events (§5.3, §5.5): end-of-statement and deferred checks and
    /// AFTER-trigger events, tagged with the seq that queued them. Opaque
    /// here; the SQL layer reads them through [`Txn::take_events`].
    events: Arc<Mutex<Vec<QueuedEvent>>>,
    pub(crate) cancel: Arc<CancelFlag>,
}

impl Txn {
    pub(crate) fn new(id: TxnId, isolation: Isolation) -> Txn {
        Txn {
            id,
            isolation,
            seq: AtomicU32::new(0),
            write_set: Arc::new(Mutex::new(Vec::new())),
            events: Arc::new(Mutex::new(Vec::new())),
            cancel: Arc::new(CancelFlag::default()),
        }
    }

    /// Allocates the next command seq: strictly above every seq used so far
    /// (§5.5: `ROLLBACK TO` does not give it back). Exhaustion at
    /// `u32::MAX` is an error, never a wrap: a wrapped seq would reuse a
    /// command id and break I-HALLOWEEN.
    pub fn next_seq(&self) -> Result<Seq, crate::TxnError> {
        let mut cur = self.seq.load(Ordering::SeqCst);
        loop {
            if cur == u32::MAX {
                return Err(crate::TxnError::Invariant(format!(
                    "txn {:?} exhausted the seq space (u32::MAX)",
                    self.id
                )));
            }
            match self
                .seq
                .compare_exchange(cur, cur + 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Ok(cur + 1),
                Err(c) => cur = c,
            }
        }
    }

    /// The current command seq (the last one handed out).
    pub fn seq(&self) -> Seq {
        self.seq.load(Ordering::SeqCst)
    }

    /// Appends `(seq, key, latch_prefix)` to the write-set log, once per
    /// `(seq, key)` (§5.1): a command that modifies an existing layer in
    /// place logs the same pair it logged when the layer was pushed, so the
    /// log names every layer exactly once (seed 50). The latch prefix
    /// (§5.0) travels with the entry so every removal path — abort cleanup,
    /// the resolver, `ROLLBACK TO` — latches `latch_key(k)` (seed 25). The
    /// C-T2 write path calls this before each placement write.
    pub fn log_write(&self, seq: Seq, key: &[u8], latch_prefix: Option<usize>) {
        let mut ws = self.lock_write_set();
        if !ws
            .iter()
            .any(|&(s, ref k, _)| s == seq && k.as_slice() == key)
        {
            ws.push((seq, key.to_vec(), latch_prefix));
        }
    }

    /// The write-set log, copied out.
    pub fn write_set(&self) -> Vec<WriteSetEntry> {
        self.lock_write_set().clone()
    }

    /// The distinct keys of the write-set log, in first-written order, each
    /// with its latch prefix (§5.0): what abort cleanup, the resolver and
    /// `ROLLBACK TO` must touch, and under which latch.
    pub fn write_set_keys(&self) -> Vec<(Key, Option<usize>)> {
        let ws = self.lock_write_set();
        let mut keys: Vec<(Key, Option<usize>)> = Vec::new();
        for (_, k, p) in ws.iter() {
            if !keys.iter().any(|(e, _)| e == k) {
                keys.push((k.clone(), *p));
            }
        }
        keys
    }

    /// Takes a savepoint (§5.5): returns the seq that identifies it. The
    /// next command gets a seq greater than every seq used so far (`seq`
    /// never goes back, including across `ROLLBACK TO`).
    pub fn savepoint(&self) -> Result<Seq, crate::TxnError> {
        self.next_seq()
    }

    /// Queues an event tagged with the seq that queued it (§5.3, §5.5):
    /// end-of-statement and deferred constraint checks and AFTER-trigger
    /// events, opaque to this crate. `ROLLBACK TO s` discards those with
    /// `tag >= s`.
    pub fn queue_event(&self, tag: Seq, payload: Vec<u8>) {
        self.lock_events().push((tag, payload));
    }

    /// Drains the queued events (the SQL layer reads them at end of
    /// statement / commit).
    pub fn take_events(&self) -> Vec<QueuedEvent> {
        std::mem::take(&mut *self.lock_events())
    }

    /// Discards queued events with `tag >= s` (§5.5), keeping the rest.
    pub(crate) fn discard_events_from(&self, s: Seq) {
        self.lock_events().retain(|(t, _)| *t < s);
    }

    fn lock_events(&self) -> MutexGuard<'_, Vec<QueuedEvent>> {
        self.events.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// A shareable handle to this txn's cancel flag (§6): setting it also
    /// wakes a parked session.
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle(Arc::clone(&self.cancel))
    }

    pub(crate) fn lock_write_set(&self) -> MutexGuard<'_, Vec<WriteSetEntry>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_seq_errors_at_u32_max_instead_of_wrapping() {
        let txn = Txn::new(TxnId { epoch: 1, n: 1 }, Isolation::ReadCommitted);
        txn.seq.store(u32::MAX, Ordering::SeqCst);
        assert!(txn.next_seq().is_err(), "exhaustion must not wrap");
        // Still exhausted, never wrapped back to 0.
        assert!(txn.next_seq().is_err());
        assert_eq!(txn.seq(), u32::MAX);
    }
}
