//! C-T0 §5.5: `ROLLBACK TO SAVEPOINT`.

use nucleus_kv::OrderedKv;

use crate::boot::Core;
use crate::latch::latch_prefix_of;
use crate::removal::{remove_intent, RemovalMode};
use crate::txn::Txn;
use crate::{Seq, TxnError};

impl<K: OrderedKv> Core<K> {
    /// `ROLLBACK TO SAVEPOINT s` (§5.5), exactly:
    ///
    /// - For every distinct key in the write-set log with an entry at
    ///   `seq >= s`: `remove_intent(.., DropLayersFrom(s))` under that
    ///   entry's latch prefix (seed 25). The restored top layer keeps its
    ///   data, `data_seq` and `lock` (seed 16).
    /// - Shared row locks taken at `seq >= s` are released, each under its
    ///   key's latch (seed 46).
    /// - Queued end-of-statement / deferred checks and AFTER-trigger
    ///   events tagged `>= s` are discarded.
    /// - Relation and advisory locks taken at `seq >= s` go through
    ///   `ReleaseHook::release_from` (C-T2b; no-op until then).
    /// - The txn's wake generation is bumped and all its waiters woken
    ///   (seed 26) **only if the rollback released something** — dropped
    ///   or removed a layer, or released a shared row, relation or
    ///   advisory lock. A rollback that released nothing cannot unblock
    ///   any waiter, and a bump would still remove the waiters' edges
    ///   (§6 "Waking"), so two restarting ON CONFLICT statements would
    ///   keep waking each other and never leave a stable cycle for the
    ///   deadlock check (I-LIVE (b), ESCALATE-1). Discarding queued
    ///   checks and events does not count as a release: nobody waits on
    ///   events. `seq` never goes back.
    ///
    /// **Invariant (I-LIVE a):** a waiter on T can only be unblocked by T
    /// releasing something, by T ending, or by T's commit becoming
    /// visible. Every release path still bumps — commit step 5 (§3), abort
    /// (§7.1), every in-memory lock release (§6), and a `rollback_to`
    /// that dropped a layer or a lock — so no waiter stays parked past
    /// the end of a releasing actor's sequence.
    ///
    /// The write-set log keeps its entries: stale entries make later
    /// removals no-ops (§7.3 step 2), and an entry whose layer may still
    /// exist must never be dropped.
    pub fn rollback_to(&self, txn: &Txn, s: Seq) -> Result<(), TxnError> {
        // Whether this rollback released anything a waiter can observe.
        // The layer test over-approximates on purpose — entries are
        // logged before their write, so a key with an entry at `seq >= s`
        // had a layer written by this txn — because under-approximating is
        // the forbidden direction (I-LIVE a: a released waiter must be
        // woken).
        let mut released = false;
        let keys = txn.write_set_keys();
        for (key, prefix) in keys {
            if txn.write_set().iter().any(|(q, k, _)| *q >= s && k == &key) {
                remove_intent(
                    self,
                    &key,
                    latch_prefix_of(&key, prefix),
                    txn.id,
                    RemovalMode::DropLayersFrom(s),
                )?;
                released = true;
            }
        }
        // Shared row locks taken at seq >= s, each under its key's latch
        // (§5.5, §6; seed 46). C-T2 rework 7b: the latch is
        // `latch_key(key, prefix)` with the prefix the grant ran under
        // (from the lock table), never plain `&key`.
        let shared = self.row_locks().keys_of(txn.id, s);
        if !shared.is_empty() {
            released = true;
        }
        for (key, prefix) in shared {
            let lk = latch_prefix_of(&key, prefix).unwrap_or(&key);
            let _latch = self.latches.lock(lk);
            self.row_locks().release(&key, txn.id, s);
        }
        // Queued checks and AFTER-trigger events tagged >= s. Discarding
        // them is not a release: nobody waits on events.
        txn.discard_events_from(s);
        // Relation and advisory locks taken after the savepoint (§6).
        if self.release_hook().release_from(txn.id, s) {
            released = true;
        }
        // Bump and wake only if something was released (seed 26): the
        // ordering is unchanged — the bump comes after every release (§6
        // "Release") — but a rollback that released nothing bumps nothing,
        // or its waiters' edges would be deleted without any of their
        // waits being satisfiable (ESCALATE-1's livelock).
        if released {
            self.bump_and_wake(txn.id)?;
        }
        Ok(())
    }
}
