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
    ///   (seed 26). `seq` never goes back.
    ///
    /// The write-set log keeps its entries: stale entries make later
    /// removals no-ops (§7.3 step 2), and an entry whose layer may still
    /// exist must never be dropped.
    pub fn rollback_to(&self, txn: &Txn, s: Seq) -> Result<(), TxnError> {
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
            }
        }
        // Shared row locks taken at seq >= s, each under its key's latch
        // (§5.5, §6; seed 46). C-T2 rework 7b: the latch is
        // `latch_key(key, prefix)` with the prefix the grant ran under
        // (from the lock table), never plain `&key`.
        for (key, prefix) in self.row_locks().keys_of(txn.id, s) {
            let lk = latch_prefix_of(&key, prefix).unwrap_or(&key);
            let _latch = self.latches.lock(lk);
            self.row_locks().release(&key, txn.id, s);
        }
        // Queued checks and AFTER-trigger events tagged >= s.
        txn.discard_events_from(s);
        // Relation and advisory locks taken after the savepoint (§6).
        self.release_hook().release_from(txn.id, s);
        // Bump and wake (seed 26): waiters re-run §5; spurious wakeups are
        // harmless.
        self.bump_and_wake(txn.id)?;
        Ok(())
    }
}
