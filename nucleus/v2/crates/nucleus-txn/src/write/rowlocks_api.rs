//! The C-T2b seam: the shared row-lock table (§6). Shared modes (KEY SHARE,
//! SHARE) never appear in intents (§2.1); they live in this in-memory table
//! keyed by logical key, tagged with the acquiring seq, and are checked
//! under the same latch as `k@INTENT`.
//!
//! C-T2 implements only the trait and [`NoRowLocks`]; the real table (with
//! the wait-for-graph edges and `lock_timeout`) is C-T2b's. Tests use a
//! Vec-backed double (`TestRowLocks` in `tests/common`).

use crate::{RowLockMode, Seq, TxnError, TxnId};
use nucleus_kv::Key;

/// The shared row-lock table (§6). `holders`, `grant` and `release` are
/// called only under `latch(latch_key(key))` (§5.0): the §5.1 loop reads
/// holders and grants under its latch; every release path (commit step 5,
/// abort, `ROLLBACK TO`) takes the key's latch around its call. A crash
/// aborts every holder, so the table is safe in memory.
pub trait RowLocks: Send + Sync {
    /// The holders of a shared lock on `key`: `(txn, mode, acquiring seq)`.
    /// Callers filter out ended txns themselves (§6: conflict checks ignore
    /// ended holders).
    fn holders(&self, key: &[u8]) -> Vec<(TxnId, RowLockMode, Seq)>;

    /// Records that `txn` holds `mode` on `key` from `seq` (§5.1).
    fn grant(&self, key: &[u8], txn: TxnId, mode: RowLockMode, seq: Seq) -> Result<(), TxnError>;

    /// Every key `txn` holds a shared lock on with `seq >= from_seq` (§5.5).
    fn keys_of(&self, txn: TxnId, from_seq: Seq) -> Vec<Key>;

    /// Drops `txn`'s shared locks on `key` with `seq >= from_seq` (§6, §5.5).
    /// The caller bumps the holder's wake generation after the removal.
    fn release(&self, key: &[u8], txn: TxnId, from_seq: Seq);
}

/// The default table: none installed. Holders are always empty, `keys_of`
/// is empty, `release` is a no-op, and `grant` is an invariant error — the
/// write path grants shared locks only through a real table (C-T2b; tests
/// install their double).
#[derive(Default)]
pub struct NoRowLocks;

impl RowLocks for NoRowLocks {
    fn holders(&self, _key: &[u8]) -> Vec<(TxnId, RowLockMode, Seq)> {
        Vec::new()
    }

    fn grant(
        &self,
        _key: &[u8],
        _txn: TxnId,
        _mode: RowLockMode,
        _seq: Seq,
    ) -> Result<(), TxnError> {
        Err(TxnError::Invariant(
            "no row lock table installed (shared row locks need C-T2b's RowLocks)".into(),
        ))
    }

    fn keys_of(&self, _txn: TxnId, _from_seq: Seq) -> Vec<Key> {
        Vec::new()
    }

    fn release(&self, _key: &[u8], _txn: TxnId, _from_seq: Seq) {}
}
