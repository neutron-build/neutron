//! The C-T3 seam: SSI hooks (§8). C-T2 defines the trait and
//! [`NoSsiHook`]; the real SIREAD/conflict state is C-T3's.
//!
//! Call sites (§8.2, §8.4, §8.6): the read path's edges flow through
//! C-T1a's `ReadObserver`; the write path calls `before_point_read`
//! (before a view opens), `covers` (under the key's latch, §5.3's
//! SERIALIZABLE unique rule) and `on_data_placed` (after the latch is
//! released, §5.1); the commit pipeline wraps every channel enqueue in
//! `pre_commit` (§8.4) and reports aborts (`on_abort`, §8.6).
//!
//! `covers` may be called with the key's latch held: C-T3 implements it
//! without taking the registry mutex (the card's §5.1 note). Every other
//! hook runs outside any latch.

use crate::txn::Isolation;
use crate::{TxnError, TxnId};

/// The SSI hook (§8). C-T3 implements the real one; [`NoSsiHook`] is the
/// default.
pub trait SsiHook: Send + Sync {
    /// Whether `txn` holds a SIREAD covering `key` (§5.3's SERIALIZABLE
    /// unique rule). May be called under `latch(latch_key(key))`.
    fn covers(&self, txn: TxnId, key: &[u8]) -> bool;

    /// Registers a SIREAD on `key` for `txn` **before** the view the read
    /// runs through is opened (§8.1 I-SSI-ORDER). Called outside any latch.
    fn before_point_read(&self, txn: TxnId, key: &[u8]);

    /// §8.2 writer side: after a data-changing intent was placed, check
    /// SIREADs covering `key` held by other SERIALIZABLE txns (each gives
    /// `reader -> W`). Called after the latch is released; lock-only
    /// placements and shared locks never call it.
    fn on_data_placed(
        &self,
        writer: TxnId,
        isolation: Isolation,
        key: &[u8],
    ) -> Result<(), TxnError>;

    /// §8.4 pre-commit: the dangerous-structure check and prepare, with the
    /// commit-request enqueue **inside** the same critical section (commit
    /// order equals prepare order). `enqueue` puts the request on the
    /// commit channel; call it exactly once, inside the hook.
    fn pre_commit(
        &self,
        txn: TxnId,
        isolation: Isolation,
        enqueue: &mut dyn FnMut() -> Result<(), TxnError>,
    ) -> Result<(), TxnError>;

    /// §8.6: an aborted txn's SIREADs and edges are removed when it aborts.
    fn on_abort(&self, txn: TxnId);
}

/// The no-op hook: no SIREADs, no edges, commits always proceed.
#[derive(Default)]
pub struct NoSsiHook;

impl SsiHook for NoSsiHook {
    fn covers(&self, _txn: TxnId, _key: &[u8]) -> bool {
        false
    }

    fn before_point_read(&self, _txn: TxnId, _key: &[u8]) {}

    fn on_data_placed(
        &self,
        _writer: TxnId,
        _isolation: Isolation,
        _key: &[u8],
    ) -> Result<(), TxnError> {
        Ok(())
    }

    fn pre_commit(
        &self,
        _txn: TxnId,
        _isolation: Isolation,
        enqueue: &mut dyn FnMut() -> Result<(), TxnError>,
    ) -> Result<(), TxnError> {
        enqueue()
    }

    fn on_abort(&self, _txn: TxnId) {}
}
