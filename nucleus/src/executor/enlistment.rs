//! Coordinating transaction identity for cross-model atomicity (S63).
//!
//! The SQL engine's own transaction id (`DiskEngine::next_txn_id`) is minted
//! at COMMIT — long after every specialty write of the transaction has hit
//! its log — and, because its recovery floor is derived from *surviving* WAL
//! records, it can be REUSED after a checkpoint prunes segments and the
//! process restarts. Neither property is survivable for an id whose job is to
//! tag specialty records at write time and decide their fate at replay time:
//!
//! * minted-too-late means there is nothing to tag with;
//! * reusable means a stale specialty record from a previous incarnation of
//!   id 7 would match a fresh committed 7 and be RESURRECTED by the very
//!   filter built to discard it — the failure in the unsafe direction.
//!
//! `XactId` is that separate id: minted at `BEGIN`, never persisted as a
//! counter, and seeded at open above every id that any surviving record could
//! still reference. The seed is
//!
//! ```text
//! max( xids in surviving COMMIT-record bodies,   (SQL side)
//!      xids tagged in surviving streams records ) (specialty side)
//! ```
//!
//! which is exactly the set of ids a future filter decision could consult.
//! After a full reclaim (SQL segments pruned AND streams log snapshotted) the
//! seed legitimately falls back to 0 and ids restart at 1 — safe, because at
//! that point no record referencing an old id exists anywhere to be fooled.
//!
//! ## The commit-record body
//!
//! An enlisted transaction's COMMIT record carries ten extra bytes,
//! `[xact_id u64 LE][enlisted u16 LE]`, CRC-covered like the rest of the
//! record. A zero-body COMMIT record is a pre-S63 one and means "no
//! enlistment", so old data directories replay unchanged.

/// The reserved id carried by specialty records written OUTSIDE any explicit
/// transaction. Always kept by the recovery filter: its durability point is
/// its own log's fsync, and there is no commit record and never was one.
pub(crate) const XACT_AUTOCOMMIT: u64 = 0;

/// The fixed-width COMMIT-record body: `[xact_id u64 LE][enlisted u16 LE]`.
pub(crate) const XACT_BODY_LEN: usize = 10;

/// Which specialty models a transaction enlisted. One bit per model.
///
/// The discriminants are the on-disk contract inside the COMMIT-record body
/// and may never be renumbered. Only `Streams` is enlisted in the first
/// slice; the rest arrive one log per S4 slice, so they sit unused until
/// their slice lands (the alternative — renumbering later — would corrupt
/// every body written in between).
#[allow(dead_code)]
#[derive(Copy, Clone, Default, PartialEq, Eq)]
pub(crate) struct EnlistedSet(u16);

#[allow(dead_code)]
#[repr(u16)]
pub(crate) enum Model {
    Kv = 1 << 0,
    Doc = 1 << 1,
    Graph = 1 << 2,
    Fts = 1 << 3,
    Vector = 1 << 4,
    Ts = 1 << 5,
    Datalog = 1 << 6,
    Streams = 1 << 7,
    Blob = 1 << 8,
    Cdc = 1 << 9,
}

impl EnlistedSet {
    pub(crate) fn enlist(&mut self, model: Model) {
        self.0 |= model as u16;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub(crate) fn bits(&self) -> u16 {
        self.0
    }
}

/// Encode the COMMIT-record body for `xact`.
pub(crate) fn encode_xact_body(xact: u64, enlisted: &EnlistedSet) -> [u8; XACT_BODY_LEN] {
    let mut body = [0u8; XACT_BODY_LEN];
    body[..8].copy_from_slice(&xact.to_le_bytes());
    body[8..].copy_from_slice(&enlisted.bits().to_le_bytes());
    body
}
