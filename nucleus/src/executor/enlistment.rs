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
//! max( xids in surviving COMMIT-record bodies,      (SQL side)
//!      xids tagged in surviving kv/doc/streams/graph/ts/datalog/columnar/
//!      blob/collections/cdc records )               (specialty side)
//! ```
//!
//! which is exactly the set of ids a future filter decision could consult.
//! After a full reclaim (SQL segments pruned AND every tagged log
//! snapshotted) the seed legitimately falls back to 0 and ids restart at 1 —
//! safe, because at that point no record referencing an old id exists
//! anywhere to be fooled.
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
/// and may never be renumbered. `Streams`, `Kv`, `Doc`, `Graph`, `Ts`,
/// `Datalog`, `Columnar` and `Blob` are enlisted by the landed S63 slices
/// (Blob's write-set rollback predates its slice; the bit now also drives
/// the tagged `blob.wal` records and its checkpoint gate). `Collections` is
/// wired but cannot fire today: M8's fail-loud boundary refuses collection
/// mutators inside a transaction, so its hook only ever returns
/// `XACT_AUTOCOMMIT` — the bit takes its seat now because renumbering
/// later would corrupt every body written in between. `Vector` is enlisted
/// by the S63 vector slice (2026-08-26: tagged vector.wal records, the
/// recovery filter, and the S7 gate). `Fts` and `Cdc` sit unused until
/// their slices land (the same reason; CDC is decided fire-and-forget, FTS
/// design-never).
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
    /// The columnar MODEL store (`columnar/columnar.wal`, written by
    /// `COLUMNAR_INSERT`). Takes the first free bit — 10 — rather than
    /// squeezing between landed slices: renumbering is corruption.
    Columnar = 1 << 10,
    /// The KV COLLECTIONS store (`kv/collections.wal`, written by
    /// `KV_HSET`/`KV_ZADD`/`KV_LPUSH`/... and the RESP twins). Distinct from
    /// [`Model::Kv`], which is the string store's `kv.wal`. Wired by the S63
    /// slice but unreachable today behind the M8 `refused_in_transaction`
    /// boundary; see `cross_model_before_collections`.
    Collections = 1 << 11,
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
