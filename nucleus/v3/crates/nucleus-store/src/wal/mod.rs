//! The WAL record codec (card A-6a; S-STORE §12.2–§12.3): the record
//! framing and its four kinds, the six ops, encode and decode, the frame
//! checks the walk needs, the two-phase seal, and the placement rule that
//! keeps records inside segments. It is a pure codec: it does no I/O
//! (§2: all I/O goes through `nucleus-env`).
//!
//! # Invariants this module owns
//!
//! - **The byte layouts** in the two tables below: §12.2's framing, and
//!   §12.3's ops as card A-6a pins them under "Card A-6a pins the op
//!   encoding". Every integer is little-endian, as card A-2-0 pins every
//!   on-disk integer.
//! - **Every field after `crc` is under the CRC** (§12.2): `crc` is the
//!   CRC32C of bytes 12..`len`, so the chain fields `lsn`, `prev_lsn` and
//!   `prev_crc` (I-WAL-CHAIN) are covered with the rest.
//! - **A malformed record is `Corrupt`, never a panic** (§12.5; PLAN-v3
//!   §6.1). [`read_frame`] classifies what the walk may treat as a stop
//!   (a bad magic, a bad `len`, a bad CRC); [`decode_body`] refuses with
//!   `StoreError::Corrupt` (XX001) every record whose kind, kind's rules,
//!   payload or ops break §12.2, §12.3 or this card's pins. Every read is
//!   bounded, every sum checked.
//! - **Only canonical records decode.** Every field has one encoding (fixed
//!   widths, flag bits and presence bytes with no spare values), so a
//!   record that decodes encodes back to the same bytes.
//! - **The kinds' rules and the op order rule** (§7.1, §12.2, §12.3), one
//!   implementation each, refused by [`encode`] with `StoreError::Invalid`
//!   (a caller's bug) and by [`decode_body`] with `StoreError::Corrupt`.
//! - **Sealing at reservation is O(log len)** (§12.2's design note):
//!   [`encode`] does the O(len) work, the CRC of everything after the chain
//!   fields; [`Unsealed::seal`] writes the chain fields and finishes the
//!   CRC with a 20-byte CRC and a CRC32C combine, taking no lock, starting
//!   no I/O and waiting for nothing (§12.4).
//! - **Records never span segments** (§12.2): [`place`] puts a record at
//!   the append position only if it ends exactly at its segment's end or
//!   leaves room for a `SegmentEnd`, and otherwise places a `SegmentEnd`
//!   there and the record at the next segment's first LSN.
//!
//! # What it does not own
//!
//! Segment files, segment 0 and recycling, and the walk with its `lsn`,
//! boot-id and chain checks on a [`Frame`]'s fields (card A-6b); the
//! writer, the append critical section, the chain head, sealing in LSN
//! order and group commit (card A-6c); the open-chunk set and chunking
//! (card A-6d); recovery (card A-7d).
//!
//! # The record (§12.2)
//!
//! | offset | size | field |
//! |---|---|---|
//! | 0 | 4 | magic `NWAL` |
//! | 4 | 4 | `len`: bytes of the whole record |
//! | 8 | 4 | `crc`: CRC32C of bytes 12..`len` |
//! | 12 | 8 | `lsn` |
//! | 20 | 8 | `prev_lsn` (all-ones if none) |
//! | 28 | 4 | `prev_crc` (0 if none) |
//! | 32 | 1 | `kind`: 1 Commit, 2 Chunk, 3 CommitMarker, 4 SegmentEnd |
//! | 33 | 4 | `txn.boot` |
//! | 37 | 8 | `txn.n` |
//! | 45 | 8 | `commit_ts` |
//! | 53 | … | payload |
//!
//! Payloads: a Commit, its ops; a Chunk, `seq` u32 then its ops; a
//! CommitMarker, `nchunks` u32 (exactly 4 bytes); a SegmentEnd, nothing. A
//! `SegmentEnd` is therefore a bare 53-byte header.
//!
//! # The ops (§12.3; card A-6a's pin)
//!
//! A tag byte, then the fields in the spec's order. A byte string is a
//! `u32` length then its bytes; an optional `before` is a presence byte (0
//! absent, 1 present) then, if present, a byte string.
//!
//! | tag | op | bytes after the tag |
//! |---|---|---|
//! | 1 | CreateTree | `tree_id` u64, `kind` u8 |
//! | 2 | DropTree | `tree_id` u64, `w` u64 |
//! | 3 | Ddl | `bytes` (catalog-owned, opaque here) |
//! | 4 | Put | `tree_id` u64, `key`, `flags` u8 (bit 0 `key_changed`), `value`, `before?` |
//! | 5 | Delete | `tree_id` u64, `key`, `flags` u8 (bit 0 `moved`, bit 1 `revert`), `before?` |
//! | 6 | RequireCkpt | `ckpt_id` u64 |
//!
//! Every other flag bit is 0; tags 0 and 7–255 are not ops. A record's ops
//! are at most one `RequireCkpt`, which is then op 0; then any number of
//! `CreateTree`, `DropTree` and `Ddl` in any order; then any number of
//! `Put` and `Delete`. A `Delete` with `revert` occurs only in a Chunk. A
//! Commit or a Chunk may carry no op (card A-6a, open point 6).

mod op;
mod place;
mod record;
mod seal;

pub use op::Op;
pub use place::{Placement, max_record_len, place};
pub use record::{
    Body, Frame, FrameError, PREV_NONE, RECORD_HEADER_LEN, RECORD_MAGIC, Record, RecordKind,
    SEGMENT_END_LEN, TxnId, decode_body, encode, read_frame,
};
pub use seal::{Chain, Sealed, Unsealed};
