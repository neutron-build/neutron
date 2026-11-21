//! The record framing (S-STORE §12.2; card A-6a, Work 2, 4 and 5): the
//! 53-byte header, the four kinds and their rules, [`encode`], the frame
//! reader [`read_frame`] and the body decoder [`decode_body`].
//!
//! Invariants this file owns (the module doc has the table):
//! - the header's fields at their offsets, little-endian, and `len` equal
//!   to the number of bytes [`encode`] wrote;
//! - the kinds' rules, one implementation for both directions: a Commit, a
//!   Chunk and a CommitMarker have `txn.n ≥ 1`; a SegmentEnd has `txn.n =
//!   0` (§12.2); a Commit and a CommitMarker have a `commit_ts` that is
//!   neither 0 nor `u64::MAX` (§7.1); a Chunk and a SegmentEnd have
//!   `commit_ts` 0 (§12.2); a CommitMarker has `nchunks ≥ 1` (card A-6a,
//!   open point 6); a Chunk's payload starts with its 4-byte `seq`, a
//!   CommitMarker's is exactly its 4-byte `nchunks`, and a SegmentEnd's is
//!   empty;
//! - [`read_frame`] reads no byte at or after `len` and checks only the
//!   length, the magic, `len` and the CRC, in that order (§12.5's frame
//!   checks; the `lsn`, boot-id and chain checks are the walk's, card A-6b).

use crate::codec::{Reader, Writer, put_u32};
use crate::crc::crc32c;
use crate::store::StoreError;
use crate::wal::op::{self, Op};
use crate::wal::seal::Unsealed;

/// The record magic, the four bytes `NWAL` in file order (§12.2: `magic u32
/// "NWAL"`).
pub const RECORD_MAGIC: [u8; 4] = *b"NWAL";

/// The length of the record header, from the magic through `commit_ts`
/// (§12.2; the module doc's table). Every record is at least this long.
pub const RECORD_HEADER_LEN: usize = 53;

/// The length of a `SegmentEnd`, a bare header (§12.2): the least room a
/// segment's tail may leave after a record (§12.2, "Records never span
/// segments"; §12.5, SEG-TAIL-SHORT).
pub const SEGMENT_END_LEN: u64 = 53;

/// The chain fields of a record with no predecessor (§12.2: `prev_lsn`
/// "all-ones if none", `prev_crc` "0 if none"), as `(prev_lsn, prev_crc)`.
pub const PREV_NONE: (u64, u32) = (u64::MAX, 0);

/// Where `len` sits.
pub(crate) const LEN_AT: usize = 4;
/// Where `crc` sits.
pub(crate) const CRC_AT: usize = 8;
/// Where `lsn` sits: the first byte under the CRC and the first chain field.
pub(crate) const LSN_AT: usize = 12;
/// Where `prev_lsn` sits.
const PREV_LSN_AT: usize = 20;
/// Where `prev_crc` sits.
const PREV_CRC_AT: usize = 28;
/// Where `kind` sits: the first byte after the chain fields, where the part
/// of the CRC that [`encode`] computes starts (§12.2's design note).
pub(crate) const KIND_AT: usize = 32;

// The table, checked at compile time: each field starts where the one
// before it ends, and the header ends after `kind` (1), `txn.boot` (4),
// `txn.n` (8) and `commit_ts` (8).
const _: () = assert!(LEN_AT == RECORD_MAGIC.len());
const _: () = assert!(CRC_AT == LEN_AT + 4);
const _: () = assert!(LSN_AT == CRC_AT + 4);
const _: () = assert!(PREV_LSN_AT == LSN_AT + 8);
const _: () = assert!(PREV_CRC_AT == PREV_LSN_AT + 8);
const _: () = assert!(KIND_AT == PREV_CRC_AT + 4);
const _: () = assert!(RECORD_HEADER_LEN == KIND_AT + 1 + 4 + 8 + 8);
const _: () = assert!(SEGMENT_END_LEN == RECORD_HEADER_LEN as u64);

/// A record's kind (§12.2: `kind u8 1 Commit | 2 Chunk | 3 CommitMarker |
/// 4 SegmentEnd`). The discriminant is the kind byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RecordKind {
    /// One small transaction at its `commit_ts` (§12.3).
    Commit = 1,
    /// Part of a large transaction, written before its commit (§12.3,
    /// §12.7).
    Chunk = 2,
    /// The commit of a chunked transaction (§12.3).
    CommitMarker = 3,
    /// The end of a segment's records (§12.2, §12.5).
    SegmentEnd = 4,
}

impl RecordKind {
    /// The kind that byte `b` names, or `None` for 0 and 5–255, which name
    /// none (§12.2).
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Commit),
            2 => Some(Self::Chunk),
            3 => Some(Self::CommitMarker),
            4 => Some(Self::SegmentEnd),
            _ => None,
        }
    }

    /// The kind's byte (§12.2).
    pub fn byte(self) -> u8 {
        // A fieldless `#[repr(u8)]` enum: the cast is its discriminant.
        self as u8
    }
}

/// C-T0 v8's transaction id as a record carries it (§12.2: `txn (u32 boot
/// id, u64 n)`). Every record carries the id of the boot that wrote it; `n`
/// is at least 1, except in a `SegmentEnd`, whose `n` is 0 and names no
/// transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxnId {
    /// The writing boot's id (§12.2, §12.5).
    pub boot: u32,
    /// The transaction's number within its boot, from 1 (C-T0 v8 §2.5); 0
    /// in a `SegmentEnd`.
    pub n: u64,
}

/// A WAL record without its chain fields (§12.2, §12.3): what [`encode`]
/// takes and [`decode_body`] gives back. Its byte strings are borrowed, so
/// a decode copies nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<'a> {
    /// The record's transaction id (§12.2).
    pub txn: TxnId,
    /// The record's kind and payload (§12.3).
    pub body: Body<'a>,
}

/// A record's kind and payload (§12.3). The header's `commit_ts` is the
/// body's for a Commit and a CommitMarker, and 0 for a Chunk and a
/// SegmentEnd (§12.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body<'a> {
    /// `Commit{ops}`: one small transaction at `commit_ts` (§12.3).
    Commit {
        /// The commit timestamp, neither 0 nor `u64::MAX` (§7.1).
        commit_ts: u64,
        /// The ops, in §12.3's order.
        ops: Vec<Op<'a>>,
    },
    /// `Chunk{seq u32, ops}`: part of a large transaction (§12.3, §12.7).
    Chunk {
        /// The chunk's sequence number (§12.3).
        seq: u32,
        /// The ops, in §12.3's order.
        ops: Vec<Op<'a>>,
    },
    /// `CommitMarker{nchunks u32}` at `commit_ts`: commits the chunked
    /// transaction (§12.3).
    CommitMarker {
        /// The commit timestamp, neither 0 nor `u64::MAX` (§7.1).
        commit_ts: u64,
        /// The number of the transaction's chunks, at least 1 (card A-6a,
        /// open point 6).
        nchunks: u32,
    },
    /// A `SegmentEnd`: no payload (§12.2).
    SegmentEnd,
}

impl Body<'_> {
    /// The body's kind (§12.2).
    pub fn kind(&self) -> RecordKind {
        match self {
            Self::Commit { .. } => RecordKind::Commit,
            Self::Chunk { .. } => RecordKind::Chunk,
            Self::CommitMarker { .. } => RecordKind::CommitMarker,
            Self::SegmentEnd => RecordKind::SegmentEnd,
        }
    }

    /// The header's `commit_ts`: the body's for a Commit and a
    /// CommitMarker, 0 for a Chunk and a SegmentEnd (§12.2).
    pub fn commit_ts(&self) -> u64 {
        match self {
            Self::Commit { commit_ts, .. } | Self::CommitMarker { commit_ts, .. } => *commit_ts,
            Self::Chunk { .. } | Self::SegmentEnd => 0,
        }
    }
}

/// Why [`read_frame`] refused a buffer: the codec's share of §12.5's stop
/// list, a classification for the walk (card A-6b), not an error type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameError {
    /// The first four bytes are not [`RECORD_MAGIC`] (§12.5: "a bad
    /// magic").
    Magic,
    /// The buffer is shorter than a header, or `len` is shorter than a
    /// header or runs past the buffer (§12.5: "`len` running past the
    /// segment").
    Length,
    /// `crc` is not the CRC32C of bytes 12..`len` (§12.5: "a bad ...
    /// CRC").
    Crc,
}

/// A record as [`read_frame`] found it, every header field as written and
/// the payload borrowed (§12.2). Only the length, the magic, `len` and the
/// CRC have been checked; the walk (card A-6b) checks `lsn`, the boot id
/// and the chain on these fields, and [`decode_body`] the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Frame<'a> {
    /// Bytes of the whole record (§12.2).
    pub len: u32,
    /// CRC32C of bytes 12..`len` (§12.2).
    pub crc: u32,
    /// The record's own start position (§12.2).
    pub lsn: u64,
    /// The start of the record appended immediately before it; all-ones if
    /// none (§12.2).
    pub prev_lsn: u64,
    /// The `crc` field of that record; 0 if none (§12.2).
    pub prev_crc: u32,
    /// The kind byte as written, not yet checked (§12.2).
    pub kind: u8,
    /// The transaction id as written, not yet checked (§12.2).
    pub txn: TxnId,
    /// The header's `commit_ts` as written, not yet checked (§12.2).
    pub commit_ts: u64,
    /// The bytes after the header, to `len` (§12.2).
    pub payload: &'a [u8],
}

/// The kinds' rules on a header (§7.1, §12.2), one implementation for
/// [`encode`] (as `Invalid`) and [`decode_body`] (as `Corrupt`): the reason
/// a header breaks them.
fn check_header(kind: RecordKind, txn: TxnId, commit_ts: u64) -> Result<(), String> {
    match kind {
        RecordKind::Commit | RecordKind::Chunk | RecordKind::CommitMarker if txn.n == 0 => {
            return Err(format!(
                "a {kind:?} has txn.n 0; a transaction's n is at least 1 (S-STORE §12.2)"
            ));
        }
        RecordKind::SegmentEnd if txn.n != 0 => {
            return Err(format!(
                "a SegmentEnd has txn.n {}, not 0 (S-STORE §12.2)",
                txn.n
            ));
        }
        _ => {}
    }
    match kind {
        RecordKind::Commit | RecordKind::CommitMarker
            if commit_ts == 0 || commit_ts == u64::MAX =>
        {
            Err(format!(
                "a {kind:?} has commit_ts {commit_ts}, which is never a commit timestamp (S-STORE §7.1)"
            ))
        }
        RecordKind::Chunk | RecordKind::SegmentEnd if commit_ts != 0 => Err(format!(
            "a {kind:?} has commit_ts {commit_ts}, not 0 (S-STORE §12.2)"
        )),
        _ => Ok(()),
    }
}

/// A CommitMarker's `nchunks` rule (card A-6a, open point 6): a marker
/// commits a chunked transaction, which has at least one chunk.
fn check_nchunks(nchunks: u32) -> Result<(), String> {
    if nchunks == 0 {
        Err("a CommitMarker has nchunks 0; a chunked transaction has at least one chunk (card A-6a)".into())
    } else {
        Ok(())
    }
}

/// The length [`encode`] will write for `body`, checked before anything is
/// written: `Invalid` when a byte string or the record is longer than
/// `u32::MAX` (§12.2: `len u32`), so an impossible record is refused before
/// its bytes are copied.
fn encoded_len(body: &Body<'_>) -> Result<usize, StoreError> {
    let too_long = || {
        StoreError::Invalid(
            "a WAL record is longer than u32::MAX bytes (S-STORE §12.2: len u32)".into(),
        )
    };
    let payload = match body {
        Body::Commit { ops, .. } => op::list_len(ops)?,
        Body::Chunk { ops, .. } => op::list_len(ops)?.checked_add(4).ok_or_else(too_long)?,
        Body::CommitMarker { .. } => 4,
        Body::SegmentEnd => 0,
    };
    payload
        .checked_add(RECORD_HEADER_LEN as u64)
        .filter(|len| *len <= u64::from(u32::MAX))
        .and_then(|len| usize::try_from(len).ok())
        .ok_or_else(too_long)
}

/// Encodes `record` with its `crc` and chain fields zero, the O(len) half
/// of sealing, done before the record's position is reserved (S-STORE
/// §12.2's order of sealing and design note; card A-6a, Work 4).
///
/// Checks the kinds' rules, the op order rule and every length first, each
/// failure `StoreError::Invalid` (a caller's bug): a byte string or a
/// record longer than `u32::MAX` included. The returned [`Unsealed`]
/// carries the CRC32C of every byte after the chain fields; its
/// [`Unsealed::seal`] finishes the record in O(log len).
pub fn encode(record: &Record<'_>) -> Result<Unsealed, StoreError> {
    let kind = record.body.kind();
    let commit_ts = record.body.commit_ts();
    check_header(kind, record.txn, commit_ts).map_err(StoreError::Invalid)?;
    match &record.body {
        Body::Commit { ops, .. } => op::check_list(ops, false).map_err(StoreError::Invalid)?,
        Body::Chunk { ops, .. } => op::check_list(ops, true).map_err(StoreError::Invalid)?,
        Body::CommitMarker { nchunks, .. } => {
            check_nchunks(*nchunks).map_err(StoreError::Invalid)?
        }
        Body::SegmentEnd => {}
    }
    let mut out = Writer::with_capacity(encoded_len(&record.body)?);
    out.bytes(&RECORD_MAGIC);
    // `len` is set below from the bytes written, so it always counts them.
    out.u32(0);
    // `crc`, `lsn`, `prev_lsn` and `prev_crc` are the seal's (§12.2).
    out.u32(0);
    out.u64(0);
    out.u64(0);
    out.u32(0);
    out.u8(kind.byte());
    out.u32(record.txn.boot);
    out.u64(record.txn.n);
    out.u64(commit_ts);
    match &record.body {
        Body::Commit { ops, .. } => op::write_list(&mut out, ops)?,
        Body::Chunk { seq, ops } => {
            out.u32(*seq);
            op::write_list(&mut out, ops)?;
        }
        Body::CommitMarker { nchunks, .. } => out.u32(*nchunks),
        Body::SegmentEnd => {}
    }
    let mut bytes = out.into_vec();
    let len = u32::try_from(bytes.len()).map_err(|_| {
        StoreError::Invalid(format!(
            "a WAL record of {} bytes is longer than u32::MAX (S-STORE §12.2: len u32)",
            bytes.len()
        ))
    })?;
    put_u32(&mut bytes, LEN_AT, len)?;
    Unsealed::new(bytes)
}

/// Reads the record at the start of `buf`, which runs from the record's
/// expected start to the end of its segment (§12.5's frame checks; card
/// A-6a, Work 5). In this order: fewer than 53 bytes is
/// [`FrameError::Length`]; a magic other than `NWAL` is
/// [`FrameError::Magic`]; `len` below 53 or above `buf.len()` is
/// [`FrameError::Length`]; a `crc` other than the CRC32C of bytes
/// 12..`len` is [`FrameError::Crc`]. It checks nothing else and reads no
/// byte at or after `len`.
pub fn read_frame(buf: &[u8]) -> Result<Frame<'_>, FrameError> {
    let header = buf
        .first_chunk::<RECORD_HEADER_LEN>()
        .ok_or(FrameError::Length)?;
    // Reading a whole 53-byte header field by field cannot fail.
    let (magic, mut frame) = read_header(header).map_err(|_| FrameError::Length)?;
    if magic != RECORD_MAGIC {
        return Err(FrameError::Magic);
    }
    let record = usize::try_from(frame.len)
        .ok()
        .filter(|len| *len >= RECORD_HEADER_LEN)
        .and_then(|len| buf.get(..len))
        .ok_or(FrameError::Length)?;
    let covered = record.get(LSN_AT..).ok_or(FrameError::Length)?;
    if crc32c(0, covered) != frame.crc {
        return Err(FrameError::Crc);
    }
    frame.payload = record.get(RECORD_HEADER_LEN..).ok_or(FrameError::Length)?;
    Ok(frame)
}

/// The magic and the header's fields, in the table's order, with an empty
/// payload: the header half of [`read_frame`].
fn read_header<'a>(header: &[u8; RECORD_HEADER_LEN]) -> Result<([u8; 4], Frame<'a>), StoreError> {
    let mut r = Reader::new(header);
    let magic = r.array()?;
    let len = r.u32()?;
    let crc = r.u32()?;
    let lsn = r.u64()?;
    let prev_lsn = r.u64()?;
    let prev_crc = r.u32()?;
    let kind = r.u8()?;
    let boot = r.u32()?;
    let n = r.u64()?;
    let commit_ts = r.u64()?;
    r.finish()?;
    let frame = Frame {
        len,
        crc,
        lsn,
        prev_lsn,
        prev_crc,
        kind,
        txn: TxnId { boot, n },
        commit_ts,
        payload: &[],
    };
    Ok((magic, frame))
}

/// Decodes a frame's kind, header rules and payload (§12.2, §12.3; card
/// A-6a, Work 5). Every failure is `StoreError::Corrupt` (XX001) naming the
/// record's LSN and what did not decode: an unknown kind; a broken kind's
/// rule; a payload of the wrong length for its kind; an op tag that is not
/// an op, an op running past the payload, a flag bit or presence byte
/// outside the pin; ops out of §12.3's order, or a `revert` outside a
/// Chunk. The frame passed its CRC, so a failure here is not a torn write
/// (card A-6a, open point 5).
pub fn decode_body<'a>(frame: &Frame<'a>) -> Result<Record<'a>, StoreError> {
    let at_lsn = |e: StoreError| match e {
        StoreError::Corrupt(why) => {
            StoreError::Corrupt(format!("WAL record at LSN {}: {why}", frame.lsn))
        }
        other => other,
    };
    let corrupt = |why: String| at_lsn(StoreError::Corrupt(why));
    let kind = RecordKind::from_byte(frame.kind).ok_or_else(|| {
        corrupt(format!(
            "kind {} is not a record kind (S-STORE §12.2)",
            frame.kind
        ))
    })?;
    check_header(kind, frame.txn, frame.commit_ts).map_err(corrupt)?;
    let mut r = Reader::new(frame.payload);
    let body = match kind {
        RecordKind::Commit => Body::Commit {
            commit_ts: frame.commit_ts,
            ops: op::read_list(&mut r, false).map_err(at_lsn)?,
        },
        RecordKind::Chunk => {
            let seq = r.u32().map_err(|_| {
                corrupt(format!(
                    "a Chunk's payload of {} bytes is shorter than its 4-byte seq (S-STORE §12.3)",
                    frame.payload.len()
                ))
            })?;
            Body::Chunk {
                seq,
                ops: op::read_list(&mut r, true).map_err(at_lsn)?,
            }
        }
        RecordKind::CommitMarker => {
            let nchunks = r.u32().map_err(|_| {
                corrupt(format!(
                    "a CommitMarker's payload is {} bytes, not 4 (S-STORE §12.3)",
                    frame.payload.len()
                ))
            })?;
            check_nchunks(nchunks).map_err(corrupt)?;
            Body::CommitMarker {
                commit_ts: frame.commit_ts,
                nchunks,
            }
        }
        RecordKind::SegmentEnd => Body::SegmentEnd,
    };
    // No byte is left over: a CommitMarker's payload is exactly 4 bytes, a
    // SegmentEnd's is empty, and the op lists read to the payload's end.
    r.finish().map_err(|e| match e {
        StoreError::Corrupt(why) => corrupt(format!("a {kind:?}'s payload: {why}")),
        other => other,
    })?;
    Ok(Record {
        txn: frame.txn,
        body,
    })
}
