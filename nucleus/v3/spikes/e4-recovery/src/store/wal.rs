#![deny(clippy::indexing_slicing)]
// The unit `Err(())` of the record decoder is the §12.5 stop signal (no
// payload; the reader adds its position context).
#![allow(clippy::result_unit_err)]
//! The WAL (S-STORE §12; card X-5 Work 4.1–4.5), converted to the S-BATON
//! step discipline (card X-5b Work 11).
//!
//! Segments are `wal/<16 hex>.log`, 64 KiB, preallocated; LSNs are byte
//! positions in the logical stream, so a segment covers
//! `[first_lsn, first_lsn + 64 KiB)` and first LSNs are multiples of the
//! segment size. Records never span segments: a writer that cannot fit a
//! record writes `SegmentEnd` and the next record starts at the next
//! segment's first LSN. A single record is at most `wal_seg_size / 4`.
//!
//! Framing (§12.2): `magic u32 "NWAL", len u32, crc u32 (over the bytes
//! after it), lsn u64, kind u8, txn (u32, u64), commit_ts u64, payload`.
//! Kinds: 1 Commit, 2 Chunk, 3 CommitMarker, 4 SegmentEnd. Ops:
//! CreateTree, DropTree, Put, Delete with user keys (no `before`).
//!
//! Group commit (§12.4): writers drain the queue in batches — one
//! batch write, then one `sync_data` per touched segment, then the
//! frontier advances and the ack waiters wake; SyncOff commits are
//! acked after the write. The commit critical section assigns the ts
//! and enqueues the record at its LSN (I-WAL-TS-ORDER; the
//! TS-AFTER-APPEND mutant ends the critical section between the two).
//! **More than one batch may be in flight (S-1-F3 W3, X-5c-F3-F3a
//! step 3):** `wal_writer_step` takes a batch whenever the buffer is
//! non-empty, and the two frontiers — `written` and `durable` —
//! advance only over the **contiguous prefix** through interval sets,
//! so a commit is acked durable only through the contiguous
//! written-and-synced prefix of the WAL (I-ACK-PREFIX /
//! I-REDO-COMPLETE; F3-F1's one-in-flight serialization was the
//! stricter interim shape this replaces).
//!
//! G1: `sync_dir("/wal/")` after every segment create/rename and before
//! any record in a created or renamed segment is counted durable or
//! acked; `sync_dir("/")` after creating the page file and `wal/`.

use std::collections::{BTreeMap, BTreeSet};

use super::{Cx, Fail, Mutant, StoreHost, StoreInner, WAL_SEG};
use crate::crc::crc32c;
use crate::sim::E4Env;

/// Record kinds (§12.2).
pub const K_COMMIT: u8 = 1;
/// Chunk records (§12.3).
pub const K_CHUNK: u8 = 2;
/// The commit of a chunked transaction (§12.3).
pub const K_MARKER: u8 = 3;
/// The end-of-segment filler (§12.2).
pub const K_SEGEND: u8 = 4;

/// The fixed framing size (§12.2): magic, len, crc + the fixed body
/// fields (lsn, kind, txn, commit_ts).
pub const REC_FIXED: u64 = 16 + 8 + 1 + 4 + 8 + 8;

/// One logged op (§12.3; the card's subset: user keys, no `before`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WOp {
    /// Create a tree.
    CreateTree {
        /// The tree id.
        tree: u64,
    },
    /// Drop a tree.
    DropTree {
        /// The tree id.
        tree: u64,
    },
    /// Put a value under a user key.
    Put {
        /// The tree id.
        tree: u64,
        /// The user key.
        key: Vec<u8>,
        /// The value.
        value: Vec<u8>,
    },
    /// Delete a user key.
    Delete {
        /// The tree id.
        tree: u64,
        /// The user key.
        key: Vec<u8>,
    },
}

fn wr_u16(b: &mut Vec<u8>, v: u16) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn wr_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn wr_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn rd_u32(b: &[u8], off: usize) -> Option<u32> {
    let s: [u8; 4] = b.get(off..off + 4)?.try_into().ok()?;
    Some(u32::from_le_bytes(s))
}
fn rd_u64(b: &[u8], off: usize) -> Option<u64> {
    let s = b.get(off..off + 8)?;
    let mut a = [0u8; 8];
    a.copy_from_slice(s);
    Some(u64::from_le_bytes(a))
}

/// Encode one op.
pub fn enc_op(op: &WOp, out: &mut Vec<u8>) {
    match op {
        WOp::CreateTree { tree } => {
            out.push(1);
            wr_u64(out, *tree);
        }
        WOp::DropTree { tree } => {
            out.push(2);
            wr_u64(out, *tree);
        }
        WOp::Put { tree, key, value } => {
            out.push(3);
            wr_u64(out, *tree);
            wr_u16(out, key.len() as u16);
            out.extend_from_slice(key);
            wr_u32(out, value.len() as u32);
            out.extend_from_slice(value);
        }
        WOp::Delete { tree, key } => {
            out.push(4);
            wr_u64(out, *tree);
            wr_u16(out, key.len() as u16);
            out.extend_from_slice(key);
        }
    }
}

/// Decode one op from `b` (advancing by the returned size; `None` = the
/// bytes are not an op — §12.5 stops the reader there).
pub fn dec_op(b: &[u8]) -> Option<(WOp, usize)> {
    let kind = *b.first()?;
    let mut i = 1usize;
    match kind {
        1 => {
            let t = rd_u64(b, i)?;
            i += 8;
            Some((WOp::CreateTree { tree: t }, i))
        }
        2 => {
            let t = rd_u64(b, i)?;
            i += 8;
            Some((WOp::DropTree { tree: t }, i))
        }
        3 => {
            let t = rd_u64(b, i)?;
            i += 8;
            let klen = u16::from_le_bytes([*b.get(i)?, *b.get(i + 1)?]) as usize;
            i += 2;
            let key = b.get(i..i + klen)?.to_vec();
            i += klen;
            let vlen = rd_u32(b, i)? as usize;
            i += 4;
            let value = b.get(i..i + vlen)?.to_vec();
            i += vlen;
            Some((
                WOp::Put {
                    tree: t,
                    key,
                    value,
                },
                i,
            ))
        }
        4 => {
            let t = rd_u64(b, i)?;
            i += 8;
            let klen = u16::from_le_bytes([*b.get(i)?, *b.get(i + 1)?]) as usize;
            i += 2;
            let key = b.get(i..i + klen)?.to_vec();
            i += klen;
            Some((WOp::Delete { tree: t, key }, i))
        }
        _ => None,
    }
}

/// Encode a whole record at `lsn` (§12.2: the crc covers the body,
/// lsn included — the enqueuer knows the position before it builds).
pub fn enc_record(kind: u8, txn: (u32, u64), ts: u64, payload: &[u8], lsn: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(payload.len() + 29);
    wr_u64(&mut body, lsn);
    body.push(kind);
    wr_u32(&mut body, txn.0);
    wr_u64(&mut body, txn.1);
    wr_u64(&mut body, ts);
    body.extend_from_slice(payload);
    let mut rec = Vec::with_capacity(body.len() + 16);
    wr_u32(&mut rec, 0x4C41574E); // "NWAL" LE
    wr_u32(&mut rec, (body.len() + 16) as u32);
    wr_u32(&mut rec, 0); // crc
    let crc = crc32c(0, &body);
    rec.extend_from_slice(&crc.to_le_bytes());
    rec.extend_from_slice(&body);
    rec
}

/// Patch a record's own lsn field (body offset 0 → record offset 16).
/// A decoded record.
#[derive(Debug, Clone)]
#[allow(clippy::len_without_is_empty)]
pub struct Rec {
    /// Its LSN.
    pub lsn: u64,
    /// Its kind.
    pub kind: u8,
    /// Its txn id.
    pub txn: (u32, u64),
    /// Its commit ts (0 in Chunk/SegmentEnd).
    pub ts: u64,
    /// The payload (ops, or `nchunks` for a marker).
    pub payload: Vec<u8>,
}

impl Rec {
    /// The record's whole length in bytes.
    pub fn len(&self) -> u64 {
        REC_FIXED + self.payload.len() as u64
    }
}

/// Decode a record at `lsn` from `b` (its bytes), validating magic, crc,
/// `len` and the §12.5 stale check. `Err(())` = not a valid record — bad
/// magic or CRC (a torn tail or an unwritten preallocated region), `len`
/// past the segment end, or the record's own lsn ≠ the expected position
/// (a recycled segment's stale content).
pub fn dec_record(b: &[u8], lsn: u64, seg_end: u64) -> Result<Option<Rec>, ()> {
    dec_record_inner(b, lsn, seg_end, false)
}

/// The decode with the LSN-CHECK-SKIP hook: under the mutant the
/// §12.5 stale check (the record's own lsn ≠ its expected position) is
/// skipped, so a recycled segment's stale records — whose magic and CRC
/// are intact — decode as if they were new. The CRC itself stays
/// checked (patching the embedded lsn would break it and make the
/// mutant self-defeating).
pub fn dec_record_with_lsn_check(
    b: &[u8],
    lsn: u64,
    seg_end: u64,
    mutant: Option<Mutant>,
) -> Result<Option<Rec>, ()> {
    dec_record_inner(
        b,
        lsn,
        seg_end,
        matches!(mutant, Some(Mutant::LsnCheckSkip)),
    )
}

/// **Q11 (card X-5c-F4-F1 step 2; P ruling nv3-X-5c-F4-esc-1's
/// ESCALATE 3):** do the bytes at `lsn` decode as a **stale** record —
/// magic, `len` and CRC all valid, but the embedded lsn ≠ the expected
/// position (a recycled segment's previous-life content at its start)?
/// Implemented through [`dec_record_inner`] (the skip-lsn-check decode
/// succeeds where the checked decode fails — never a second copy of
/// the check). The counter it feeds (`stale_seg_reads`) is an
/// observable only: the walk's stop never changes.
pub fn is_stale_record(b: &[u8], lsn: u64, seg_end: u64) -> bool {
    dec_record_inner(b, lsn, seg_end, true).is_ok_and(|r| r.is_some())
        && dec_record_inner(b, lsn, seg_end, false).is_err()
}

fn dec_record_inner(
    b: &[u8],
    lsn: u64,
    seg_end: u64,
    skip_lsn_check: bool,
) -> Result<Option<Rec>, ()> {
    if b.len() < 16 || b.get(0..4) != Some(&0x4C41574Eu32.to_le_bytes()) {
        return Err(());
    }
    let len = rd_u32(b, 4).ok_or(())? as usize;
    if len < 16 || (lsn + len as u64) > seg_end {
        // `len` running past the segment (§12.5).
        return Err(());
    }
    let body = b.get(16..len).ok_or(())?;
    let crc_stored = rd_u32(b, 12).ok_or(())?;
    if crc32c(0, body) != crc_stored {
        return Err(());
    }
    let rlsn = rd_u64(body, 0).ok_or(())?;
    if rlsn != lsn && !skip_lsn_check {
        // The §12.5 stale check (LSN-CHECK-SKIP removes it).
        return Err(());
    }
    // Body layout: lsn u64, kind u8, txn (u32, u64), commit_ts u64.
    let kind = *body.get(8).ok_or(())?;
    let txn = (rd_u32(body, 9).ok_or(())?, rd_u64(body, 13).ok_or(())?);
    let ts = rd_u64(body, 21).ok_or(())?;
    let payload = body.get(29..).unwrap_or(&[]).to_vec();
    Ok(Some(Rec {
        lsn,
        kind,
        txn,
        ts,
        payload,
    }))
}

/// The decoded ops of a record's payload.
pub fn rec_ops(r: &Rec) -> Vec<WOp> {
    let mut out = Vec::new();
    let mut b = r.payload.as_slice();
    while !b.is_empty() {
        match dec_op(b) {
            Some((op, used)) => {
                out.push(op);
                b = b.get(used..).unwrap_or(&[]);
            }
            None => break,
        }
    }
    out
}

/// The writer-side WAL state.
#[derive(Debug, Default, Clone)]
pub struct WalState {
    /// The append position (next LSN).
    pub append: u64,
    /// Enqueued-but-unwritten record bytes (the writer's buffer, G5).
    pub buf: Vec<u8>,
    /// Enqueued records by LSN → `(ts, is_marker, txn)` (this boot only;
    /// replayed records below the append position are in the image).
    pub buf_tags: BTreeMap<u64, (u64, bool, (u32, u64))>,
    /// Open segment files by first LSN.
    pub segs: BTreeMap<u64, usize>,
    /// The durable-through LSN (advanced by the writer's syncs).
    pub durable: u64,
    /// The written-through LSN (advanced by the writer's writes).
    pub written: u64,
    /// **S-1-F3 W3 / I-ACK-PREFIX (X-5c-F3-F3a step 3): the set of
    /// taken, unfinished batches**, as LSN ranges. "More than one batch
    /// may be in flight. The rule below holds however batches are
    /// scheduled" — `wal_writer_step` takes a batch whenever the buffer
    /// is non-empty; there is no park on another task's batch. The
    /// store's invariant: **`durable ≤ written ≤ append` at all times**
    /// (a breach returns `Fail::Invalid`, loudly), and both frontiers
    /// advance **only over the contiguous prefix**: "writing or syncing
    /// a batch while an earlier one is unwritten advances neither past
    /// the earlier one" (§12.4's two frontiers, S-1-F3's carried W3
    /// text; gate (b) item 4). F3-F1's one-in-flight serialization
    /// (`in_flight: Option<_>`, the `Busy` park) was the stricter
    /// interim shape this replaces.
    pub in_flight: Vec<(u64, u64)>,
    /// Completed write intervals not yet absorbed into `written`
    /// (X-5c-F3-F3a step 3): `written` advances while a completed write
    /// starts at `written`.
    pub written_iv: Vec<(u64, u64)>,
    /// Synced batch intervals not yet absorbed into `durable`
    /// (X-5c-F3-F3a step 3): `durable` advances while a synced batch
    /// starts at `durable` and ends at or below `written`. A batch
    /// counts synced once a `sync_data` of each segment it touched
    /// completes after its write returned.
    pub synced_iv: Vec<(u64, u64)>,
    /// Per-segment created-but-unsynced-dir flags (G1 bookkeeping).
    pub seg_needs_dirsync: Vec<u64>,
    /// Chunked transactions: txn → first chunk LSN (§12.6). An entry
    /// lives from its first chunk until a flip whose `T` covers its
    /// marker (`marker_ts ≤ T` — wholly in the image); a marker's
    /// enqueue does **not** remove it, because the flip still needs the
    /// first-chunk LSN while the marker is above `T` (the marked case).
    pub open_chunks: BTreeMap<(u32, u64), u64>,
    /// **S-1-F3 §13 step 0 (W5 (i); X-5c-F3-F3b step 1):** the commit_ts
    /// of the last `Commit` or `CommitMarker` this boot enqueued, set in
    /// [`wal_enqueue`]'s placement step (a `Chunk` or `SegmentEnd`
    /// placement does not move it). It starts at the frontier the boot
    /// started from: 0 at `create`, the recovered frontier after
    /// `recover` — the flip's `ts_ckpt_hi` reads it beside `append`, in
    /// one step inside the commit critical section's exclusion.
    pub last_commit_ts: u64,
    /// The flips' enqueue positions, in flip order (M-CKPT-T's hook: the
    /// card names this supply, Work 3.10; read only by the monitors).
    pub flip_enqueue_pos: Vec<u64>,
    /// **X5-R38 (a store observable's support set, never a behaviour
    /// input):** segment boundaries a `wal_recycle` rename has bound,
    /// with no completed `sync_data` of a segment at or above them
    /// since — the recycled segments whose durable start still holds
    /// the previous life's records (W11's condition "a recycled
    /// segment's untouched start"). A completed sync of segment `s`
    /// removes every boundary `≤ s`.
    pub recycled_unsynced: BTreeSet<u64>,
}

/// The first LSN of the segment containing `lsn`.
pub fn seg_of(lsn: u64) -> u64 {
    lsn - (lsn % WAL_SEG)
}

/// The maximum record size (§12.2: "a single record is at most
/// `wal_seg_size / 4`; larger transactions are chunked"). An enqueue
/// above it is refused loudly — never placed (the gate's round-3 F1: the
/// draft had no cap, so a 177-KiB `Commit` was spread across 64-KiB
/// segments, §12.5's reader stopped before it, and both recovery and the
/// Work 2.1 derivation silently dropped the acked commit).
pub const REC_MAX: u64 = WAL_SEG / 4;

/// Enqueue a record at its LSN (the commit critical section's second
/// half; I-WAL-TS-ORDER). Returns `(lsn, len)` of the record itself
/// (excluding any `SegmentEnd` filler). A WAL enqueue is a yield point
/// (card 1.2), then the whole placement is one step. A payload whose
/// record exceeds [`REC_MAX`] is refused with [`Fail::Invalid`] — the
/// caller chunks the transaction (§12.2).
pub fn wal_enqueue<H: StoreHost>(
    cx: &Cx<'_, H>,
    kind: u8,
    txn: (u32, u64),
    ts: u64,
    payload: &[u8],
) -> Result<(u64, u64), Fail> {
    cx.yield_now("wal_enqueue")?;
    if REC_FIXED + payload.len() as u64 > REC_MAX {
        return Err(Fail::Invalid(format!(
            "record over the §12.2 cap: {} payload bytes ({} + fixed {} > {REC_MAX}) — chunk the transaction",
            payload.len(),
            payload.len(),
            REC_FIXED
        )));
    }
    cx.with("wal_enqueue:place", |c| {
        let st = c.store();
        st.alive()?;
        let lsn0 = st.wal.append;
        // The record's own lsn depends on the room left in the segment;
        // the encoder needs it up front (the crc covers the lsn).
        let probe = REC_FIXED + payload.len() as u64;
        let seg = seg_of(lsn0);
        let room = seg + WAL_SEG - lsn0;
        // **The carried §12.2 tail rule (S-1-F3 gate G2, edit 2; X-5c-
        // F3-F3a step 2):** "A record is placed at the append position
        // only if it ends exactly at its segment's end or leaves room
        // for a `SegmentEnd` after it. Otherwise the writer writes
        // `SegmentEnd` there, and the record starts at the next
        // segment's first LSN. So a segment ends with `SegmentEnd` or
        // with a record that ends exactly at its end, never with a
        // shorter gap." (B5's prototype-deviation fix first enforced
        // this: the draft compared only `probe ≤ room`, so a record
        // could land with a tail the 45-byte filler cannot fill — the
        // record then straddled the boundary while carrying the next
        // segment's lsn, and §12.5's position check rejected its bytes
        // on read.)
        let fits = probe <= room && (room - probe == 0 || room - probe >= REC_FIXED);
        let rec_lsn = if fits { lsn0 } else { seg + WAL_SEG };
        let rec = enc_record(kind, txn, ts, payload, rec_lsn);
        let rec_len = rec.len() as u64;
        let mut placed: Vec<u8> = Vec::new();
        if !fits && lsn0 != seg + WAL_SEG {
            // A SegmentEnd filler at lsn0, then the lsn space padded to
            // the boundary (the reader never reads the pad: it hops on
            // the filler, §12.5). Its txn id is `(boot_id, 0)` — the
            // writing boot's id with n = 0 (§12.2 W2; X-5c-F3-F3a
            // step 4).
            let se = enc_record(K_SEGEND, (st.boot_id, 0), 0, &[], lsn0);
            placed.extend_from_slice(&se);
            let gap = ((seg + WAL_SEG) - (lsn0 + se.len() as u64)) as usize;
            placed.extend_from_slice(&vec![0u8; gap]);
        }
        placed.extend_from_slice(&rec);
        st.wal.buf_tags.insert(rec_lsn, (ts, kind == K_MARKER, txn));
        // **S-1-F3 §13 step 0 (W5 (i); X-5c-F3-F3b step 1):** the last
        // `Commit`/`CommitMarker` ts this boot appended — the flip's
        // `ts_ckpt_hi` supply. A `Chunk` (ts reserved 0) or the
        // `SegmentEnd` filler never moves it.
        if kind == K_COMMIT || kind == K_MARKER {
            st.wal.last_commit_ts = st.wal.last_commit_ts.max(ts);
        }
        st.wal.buf.extend_from_slice(&placed);
        st.wal.append = lsn0 + placed.len() as u64;
        Ok((rec_lsn, rec_len))
    })
}

/// The writer step: take a batch whenever the buffer is non-empty
/// (**more than one batch may be in flight** — S-1-F3 W3, X-5c-F3-F3a
/// step 3; there is no `Busy` park on another task's batch), drain it
/// to the segments, then one `sync_data` per touched segment, then
/// advance the frontiers over the contiguous prefix and wake the ack
/// waiters. SyncOff acks arrive from `written`. G1: a directory sync
/// after every create, before any record in the segment counts
/// durable (RENAME-NO-DIRSYNC skips it).
///
/// **The two frontiers (§12.4, S-1-F3's carried W3 text):**
/// `written` is the largest LSN `L` such that every record byte below
/// `L` has been written; `durable` the largest `L ≤ written` such that
/// every record byte below `L` was written before the start of a
/// `sync_data` on its segment that has completed. Both advance only
/// over the contiguous prefix — writing or syncing a batch while an
/// earlier one is unwritten advances neither past the earlier one —
/// through the interval sets [`WalState::written_iv`] and
/// [`WalState::synced_iv`]. The holder removes its batch from
/// [`WalState::in_flight`] and wakes `WalAcks` on **every** exit, its
/// error paths included, so no waiter is stranded.
pub fn wal_writer_step<H: StoreHost>(cx: &Cx<'_, H>) -> Result<(), Fail> {
    /// The take step's outcome.
    enum Take {
        /// This task holds the batch: its bytes and LSN range.
        Batch(Vec<u8>, u64, u64),
        /// Nothing enqueued.
        Empty,
    }
    // Take the batch (one step).
    let take = cx.with("wal_writer:take", |c| {
        let st = c.store();
        // G8 (9.2, W8 (e)): the writer issues no env write after the
        // mark — it checks `alive()` before taking (and so before every
        // env call of the write-out below).
        st.alive()?;
        if st.wal.buf.is_empty() {
            return Ok(Take::Empty);
        }
        let buf = std::mem::take(&mut st.wal.buf);
        let start = st.wal.append - buf.len() as u64;
        let end = start + buf.len() as u64;
        st.wal.in_flight.push((start, end));
        Ok(Take::Batch(buf, start, end))
    })?;
    let (buf, start, end) = match take {
        Take::Empty => return Ok(()),
        Take::Batch(buf, start, end) => (buf, start, end),
    };
    let res = wal_write_batch(cx, &buf, start, end);
    // The holder removes its batch and wakes `WalAcks` on every exit —
    // the error paths above included — so no waiter parked below is
    // ever stranded. A non-crash failure poisons the store (W8: the WAL
    // path) **before** this release step, and the broadcast below (G8
    // (d)) wakes every other wait key once the batch is released — no
    // waker holds a tree mutex or a pin here.
    if let Err(e) = &res
        && !matches!(e, Fail::Sim(_))
    {
        cx.with("wal_writer:poison", |c| {
            let st = c.store();
            if st.poisoned.is_none() {
                st.poisoned = Some(format!("WAL path failure: {e} (9.2, G8)"));
            }
            Ok(())
        })
        .ok();
    }
    let _ = cx.with("wal_writer:release", |c| {
        c.store()
            .wal
            .in_flight
            .retain(|&(a, b)| !(a == start && b == end));
        Ok(())
    });
    cx.w.wake(crate::sim::WaitKey::WalAcks);
    if matches!(res, Err(Fail::Io(_)) | Err(Fail::Invalid(_))) {
        super::prims::poison_broadcast(cx);
    }
    res
}

/// One in-flight batch's write-out: the optional `writer:prewrite`
/// hold, the segment loop, the `written` advance over the interval
/// set, the `writer:presync` park, the group sync and the `durable`
/// advance. Called only by [`wal_writer_step`] with the batch's range
/// recorded in [`WalState::in_flight`].
fn wal_write_batch<H: StoreHost>(
    cx: &Cx<'_, H>,
    buf: &[u8],
    start: u64,
    end: u64,
) -> Result<(), Fail> {
    // A named park point (X-5c-F3-F3a step 3, S-1-F3's `t_ack_prefix`):
    // the holder has taken its batch and not yet written it, so another
    // task can take the batch above it — the two-batches-in-flight
    // shape the frontier rules must hold over.
    cx.park("writer:prewrite")?;
    // G8 (9.2, W8 (e)): the mark may have landed while this batch was
    // held at the park — nothing is written after it (each env call
    // below is guarded by this check and the sync loop's own).
    cx.with("wal_write_batch:alive", |c| c.store().alive())?;
    let mut lsn = start;
    let mut touched: Vec<u64> = Vec::new();
    let mut i = 0usize;
    while i < buf.len() {
        let seg = seg_of(lsn);
        let off = lsn - seg;
        let room = (WAL_SEG - off) as usize;
        let chunk = (buf.len() - i).min(room);
        let bytes = buf
            .get(i..i + chunk)
            .ok_or_else(|| Fail::Invalid("batch".to_owned()))?;
        let fid = {
            let existing = cx.with("wal_writer:seg", |c| {
                let st = c.store();
                Ok(st.wal.segs.get(&seg).copied())
            })?;
            match existing {
                Some(f) => f,
                None => {
                    // Create + preallocate + G1. With more than one
                    // batch in flight (X-5c-F3-F3a step 3) two tasks
                    // can race this create for the same segment: the
                    // loser's `create` fails "exists" and falls back to
                    // `open`, which resolves to the same inode; the
                    // second `set_len` and `sync_dir` are idempotent,
                    // and durability of the segment's records is never
                    // counted before at least one completed dirsync.
                    let name = format!("/wal/{seg:016x}.log");
                    // G8 (9.2, W8): a segment's creation, preallocation
                    // and `/wal/` directory sync are WAL-path operations
                    // — a non-crash failure there poisons the store (the
                    // hook below can only fire on the write/sync shapes;
                    // the create/set_len/dirsync sites share the poison
                    // rule through their callers' error exit above).
                    let created = cx.env().create(&name);
                    let f = match created {
                        Ok(f) => f,
                        Err(_) => cx.env().open(&name).map_err(Fail::Sim)?,
                    };
                    cx.env().set_len(f, WAL_SEG).map_err(Fail::Sim)?;
                    cx.with("wal_writer:bind", |c| {
                        let st = c.store();
                        st.wal.segs.insert(seg, f);
                        if !st.wal.seg_needs_dirsync.contains(&seg) {
                            st.wal.seg_needs_dirsync.push(seg);
                        }
                        Ok(())
                    })?;
                    if !cx.with("wal_writer:mut", |c| {
                        Ok(matches!(
                            c.store().cfg.mutant,
                            Some(Mutant::RenameNoDirsync)
                        ))
                    })? {
                        cx.env().sync_dir("/wal/").map_err(Fail::Sim)?;
                        cx.with("wal_writer:dirsynced", |c| {
                            let st = c.store();
                            st.wal.seg_needs_dirsync.retain(|s| *s != seg);
                            Ok(())
                        })?;
                    }
                    f
                }
            }
        };
        // **The one store test hook (X-5c-F3-F3b step 3; Limits 3):** a
        // chosen WAL write returns a non-crash I/O failure. The sim maps
        // every disk error to a crash, so no non-crash WAL failure is
        // otherwise reachable; the hook exists only for step 3's
        // literal-config pins and is default-off (never set by the
        // workload or the campaign).
        if wal_io_hook_fails(cx, WalIoOp::Write, start)? {
            return Err(Fail::Io(format!("WAL write at lsn {lsn} (test hook)")));
        }
        cx.env().write_at(fid, off, bytes).map_err(Fail::Sim)?;
        if !touched.contains(&seg) {
            touched.push(seg);
        }
        i += chunk;
        lsn += chunk as u64;
    }
    cx.with("wal_writer:written", |c| {
        let st = c.store();
        // §12.4's `written` frontier over the interval set (X-5c-F3-F3a
        // step 3): a completed write advances the frontier only while
        // it starts at the frontier — writing a batch while an earlier
        // one is unwritten advances neither past the earlier one. The
        // batch's own `[start, end)` may complete out of order; it sits
        // in the set until the prefix reaches it.
        st.wal.written_iv.push((start, end));
        absorb_interval(&mut st.wal.written, &mut st.wal.written_iv);
        check_wal_inv(st)
    })?;
    cx.w.wake(crate::sim::WaitKey::WalAcks);
    // A named park point (card Work 13): D4 holds the writer between
    // its batch write and its group sync, so a flip's step 1 is the
    // only thing that can make the batch durable (G5's "except by step
    // 1" shape).
    cx.park("writer:presync")?;
    // G8 (9.2, W8 (e)): as above — no sync after the mark.
    cx.with("wal_write_batch:alive2", |c| c.store().alive())?;
    // One sync per touched segment (a batch's group commit). A batch
    // counts synced once a `sync_data` of each segment it touched
    // completes after its write returned — the loop below.
    for seg in touched {
        let f = cx.with("wal_writer:fid", |c| {
            Ok(c.store().wal.segs.get(&seg).copied())
        })?;
        if let Some(f) = f {
            // The test hook (see the write site): a chosen WAL
            // `sync_data` returns a non-crash I/O failure. The failed
            // sync is never retried and its batch's bytes never count
            // toward `durable` (the advance step below is unreachable
            // on this path).
            if wal_io_hook_fails(cx, WalIoOp::SyncData, start)? {
                return Err(Fail::Io(format!(
                    "WAL sync_data of segment {seg:016x} (test hook)"
                )));
            }
            cx.env().sync_data(f).map_err(Fail::Sim)?;
            // X5-R38: the W11 window note after this segment's sync.
            cx.with("wal_writer:w11", |c| {
                let y = c.sched_mut().yields + 1;
                w11_window_note(c.store(), seg, y);
                Ok(())
            })?;
        }
    }
    cx.with("wal_writer:durable", |c| {
        let st = c.store();
        // §12.4's `durable` frontier over the interval set: a synced
        // batch advances it only while it starts at the frontier (and
        // so ends at or below `written`, whose contiguous prefix must
        // already cover the batch — the write completed before the
        // sync). The invariant check stays loud.
        st.wal.synced_iv.push((start, end));
        absorb_interval(&mut st.wal.durable, &mut st.wal.synced_iv);
        check_wal_inv(st)
    })?;
    cx.w.wake(crate::sim::WaitKey::WalAcks);
    Ok(())
}

/// Advance `frontier` while a completed interval starts exactly at it,
/// dropping intervals the frontier has already passed (S-1-F3 W3's
/// interval sets; X-5c-F3-F3a step 3). Never a `max` over a hole: an
/// interval above a gap waits until the gap fills.
fn absorb_interval(frontier: &mut u64, ivs: &mut Vec<(u64, u64)>) {
    loop {
        ivs.retain(|&(_, b)| b > *frontier);
        let Some(p) = ivs.iter().position(|&(a, _)| a == *frontier) else {
            break;
        };
        let (_, b) = ivs.remove(p);
        if b > *frontier {
            *frontier = b;
        }
    }
}

/// The §12.4 invariant `durable ≤ written ≤ append`, checked at every
/// frontier advance (X-5c-F3-F3a step 3): a breach returns
/// [`Fail::Invalid`] loudly, never a silent skip-ahead.
fn check_wal_inv(st: &StoreInner) -> Result<(), Fail> {
    if st.wal.durable > st.wal.written || st.wal.written > st.wal.append {
        return Err(Fail::Invalid(format!(
            "12.4 invariant durable <= written <= append breached: durable={} written={} append={}",
            st.wal.durable, st.wal.written, st.wal.append
        )));
    }
    Ok(())
}

/// **X5-R38 (card X-5c-F4-F1 step 3; observables only; rework 2 of the
/// P gate verdict X-5c-F4-F1, ruling (i) binding): the W11 window note,
/// called right after a WAL `sync_data` of segment `seg` completes**
/// (the writer's group-sync loop and `wal_sync_through`'s helper loop —
/// the two sync sites). Ruling (i): "**W11.** Let `b` be a segment
/// boundary whose segment a `wal_recycle` rename bound and which has
/// had no completed `sync_data` since. W11 opens when a `sync_data` of
/// the segment below `b` completes with `wal.written ≥ b`: that
/// segment's whole extent, filler or boundary-ending record included,
/// is then durable, so the walk reaches `b`. It closes when a
/// `sync_data` of a segment at or above `b` completes. 'Untouched'
/// means the durable image: a write at ≥ `b` may be issued and still
/// volatile, and F-CRASH decides whether it survives. Q11 then requires
/// that `b`'s first sector does not survive with new content." The
/// `wal.written ≥ b` term is the rework's fix (the gate's finding 2):
/// without it the window opened on **any** completed sync of the
/// segment below `b` — durable 261,147, written 263,952, `b` 262,144
/// at seed 4 — so the crash landed where the walk stops on zeros below
/// `b` and Q11 could never hit (0 of 150 picks as built; 45 of 79 with
/// the fix over 0..200, gate-measured). `y` is the next yield index
/// (the caller's current count + 1 — the window's first crashable
/// yield; the note runs between two yield events, so the current count
/// itself names a yield that has already fired).
fn w11_window_note(st: &mut StoreInner, seg: u64, y: u64) {
    let b = seg + WAL_SEG;
    // This sync made segment `seg`'s extent durable — every pending
    // boundary at or below `seg` is retired, and a W11 window whose own
    // boundary just became durable closes.
    if st.w11_window.is_some_and(|w| w <= seg) {
        st.w11_window = None;
    }
    st.wal.recycled_unsynced.retain(|x| *x > seg);
    if st.wal.segs.contains_key(&b)
        && st.wal.recycled_unsynced.contains(&b)
        // Ruling (i)'s opening term: the segment below `b` synced only
        // with its whole extent written — `written ≥ b` — does the walk
        // reach `b` (the rework's fix).
        && st.wal.written >= b
        && st.w11_window.is_none()
    {
        st.w11_window = Some(b);
        st.w11_yields.push(y);
    }
}

/// Wait until the WAL is durable through `lsn` (a SyncOn ack). Waiting
/// tasks help drain the writer queue cooperatively first (§12.4's
/// embedded mode), so single-task worlds (recovery, directed tests)
/// cannot starve — a single-task world never observes another task's
/// batch, so the helper take below always fires there. Helping is only
/// legal when this task can make progress itself — the buffer is
/// non-empty **and no batch is in flight** (adding a batch above an
/// unfinished one is the writer service's and direct
/// [`wal_writer_step`] callers' business; `wal_writer_step` itself
/// never parks — more than one batch may be in flight, X-5c-F3-F3a
/// step 3), or the records are written but unsynced with no in-flight
/// batch below the target. A waiter whose gap runs through another
/// task's in-flight batch parks on [`WaitKey::WalAcks`] — with its
/// check and its park around one baton hold, never spinning — and the
/// holder's advances wake it (S-1-F3 W3; the frozen
/// `cdiv_syncon_ack_never_crosses_an_unwritten_batch` pin's premise:
/// a committer's SyncOn wait crosses no held batch).
pub fn wal_wait_durable<H: StoreHost>(cx: &Cx<'_, H>, lsn: u64) -> Result<(), Fail> {
    loop {
        let (done, can_write, can_sync) = cx.with("wal_durable:chk", |c| {
            let st = c.store();
            st.alive()?;
            Ok((
                st.wal.durable >= lsn,
                !st.wal.buf.is_empty() && st.wal.in_flight.is_empty(),
                st.wal.durable < lsn
                    && st.wal.written >= lsn
                    // A waiter whose gap runs through another task's
                    // in-flight batch parks (S-1-F3 W3 / the card's
                    // ack-rule clause): the batch's own group sync is
                    // its durability, never a stranger's sync-through —
                    // and without this guard the wait would spin on
                    // `wal_sync_through` credits that never come.
                    && st.wal.in_flight.iter().all(|&(a, _)| a >= lsn),
            ))
        })?;
        if done {
            return Ok(());
        }
        if can_write {
            wal_writer_step(cx)?;
            continue;
        }
        if can_sync {
            // Written (by whoever) but not counted durable: sync the
            // covering segments here and advance the accounting.
            wal_sync_through(cx, lsn)?;
            continue;
        }
        cx.wait_for(crate::sim::WaitKey::WalAcks)?;
    }
}

/// Wait until the WAL is written through `lsn` (a SyncOff ack).
pub fn wal_wait_written<H: StoreHost>(cx: &Cx<'_, H>, lsn: u64) -> Result<(), Fail> {
    loop {
        let (done, can_write) = cx.with("wal_written:chk", |c| {
            let st = c.store();
            st.alive()?;
            Ok((
                st.wal.written >= lsn,
                !st.wal.buf.is_empty() && st.wal.in_flight.is_empty(),
            ))
        })?;
        if done {
            return Ok(());
        }
        if can_write {
            wal_writer_step(cx)?;
            continue;
        }
        // An in-flight batch (another task) or nothing to do: park; the
        // holder's write wakes WalAcks.
        cx.wait_for(crate::sim::WaitKey::WalAcks)?;
    }
}

/// `sync_data` every segment covering `[durable, min(lsn, written))`
/// and credit the synced range as an interval: **`[durable, written)`
/// as it stood when the first sync started, on the segments it synced.
/// It never credits bytes above `written`** (X-5c-F3-F3a step 3;
/// D4's path, the helpers, checkpoint step 1) — **nor the bytes of a
/// batch still in flight**: the accounting's units are each batch's
/// **own** group sync ("completed own-segment syncs", §12.4's carried
/// W3 text), so an unfinished batch's durability stays its holder's to
/// finish, and a waiter whose gap runs through another task's in-flight
/// batch parks on `WalAcks` instead of syncing over it (the
/// `cdiv_syncon_ack_never_crosses_an_unwritten_batch` shape: a
/// committer's SyncOn wait must not cross a held batch; a completed
/// write below with no batch in flight is creditable). Only the
/// segments the store has open are synced; a range the store never
/// wrote cannot be made durable by syncing (G5: appending means
/// enqueued, durable means synced where written). The frontier then
/// advances only over the contiguous prefix, so this can never leap an
/// unwritten batch.
fn wal_sync_through<H: StoreHost>(cx: &Cx<'_, H>, lsn: u64) -> Result<(), Fail> {
    let (d, w, inflight_start) = cx.with("wal_sync:range", |c| {
        let st = c.store();
        Ok((
            st.wal.durable,
            st.wal.written,
            st.wal
                .in_flight
                .iter()
                .map(|&(a, _)| a)
                .min()
                .unwrap_or(u64::MAX),
        ))
    })?;
    let top = w.min(lsn).max(d).min(inflight_start);
    let mut seg = seg_of(d);
    let last = seg_of(top);
    while seg <= last {
        let f = cx.with("wal_sync:fid", |c| {
            Ok(c.store().wal.segs.get(&seg).copied())
        })?;
        if let Some(f) = f {
            cx.env().sync_data(f).map_err(Fail::Sim)?;
            // X5-R38: the W11 window note after this segment's sync.
            cx.with("wal_sync:w11", |c| {
                let y = c.sched_mut().yields + 1;
                w11_window_note(c.store(), seg, y);
                Ok(())
            })?;
        }
        seg += WAL_SEG;
    }
    cx.with("wal_sync:adv", |c| {
        let st = c.store();
        // The credit is capped at `written` as it stood when the first
        // sync started (never above the current `written`, which only
        // grows) — one interval into the synced set, absorbed by the
        // frontier's contiguous rule.
        let credit = top.min(st.wal.written);
        if credit > d {
            st.wal.synced_iv.push((d, credit));
        }
        absorb_interval(&mut st.wal.durable, &mut st.wal.synced_iv);
        check_wal_inv(st)
    })?;
    cx.w.wake(crate::sim::WaitKey::WalAcks);
    Ok(())
}

/// Step 1: sync the WAL through every record enqueued before the flip
/// (§13; G5). **S-1-F3 §13 step 1 (W5 (ii); X-5c-F3-F3b step 1):** the
/// step ends only when `durable_end ≥ wal_ckpt_end` (I-ACK-PREFIX,
/// §12.4) — `wal_wait_durable`'s contiguous-prefix rule supplies exactly
/// that, so every byte below `wal_ckpt_end` is written and synced, not
/// just the last batch's. The caller passes the flip's `wal_ckpt_end`
/// (the append position read inside the commit critical section's
/// exclusion). WAL-SYNC-T-ONLY syncs only through the last record with
/// `commit_ts ≤ T`.
pub fn wal_step1<H: StoreHost>(cx: &Cx<'_, H>, t: u64, wal_ckpt_end: u64) -> Result<(), Fail> {
    let end = cx.with("wal_step1:end", |c| {
        let st = c.store();
        if matches!(st.cfg.mutant, Some(Mutant::WalSyncTOnly)) {
            Ok(st
                .wal
                .buf_tags
                .iter()
                .rev()
                .find(|(_, (ts, _, _))| *ts <= t && *ts > 0)
                .map(|(l, _)| *l)
                .unwrap_or(0))
        } else {
            Ok(wal_ckpt_end)
        }
    })?;
    wal_wait_durable(cx, end)
}

/// Is boot id `a` **older** than `b` in §12.5's serial-number order
/// (S-1-F3 W4 (i)): `a` is older than `b` iff `(a − b) mod 2³² ≥ 2³¹`.
/// The same comparison `recover`'s walk uses for the ordered boot-id
/// stop; shared with the `wal_start_lsn` filter below.
pub fn boot_id_older(a: u32, b: u32) -> bool {
    a.wrapping_sub(b) >= 1u32 << 31
}

/// Compute `wal_start_lsn` at the flip (§12.6): the minimum of the
/// LSN of the first **Commit or CommitMarker** with `commit_ts > T`
/// (or the append position if there is none), and the first Chunk LSN
/// of every chunked transaction whose marker is not yet below `T`.
/// (Chunk records carry ts 0, reserved, so only the second term counts
/// them; SegmentEnd fillers likewise never pull the start.)
/// START-LSN-MARKED-CHUNK drops the marked case; START-LSN-PAST-CHUNK
/// drops the open-txn case.
/// **The open-txn (`None`) arm holds back only for chunks of the
/// current boot** (S-STORE §14 step 8, as merged at the base: "Chunks
/// of the previous boot that have no marker belong to transactions
/// that ended without one …, all ended before this flip, so they do
/// not hold back `wal_start_lsn` (§12.6)"; the X-5c-F3-F3a rework's
/// Finding 1). A previous boot's unmarked chunk can never gain its
/// marker — its transaction ended with the boot — so retaining it in
/// the arm held the start back at an older boot's record, the next
/// meta's `wal_start_lsn` pointed at it, and §12.5's first-record
/// floor (the opened meta's `boot_id`) stopped the next walk there,
/// losing every acked commit of the new boot (100 class-C
/// `frontier < lo` store losses in the first run's Acceptance file).
pub fn wal_start_lsn(st: &StoreInner, t: u64, open_chunks: &BTreeMap<(u32, u64), u64>) -> u64 {
    let mut start = st.wal.append;
    for (l, (ts, _marker, _txn)) in st.wal.buf_tags.iter() {
        if *ts > t {
            start = start.min(*l);
        }
    }
    let skip_marked = matches!(st.cfg.mutant, Some(Mutant::StartLsnMarkedChunk));
    let skip_open = matches!(st.cfg.mutant, Some(Mutant::StartLsnPastChunk));
    for (txn, first) in open_chunks.iter() {
        // This txn's marker, if any (enqueued after its first chunk).
        let marker = chunk_txn_marker(st, txn, first);
        match marker {
            Some(ts) if ts <= t => continue, // wholly in the image
            Some(_) => {
                // Marked above T (the §12.6 marked case).
                if !skip_marked {
                    start = start.min(*first);
                }
            }
            None => {
                // An open transaction (no marker yet) — of **this boot
                // only**: a previous boot's unmarked chunk never gains a
                // marker, so §14 step 8 keeps it out of the hold-back
                // (the arm's boot filter; the rework's Finding 1). The
                // chunks still sit in `open_chunks` (a marker of the
                // same id from a *newer* boot could name them), but the
                // start never points at an older boot's record.
                if !skip_open && !boot_id_older(txn.0, st.boot_id) {
                    start = start.min(*first);
                }
            }
        }
    }
    start
}

/// The commit ts of `txn`'s CommitMarker (the marker enqueued after its
/// first chunk LSN `first`), if it has one. Shared by the flip's
/// second term and the entry-retention rule.
pub fn chunk_txn_marker(st: &StoreInner, txn: &(u32, u64), first: &u64) -> Option<u64> {
    st.wal
        .buf_tags
        .iter()
        .find(|(l, (_, m, tx))| *m && **l > *first && *tx == *txn)
        .map(|(_, (ts, _, _))| *ts)
}

/// **The one store test hook (X-5c-F3-F3b step 3; Limits 3):** which
/// WAL-path operation fails, and on which batch (identified by its
/// start LSN). Default-off (`StoreCfg::wal_io_fail = None`); used only
/// by step 3's literal-config pins, never by the workload or the
/// campaign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalIoFail {
    /// The operation that fails.
    pub op: WalIoOp,
    /// The failing batch's start LSN.
    pub batch_start: u64,
}

/// The WAL-path operation kinds the hook can fail (see [`WalIoFail`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalIoOp {
    /// A batch's `write_at`.
    Write,
    /// A batch's group `sync_data`.
    SyncData,
}

/// Whether this op on the batch starting at `start` is the hooked one.
/// A poisoned store never retries (the caller's error exit poisons, and
/// every later take refuses at the alive check).
fn wal_io_hook_fails<H: StoreHost>(cx: &Cx<'_, H>, op: WalIoOp, start: u64) -> Result<bool, Fail> {
    cx.with("wal_io:hook", move |c| {
        let st = c.store();
        Ok(matches!(
            st.cfg.wal_io_fail,
            Some(WalIoFail { op: want, batch_start }) if want == op && batch_start == start
        ))
    })
}

/// Step 6's recycling (§12.1/§13): rename each segment wholly below
/// `wal_start_lsn` to a future first LSN (the next boundaries after the
/// append position), with G1's dirsync after each rename. Returns the
/// number of recycled segments.
///
/// **A rename that fails while the world has crashed is a delivered
/// crash and is returned, never swallowed** (X-5c-F4-F1's re-gate,
/// finding 1; card X-5c-F4-F1-F1 step 0 (a)): the sim's env maps every
/// disk error to `SimFail::Crashed`, so a crash delivered at the
/// rename's yield and a plain "`to` exists" failure carry the same
/// `Err` — the world's `crashed` flag is what tells them apart. A plain
/// `?` would be wrong (measured by the gate: "`to` exists" is routine,
/// 111 times over 0..200, and the store relies on that quiet skip —
/// `M-RENAME-ANYERR` moved 67 of 200 digests); swallowing the crash let
/// step 6's tail (`After6`, `ckpt:end`, the next poll's final
/// checkpoint) run on a crashed world and clear the SIZING-C fold, so
/// seed 2376 r2 undercounted (`szc` false with the fold at 235 ≥ 4·F).
pub fn wal_recycle<H: StoreHost>(cx: &Cx<'_, H>, wal_start: u64) -> Result<u64, Fail> {
    let mut recycled = 0u64;
    let plan = cx.with("wal_recycle:plan", |c| {
        let st = c.store();
        let segs: Vec<(u64, usize)> = st.wal.segs.iter().map(|(k, v)| (*k, *v)).collect();
        let mut next_bound = seg_of(st.wal.append) + WAL_SEG;
        let mut out = Vec::new();
        for (first, fid) in segs {
            if first + WAL_SEG <= wal_start {
                out.push((first, fid, next_bound));
                next_bound += WAL_SEG;
            }
        }
        Ok(out)
    })?;
    for (first, fid, to) in plan {
        let name = format!("/wal/{first:016x}.log");
        let to_name = format!("/wal/{to:016x}.log");
        let renamed = cx.env().rename(&name, &to_name).map_err(Fail::Sim);
        if let Err(e) = &renamed
            && cx.with("wal_recycle:crashed", |c| Ok(c.sched_mut().crashed))?
        {
            return Err(e.clone());
        }
        if renamed.is_ok() {
            recycled += 1;
            let skip = cx.with("wal_recycle:mut", |c| {
                Ok(matches!(
                    c.store().cfg.mutant,
                    Some(Mutant::RenameNoDirsync)
                ))
            })?;
            cx.with("wal_recycle:rebind", |c| {
                // X5-R38: the current yield count, read before the
                // store borrow (the set records `current + 1`).
                let y = c.sched_mut().yields + 1;
                let st = c.store();
                st.wal.segs.remove(&first);
                st.wal.segs.insert(to, fid);
                if skip {
                    st.wal.seg_needs_dirsync.push(to);
                }
                // **X5-R38 (card X-5c-F4-F1 step 3; observables only):
                // the W12 window opens** — the rename returned, its
                // `sync_dir` has not. The recorded yield index is the
                // next one: the set happens between two yield events,
                // so `current + 1` is the first index a
                // `CrashSpec::Yield(k)` can fire on with the rename
                // done and the dirsync not (firing at `current` itself
                // is in the past — the crash would have preceded the
                // rename). The boundary also becomes
                // `recycled_unwritten` (W11's condition's input).
                st.w12_window = true;
                st.w12_yields.push(y);
                st.wal.recycled_unsynced.insert(to);
                Ok(())
            })?;
            if !skip {
                cx.env().sync_dir("/wal/").map_err(Fail::Sim)?;
                cx.with("wal_recycle:dirsynced", |c| {
                    let st = c.store();
                    st.wal.seg_needs_dirsync.clear();
                    // X5-R38: the dirsync completed — the W12 window
                    // closes.
                    st.w12_window = false;
                    Ok(())
                })?;
            }
        }
    }
    Ok(recycled)
}
