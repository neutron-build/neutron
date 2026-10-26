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
//! Group commit (§12.4): one writer drains the queue, writes one batch,
//! does one `sync_data` per touched segment, then acks the SyncOn commits
//! it covered; SyncOff commits are acked after the write. The commit
//! critical section assigns the ts and enqueues the record at its LSN
//! (I-WAL-TS-ORDER; the TS-AFTER-APPEND mutant ends the critical section
//! between the two).
//!
//! G1: `sync_dir("/wal/")` after every segment create/rename and before
//! any record in a created or renamed segment is counted durable or
//! acked; `sync_dir("/")` after creating the page file and `wal/`.

use std::collections::BTreeMap;

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
    /// Per-segment created-but-unsynced-dir flags (G1 bookkeeping).
    pub seg_needs_dirsync: Vec<u64>,
    /// Chunked transactions: txn → first chunk LSN (§12.6). An entry
    /// lives from its first chunk until a flip whose `T` covers its
    /// marker (`marker_ts ≤ T` — wholly in the image); a marker's
    /// enqueue does **not** remove it, because the flip still needs the
    /// first-chunk LSN while the marker is above `T` (the marked case).
    pub open_chunks: BTreeMap<(u32, u64), u64>,
    /// The flips' enqueue positions, in flip order (M-CKPT-T's hook: the
    /// card names this supply, Work 3.10; read only by the monitors).
    pub flip_enqueue_pos: Vec<u64>,
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
        // A record placed here must either end exactly at the segment
        // boundary or leave room enough for a later SegmentEnd filler
        // (§12.2: no record spans segments, and the reader hops segments
        // only on a filler it can decode). B5 prototype-deviation fix:
        // the draft compared only `probe ≤ room`, so a record could land
        // with a tail the 45-byte filler cannot fill — the record then
        // straddled the boundary while carrying the next segment's lsn,
        // and §12.5's position check rejected its bytes on read.
        let fits = probe <= room && (room - probe == 0 || room - probe >= REC_FIXED);
        let rec_lsn = if fits { lsn0 } else { seg + WAL_SEG };
        let rec = enc_record(kind, txn, ts, payload, rec_lsn);
        let rec_len = rec.len() as u64;
        let mut placed: Vec<u8> = Vec::new();
        if !fits && lsn0 != seg + WAL_SEG {
            // A SegmentEnd filler at lsn0, then the lsn space padded to
            // the boundary (the reader never reads the pad: it hops on
            // the filler, §12.5).
            let se = enc_record(K_SEGEND, (0, 0), 0, &[], lsn0);
            placed.extend_from_slice(&se);
            let gap = ((seg + WAL_SEG) - (lsn0 + se.len() as u64)) as usize;
            placed.extend_from_slice(&vec![0u8; gap]);
        }
        placed.extend_from_slice(&rec);
        st.wal.buf_tags.insert(rec_lsn, (ts, kind == K_MARKER, txn));
        st.wal.buf.extend_from_slice(&placed);
        st.wal.append = lsn0 + placed.len() as u64;
        Ok((rec_lsn, rec_len))
    })
}

/// The writer step: drain the buffer to the segments (one batch), then
/// one `sync_data` per touched segment, then advance `durable` and wake
/// the ack waiters. SyncOff acks arrive from `written`. G1: a directory
/// sync after every create, before any record in the segment counts
/// durable (RENAME-NO-DIRSYNC skips it).
pub fn wal_writer_step<H: StoreHost>(cx: &Cx<'_, H>) -> Result<(), Fail> {
    // Take the batch (one step).
    let batch = cx.with("wal_writer:take", |c| {
        let st = c.store();
        if st.wal.buf.is_empty() {
            return Ok(None);
        }
        let buf = std::mem::take(&mut st.wal.buf);
        let start = st.wal.append - buf.len() as u64;
        Ok(Some((buf, start)))
    })?;
    let Some((buf, mut lsn)) = batch else {
        return Ok(());
    };
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
                    // Create + preallocate + G1. Two tasks can reach
                    // here for the same segment (the writer task and a
                    // committer's help call, S-BATON interleaving at
                    // every env call): the loser's create fails with
                    // "exists" and falls back to opening the winner's
                    // file, re-running the same bind and dirsync (G1 is
                    // idempotent — a second sync_dir of "/wal/" is a
                    // no-op ordering-wise, and durability of this
                    // segment's records is never counted before at
                    // least one completed dirsync).
                    let name = format!("/wal/{seg:016x}.log");
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
        cx.env().write_at(fid, off, bytes).map_err(Fail::Sim)?;
        if !touched.contains(&seg) {
            touched.push(seg);
        }
        i += chunk;
        lsn += chunk as u64;
    }
    cx.with("wal_writer:written", |c| {
        let st = c.store();
        st.wal.written = st.wal.written.max(lsn);
        Ok(())
    })?;
    cx.w.wake(crate::sim::WaitKey::WalAcks);
    // A named park point (card Work 13): D4 holds the writer between
    // its batch write and its group sync, so a flip's step 1 is the
    // only thing that can make the batch durable (G5's "except by step
    // 1" shape).
    cx.park("writer:presync")?;
    // One sync per touched segment (a batch's group commit).
    for seg in touched {
        let f = cx.with("wal_writer:fid", |c| {
            Ok(c.store().wal.segs.get(&seg).copied())
        })?;
        if let Some(f) = f {
            cx.env().sync_data(f).map_err(Fail::Sim)?;
        }
    }
    cx.with("wal_writer:durable", |c| {
        let st = c.store();
        st.wal.durable = st.wal.durable.max(lsn);
        Ok(())
    })?;
    cx.w.wake(crate::sim::WaitKey::WalAcks);
    Ok(())
}

/// Wait until the WAL is durable through `lsn` (a SyncOn ack). Waiting
/// tasks help drain the writer queue cooperatively first (§12.4's
/// embedded mode), so single-task worlds (recovery, directed tests)
/// cannot starve. Helping is only legal when this task can make
/// progress itself — the buffer is non-empty, or the records are
/// written but unsynced. A batch taken but not yet written by another
/// task (S-BATON: `wal_writer_step` holds the taken bytes across its
/// env yields) is invisible here: waiting on it without parking would
/// spin without yielding the baton, so the holder could never run —
/// the wait parks on [`WaitKey::WalAcks`] instead and the holder's
/// write wakes it (prototype deviation fixed in B5; see
/// `tests/directed.rs` D4).
pub fn wal_wait_durable<H: StoreHost>(cx: &Cx<'_, H>, lsn: u64) -> Result<(), Fail> {
    loop {
        let (done, can_write, can_sync) = cx.with("wal_durable:chk", |c| {
            let st = c.store();
            st.alive()?;
            Ok((
                st.wal.durable >= lsn,
                !st.wal.buf.is_empty(),
                st.wal.durable < lsn && st.wal.written >= lsn,
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
            Ok((st.wal.written >= lsn, !st.wal.buf.is_empty()))
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

/// `sync_data` every segment covering `[durable, lsn)` and advance the
/// durable-through accounting to `written` (one step per segment, then
/// one for the accounting). Only the segments the store has open are
/// synced; a range the store never wrote cannot be made durable by
/// syncing (G5: appending means enqueued, durable means synced where
/// written).
fn wal_sync_through<H: StoreHost>(cx: &Cx<'_, H>, lsn: u64) -> Result<(), Fail> {
    let (d, w) = cx.with("wal_sync:range", |c| {
        let st = c.store();
        Ok((st.wal.durable, st.wal.written))
    })?;
    let mut seg = seg_of(d);
    let last = seg_of(w.min(lsn).max(d));
    while seg <= last {
        let f = cx.with("wal_sync:fid", |c| {
            Ok(c.store().wal.segs.get(&seg).copied())
        })?;
        if let Some(f) = f {
            cx.env().sync_data(f).map_err(Fail::Sim)?;
        }
        seg += WAL_SEG;
    }
    cx.with("wal_sync:adv", |c| {
        let st = c.store();
        st.wal.durable = st.wal.durable.max(st.wal.written.min(lsn));
        Ok(())
    })?;
    cx.w.wake(crate::sim::WaitKey::WalAcks);
    Ok(())
}

/// Step 1: sync the WAL through every record enqueued before the flip
/// (§13; G5). WAL-SYNC-T-ONLY syncs only through the last record with
/// `commit_ts ≤ T`.
pub fn wal_step1<H: StoreHost>(cx: &Cx<'_, H>, t: u64, enqueue_end: u64) -> Result<(), Fail> {
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
            Ok(enqueue_end)
        }
    })?;
    wal_wait_durable(cx, end)
}

/// Compute `wal_start_lsn` at the flip (§12.6): the minimum of the
/// LSN of the first **Commit or CommitMarker** with `commit_ts > T`
/// (or the append position if there is none), and the first Chunk LSN
/// of every chunked transaction whose marker is not yet below `T`.
/// (Chunk records carry ts 0, reserved, so only the second term counts
/// them; SegmentEnd fillers likewise never pull the start.)
/// START-LSN-MARKED-CHUNK drops the marked case; START-LSN-PAST-CHUNK
/// drops the open-txn case.
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
                // An open transaction (no marker yet).
                if !skip_open {
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

/// Step 6's recycling (§12.1/§13): rename each segment wholly below
/// `wal_start_lsn` to a future first LSN (the next boundaries after the
/// append position), with G1's dirsync after each rename. Returns the
/// number of recycled segments.
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
        if cx.env().rename(&name, &to_name).map_err(Fail::Sim).is_ok() {
            recycled += 1;
            let skip = cx.with("wal_recycle:mut", |c| {
                Ok(matches!(
                    c.store().cfg.mutant,
                    Some(Mutant::RenameNoDirsync)
                ))
            })?;
            cx.with("wal_recycle:rebind", |c| {
                let st = c.store();
                st.wal.segs.remove(&first);
                st.wal.segs.insert(to, fid);
                if skip {
                    st.wal.seg_needs_dirsync.push(to);
                }
                Ok(())
            })?;
            if !skip {
                cx.env().sync_dir("/wal/").map_err(Fail::Sim)?;
                cx.with("wal_recycle:dirsynced", |c| {
                    c.store().wal.seg_needs_dirsync.clear();
                    Ok(())
                })?;
            }
        }
    }
    Ok(recycled)
}
