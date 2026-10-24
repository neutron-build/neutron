#![deny(clippy::indexing_slicing)]
//! The WAL (S-STORE §12; card X-5 Work 4.1–4.5).
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
//! (I-WAL-TS-ORDER; the TS-AFTER-APPEND mutant yields between the two).
//!
//! G1: `sync_dir("/wal/")` after every segment create/rename and before
//! any record in a created or renamed segment is counted durable or
//! acked; `sync_dir("/")` after creating the page file and `wal/`.

use std::collections::BTreeMap;

use super::{Fail, Mutant, StoreInner, Cx, WAL_SEG};
use crate::sim::E4Env;
use crate::crc::crc32c;

/// Record kinds (§12.2).
pub const K_COMMIT: u8 = 1;
pub const K_CHUNK: u8 = 2;
pub const K_MARKER: u8 = 3;
pub const K_SEGEND: u8 = 4;

/// One logged op (§12.3).
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
    let s = b.get(off..off + 4)?;
    Some(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
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

/// Decode one op from `b` (advancing the cursor by reference).
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
            Some((WOp::Put { tree: t, key, value }, i))
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

/// Encode a whole record.
pub fn enc_record(kind: u8, txn: (u32, u64), ts: u64, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(payload.len() + 29);
    wr_u64(&mut body, 0); // lsn patched by the enqueuer
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

/// Decode a record at `lsn` from `b`, with the LSN-CHECK-SKIP mutant hook
/// (the §12.5 stale check).
pub fn dec_record_with_lsn_check(
    b: &[u8],
    lsn: u64,
    seg_end: u64,
    mutant: Option<super::Mutant>,
) -> Result<Option<Rec>, ()> {
    if matches!(mutant, Some(super::Mutant::LsnCheckSkip)) {
        // Skip the lsn check: decode with the expected position patched.
        let mut b2 = b.to_vec();
        if b2.len() >= 24 {
            let l = lsn.to_le_bytes();
            for (i, byte) in l.iter().enumerate() {
                if let Some(dst) = b2.get_mut(16 + i) {
                    *dst = *byte;
                }
            }
        }
        return dec_record(&b2, lsn, seg_end);
    }
    dec_record(b, lsn, seg_end)
}

/// A decoded record.
#[derive(Debug, Clone)]
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

/// Decode a record at `lsn` from `b` (its bytes), validating magic, crc
/// and len (§12.2). `Err(())` = not a valid record.
pub fn dec_record(b: &[u8], lsn: u64, seg_end: u64) -> Result<Option<Rec>, ()> {
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
    if rlsn != lsn {
        // The §12.5 stale check (LSN-CHECK-SKIP removes it).
        return Err(());
    }
    let kind = *body.first().ok_or(())?;
    let txn = (
        rd_u32(body, 9).ok_or(())?,
        rd_u64(body, 13).ok_or(())?,
    );
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
#[derive(Debug, Default)]
pub struct WalState {
    /// The append position (next LSN).
    pub append: u64,
    /// Enqueued-but-unwritten record bytes (the writer's buffer, G5).
    pub buf: Vec<u8>,
    /// Enqueued records by LSN → `(ts, is_marker, txn)`.
    pub buf_tags: BTreeMap<u64, (u64, bool, (u32, u64))>,
    /// Open segment files by first LSN.
    pub segs: BTreeMap<u64, usize>,
    /// The durable-through LSN (advanced by the writer's syncs).
    pub durable: u64,
    /// The written-through LSN (advanced by the writer's writes).
    pub written: u64,
    /// Per-segment created-but-unsynced-dir flags (G1).
    pub seg_needs_dirsync: Vec<u64>,
    /// Open chunked transactions: txn → first chunk LSN (§12.6).
    pub open_chunks: BTreeMap<(u32, u64), u64>,
}

impl WalState {
    /// The first LSN of the segment containing `lsn`.
    pub fn seg_of(&self, lsn: u64) -> u64 {
        lsn - (lsn % WAL_SEG)
    }
}

impl StoreInner {
    /// Enqueue a record at its LSN (the commit critical section's second
    /// half; I-WAL-TS-ORDER). Returns `(lsn, len)` of the record itself
    /// (excluding any `SegmentEnd` filler).
    pub fn wal_enqueue(
        &mut self,
        cx: &Cx,
        kind: u8,
        txn: (u32, u64),
        ts: u64,
        payload: &[u8],
    ) -> Result<(u64, u64), Fail> {
        // A WAL enqueue is a yield point (card 1.2).
        cx.yield_now()?;
        let lsn0 = self.wal.append;
        let mut rec = enc_record(kind, txn, ts, payload);
        let seg = lsn0 - (lsn0 % WAL_SEG);
        let room = seg + WAL_SEG - lsn0;
        let rec_lsn = if (rec.len() as u64) <= room {
            lsn0
        } else {
            seg + WAL_SEG
        };
        // Patch the lsn field (body offset 0 → record offset 16).
        if rec.len() >= 24 {
            let l = rec_lsn.to_le_bytes();
            for (i, byte) in l.iter().enumerate() {
                if let Some(dst) = rec.get_mut(16 + i) {
                    *dst = *byte;
                }
            }
        }
        let rec_len = rec.len() as u64;
        let mut placed: Vec<u8> = Vec::new();
        if rec_lsn != lsn0 {
            // A record that does not fit the rest of its segment is
            // preceded by SegmentEnd (§12.2).
            let mut se = enc_record(K_SEGEND, (0, 0), 0, &[]);
            if se.len() >= 24 {
                let l = lsn0.to_le_bytes();
                for (i, byte) in l.iter().enumerate() {
                    if let Some(dst) = se.get_mut(16 + i) {
                        *dst = *byte;
                    }
                }
            }
            placed.extend_from_slice(&se);
        }
        placed.extend_from_slice(&rec);
        self.wal
            .buf_tags
            .insert(rec_lsn, (ts, kind == K_MARKER, txn));
        self.wal.buf.extend_from_slice(&placed);
        self.wal.append = lsn0 + placed.len() as u64;
        Ok((rec_lsn, rec_len))
    }

    /// The writer step: drain the buffer to the segments (one batch), then
    /// one `sync_data` per touched segment, then advance `durable` and
    /// wake the ack waiters. SyncOff acks arrive from `written`. G1: the
    /// directory sync after every create/rename, before any record in the
    /// segment counts durable.
    pub fn wal_writer_step(&mut self, cx: &Cx) -> Result<(), Fail> {
        if self.wal.buf.is_empty() {
            return Ok(());
        }
        let buf = std::mem::take(&mut self.wal.buf);
        let mut lsn = self.wal.append - buf.len() as u64;
        // Write out the batch, creating segments (with G1 dirsyncs) as
        // needed.
        let mut touched: Vec<u64> = Vec::new();
        let mut i = 0usize;
        while i < buf.len() {
            let seg = lsn - (lsn % WAL_SEG);
            let off = lsn - seg;
            let room = (WAL_SEG - off) as usize;
            let chunk = (buf.len() - i).min(room);
            let bytes = buf.get(i..i + chunk).ok_or_else(|| Fail::Invalid("batch".to_owned()))?;
            let fid = match self.wal.segs.get(&seg) {
                Some(f) => *f,
                None => {
                    // Create + preallocate + G1 (RENAME-NO-DIRSYNC skips it).
                    let name = format!("/wal/{seg:016x}.log");
                    let f = cx.env().create(&name).map_err(Fail::Sim)?;
                    cx.env().set_len(f, WAL_SEG).map_err(Fail::Sim)?;
                    self.wal.segs.insert(seg, f);
                    self.wal.seg_needs_dirsync.push(seg);
                    if !matches!(self.cfg.mutant, Some(Mutant::RenameNoDirsync)) {
                        cx.env().sync_dir("/wal/").map_err(Fail::Sim)?;
                        self.wal.seg_needs_dirsync.clear();
                    }
                    f
                }
            };
            cx.env().write_at(fid, off, bytes).map_err(Fail::Sim)?;
            if !touched.contains(&seg) {
                touched.push(seg);
            }
            i += chunk;
            lsn += chunk as u64;
        }
        self.wal.written = lsn;
        cx.w.wake(crate::sim::WaitKey::WalAcks);
        // One sync per touched segment (a batch's group commit).
        for seg in touched {
            if let Some(f) = self.wal.segs.get(&seg).copied() {
                cx.env().sync_data(f).map_err(Fail::Sim)?;
            }
        }
        self.wal.durable = lsn;
        cx.w.wake(crate::sim::WaitKey::WalAcks);
        Ok(())
    }

    /// Wait until the WAL is durable through `lsn` (a SyncOn ack).
    pub fn wal_wait_durable(&mut self, cx: &Cx, lsn: u64) -> Result<(), Fail> {
        loop {
            let ok = self.wal.durable >= lsn;
            cx.yield_now()?;
            if ok {
                return Ok(());
            }
            cx.wait_for(crate::sim::WaitKey::WalAcks)?;
        }
    }

    /// Wait until the WAL is written through `lsn` (a SyncOff ack).
    pub fn wal_wait_written(&mut self, cx: &Cx, lsn: u64) -> Result<(), Fail> {
        loop {
            let ok = self.wal.written >= lsn;
            cx.yield_now()?;
            if ok {
                return Ok(());
            }
            cx.wait_for(crate::sim::WaitKey::WalAcks)?;
        }
    }

    /// Step 1: sync the WAL through every record enqueued before the flip
    /// (§13; G5). WAL-SYNC-T-ONLY syncs only through the last record with
    /// `commit_ts ≤ T`.
    pub fn wal_step1(&mut self, cx: &Cx, t: u64, enqueue_end: u64) -> Result<(), Fail> {
        let end = if matches!(self.cfg.mutant, Some(Mutant::WalSyncTOnly)) {
            self.wal
                .buf_tags
                .iter()
                .rev()
                .find(|(_, (ts, _, _))| *ts <= t && *ts > 0)
                .map(|(l, _)| *l)
                .unwrap_or(0)
        } else {
            enqueue_end
        };
        if self.wal.durable >= end {
            return Ok(());
        }
        let written_pos = self.wal.append - self.wal.buf.len() as u64;
        if !self.wal.buf.is_empty() || end > written_pos {
            // Write and sync the buffered batch (which covers `end`).
            self.wal_writer_step(cx)?;
            return Ok(());
        }
        // Already written: sync the tail segments through `end`.
        let mut seg = self.wal.durable - (self.wal.durable % WAL_SEG);
        let last = end - (end % WAL_SEG);
        while seg <= last {
            if let Some(f) = self.wal.segs.get(&seg).copied() {
                cx.env().sync_data(f).map_err(Fail::Sim)?;
            }
            seg += WAL_SEG;
        }
        self.wal.durable = self.wal.durable.max(end);
        Ok(())
    }

    /// Compute `wal_start_lsn` at the flip (§12.6): the minimum of the
    /// first Commit/CommitMarker with `commit_ts > T` (or the append
    /// position if none), and the first Chunk LSN of every chunked
    /// transaction whose marker is not yet below `T`. START-LSN-MARKED-
    /// CHUNK drops the marked case; START-LSN-PAST-CHUNK drops the
    /// open-txn case.
    pub fn wal_start_lsn(&self, t: u64, open_chunks: &BTreeMap<(u32, u64), u64>) -> u64 {
        let mut start = self.wal.append;
        for (l, (ts, marker, _txn)) in self.wal.buf_tags.iter() {
            if *marker && *ts > t {
                start = start.min(*l);
            }
        }
        let skip_marked = matches!(self.cfg.mutant, Some(Mutant::StartLsnMarkedChunk));
        let skip_open = matches!(self.cfg.mutant, Some(Mutant::StartLsnPastChunk));
        for (txn, first) in open_chunks.iter() {
            // This txn's marker, if any.
            let marker = self
                .wal
                .buf_tags
                .iter()
                .find(|(l, (_, m, tx))| *m && **l > *first && *tx == *txn)
                .map(|(_, (ts, _, _))| *ts);
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

    /// Step 6's recycling (§12.1/§13): rename each segment wholly below
    /// `wal_start_lsn` to a future first LSN (the next boundaries after
    /// the append position), with G1's dirsync after each rename.
    pub fn wal_recycle(&mut self, cx: &Cx, wal_start: u64) -> Result<u64, Fail> {
        let mut recycled = 0u64;
        let segs: Vec<(u64, usize)> = self.wal.segs.iter().map(|(k, v)| (*k, *v)).collect();
        let mut next_bound = self.wal.append - (self.wal.append % WAL_SEG) + WAL_SEG;
        for (first, _fid) in segs {
            if first + WAL_SEG <= wal_start {
                let name = format!("/wal/{first:016x}.log");
                let to = format!("/wal/{next_bound:016x}.log");
                if cx.env().rename(&name, &to).map_err(Fail::Sim).is_ok() {
                    recycled += 1;
                    self.wal.segs.remove(&first);
                    self.wal.segs.insert(next_bound, _fid);
                    if !matches!(self.cfg.mutant, Some(Mutant::RenameNoDirsync)) {
                        cx.env().sync_dir("/wal/").map_err(Fail::Sim)?;
                        self.wal.seg_needs_dirsync.clear();
                    } else {
                        self.wal.seg_needs_dirsync.push(next_bound);
                    }
                    next_bound += WAL_SEG;
                }
            }
        }
        Ok(recycled)
    }
}
