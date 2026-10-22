//! The undo log of E1′ arm (c): the newest committed version lives in
//! place in the X-0 B+tree and every version it replaces is appended here
//! (card X-3 Work 3; PLAN-v3 §2 N3 arm (c), §4 E1′ — the HyPer/InnoDB
//! scheme).
//!
//! A record names the commit ts that superseded the version it holds
//! (`superseded_at`), so a reader at snapshot `S` walks from the tree
//! value backwards until a record's `prev_ts <= S` (card Work 4). Undo
//! past the in-memory budget **spills to a temp file and readers are
//! never aborted** (N3's undo policy — there is no "snapshot too old").
//!
//! Invariants owned here:
//! - U-PTR: record pointers are `1, 2, 3, …` — monotonic, never reused.
//!   The in-memory deque holds the newest consecutive run of pointers and
//!   the spill index the immediately older run, so a pointer's location
//!   is derived from the two range bounds, not stored per record.
//! - U-SPILL: the file holds the **oldest** records, in pointer order.
//!   When the in-memory bytes exceed the budget the oldest records move
//!   to the append-only spill file ([`FileExt::write_at`]) and their
//!   `(offset, len)` entries join the index, so a read of a spilled
//!   record goes to the file and returns byte-for-byte the same record.
//! - U-TRIM-SAFE: [`UndoLog::trim`] removes only the prefix of records
//!   with `superseded_at <= watermark`, stopping at the first record
//!   above it. A record superseded at `c` is reachable only by a
//!   snapshot `S < c`, and every live or future snapshot is
//!   `S >= watermark` (C-SNAP-REG, C-T0 §9.2 I-GC), so `c <= watermark`
//!   implies no reader can reach it; stopping early can only leave a
//!   dead record behind, never remove a live one (applies serialize per
//!   key through the lock table, so one key's records arrive in ts
//!   order; a lower-ts record parked behind a higher-ts cross-key apply
//!   merely survives one gc round longer).
//! - U-FILE-RECLAIM: dead file records leave the index immediately, but
//!   the file's bytes are reclaimed only when the whole file is dead
//!   (truncate to zero) — the spike does no hole punching and never
//!   rewrites the middle of the file.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use e1_mvcc::api::Ts;

/// The replaced version was a live value (card Work 2's tag encoding).
pub const TAG_VALUE: u8 = 0;

/// The replaced version was a tombstone (a delete).
pub const TAG_TOMBSTONE: u8 = 1;

/// Serialized size of a record's fixed header: `ptr u64 | superseded_at
/// u64 | prev_ts u64 | prev_undo_ptr u64 | prev_tag u8 | key_len u32`.
const HEADER_BYTES: u64 = 8 + 8 + 8 + 8 + 1 + 4;

/// Error type of the undo log: spill-file I/O, or bytes that do not
/// decode. Fail-stop; the engine maps both to [`e1_mvcc::api::TxnErr`].
#[derive(Debug, thiserror::Error)]
pub enum UndoError {
    /// Spill-file I/O failed.
    #[error("undo spill io: {0}")]
    Io(#[from] io::Error),
    /// A record's bytes did not decode.
    #[error("undo record did not decode: {0}")]
    Decode(String),
}

/// One undo record (card X-3 Work 3): the version the commit at
/// `superseded_at` replaced. `prev_undo_ptr` chains to the record
/// holding the version before that one; `0` ends the chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndoRecord {
    /// This record's pointer; monotonic from 1 (U-PTR).
    pub ptr: u64,
    /// The commit ts that replaced the version this record holds.
    pub superseded_at: Ts,
    /// The key whose version was replaced.
    pub key: Vec<u8>,
    /// The ts of the replaced version (never 0: a record is pushed only
    /// when a tree value existed to replace).
    pub prev_ts: Ts,
    /// [`TAG_VALUE`] or [`TAG_TOMBSTONE`]: what the replaced version was.
    pub prev_tag: u8,
    /// The replaced version's own undo pointer (0 = chain end).
    pub prev_undo_ptr: u64,
    /// The replaced version's bytes (empty for a tombstone).
    pub prev_bytes: Vec<u8>,
}

/// The tracked byte cost of one record: its serialized size, with a
/// tombstone's empty payload accounted as one byte — the same convention
/// as the write set's W-BYTES and the reference engine's GC accounting.
pub fn record_bytes(key: &[u8], prev_tag: u8, prev_bytes: &[u8]) -> u64 {
    let payload = if prev_tag == TAG_TOMBSTONE {
        1
    } else {
        prev_bytes.len() as u64
    };
    HEADER_BYTES + key.len() as u64 + payload
}

/// Serialize one record (the spill-file payload; see [`HEADER_BYTES`]).
fn encode_record(rec: &UndoRecord) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_BYTES as usize + rec.key.len() + rec.prev_bytes.len());
    buf.extend_from_slice(&rec.ptr.to_be_bytes());
    buf.extend_from_slice(&rec.superseded_at.to_be_bytes());
    buf.extend_from_slice(&rec.prev_ts.to_be_bytes());
    buf.extend_from_slice(&rec.prev_undo_ptr.to_be_bytes());
    buf.push(rec.prev_tag);
    buf.extend_from_slice(&(rec.key.len() as u32).to_be_bytes());
    buf.extend_from_slice(&rec.key);
    buf.extend_from_slice(&rec.prev_bytes);
    buf
}

/// Read one big-endian `u64` at `at`; `None` when the buffer is short.
/// Shared by the record and tree-value codecs of this crate.
pub fn be_u64(buf: &[u8], at: usize) -> Option<u64> {
    let end = at.checked_add(8)?;
    let slice = buf.get(at..end)?;
    let mut b = [0u8; 8];
    b.copy_from_slice(slice);
    Some(u64::from_be_bytes(b))
}

/// Read one big-endian `u32` at `at`; `None` when the buffer is short.
fn be_u32(buf: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    let slice = buf.get(at..end)?;
    let mut b = [0u8; 4];
    b.copy_from_slice(slice);
    Some(u32::from_be_bytes(b))
}

/// Deserialize one record ([`encode_record`]'s inverse); `Err` on short
/// or inconsistent bytes, never a panic.
fn decode_record(buf: &[u8]) -> Result<UndoRecord, UndoError> {
    let short = |what: &str| UndoError::Decode(format!("short {what}"));
    let ptr = be_u64(buf, 0).ok_or_else(|| short("ptr"))?;
    let superseded_at = be_u64(buf, 8).ok_or_else(|| short("superseded_at"))?;
    let prev_ts = be_u64(buf, 16).ok_or_else(|| short("prev_ts"))?;
    let prev_undo_ptr = be_u64(buf, 24).ok_or_else(|| short("prev_undo_ptr"))?;
    let prev_tag = *buf.get(32).ok_or_else(|| short("prev_tag"))?;
    let key_len = be_u32(buf, 33).ok_or_else(|| short("key_len"))? as usize;
    let key_start = HEADER_BYTES as usize;
    let key_end = key_start.checked_add(key_len).ok_or_else(|| short("key"))?;
    let key = buf
        .get(key_start..key_end)
        .ok_or_else(|| short("key"))?
        .to_vec();
    let prev_bytes = match buf.get(key_end..) {
        Some(rest) => rest.to_vec(),
        None => return Err(short("prev_bytes")),
    };
    Ok(UndoRecord {
        ptr,
        superseded_at,
        key,
        prev_ts,
        prev_tag,
        prev_undo_ptr,
        prev_bytes,
    })
}

/// Index entry of one spilled record: where its payload sits in the file
/// (`offset`, `len`), the ts it was superseded at (for [`UndoLog::trim`])
/// and its accounted bytes (for the stats and the reclaim total).
struct SpillEntry {
    offset: u64,
    len: u32,
    superseded_at: Ts,
    bytes: u64,
}

/// Interior state of [`UndoLog`]; every field is under `state`'s mutex.
struct UndoState {
    /// The newest records, oldest first (U-PTR's newest run).
    mem: VecDeque<UndoRecord>,
    /// Accounted bytes of `mem` (the budget applies to this).
    mem_bytes: u64,
    /// The next pointer to hand out (U-PTR).
    next_ptr: u64,
    /// The lazily created spill file, positioned by `file_len`.
    file: Option<File>,
    /// `(offset, len)` of every spilled record, oldest first.
    index: VecDeque<SpillEntry>,
    /// The pointer of the oldest live record (memory or file); records
    /// below it have been trimmed away.
    spill_base: u64,
    /// The file's append offset (also its physical length).
    file_len: u64,
    /// Accounted bytes of the live records in the file.
    spill_live_bytes: u64,
}

/// The append-only undo log with a memory budget and file spill (card
/// X-3 Work 3; see the module docs for the invariants).
///
/// Every method is `&self`: the state sits behind one mutex whose
/// critical sections are infallible by construction (no indexing, no
/// unwrapping), so a poison is unreachable and recovered with
/// `into_inner` — the same convention as the shared components.
pub struct UndoLog {
    budget: u64,
    path: PathBuf,
    state: Mutex<UndoState>,
}

/// Lock a mutex whose critical sections are panic-free by construction
/// (the e1-mvcc `lock_unpoisoned` convention; pub(crate) there, so this
/// crate carries its own).
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    }
}

impl UndoLog {
    /// An empty log that spills to `path` once its in-memory bytes exceed
    /// `undo_budget_bytes` (U-SPILL). The file is created lazily, on the
    /// first spill.
    pub fn new(undo_budget_bytes: u64, path: PathBuf) -> Self {
        Self {
            budget: undo_budget_bytes,
            path,
            state: Mutex::new(UndoState {
                mem: VecDeque::new(),
                mem_bytes: 0,
                next_ptr: 1,
                file: None,
                index: VecDeque::new(),
                spill_base: 1,
                file_len: 0,
                spill_live_bytes: 0,
            }),
        }
    }

    /// Append the record for the version the commit at `superseded_at`
    /// replaced, and return its pointer (U-PTR). Spills the oldest
    /// records while the in-memory bytes exceed the budget (U-SPILL).
    ///
    /// # Errors
    /// [`UndoError::Io`] when the spill file cannot be created or
    /// written (fail-stop; the engine poisons itself).
    pub fn append(
        &self,
        superseded_at: Ts,
        key: &[u8],
        prev_ts: Ts,
        prev_tag: u8,
        prev_undo_ptr: u64,
        prev_bytes: &[u8],
    ) -> Result<u64, UndoError> {
        let mut s = lock(&self.state);
        let ptr = s.next_ptr;
        s.next_ptr += 1;
        s.mem_bytes += record_bytes(key, prev_tag, prev_bytes);
        s.mem.push_back(UndoRecord {
            ptr,
            superseded_at,
            key: key.to_vec(),
            prev_ts,
            prev_tag,
            prev_undo_ptr,
            prev_bytes: prev_bytes.to_vec(),
        });
        while s.mem_bytes > self.budget {
            let front = match s.mem.pop_front() {
                Some(f) => f,
                None => break, // one record over budget: it spills below
            };
            s.mem_bytes = s.mem_bytes.saturating_sub(record_bytes(
                &front.key,
                front.prev_tag,
                &front.prev_bytes,
            ));
            spill_record(&mut s, &self.path, front)?;
        }
        Ok(ptr)
    }

    /// The record for `ptr`, reading the spill file for spilled records
    /// (U-SPILL); `Ok(None)` when no live record has that pointer
    /// (trimmed away, or never allocated).
    ///
    /// # Errors
    /// [`UndoError::Io`] on a file read, [`UndoError::Decode`] on bytes
    /// that do not parse.
    pub fn get(&self, ptr: u64) -> Result<Option<UndoRecord>, UndoError> {
        let s = lock(&self.state);
        let spilled = s.index.len() as u64;
        if ptr >= s.spill_base && ptr < s.spill_base + spilled {
            let entry = match s.index.get((ptr - s.spill_base) as usize) {
                Some(e) => e,
                None => {
                    return Err(UndoError::Decode(format!(
                        "spill index has no entry for ptr {ptr}"
                    )));
                }
            };
            let file = match &s.file {
                Some(f) => f,
                None => return Err(UndoError::Decode("spill index without a file".into())),
            };
            let mut buf = vec![0u8; entry.len as usize];
            file.read_exact_at(&mut buf, entry.offset)?;
            return Ok(Some(decode_record(&buf)?));
        }
        let mem_base = s.spill_base + spilled;
        if ptr >= mem_base && ptr < s.next_ptr {
            return Ok(s.mem.get((ptr - mem_base) as usize).cloned());
        }
        Ok(None)
    }

    /// Whether a live record has `ptr` (the GC tombstone rule of card
    /// Work 7 consults this).
    pub fn contains(&self, ptr: u64) -> bool {
        let s = lock(&self.state);
        ptr >= s.spill_base && ptr < s.next_ptr
    }

    /// Remove the prefix of records with `superseded_at <= watermark`
    /// (U-TRIM-SAFE) and return the accounted bytes reclaimed. When the
    /// whole file is dead it is truncated to zero (U-FILE-RECLAIM).
    ///
    /// # Errors
    /// [`UndoError::Io`] when the truncation fails; the already-trimmed
    /// prefix stays trimmed (reclaiming more is always safe).
    pub fn trim(&self, watermark: Ts) -> Result<u64, UndoError> {
        let mut s = lock(&self.state);
        let mut reclaimed = 0u64;
        while let Some(front) = s.index.front() {
            if front.superseded_at > watermark {
                break;
            }
            reclaimed += front.bytes;
            s.spill_live_bytes = s.spill_live_bytes.saturating_sub(front.bytes);
            s.index.pop_front();
            s.spill_base += 1;
        }
        if s.index.is_empty() && s.file_len > 0 {
            if let Some(f) = &s.file {
                f.set_len(0)?;
            }
            s.file_len = 0;
        }
        // The memory prefix runs only once the file is fully trimmed:
        // U-PTR derives each pointer's location from the two contiguous
        // ranges [spill_base, file_end) and [file_end, next_ptr), so a
        // gap between a live file record and a trimmed memory record
        // (possible only when a cross-key apply inverted the ts order)
        // must not open. The dead memory record simply survives until
        // the file's front dies — the conservative direction of
        // U-TRIM-SAFE.
        while s.index.is_empty() {
            let (popped, superseded, bytes) = match s.mem.front() {
                Some(f) => (
                    f.ptr,
                    f.superseded_at,
                    record_bytes(&f.key, f.prev_tag, &f.prev_bytes),
                ),
                None => break,
            };
            if superseded > watermark {
                break;
            }
            s.mem_bytes = s.mem_bytes.saturating_sub(bytes);
            reclaimed += bytes;
            s.mem.pop_front();
            s.spill_base = popped + 1;
        }
        Ok(reclaimed)
    }

    /// Live records (memory and file) — the arm's `versions` stat counts
    /// these plus the tree entries.
    pub fn record_count(&self) -> u64 {
        let s = lock(&self.state);
        s.next_ptr.saturating_sub(s.spill_base)
    }

    /// Accounted bytes of the records held in memory (the budget's
    /// metric).
    pub fn mem_bytes(&self) -> u64 {
        lock(&self.state).mem_bytes
    }

    /// Accounted bytes of the live records in the spill file.
    pub fn spill_bytes(&self) -> u64 {
        lock(&self.state).spill_live_bytes
    }
}

impl Drop for UndoLog {
    /// Remove the spill file, best-effort: it exists only while this
    /// log's records do, and a new log never reuses its name.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Move `rec` (the oldest in-memory record) to the spill file and index
/// it (U-SPILL). Creates the file on the first spill.
fn spill_record(s: &mut UndoState, path: &Path, rec: UndoRecord) -> Result<(), UndoError> {
    if s.file.is_none() {
        s.file = Some(
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(path)?,
        );
    }
    let bytes = record_bytes(&rec.key, rec.prev_tag, &rec.prev_bytes);
    let buf = encode_record(&rec);
    let len = u32::try_from(buf.len())
        .map_err(|_| UndoError::Decode("record too large for the spill index".into()))?;
    if let Some(f) = &s.file {
        f.write_at(&buf, s.file_len)?;
    }
    if s.index.is_empty() {
        s.spill_base = rec.ptr;
    }
    s.spill_live_bytes += bytes;
    s.index.push_back(SpillEntry {
        offset: s.file_len,
        len,
        superseded_at: rec.superseded_at,
        bytes,
    });
    s.file_len += buf.len() as u64;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TAG_TOMBSTONE, TAG_VALUE, UndoLog};

    fn dir(tag: &str) -> std::path::PathBuf {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "e1-arm-c-undo-unit-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::create_dir_all(&d);
        d
    }

    /// U-PTR/U-SPILL: pointers are monotonic from 1; a budget of zero
    /// spills every record; `get` returns them unchanged from the file.
    #[test]
    fn spilled_records_read_back_identically() {
        let log = UndoLog::new(0, dir("identical").join("undo.log"));
        let mut ptrs = Vec::new();
        for i in 0..25u64 {
            let p = log
                .append(i + 1, format!("k{i}").as_bytes(), i, TAG_VALUE, 0, &[7; 16])
                .unwrap();
            ptrs.push(p);
        }
        assert_eq!(ptrs, (1..=25).collect::<Vec<u64>>(), "monotonic from 1");
        assert_eq!(log.mem_bytes(), 0, "budget zero: everything spilled");
        assert!(log.spill_bytes() > 0);
        for (i, p) in ptrs.iter().enumerate() {
            let rec = log.get(*p).unwrap().expect("live record");
            assert_eq!(rec.key, format!("k{i}").as_bytes());
            assert_eq!(rec.prev_ts, i as u64);
            assert_eq!(rec.prev_bytes, vec![7u8; 16]);
        }
        assert_eq!(log.record_count(), 25);
    }

    /// U-TRIM-SAFE: trim removes exactly the records at or below the
    /// watermark, from the file and from memory, and reports their bytes;
    /// the file is truncated only once the whole file is dead.
    #[test]
    fn trim_removes_only_the_dead_prefix() {
        let log = UndoLog::new(120, dir("trim").join("undo.log"));
        for i in 1..=12u64 {
            log.append(i, b"k", i - 1, TAG_VALUE, 0, &[0; 8]).unwrap();
        }
        // Each record costs 37 + 1 + 8 = 46 bytes, so the 120-byte budget
        // keeps the newest ~3 records in memory; the rest spilled.
        assert!(log.spill_bytes() > 0, "the spill path ran");
        let reclaimed = log.trim(5).unwrap();
        assert!(reclaimed > 0, "records 1..=5 were dead");
        assert_eq!(log.record_count(), 7, "records 6..=12 survive");
        for i in 1..=5u64 {
            assert!(log.get(i).unwrap().is_none(), "ptr {i} trimmed");
        }
        for i in 6..=12u64 {
            assert!(log.get(i).unwrap().is_some(), "ptr {i} still live");
        }
        // A tombstone record round-trips too (payload accounted as 1).
        let p = log.append(13, b"k", 12, TAG_TOMBSTONE, 6, &[]).unwrap();
        let rec = log.get(p).unwrap().expect("tombstone record");
        assert_eq!(rec.prev_tag, TAG_TOMBSTONE);
        assert_eq!(rec.prev_undo_ptr, 6);
        assert!(log.trim(13).unwrap() > 0, "everything is now dead");
        assert_eq!(log.record_count(), 0);
        assert_eq!(log.spill_bytes(), 0);
    }
}
