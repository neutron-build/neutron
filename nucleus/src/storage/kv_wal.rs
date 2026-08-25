//! Write-ahead log for the KV store.
//!
//! Provides crash-recovery by recording all KV mutations to an append-only
//! log file (`kv.wal`). On restart the log is replayed from top to bottom
//! to reconstruct in-memory state.
//!
//! ## Log entry binary format
//! ```text
//! SET:    [0x01] [key_len: u32 LE] [key: bytes] [value_len: u32 LE] [value_encoded: bytes]
//! DEL:    [0x02] [key_len: u32 LE] [key: bytes]
//! EXPIRE: [0x03] [key_len: u32 LE] [key: bytes] [ttl_ms: u64 LE]
//! SNAP:   [0x04] [n_items: u32 LE] [per item: key_len + key + value_len + value + has_ttl(u8) + ttl_ms(u64)]
//! SET_TTL: [0x05] [key_len: u32 LE] [key: bytes] [value_len: u32 LE] [value]
//!          [has_ttl(u8)] [ttl_ms: u64 LE]
//! ```
//!
//! A SNAPSHOT resets all KV state. After `checkpoint()` the file is
//! truncated to a single SNAPSHOT entry so the log stays small.
//!
//! ## Transaction-tagged records (S63)
//!
//! Tags `0x06`-`0x09` are the `_XACT` twins of the four mutation records,
//! each carrying the coordinating transaction id (`u64 LE`) between the tag
//! and the twin's body. Replay keeps a tagged record only if its id is
//! `XACT_AUTOCOMMIT` (0 — written outside any explicit transaction, whose
//! durability point is this log's own fsync) or appears in the committed set
//! recovered from the SQL side; everything else was written inside a
//! transaction that never committed and is discarded — absence of a commit
//! record means discard, always. The untagged tags keep their
//! keep-unconditionally meaning, so pre-S63 logs replay unchanged. A
//! SNAPSHOT is committed by construction (the S7 checkpoint gate keeps one
//! from folding an open transaction's writes) and always replays.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use crate::executor::enlistment::XACT_AUTOCOMMIT;
use crate::storage::wal_util::WalSync;
use crate::types::Value;

// ─── Entry type tags ──────────────────────────────────────────────────────────

/// Set a value and KEEP whatever expiry the key already carries. This is what
/// INCR-family mutators need — they rewrite the value and deliberately leave
/// the TTL alone — and it is the only reason the preserve rule exists.
const ENTRY_SET: u8 = 0x01;
const ENTRY_DEL: u8 = 0x02;
const ENTRY_EXPIRE: u8 = 0x03;
const ENTRY_SNAPSHOT: u8 = 0x04;
/// Set a value AND set its expiry to exactly the carried `Option`, in one
/// record. Used by every path that *decides* an expiry (SET, SETNX).
///
/// A separate tag rather than a change to `ENTRY_SET` because the two mean
/// different things on replay, and because old logs must keep replaying: SET
/// with expiry used to be an `ENTRY_SET` followed by an independent
/// `ENTRY_EXPIRE`, so a crash between the two appends replayed a key that was
/// asked to be temporary as permanent — a leaked lock, lease, or secret that
/// never expires. `SET k v` (no TTL) over a key that already had one had the
/// mirror-image bug: the value was rewritten and the old TTL survived replay,
/// so a key the live server reported as permanent disappeared after a restart.
/// One record with an explicit `Option` closes both.
const ENTRY_SET_TTL: u8 = 0x05;
/// S63: SET carrying the coordinating transaction id. Body after the id is
/// byte-identical to [`ENTRY_SET`].
const ENTRY_SET_XACT: u8 = 0x06;
/// S63: DEL carrying the coordinating transaction id.
const ENTRY_DEL_XACT: u8 = 0x07;
/// S63: EXPIRE carrying the coordinating transaction id.
const ENTRY_EXPIRE_XACT: u8 = 0x08;
/// S63: SET-with-expiry carrying the coordinating transaction id.
const ENTRY_SET_TTL_XACT: u8 = 0x09;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Operation type for batch WAL writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvWalOp {
    /// Value only; the key keeps whatever expiry it already has.
    Set,
    /// Value plus the expiry decided with it, as one record. A `None` ttl
    /// means permanent and clears any expiry the key had.
    SetExact,
    Delete,
    Expire,
}

/// Recovered KV state from a WAL replay.
///
/// Each item is `(key, value, optional_ttl_absolute_ms)` where the TTL is
/// milliseconds since the Unix epoch (not a duration).
pub struct KvWalState {
    pub items: Vec<(String, Value, Option<u64>)>,
    /// The highest coordinating transaction id seen on a tagged record,
    /// whether that record was kept or discarded. Seeds the XactId
    /// high-water mark at executor construction (S63): a reopened process
    /// must never mint an id that a surviving tagged record already carries,
    /// or the recovery filter could resurrect stale records by matching a
    /// fresh transaction against them.
    pub max_xact_id: u64,
}

/// Append-only KV WAL.
pub struct KvWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Group-commit fsync coordinator (durability of the un-checkpointed tail).
    syncer: WalSync,
    /// The highest coordinating transaction id recovered at open (S63).
    max_xact_id: u64,
    /// The writer holds an inode a checkpoint's rename displaced: it is
    /// unlinked, so appends to it "succeed" into a file no future recovery
    /// reads while `group_sync`/`is_dirty` report healthy. Set when a
    /// checkpoint replaced the log but its reopen failed; cleared by the next
    /// successful reattach (or checkpoint reopen). See `reattach_if_stranded`.
    stranded: AtomicBool,
    /// A write to this log failed since the last time anyone asked.
    ///
    /// Every caller in `kv/mod.rs` logs the error and applies the change
    /// anyway. That keeps the live view usable and makes the reply a lie: the
    /// client was told the write was durable when the log never took it. The
    /// RESP layer drains this after each command and answers `-MISCONF`
    /// instead of `+OK`.
    write_error: AtomicBool,
    /// Test-only: fail the next record, to exercise that path without a full
    /// disk. `kv.wal_append` covers the same path out-of-process.
    #[cfg(test)]
    fail_next_append: AtomicBool,
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint`.
    #[cfg(test)]
    fail_reopen_once: AtomicBool,
}

impl KvWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// `committed` is the set of coordinating transaction ids that durably
    /// committed on the SQL side (S63); a tagged record whose id is neither in
    /// it nor `XACT_AUTOCOMMIT` is discarded — its transaction never
    /// committed, and absence of a commit record means discard, always.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored
    /// (best-effort recovery).
    pub fn open(dir: &Path, committed: &HashSet<u64>) -> io::Result<(Self, KvWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("kv.wal");
        let state = if path.exists() {
            // Streamed, not slurped: `std::fs::read` here meant a 4.8 GB log
            // cost 4.8 GB of buffer before a single key was parsed, on top of
            // the map it was being parsed into.
            let file = File::open(&path)?;
            replay_reader(BufReader::with_capacity(256 * 1024, file), committed)
        } else {
            KvWalState {
                items: Vec::new(),
                max_xact_id: 0,
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let max_xact_id = state.max_xact_id;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: WalSync::new(),
                max_xact_id,
                stranded: AtomicBool::new(false),
                write_error: AtomicBool::new(false),
                #[cfg(test)]
                fail_next_append: AtomicBool::new(false),
                #[cfg(test)]
                fail_reopen_once: AtomicBool::new(false),
            },
            state,
        ))
    }

    /// The highest coordinating transaction id this log recovered (S63), 0
    /// when it holds none. Seeds the executor's XactId counter so a reopened
    /// process never mints an id a surviving tagged record already carries.
    pub fn max_xact_id(&self) -> u64 {
        self.max_xact_id
    }

    /// Record that a write to this log failed. Drained by `take_write_error`.
    fn note_write_error(&self) {
        self.write_error.store(true, Ordering::Release);
    }

    /// Take the write-failure flag, clearing it. Edge-triggered: the caller
    /// that drains it is the one that must report the failure.
    pub fn take_write_error(&self) -> bool {
        self.write_error.swap(false, Ordering::AcqRel)
    }

    /// Test-only: arm a one-shot append failure.
    #[cfg(test)]
    pub(crate) fn fail_next_append(&self) {
        self.fail_next_append.store(true, Ordering::Release);
    }

    /// Write one already-encoded record. The single place an append can fail,
    /// so the single place that has to remember it did.
    fn write_record(&self, buf: &[u8]) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_next_append.swap(false, Ordering::AcqRel) {
            self.note_write_error();
            return Err(io::Error::other("injected KV WAL append failure"));
        }
        if let Some(e) = crate::storage::crashpoint::io_fault("kv.wal_append") {
            self.note_write_error();
            return Err(e);
        }
        let mut w = self.writer.lock();
        if let Err(e) = self.reattach_if_stranded(&mut w) {
            self.note_write_error();
            return Err(e);
        }
        let write = w.write_all(buf).and_then(|()| w.flush());
        if let Err(e) = write {
            self.note_write_error();
            return Err(e);
        }
        self.syncer.on_append();
        Ok(())
    }

    /// Re-point the writer at the live log file after a checkpoint replaced
    /// the file but could not reopen it. While stranded, `writer` holds an
    /// UNLINKED inode — appends to it succeed into a file no future recovery
    /// reads — so this runs before every append: a successful reopen recovers
    /// the writer, and a failed one fails the append loudly instead of
    /// letting it acknowledge a write to a dead inode.
    fn reattach_if_stranded(&self, w: &mut BufWriter<File>) -> io::Result<()> {
        if !self.stranded.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(e) = crate::storage::crashpoint::io_fault("kv.wal_reopen") {
            return Err(e);
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "KV WAL writer is stranded: a checkpoint replaced {} but its \
                         reopen failed; refusing to append to the unlinked old file ({e})",
                        self.path.display()
                    ),
                )
            })?;
        *w = BufWriter::new(file);
        self.stranded.store(false, Ordering::Release);
        Ok(())
    }

    /// Log a SET operation (key + value, no TTL change).
    ///
    /// `xact` is the coordinating transaction id the record is tagged with:
    /// `Some(XACT_AUTOCOMMIT)` for a write outside any explicit transaction,
    /// `Some(id)` inside one, `None` to write the legacy untagged record
    /// (kept unconditionally on replay — the pre-S63 compatibility rule).
    pub fn log_set(&self, xact: Option<u64>, key: &str, val: &Value) -> io::Result<()> {
        let mut buf = Vec::new();
        encode_set(&mut buf, xact, key, val);
        self.write_record(&buf)
    }

    /// Log a SET that also decides the key's expiry, as ONE record.
    ///
    /// `expires_ms` is absolute milliseconds since the Unix epoch; `None` means
    /// the key is permanent, and replay clears any expiry the key had. Callers
    /// that mean "leave the TTL alone" want [`log_set`] instead.
    ///
    /// `xact` mirrors [`KvWal::log_set`].
    pub fn log_set_with_expiry(
        &self,
        xact: Option<u64>,
        key: &str,
        val: &Value,
        expires_ms: Option<u64>,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        encode_set_with_expiry(&mut buf, xact, key, val, expires_ms);
        self.write_record(&buf)
    }

    /// Log a DEL operation.
    ///
    /// `xact` mirrors [`KvWal::log_set`].
    pub fn log_delete(&self, xact: Option<u64>, key: &str) -> io::Result<()> {
        let mut buf = Vec::new();
        push_tag(&mut buf, xact, ENTRY_DEL, ENTRY_DEL_XACT);
        let kb = key.as_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        buf.extend_from_slice(kb);

        self.write_record(&buf)
    }

    /// Log an EXPIRE operation (absolute TTL in milliseconds since epoch).
    ///
    /// `xact` mirrors [`KvWal::log_set`].
    pub fn log_expire(&self, xact: Option<u64>, key: &str, ttl_ms: u64) -> io::Result<()> {
        let mut buf = Vec::new();
        push_tag(&mut buf, xact, ENTRY_EXPIRE, ENTRY_EXPIRE_XACT);
        let kb = key.as_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        buf.extend_from_slice(kb);
        buf.extend_from_slice(&ttl_ms.to_le_bytes());

        self.write_record(&buf)
    }

    /// Log multiple operations in a single `write_all` + `flush` call.
    ///
    /// Each entry is `(op, key, optional_value, optional_ttl_abs_ms)`.
    /// This avoids per-entry syscall overhead for burst workloads like MSET.
    ///
    /// `xact` mirrors [`KvWal::log_set`] and tags every record in the batch.
    pub fn log_batch(
        &self,
        xact: Option<u64>,
        entries: &[(KvWalOp, &str, Option<&Value>, Option<u64>)],
    ) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for (op, key, val, ttl_ms) in entries {
            let kb = key.as_bytes();
            match op {
                KvWalOp::Set => {
                    let value = val.expect("log_batch: SET requires a value");
                    encode_set(&mut buf, xact, key, value);
                }
                KvWalOp::SetExact => {
                    let value = val.expect("log_batch: SET requires a value");
                    // One record, for the same reason `log_set_with_expiry`
                    // exists: a value and the expiry decided with it must not
                    // be separable by a crash.
                    encode_set_with_expiry(&mut buf, xact, key, value, *ttl_ms);
                }
                KvWalOp::Delete => {
                    push_tag(&mut buf, xact, ENTRY_DEL, ENTRY_DEL_XACT);
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(kb);
                }
                KvWalOp::Expire => {
                    let ms = ttl_ms.expect("log_batch: EXPIRE requires a ttl_ms");
                    push_tag(&mut buf, xact, ENTRY_EXPIRE, ENTRY_EXPIRE_XACT);
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(kb);
                    buf.extend_from_slice(&ms.to_le_bytes());
                }
            }
        }
        self.write_record(&buf)
    }

    /// Flush + `fsync` the log, capturing (under the writer lock) the highest
    /// append LSN the fsync covers. The append counter is bumped under the same
    /// lock, so every append at or below the returned mark is on stable storage.
    fn sync_covering(&self) -> io::Result<u64> {
        if let Some(e) = crate::storage::crashpoint::io_fault("kv.wal_fsync") {
            return Err(e);
        }
        let mut w = self.writer.lock();
        let covered = self.syncer.current();
        w.flush()?;
        w.get_ref().sync_all()?;
        Ok(covered)
    }

    /// Fsync the log unconditionally. Appends only `write`+`flush` into the OS
    /// page cache; a durable ack requires this.
    pub fn sync(&self) -> io::Result<()> {
        let covered = self.sync_covering()?;
        self.syncer.mark_synced(covered);
        Ok(())
    }

    /// Group-commit sync: returns only once a completed fsync covers every append
    /// made before this call. Concurrent committers share fsyncs.
    pub fn group_sync(&self) -> io::Result<()> {
        let r = self.syncer.group_sync(|| self.sync_covering());
        if r.is_err() {
            self.note_write_error();
        }
        r
    }

    /// Whether appends exist that no completed fsync covers yet.
    pub fn is_dirty(&self) -> bool {
        self.syncer.is_dirty()
    }

    /// Write the complete current state as a single SNAPSHOT entry and
    /// truncate the log to just that entry.
    ///
    /// `items` is a slice of `(key, value, optional_ttl_absolute_ms)`.
    pub fn checkpoint(&self, items: &[(String, Value, Option<u64>)]) -> io::Result<()> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(items.len() as u32).to_le_bytes());
        for (key, val, ttl) in items {
            // key
            let kb = key.as_bytes();
            payload.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            payload.extend_from_slice(kb);
            // value
            let mut val_buf = Vec::new();
            encode_value(val, &mut val_buf);
            payload.extend_from_slice(&(val_buf.len() as u32).to_le_bytes());
            payload.extend_from_slice(&val_buf);
            // TTL
            match ttl {
                Some(ms) => {
                    payload.push(1u8);
                    payload.extend_from_slice(&ms.to_le_bytes());
                }
                None => {
                    payload.push(0u8);
                    payload.extend_from_slice(&0u64.to_le_bytes());
                }
            }
        }

        // Serialize the complete new log body (SNAPSHOT tag + payload, no key prefix).
        let mut contents = Vec::with_capacity(payload.len() + 1);
        contents.push(ENTRY_SNAPSHOT);
        contents.extend_from_slice(&payload);

        // Hold the writer lock across the whole checkpoint so no append can interleave
        // between the flush and the reopen. Replace atomically — temp file + fsync +
        // rename — so a crash mid-checkpoint leaves the old log or the new snapshot,
        // never an empty file.
        let mut w = self.writer.lock();
        w.flush()?;
        crate::storage::wal_util::atomic_replace_wal(&self.path, &contents)?;
        // The reopen is the hazardous half: the rename above already unlinked
        // the inode `w` holds, so a failure here leaves the writer pointing at
        // a file no future recovery reads.
        #[cfg(test)]
        let injected: Option<io::Error> = self
            .fail_reopen_once
            .swap(false, Ordering::AcqRel)
            .then(|| io::Error::other("injected KV WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = if let Some(e) = injected {
            Err(e)
        } else if let Some(e) = crate::storage::crashpoint::io_fault("kv.wal_reopen") {
            Err(e)
        } else {
            OpenOptions::new().append(true).open(&self.path)
        };
        let file = match file {
            Ok(f) => f,
            Err(e) => {
                // The rename already happened, so the handle in `w` is now an
                // unlinked inode. Mark the writer stranded: appends must
                // reattach (or fail loudly), never write through it. The
                // checkpoint itself is also a failed write to this log —
                // flag it so a `+OK` reply cannot claim durability the log
                // no longer has a healthy writer for.
                self.note_write_error();
                self.stranded.store(true, Ordering::Release);
                return Err(e);
            }
        };
        *w = BufWriter::new(file);
        self.stranded.store(false, Ordering::Release);
        // The snapshot was fsync'd by `atomic_replace_wal`; count it as a
        // covered append so the log reads clean until the next write.
        let mark = self.syncer.on_append();
        self.syncer.mark_synced(mark);
        Ok(())
    }
}

// ─── Record encoding ─────────────────────────────────────────────────────────

/// Emit the tag for one record: the `_XACT` twin plus the id when `xact` is
/// `Some`, the legacy untagged tag when `None`.
fn push_tag(buf: &mut Vec<u8>, xact: Option<u64>, plain: u8, xact_tagged: u8) {
    match xact {
        Some(x) => {
            buf.push(xact_tagged);
            buf.extend_from_slice(&x.to_le_bytes());
        }
        None => buf.push(plain),
    }
}

/// `ENTRY_SET`: tag, key (u32 len + bytes), value (u32 len + encoded).
fn encode_set(buf: &mut Vec<u8>, xact: Option<u64>, key: &str, val: &Value) {
    push_tag(buf, xact, ENTRY_SET, ENTRY_SET_XACT);
    encode_key_value(buf, key, val);
}

/// `ENTRY_SET_TTL`: the `ENTRY_SET` body followed by a presence byte and, when
/// present, absolute expiry milliseconds — the same TTL shape the snapshot
/// entry uses.
fn encode_set_with_expiry(
    buf: &mut Vec<u8>,
    xact: Option<u64>,
    key: &str,
    val: &Value,
    expires_ms: Option<u64>,
) {
    push_tag(buf, xact, ENTRY_SET_TTL, ENTRY_SET_TTL_XACT);
    encode_key_value(buf, key, val);
    match expires_ms {
        Some(ms) => {
            buf.push(1);
            buf.extend_from_slice(&ms.to_le_bytes());
        }
        None => {
            buf.push(0);
            buf.extend_from_slice(&0u64.to_le_bytes());
        }
    }
}

fn encode_key_value(buf: &mut Vec<u8>, key: &str, val: &Value) {
    let kb = key.as_bytes();
    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
    buf.extend_from_slice(kb);
    let mut val_buf = Vec::new();
    encode_value(val, &mut val_buf);
    buf.extend_from_slice(&(val_buf.len() as u32).to_le_bytes());
    buf.extend_from_slice(&val_buf);
}

// ─── Value encoding ──────────────────────────────────────────────────────────
//
// Tag-based scheme:
//   0=Null, 1=Bool(u8), 2=Int32(i32 LE), 3=Int64(i64 LE),
//   4=Float64(f64 LE), 5=Text(len u32 + bytes)

fn encode_value(val: &Value, buf: &mut Vec<u8>) {
    match val {
        Value::Null => buf.push(0),
        Value::Bool(b) => {
            buf.push(1);
            buf.push(*b as u8);
        }
        Value::Int32(n) => {
            buf.push(2);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Int64(n) => {
            buf.push(3);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            buf.push(4);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(5);
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        other => {
            // Fallback: encode as Text (lossy for exotic types — sufficient
            // for typical KV workloads).
            let s = format!("{other}");
            let b = s.as_bytes();
            buf.push(5);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
}

fn decode_value(data: &[u8], pos: &mut usize) -> Option<Value> {
    let tag = *data.get(*pos)?;
    *pos += 1;
    match tag {
        0 => Some(Value::Null),
        1 => {
            let b = *data.get(*pos)?;
            *pos += 1;
            Some(Value::Bool(b != 0))
        }
        2 => Some(Value::Int32(read_i32(data, pos)?)),
        3 => Some(Value::Int64(read_i64(data, pos)?)),
        4 => Some(Value::Float64(read_f64(data, pos)?)),
        5 => {
            let len = read_u32(data, pos)? as usize;
            if *pos + len > data.len() {
                return None;
            }
            let s = std::str::from_utf8(&data[*pos..*pos + len])
                .ok()?
                .to_string();
            *pos += len;
            Some(Value::Text(s))
        }
        _ => None,
    }
}

// ─── Replay ───────────────────────────────────────────────────────────────────

/// Replay all entries in `data` to reconstruct KV state.
///
/// SNAPSHOT entries reset all state to their embedded snapshot, so only the
/// *last* SNAPSHOT (and subsequent incremental entries) matter in practice.
/// Parse one snapshot item: key, length-prefixed value, TTL flag + value.
///
/// Returns `None` when `data` does not yet hold the whole item, leaving `pos`
/// for the caller to restore.
fn parse_snapshot_item(data: &[u8], pos: &mut usize) -> Option<(String, Value, Option<u64>)> {
    let key = read_string(data, pos)?;
    let val_len = read_u32(data, pos)? as usize;
    if *pos + val_len > data.len() {
        return None;
    }
    let mut vpos = *pos;
    let val = decode_value(data, &mut vpos)?;
    *pos += val_len;
    let has_ttl = *data.get(*pos)?;
    *pos += 1;
    let ttl_ms = read_u64(data, pos)?;
    let ttl = if has_ttl != 0 { Some(ttl_ms) } else { None };
    Some((key, val, ttl))
}

/// Outcome of trying to parse one top-level record.
enum RecordStep {
    /// A record was applied.
    Applied,
    /// A SNAPSHOT header was applied; this many items follow.
    SnapshotHeader(u32),
    /// `data` does not yet hold the whole record.
    NeedMore,
    /// Unknown entry type — replay cannot know how much to skip, so it stops.
    Stop,
}

fn parse_record(
    data: &[u8],
    pos: &mut usize,
    store: &mut HashMap<String, (Value, Option<u64>)>,
    committed: &HashSet<u64>,
    max_xact_id: &mut u64,
) -> RecordStep {
    let start = *pos;
    let Some(&entry_type) = data.get(*pos) else {
        return RecordStep::NeedMore;
    };
    *pos += 1;

    macro_rules! need {
        ($e:expr) => {
            match $e {
                Some(v) => v,
                None => {
                    *pos = start;
                    return RecordStep::NeedMore;
                }
            }
        };
    }

    // The tagged records parse their id, then share the body parse with the
    // untagged twin. `keep_tagged` is the S63 filter in one expression: an
    // autocommit record is durable by its own fsync, a committed id was
    // vouched for by a durable COMMIT record, anything else never happened.
    // Parsing continues either way — the record must be fully consumed to
    // find the next one, since nothing length-frames these. Ids feed
    // `max_xact_id` whether kept or discarded, so the caller can seed the
    // XactId high-water mark.
    let mut keep_tagged = true;
    if matches!(
        entry_type,
        ENTRY_SET_XACT | ENTRY_DEL_XACT | ENTRY_EXPIRE_XACT | ENTRY_SET_TTL_XACT
    ) {
        let xact = need!(read_u64(data, pos));
        *max_xact_id = (*max_xact_id).max(xact);
        keep_tagged = xact == XACT_AUTOCOMMIT || committed.contains(&xact);
    }

    match entry_type {
        ENTRY_SET | ENTRY_SET_XACT => {
            let key = need!(read_string(data, pos));
            let val_len = need!(read_u32(data, pos)) as usize;
            if *pos + val_len > data.len() {
                *pos = start;
                return RecordStep::NeedMore;
            }
            let mut vpos = *pos;
            let val = need!(decode_value(data, &mut vpos));
            *pos += val_len;
            if keep_tagged {
                // Preserve any existing TTL: a plain SET does not decide
                // expiry.
                let ttl = store.get(&key).and_then(|(_, t)| *t);
                store.insert(key, (val, ttl));
            }
            RecordStep::Applied
        }
        ENTRY_SET_TTL | ENTRY_SET_TTL_XACT => {
            let key = need!(read_string(data, pos));
            let val_len = need!(read_u32(data, pos)) as usize;
            if *pos + val_len > data.len() {
                *pos = start;
                return RecordStep::NeedMore;
            }
            let mut vpos = *pos;
            let val = need!(decode_value(data, &mut vpos));
            *pos += val_len;
            let has_ttl = *need!(data.get(*pos));
            *pos += 1;
            let ttl_ms = need!(read_u64(data, pos));
            if keep_tagged {
                // This record carries the whole expiry decision, including
                // "none", so there is nothing to preserve from an earlier one.
                let ttl = if has_ttl != 0 { Some(ttl_ms) } else { None };
                store.insert(key, (val, ttl));
            }
            RecordStep::Applied
        }
        ENTRY_DEL | ENTRY_DEL_XACT => {
            let key = need!(read_string(data, pos));
            if keep_tagged {
                store.remove(&key);
            }
            RecordStep::Applied
        }
        ENTRY_EXPIRE | ENTRY_EXPIRE_XACT => {
            let key = need!(read_string(data, pos));
            let ttl_ms = need!(read_u64(data, pos));
            if keep_tagged && let Some(entry) = store.get_mut(&key) {
                entry.1 = Some(ttl_ms);
            }
            RecordStep::Applied
        }
        ENTRY_SNAPSHOT => {
            let n_items = need!(read_u32(data, pos));
            // A SNAPSHOT supersedes everything before it.
            store.clear();
            RecordStep::SnapshotHeader(n_items)
        }
        _ => RecordStep::Stop,
    }
}

/// Replay from an in-memory slice.
///
/// Test-only entry point onto the same parser — a second copy of a
/// durability-critical format is a divergence waiting to happen, so the tests
/// exercise the code that actually runs rather than a slice-shaped twin.
#[cfg(test)]
fn replay(data: &[u8], committed: &HashSet<u64>) -> KvWalState {
    replay_reader(std::io::Cursor::new(data), committed)
}

/// Replay the WAL from a reader, holding only a sliding window in memory.
///
/// The whole file used to be read into one `Vec<u8>` before parsing. That is
/// fine for a small log and ruinous for a large one: a 4.8 GB KV WAL cost 4.8 GB
/// of buffer *plus* the parsed map, so an instance that had grown could not be
/// restarted within its own memory limit — the very situation a restart is
/// supposed to resolve.
///
/// A SNAPSHOT is streamed item by item rather than treated as one record. That
/// matters because a checkpointed log is a *single* SNAPSHOT containing every
/// live key, so record-level streaming alone would still have buffered the
/// entire file.
///
/// The window grows only to the largest single item, and truncated tails are
/// tolerated exactly as before: replay stops at the last complete unit.
///
/// `committed` is the set of coordinating transaction ids that durably
/// committed on the SQL side (S63); see [`parse_record`] for the filter.
fn replay_reader<R: Read>(mut reader: R, committed: &HashSet<u64>) -> KvWalState {
    const CHUNK: usize = 256 * 1024;
    const COMPACT_THRESHOLD: usize = 1 << 20;

    let mut store: HashMap<String, (Value, Option<u64>)> = HashMap::new();
    let mut buf: Vec<u8> = Vec::new();
    let mut pos = 0usize;
    let mut snapshot_remaining: u32 = 0;
    let mut max_xact_id: u64 = 0;
    let mut eof = false;

    loop {
        // Make as much progress as the buffered bytes allow.
        let stop = loop {
            if snapshot_remaining > 0 {
                let start = pos;
                match parse_snapshot_item(&buf, &mut pos) {
                    Some((k, v, t)) => {
                        store.insert(k, (v, t));
                        snapshot_remaining -= 1;
                    }
                    None => {
                        pos = start;
                        break false;
                    }
                }
            } else {
                match parse_record(&buf, &mut pos, &mut store, committed, &mut max_xact_id) {
                    RecordStep::Applied => {}
                    RecordStep::SnapshotHeader(n) => snapshot_remaining = n,
                    RecordStep::NeedMore => break false,
                    RecordStep::Stop => break true,
                }
            }
        };
        if stop || eof {
            break;
        }

        // Drop the consumed prefix so the window tracks the largest item, not
        // the file.
        if pos == buf.len() {
            buf.clear();
            pos = 0;
        } else if pos > COMPACT_THRESHOLD {
            buf.drain(..pos);
            pos = 0;
        }

        let mut chunk = vec![0u8; CHUNK];
        match reader.read(&mut chunk) {
            Ok(0) => eof = true,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            // A read error mid-log is treated like a truncated tail: keep what
            // replayed cleanly rather than discarding the whole store.
            Err(_) => eof = true,
        }
    }

    KvWalState {
        items: store.into_iter().map(|(k, (v, t))| (k, v, t)).collect(),
        max_xact_id,
    }
}

// ─── Primitive readers ────────────────────────────────────────────────────────

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_i32(data: &[u8], pos: &mut usize) -> Option<i32> {
    read_u32(data, pos).map(|u| u as i32)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_i64(data: &[u8], pos: &mut usize) -> Option<i64> {
    read_u64(data, pos).map(|v| v as i64)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    read_u64(data, pos).map(f64::from_bits)
}

fn read_string(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32(data, pos)? as usize;
    if *pos + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .ok()?
        .to_string();
    *pos += len;
    Some(s)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    // 3.14/3.14159 here are arbitrary test fixtures, not PI approximations.
    #![allow(clippy::approx_constant)]
    use super::*;

    use std::collections::HashSet;

    /// Open with an empty committed set: every tagged record keeps, which is
    /// the pre-S63 replay contract the legacy round-trip tests exercise.
    fn open_keep_all(dir: &Path) -> io::Result<(KvWal, KvWalState)> {
        KvWal::open(dir, &HashSet::new())
    }

    // ── S63: the recovery filter ──────────────────────────────────────────

    /// One log exercising every filter decision at once: legacy and
    /// autocommit records keep, committed ids keep, unknown ids discard —
    /// and a discarded record in the MIDDLE does not stop the records after
    /// it (they are parsed past, not abandoned).
    #[test]
    fn tagged_records_filter_on_the_committed_set() {
        let mut buf = Vec::new();
        // Legacy untagged SET (pre-S63 log): keep unconditionally.
        encode_set(&mut buf, None, "legacy", &Value::Int64(0));
        let tagged_set_ttl = |buf: &mut Vec<u8>, xact: u64, key: &str| {
            encode_set_with_expiry(buf, Some(xact), key, &Value::Int64(1), None);
        };
        // Tagged autocommit (0): keep.
        tagged_set_ttl(&mut buf, XACT_AUTOCOMMIT, "auto");
        tagged_set_ttl(&mut buf, 7, "committed");
        tagged_set_ttl(&mut buf, 8, "never-committed"); // discarded, mid-log
        tagged_set_ttl(&mut buf, 9, "committed-late");

        let committed: HashSet<u64> = [7u64, 9u64].into_iter().collect();
        let state = replay(&buf, &committed);
        assert_eq!(
            state.max_xact_id, 9,
            "discarded records still feed the floor"
        );
        let mut keys: Vec<&str> = state.items.iter().map(|(k, _, _)| k.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["auto", "committed", "committed-late", "legacy"],
            "id 8 never committed; its record must be discarded, not replayed"
        );
    }

    /// DEL and EXPIRE filter the same way: an uncommitted transaction's
    /// delete and expiry change must not reach keys that survived it.
    #[test]
    fn tagged_delete_and_expire_filter_on_the_committed_set() {
        let mut buf = Vec::new();
        // Autocommit base state: k=v with a TTL, plus a second key.
        encode_set_with_expiry(
            &mut buf,
            Some(XACT_AUTOCOMMIT),
            "k",
            &Value::Int64(1),
            Some(9_000_000_000_000),
        );
        encode_set_with_expiry(
            &mut buf,
            Some(XACT_AUTOCOMMIT),
            "victim",
            &Value::Int64(2),
            None,
        );
        // Abandoned transaction (id 5): sets a marker, deletes both keys,
        // and re-expires k. None of it may reach the recovered state.
        let tagged_del = |buf: &mut Vec<u8>, xact: u64, key: &str| {
            buf.push(ENTRY_DEL_XACT);
            buf.extend_from_slice(&xact.to_le_bytes());
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key.as_bytes());
        };
        encode_set_with_expiry(
            &mut buf,
            Some(5),
            "uncommitted-marker",
            &Value::Int64(3),
            None,
        );
        tagged_del(&mut buf, 5, "k");
        tagged_del(&mut buf, 5, "victim");
        buf.push(ENTRY_EXPIRE_XACT);
        buf.extend_from_slice(&5u64.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(b"k");
        buf.extend_from_slice(&7_000_000_000_000u64.to_le_bytes());

        let state = replay(&buf, &HashSet::new());
        let map: HashMap<_, _> = state
            .items
            .iter()
            .map(|(k, v, t)| (k.as_str(), (v.clone(), *t)))
            .collect();
        assert_eq!(
            map.len(),
            2,
            "the abandoned transaction's SET, DELs and EXPIRE must all be discarded"
        );
        assert_eq!(
            map["k"].1,
            Some(9_000_000_000_000),
            "the abandoned EXPIRE must not replace the durable TTL"
        );
        assert!(map.contains_key("victim"));
        assert!(
            !map.contains_key("uncommitted-marker"),
            "the abandoned transaction's own SET is discarded with it"
        );
        assert_eq!(state.max_xact_id, 5);
    }

    /// A truncation inside a tagged record is a torn tail exactly as for the
    /// untagged ones: replay keeps the clean prefix and abandons the partial
    /// record at its boundary.
    #[test]
    fn torn_tagged_record_keeps_the_clean_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kv.wal");
        {
            let (wal, _) = KvWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_set_with_expiry(Some(XACT_AUTOCOMMIT), "ok", &Value::Int64(1), None)
                .unwrap();
            wal.log_set_with_expiry(Some(3), "torn", &Value::Int64(2), None)
                .unwrap();
            wal.sync().unwrap();
        }
        let full = std::fs::read(&path).unwrap();
        // Cut somewhere inside the second record (half its body).
        let first_len = {
            let mut probe = Vec::new();
            encode_set_with_expiry(
                &mut probe,
                Some(XACT_AUTOCOMMIT),
                "ok",
                &Value::Int64(1),
                None,
            );
            probe.len()
        };
        let cut = first_len + 6;
        let state = replay(&full[..cut], &[3u64].into_iter().collect());
        assert!(
            state.items.iter().any(|(k, _, _)| k == "ok"),
            "the complete record before the cut must replay"
        );
        assert!(
            !state.items.iter().any(|(k, _, _)| k == "torn"),
            "the partial record must be abandoned, not half-applied"
        );
    }

    // Replay reads the log in 256 KiB chunks. A value larger than one chunk must
    // still round-trip: the window has to grow to hold the largest single item,
    // and the parser must not mistake "not buffered yet" for "end of log".
    #[test]
    fn replays_value_larger_than_read_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let big = "v".repeat(700 * 1024);
        {
            let (wal, _) = open_keep_all(dir.path()).unwrap();
            wal.log_set(None, "huge", &Value::Text(big.clone()))
                .unwrap();
            wal.log_set(None, "after", &Value::Text("tail".into()))
                .unwrap();
        }
        let (_, state) = open_keep_all(dir.path()).unwrap();
        let map: HashMap<_, _> = state
            .items
            .iter()
            .map(|(k, v, _)| (k.as_str(), v))
            .collect();
        assert_eq!(map.get("huge"), Some(&&Value::Text(big)));
        assert_eq!(map.get("after"), Some(&&Value::Text("tail".into())));
    }

    /// S31-14: a checkpoint whose reopen fails must not leave the writer
    /// appending into the unlinked inode the rename displaced. Those appends
    /// report success while no future recovery can ever read them, so an
    /// acknowledged key silently vanishes at restart. The discriminator is
    /// durability: the post-failure write must land in the replaced file.
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = open_keep_all(dir.path()).unwrap();
            wal.log_set(None, "before", &Value::Int64(1)).unwrap();
            wal.fail_reopen_once.store(true, Ordering::SeqCst);
            wal.checkpoint(&[("before".into(), Value::Int64(1), None)])
                .expect_err("the injected reopen failure must fail the checkpoint");
            assert!(
                wal.take_write_error(),
                "a failed checkpoint must flag the write error, or the next \
                 reply claims durability the log cannot provide"
            );
            wal.log_set(None, "after", &Value::Int64(2))
                .expect("a later append must reattach, not strand");
        }
        let (_, state) = open_keep_all(dir.path()).unwrap();
        let map: HashMap<_, _> = state
            .items
            .iter()
            .map(|(k, v, _)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(
            map.len(),
            2,
            "the post-checkpoint-failure write went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
        assert_eq!(map.get("after"), Some(&Value::Int64(2)));
    }

    // A checkpointed log is ONE snapshot record holding every live key, so
    // record-level streaming alone would still buffer the whole file. The
    // snapshot has to be consumed item by item.
    #[test]
    fn replays_snapshot_spanning_many_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let chunk = "s".repeat(8 * 1024);
        let items: Vec<(String, Value, Option<u64>)> = (0..200)
            .map(|i| (format!("k{i}"), Value::Text(format!("{chunk}{i}")), None))
            .collect();
        {
            let (wal, _) = open_keep_all(dir.path()).unwrap();
            wal.checkpoint(&items).unwrap();
        }
        let (_, state) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state.items.len(), 200, "every snapshot item must replay");
        let map: HashMap<_, _> = state
            .items
            .iter()
            .map(|(k, v, _)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(map.get("k0"), Some(&Value::Text(format!("{chunk}0"))));
        assert_eq!(map.get("k199"), Some(&Value::Text(format!("{chunk}199"))));
    }

    // A torn tail is normal after a crash. Replay must keep every complete unit
    // before it rather than discarding the log — and must not spin.
    #[test]
    fn truncated_snapshot_keeps_complete_items() {
        let dir = tempfile::tempdir().unwrap();
        let items: Vec<(String, Value, Option<u64>)> = (0..50)
            .map(|i| (format!("k{i}"), Value::Text("x".repeat(4096)), None))
            .collect();
        {
            let (wal, _) = open_keep_all(dir.path()).unwrap();
            wal.checkpoint(&items).unwrap();
        }
        let path = dir.path().join("kv.wal");
        let full = std::fs::read(&path).unwrap();
        // Cut mid-way through the item stream.
        std::fs::write(&path, &full[..full.len() * 2 / 3]).unwrap();

        let (_, state) = open_keep_all(dir.path()).unwrap();
        assert!(
            !state.items.is_empty() && state.items.len() < 50,
            "expected a partial replay, got {} items",
            state.items.len()
        );
    }

    #[test]
    fn group_sync_marks_clean_and_data_persists() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        // A fresh WAL with no appends is clean.
        assert!(!wal.is_dirty(), "no appends yet");

        wal.log_set(None, "k1", &Value::Int64(1)).unwrap();
        wal.log_set(None, "k2", &Value::Text("two".into())).unwrap();
        assert!(wal.is_dirty(), "appends are not yet covered by an fsync");

        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "group_sync fsyncs — nothing left to force");

        // A redundant sync is a no-op and stays clean.
        wal.group_sync().unwrap();
        assert!(!wal.is_dirty());

        // A further append re-dirties; the fsynced records still recover.
        wal.log_delete(None, "k1").unwrap();
        assert!(wal.is_dirty(), "new appends after a sync are uncovered");
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state.items.len(), 1, "k1 deleted, k2 remains");
        assert!(state.items.iter().all(|(k, _, _)| k != "k1"));
    }

    #[test]
    fn checkpoint_leaves_wal_clean() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();
        wal.log_set(None, "a", &Value::Int64(1)).unwrap();
        assert!(wal.is_dirty());
        // The checkpoint fsyncs its snapshot atomically, so the WAL must read
        // clean afterward (its snapshot append is counted as covered).
        wal.checkpoint(&[("a".into(), Value::Int64(1), None)])
            .unwrap();
        assert!(
            !wal.is_dirty(),
            "checkpoint durably rewrote the log — nothing left to force"
        );
    }

    #[test]
    fn test_set_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = open_keep_all(dir.path()).unwrap();
        assert!(state.items.is_empty());

        wal.log_set(None, "name", &Value::Text("Nucleus".into()))
            .unwrap();
        wal.log_set(None, "count", &Value::Int64(42)).unwrap();
        drop(wal);

        let (_wal2, state2) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state2.items.len(), 2);
        let name = state2.items.iter().find(|(k, _, _)| k == "name").unwrap();
        assert_eq!(name.1, Value::Text("Nucleus".into()));
        let count = state2.items.iter().find(|(k, _, _)| k == "count").unwrap();
        assert_eq!(count.1, Value::Int64(42));
    }

    #[test]
    fn test_delete_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        wal.log_set(None, "a", &Value::Int32(1)).unwrap();
        wal.log_set(None, "b", &Value::Int32(2)).unwrap();
        wal.log_delete(None, "a").unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state.items.len(), 1);
        assert!(state.items.iter().all(|(k, _, _)| k != "a"));
        let b = state.items.iter().find(|(k, _, _)| k == "b").unwrap();
        assert_eq!(b.1, Value::Int32(2));
    }

    #[test]
    fn test_set_with_ttl_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        wal.log_set(None, "temp", &Value::Text("ephemeral".into()))
            .unwrap();
        let ttl_ms = 1_700_000_000_000u64; // some future epoch ms
        wal.log_expire(None, "temp", ttl_ms).unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state.items.len(), 1);
        let item = &state.items[0];
        assert_eq!(item.0, "temp");
        assert_eq!(item.1, Value::Text("ephemeral".into()));
        assert_eq!(item.2, Some(ttl_ms));
    }

    #[test]
    fn test_checkpoint_truncates() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        // Write many entries
        for i in 0..100 {
            wal.log_set(None, &format!("k{i}"), &Value::Int64(i))
                .unwrap();
        }
        let size_before = std::fs::metadata(dir.path().join("kv.wal")).unwrap().len();

        // Checkpoint with only 3 items
        let items = vec![
            ("a".to_string(), Value::Int32(1), None),
            ("b".to_string(), Value::Int32(2), Some(9_999_999_999_999u64)),
            ("c".to_string(), Value::Text("hello".into()), None),
        ];
        wal.checkpoint(&items).unwrap();
        let size_after = std::fs::metadata(dir.path().join("kv.wal")).unwrap().len();
        assert!(size_after < size_before, "checkpoint should shrink WAL");

        // Can still write after checkpoint
        wal.log_set(None, "d", &Value::Int64(4)).unwrap();
        drop(wal);

        // Verify replay
        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state.items.len(), 4);
        let b = state.items.iter().find(|(k, _, _)| k == "b").unwrap();
        assert_eq!(b.2, Some(9_999_999_999_999u64));
        let d = state.items.iter().find(|(k, _, _)| k == "d").unwrap();
        assert_eq!(d.1, Value::Int64(4));
    }

    #[test]
    fn test_corrupt_trailing_bytes_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        wal.log_set(None, "good", &Value::Int32(42)).unwrap();
        drop(wal);

        // Append garbage bytes to the WAL file
        let wal_path = dir.path().join("kv.wal");
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[0xFF, 0xFE, 0xFD, 0x00, 0x01]).unwrap();
        file.flush().unwrap();
        drop(file);

        // Should recover the good entry and skip the corrupt tail
        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state.items.len(), 1);
        assert_eq!(state.items[0].0, "good");
        assert_eq!(state.items[0].1, Value::Int32(42));
    }

    #[test]
    fn test_incr_survives_restart() {
        // Simulate INCR by logging successive SET operations (INCR is a
        // read-modify-write that the KvStore turns into a SET at the WAL level).
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        wal.log_set(None, "counter", &Value::Int64(1)).unwrap();
        wal.log_set(None, "counter", &Value::Int64(2)).unwrap();
        wal.log_set(None, "counter", &Value::Int64(3)).unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        let counter = state.items.iter().find(|(k, _, _)| k == "counter").unwrap();
        assert_eq!(counter.1, Value::Int64(3));
    }

    #[test]
    fn test_empty_wal_fresh_store() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = open_keep_all(dir.path()).unwrap();
        assert!(state.items.is_empty());
    }

    #[test]
    fn test_value_types_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        wal.log_set(None, "null", &Value::Null).unwrap();
        wal.log_set(None, "bool", &Value::Bool(true)).unwrap();
        wal.log_set(None, "i32", &Value::Int32(-42)).unwrap();
        wal.log_set(None, "i64", &Value::Int64(i64::MAX)).unwrap();
        wal.log_set(None, "f64", &Value::Float64(3.14159)).unwrap();
        wal.log_set(None, "text", &Value::Text("hello world".into()))
            .unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state.items.len(), 6);

        let find = |k: &str| {
            state
                .items
                .iter()
                .find(|(key, _, _)| key == k)
                .unwrap()
                .1
                .clone()
        };
        assert_eq!(find("null"), Value::Null);
        assert_eq!(find("bool"), Value::Bool(true));
        assert_eq!(find("i32"), Value::Int32(-42));
        assert_eq!(find("i64"), Value::Int64(i64::MAX));
        assert_eq!(find("f64"), Value::Float64(3.14159));
        assert_eq!(find("text"), Value::Text("hello world".into()));
    }

    #[test]
    fn test_overwrite_preserves_ttl() {
        // When a SET replays over an existing key, it should preserve the TTL
        // (the WAL logs SET and EXPIRE separately).
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        wal.log_set(None, "k", &Value::Int32(1)).unwrap();
        wal.log_expire(None, "k", 5_000_000_000_000u64).unwrap();
        wal.log_set(None, "k", &Value::Int32(2)).unwrap(); // overwrite value, keep TTL
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        let item = state.items.iter().find(|(k, _, _)| k == "k").unwrap();
        assert_eq!(item.1, Value::Int32(2));
        assert_eq!(item.2, Some(5_000_000_000_000u64));
    }

    #[test]
    fn test_snapshot_then_incremental() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        // Checkpoint with initial data
        let items = vec![
            ("x".to_string(), Value::Int64(10), None),
            (
                "y".to_string(),
                Value::Int64(20),
                Some(8_000_000_000_000u64),
            ),
        ];
        wal.checkpoint(&items).unwrap();

        // Incremental ops after checkpoint
        wal.log_set(None, "z", &Value::Int64(30)).unwrap();
        wal.log_delete(None, "x").unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state.items.len(), 2); // y and z remain
        assert!(state.items.iter().all(|(k, _, _)| k != "x"));
        let y = state.items.iter().find(|(k, _, _)| k == "y").unwrap();
        assert_eq!(y.1, Value::Int64(20));
        assert_eq!(y.2, Some(8_000_000_000_000u64));
        let z = state.items.iter().find(|(k, _, _)| k == "z").unwrap();
        assert_eq!(z.1, Value::Int64(30));
    }

    #[test]
    fn test_batch_set_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = open_keep_all(dir.path()).unwrap();
        assert!(state.items.is_empty());

        let v1 = Value::Text("hello".into());
        let v2 = Value::Int64(42);
        let v3 = Value::Float64(3.14);
        wal.log_batch(
            None,
            &[
                (KvWalOp::Set, "a", Some(&v1), None),
                (KvWalOp::Set, "b", Some(&v2), None),
                (KvWalOp::Set, "c", Some(&v3), None),
            ],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state2) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state2.items.len(), 3);
        let a = state2.items.iter().find(|(k, _, _)| k == "a").unwrap();
        assert_eq!(a.1, Value::Text("hello".into()));
        let b = state2.items.iter().find(|(k, _, _)| k == "b").unwrap();
        assert_eq!(b.1, Value::Int64(42));
        let c = state2.items.iter().find(|(k, _, _)| k == "c").unwrap();
        assert_eq!(c.1, Value::Float64(3.14));
    }

    #[test]
    fn test_batch_mixed_ops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        // First set some keys individually
        wal.log_set(None, "x", &Value::Int32(1)).unwrap();
        wal.log_set(None, "y", &Value::Int32(2)).unwrap();
        wal.log_set(None, "z", &Value::Int32(3)).unwrap();

        // Now use a batch with mixed ops: delete x, expire y, set w
        let w_val = Value::Int64(99);
        wal.log_batch(
            None,
            &[
                (KvWalOp::Delete, "x", None, None),
                (KvWalOp::Expire, "y", None, Some(9_000_000_000_000u64)),
                (KvWalOp::Set, "w", Some(&w_val), None),
            ],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        // x deleted, y still present with TTL, z unchanged, w added
        assert_eq!(state.items.len(), 3);
        assert!(state.items.iter().all(|(k, _, _)| k != "x"));
        let y = state.items.iter().find(|(k, _, _)| k == "y").unwrap();
        assert_eq!(y.1, Value::Int32(2));
        assert_eq!(y.2, Some(9_000_000_000_000u64));
        let z = state.items.iter().find(|(k, _, _)| k == "z").unwrap();
        assert_eq!(z.1, Value::Int32(3));
        let w = state.items.iter().find(|(k, _, _)| k == "w").unwrap();
        assert_eq!(w.1, Value::Int64(99));
    }

    /// A key asked to be temporary must never replay as permanent, no matter
    /// where the crash lands. Truncating the log at every byte offset models a
    /// crash after every byte of the record.
    ///
    /// The old encoding could not pass this: SET-with-expiry was an
    /// `ENTRY_SET` append followed by an independent `ENTRY_EXPIRE` append, so
    /// every truncation inside that gap replayed the value with no TTL — a
    /// lock or lease held forever. The next test pins that this is what the
    /// two-record shape actually did.
    #[test]
    fn expiring_set_never_replays_as_permanent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kv.wal");
        let val = Value::Text("lease-holder-7".into());
        let expiry = 9_000_000_000_000u64;

        let (wal, _) = open_keep_all(dir.path()).unwrap();
        wal.log_set_with_expiry(None, "lock:a", &val, Some(expiry))
            .unwrap();
        wal.sync().unwrap();
        drop(wal);
        let full = std::fs::read(&path).unwrap();

        for cut in 0..=full.len() {
            let state = replay(&full[..cut], &HashSet::new());
            match state.items.iter().find(|(k, _, _)| k == "lock:a") {
                // Not yet durable: the record was cut before it was complete.
                None => {}
                // Durable: the value and its expiry arrive together or not at all.
                Some((_, v, ttl)) => {
                    assert_eq!(v, &val, "truncated at {cut}: wrong value");
                    assert_eq!(
                        *ttl,
                        Some(expiry),
                        "truncated at {cut}: the key replayed WITHOUT its expiry — \
                         a lease that outlives the process that took it"
                    );
                }
            }
        }
    }

    /// The two-record shape this replaced, kept as evidence of what changed.
    /// Old logs still hold these pairs, so the legacy tags must keep replaying,
    /// and a cut between them still yields a permanent key — that is history,
    /// not something a new append can produce.
    #[test]
    fn legacy_set_then_expire_still_replays_and_shows_the_old_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kv.wal");
        let val = Value::Text("v".into());

        let (wal, _) = open_keep_all(dir.path()).unwrap();
        wal.log_set(None, "k", &val).unwrap();
        let after_set = std::fs::metadata(&path).unwrap().len() as usize;
        wal.log_expire(None, "k", 9_000_000_000_000).unwrap();
        wal.sync().unwrap();
        drop(wal);

        let full = std::fs::read(&path).unwrap();
        // Whole log: the pair replays, so existing on-disk logs keep working.
        let state = replay(&full, &HashSet::new());
        let (_, v, ttl) = state.items.iter().find(|(k, _, _)| k == "k").unwrap();
        assert_eq!(v, &val);
        assert_eq!(*ttl, Some(9_000_000_000_000));

        // Cut in the gap: the value is durable and permanent. This is the
        // defect, reproduced — and unreachable for anything written now.
        let torn = replay(&full[..after_set], &HashSet::new());
        let (_, _, ttl) = torn.items.iter().find(|(k, _, _)| k == "k").unwrap();
        assert_eq!(*ttl, None, "the old shape did lose the expiry");
    }

    /// `SET k v` with no TTL over a key that had one clears the expiry in
    /// memory; replay has to agree, or a key the server reports as permanent
    /// disappears after a restart.
    #[test]
    fn set_with_no_expiry_clears_an_existing_one() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();
        wal.log_set_with_expiry(None, "k", &Value::Int32(1), Some(9_000_000_000_000))
            .unwrap();
        wal.log_set_with_expiry(None, "k", &Value::Int32(2), None)
            .unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        let (_, v, ttl) = state.items.iter().find(|(k, _, _)| k == "k").unwrap();
        assert_eq!(v, &Value::Int32(2));
        assert_eq!(*ttl, None, "the expiry outlived the SET that replaced it");
    }

    /// `ENTRY_SET` still means "keep the expiry" — INCR depends on it.
    #[test]
    fn plain_set_preserves_an_existing_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();
        wal.log_set_with_expiry(None, "k", &Value::Int64(1), Some(9_000_000_000_000))
            .unwrap();
        wal.log_set(None, "k", &Value::Int64(2)).unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        let (_, v, ttl) = state.items.iter().find(|(k, _, _)| k == "k").unwrap();
        assert_eq!(v, &Value::Int64(2));
        assert_eq!(*ttl, Some(9_000_000_000_000));
    }

    /// A batched SET that carries an expiry is one record too.
    #[test]
    fn batched_set_exact_carries_its_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();
        let a = Value::Int32(1);
        let b = Value::Int32(2);
        wal.log_batch(
            None,
            &[
                (KvWalOp::SetExact, "a", Some(&a), Some(9_000_000_000_000)),
                (KvWalOp::SetExact, "b", Some(&b), None),
            ],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        let a = state.items.iter().find(|(k, _, _)| k == "a").unwrap();
        assert_eq!(a.2, Some(9_000_000_000_000));
        let b = state.items.iter().find(|(k, _, _)| k == "b").unwrap();
        assert_eq!(b.2, None);
    }

    #[test]
    fn test_batch_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_keep_all(dir.path()).unwrap();

        wal.log_set(None, "pre", &Value::Int32(1)).unwrap();
        // Empty batch should be a no-op
        wal.log_batch(None, &[]).unwrap();
        drop(wal);

        let (_wal2, state) = open_keep_all(dir.path()).unwrap();
        assert_eq!(state.items.len(), 1);
        assert_eq!(state.items[0].0, "pre");
    }
}
