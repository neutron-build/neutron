//! Write-ahead log for the MVCC storage adapter.
//!
//! Provides crash-safe durability by logging all mutations (DDL + DML)
//! as logical records.  On recovery, committed transactions are replayed
//! in order while aborted/in-flight transactions are skipped.
//!
//! ## Binary entry format
//! ```text
//! [record_len: u32 LE] [tag: u8] [payload ...] [crc32: u32 LE]
//! ```

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::PathBuf;

use parking_lot::Mutex;

use crate::types::{DataType, Value};

// ── Record tags ──────────────────────────────────────────────────────────────

const TAG_CREATE_TABLE: u8 = 0x01;
const TAG_DROP_TABLE: u8 = 0x02;
const TAG_INSERT: u8 = 0x03;
const TAG_DELETE: u8 = 0x04;
const TAG_UPDATE: u8 = 0x05;
const TAG_BEGIN: u8 = 0x10;
const TAG_COMMIT: u8 = 0x11;
const TAG_ABORT: u8 = 0x12;
/// S63 marker: the coordinating (cross-model) transaction id that committed
/// alongside this WAL's own txn id. A separate record rather than a field on
/// `Commit` so the pre-S63 `Commit` byte layout is untouched — addition-only
/// compatibility, the same rule every specialty WAL follows.
const TAG_XACT_COMMIT: u8 = 0x13;
const TAG_CHECKPOINT: u8 = 0x20;

// ── Public API ───────────────────────────────────────────────────────────────

/// A logical WAL record for the MVCC engine.
#[derive(Debug, Clone)]
pub enum MvccWalRecord {
    CreateTable {
        name: String,
        columns: Vec<(String, DataType)>,
    },
    DropTable {
        name: String,
    },
    Insert {
        table: String,
        txn_id: u64,
        /// Engine version index assigned to this row (stable identity for the
        /// life of the table). Replay keys rows by this so DELETE/UPDATE address
        /// the exact row regardless of scan order.
        version_idx: u32,
        row: Vec<Value>,
    },
    Delete {
        table: String,
        txn_id: u64,
        /// Version index of the deleted row (NOT a scan position).
        version_idx: u32,
    },
    Update {
        table: String,
        txn_id: u64,
        /// Version index of the superseded row.
        old_version_idx: u32,
        /// Version index of the new row version the engine appended.
        new_version_idx: u32,
        new_row: Vec<Value>,
    },
    Begin {
        txn_id: u64,
    },
    Commit {
        txn_id: u64,
    },
    Abort {
        txn_id: u64,
    },
    /// S63: cross-model transaction `xact` committed. Written next to the
    /// `Commit` of the SQL transaction it coordinated, and rewritten by
    /// `compact` so the marker set survives log reclaim — the specialty-WAL
    /// recovery filter needs it for as long as any tagged specialty record
    /// can still be in a replay tail.
    XactCommit {
        xact: u64,
    },
    Checkpoint,
}

/// State recovered from replaying the MVCC WAL.
#[derive(Debug, Default)]
pub struct MvccWalState {
    /// Recovered tables: table_name → (columns, rows).
    pub tables: HashMap<String, RecoveredTable>,
    /// Coordinating transaction ids (S63) whose commit markers survived in
    /// this log. `compact` rewrites them into the baseline so the set cannot
    /// be erased by routine reclamation.
    pub committed_xacts: std::collections::HashSet<u64>,
}

/// A recovered table with its schema and committed rows.
#[derive(Debug, Clone)]
pub struct RecoveredTable {
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<Vec<Value>>,
}

/// Append-only WAL for MVCC durability.
pub struct MvccWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Append/fsync bookkeeping, so the engine above can answer "is there
    /// un-fsynced work?" and group concurrent committers onto one fsync.
    ///
    /// Every specialty WAL (KV, collections, timeseries, vector, graph,
    /// streams, CDC) already carries one of these; the SQL WAL did not, which
    /// is precisely why `MvccStorageAdapter` could not implement
    /// `durability_pending`/`make_durable` and inherited their trait defaults
    /// (`false` / `Ok(())`). The executor's commit-point force then skipped
    /// the engine entirely and an autocommit write was acked having only been
    /// `flush()`ed into the OS page cache.
    sync: crate::storage::wal_util::WalSync,
}

/// Encode a record and enforce the framing contract BEFORE any byte is
/// written (NU-21): replay rejects payloads above `MAX_PAYLOAD`, so the
/// writer must not accept a record its own replay would refuse — an
/// oversized append used to succeed and poison every subsequent open.
fn encode_checked(record: &MvccWalRecord) -> io::Result<Vec<u8>> {
    let payload = encode_record(record);
    if payload.is_empty() || payload.len() > MAX_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "WAL payload of {} bytes exceeds the {}-byte replay limit",
                payload.len(),
                MAX_PAYLOAD
            ),
        ));
    }
    Ok(payload)
}

/// Write one length-prefixed, CRC-suffixed record onto any writer. Shares
/// the exact framing `MvccWal::log` uses so a staged file replays
/// identically. Both paths share `encode_checked`, so an appender and
/// compaction can never diverge on the size contract (NU-21).
fn write_framed<W: Write>(w: &mut W, record: &MvccWalRecord) -> io::Result<()> {
    let payload = encode_checked(record)?;
    let crc = crc32c(&payload);
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(&payload)?;
    w.write_all(&crc.to_le_bytes())
}

impl MvccWal {
    /// Open or create the WAL file.  Returns (wal, recovered_state).
    ///
    /// Corruption is fatal (NU-04): a mid-file CRC mismatch or undecodable
    /// record surfaces as `InvalidData` with the byte offset, and the file
    /// is left exactly as found — repair is an operator decision. A torn
    /// FINAL frame (crash mid-append, nothing durable behind it) is
    /// accepted with a warning and recovers the prefix; the next compaction
    /// rewrites the log cleanly without the torn tail.
    pub fn open(dir: &std::path::Path) -> io::Result<(Self, MvccWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("mvcc.wal");
        let state = if path.exists() {
            let mut data = Vec::new();
            File::open(&path)?.read_to_end(&mut data)?;
            match replay(&data) {
                Ok((state, ReplayStop::TornTail { at })) => {
                    let recovered: usize = state.tables.values().map(|t| t.rows.len()).sum();
                    eprintln!(
                        "nucleus: MVCC WAL has a torn final record at byte {at} \
                         (crash during append); recovering {recovered} committed rows \
                         and rewriting the log without the tail"
                    );
                    state
                }
                Ok((state, ReplayStop::CleanEof)) => state,
                Err(msg) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{msg} — the WAL is left unmodified; inspect it before retrying"),
                    ));
                }
            }
        } else {
            MvccWalState::default()
        };
        // A `.wal.compacting` file is a staged baseline from a compaction that
        // crashed before its atomic rename. It was never authoritative, so
        // discard it rather than leaving it to confuse a later compaction.
        let _ = std::fs::remove_file(path.with_extension("wal.compacting"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                sync: crate::storage::wal_util::WalSync::new(),
            },
            state,
        ))
    }

    /// Log a record and flush to OS buffer.
    pub fn log(&self, record: &MvccWalRecord) -> io::Result<()> {
        // Size-check before the first byte goes out (NU-21): an accepted
        // record that replay rejects would brick every later open.
        let payload = encode_checked(record)?;
        let crc = crc32c(&payload);
        let len = payload.len() as u32; // bounded by MAX_PAYLOAD check above
        let mut w = self.writer.lock();
        crate::storage::crashpoint::io_fault_check!("wal.append");
        crate::storage::crashpoint::reach("wal.before_append");
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&payload)?;
        w.write_all(&crc.to_le_bytes())?;
        let r = w.flush();
        // Bump the LSN under the writer lock, so a concurrent `group_sync`'s
        // captured mark is exact.
        self.sync.on_append();
        crate::storage::crashpoint::reach("wal.after_append");
        r
    }

    /// Flush + fsync, returning the highest append LSN the fsync durably
    /// covered. The mark is read under the writer lock, so every append at or
    /// below it is guaranteed flushed before the `sync_all`.
    fn sync_covering(&self) -> io::Result<u64> {
        let mut w = self.writer.lock();
        let covered = self.sync.current();
        w.flush()?;
        crate::storage::crashpoint::io_fault_check!("wal.fsync");
        crate::storage::crashpoint::reach("wal.before_fsync");
        w.get_ref().sync_all()?;
        crate::storage::crashpoint::reach("wal.after_fsync");
        Ok(covered)
    }

    /// Fsync the WAL file to ensure durability.
    pub fn sync(&self) -> io::Result<()> {
        let covered = self.sync_covering()?;
        self.sync.mark_synced(covered);
        Ok(())
    }

    /// Group-commit sync: returns only once a completed fsync covers every
    /// append made before this call. Concurrent committers share fsyncs.
    pub fn group_sync(&self) -> io::Result<()> {
        self.sync.group_sync(|| self.sync_covering())
    }

    /// Whether appends exist that no completed fsync covers yet.
    pub fn is_dirty(&self) -> bool {
        self.sync.is_dirty()
    }

    /// Log a COMMIT and immediately fsync. When the committing transaction
    /// coordinated specialty models (S63), `xact` also writes a durable
    /// `XactCommit` marker under the same fsync, so a crash between the two
    /// records cannot split "SQL committed" from "specialty writes keepable".
    pub fn log_commit(&self, txn_id: u64, xact: Option<u64>) -> io::Result<()> {
        crate::storage::crashpoint::reach("wal.before_commit_record");
        self.log(&MvccWalRecord::Commit { txn_id })?;
        if let Some(xact) = xact {
            self.log(&MvccWalRecord::XactCommit { xact })?;
        }
        let r = self.sync();
        crate::storage::crashpoint::reach("wal.after_commit_record");
        r
    }

    /// Truncate the WAL (after a full snapshot has been written).
    pub fn truncate(&self) -> io::Result<()> {
        let mut w = self.writer.lock();
        w.flush()?;
        drop(w);
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        *self.writer.lock() = BufWriter::new(file);
        // Re-open in append mode
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        *self.writer.lock() = BufWriter::new(file);
        Ok(())
    }

    /// Rewrite the WAL as a clean baseline for a recovered state: one
    /// `CreateTable` plus sequential auto-committed `Insert`s (version_idx 0..n)
    /// per table. Called on open right after replay so that (a) version indices
    /// restart from 0 each run — otherwise a fresh run's new vidx would collide
    /// with a survivor's old vidx in the accumulated WAL and corrupt the NEXT
    /// recovery — and (b) the WAL stays compact. The caller reconstructs the
    /// engine from the SAME `state` in the same per-table row order, so the
    /// engine's assigned version indices match these baseline records exactly.
    pub fn compact(&self, state: &MvccWalState) -> io::Result<()> {
        // CRASH SAFETY: stage the new baseline in a temp file, fsync it, then
        // swap it in with an atomic rename.
        //
        // The previous implementation truncated the LIVE WAL in place and then
        // rewrote it. A crash in that window destroyed the only durable copy:
        // the deterministic crash matrix caught it losing all 40 fsynced rows
        // at `checkpoint.mid_rewrite`. Because compaction runs on EVERY reopen
        // of a populated database, that made a power loss during startup a
        // total-data-loss event for a database that had been fully fsynced.
        //
        // With stage-and-rename, a crash at any instant leaves either the old
        // complete WAL or the new complete one, never a truncated file.
        crate::storage::crashpoint::reach("checkpoint.before");
        let tmp_path = self.path.with_extension("wal.compacting");
        {
            let tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut w = BufWriter::new(tmp);
            for (name, tbl) in &state.tables {
                write_framed(
                    &mut w,
                    &MvccWalRecord::CreateTable {
                        name: name.clone(),
                        columns: tbl.columns.clone(),
                    },
                )?;
                for (i, row) in tbl.rows.iter().enumerate() {
                    write_framed(
                        &mut w,
                        &MvccWalRecord::Insert {
                            table: name.clone(),
                            txn_id: 0,
                            version_idx: i as u32,
                            row: row.clone(),
                        },
                    )?;
                }
            }
            // The baseline above carries rows as auto-commits, so every Commit
            // record — and with it every `XactCommit` marker — would vanish
            // from the log. The marker set must survive: compaction runs on
            // every reopen, so without this a committed cross-model
            // transaction's specialty records would lose their only durable
            // commit proof on the SECOND restart and the S63 recovery filter
            // would discard them.
            for xact in &state.committed_xacts {
                write_framed(&mut w, &MvccWalRecord::XactCommit { xact: *xact })?;
            }
            w.flush()?;
            // Dying here must be survivable: the live WAL is still intact and
            // the temp file is garbage that open() discards.
            crate::storage::crashpoint::reach("checkpoint.mid_rewrite");
            w.get_ref().sync_all()?;
        }

        let mut guard = self.writer.lock();
        guard.flush()?;
        std::fs::rename(&tmp_path, &self.path)?;
        // Fsync the directory so the rename itself survives a crash; without
        // this the swap can be lost even though both files were fsynced.
        if let Some(dir) = self.path.parent()
            && let Ok(d) = File::open(dir)
        {
            let _ = d.sync_all();
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        *guard = BufWriter::new(file);
        drop(guard);
        crate::storage::crashpoint::reach("checkpoint.after");
        Ok(())
    }
}

// ── Encoding ─────────────────────────────────────────────────────────────────

fn encode_record(rec: &MvccWalRecord) -> Vec<u8> {
    let mut buf = Vec::new();
    match rec {
        MvccWalRecord::CreateTable { name, columns } => {
            buf.push(TAG_CREATE_TABLE);
            write_str(&mut buf, name);
            write_u32(&mut buf, columns.len() as u32);
            for (col_name, col_type) in columns {
                write_str(&mut buf, col_name);
                write_u8(&mut buf, datatype_to_u8(col_type));
            }
        }
        MvccWalRecord::DropTable { name } => {
            buf.push(TAG_DROP_TABLE);
            write_str(&mut buf, name);
        }
        MvccWalRecord::Insert {
            table,
            txn_id,
            version_idx,
            row,
        } => {
            buf.push(TAG_INSERT);
            write_str(&mut buf, table);
            write_u64(&mut buf, *txn_id);
            write_u32(&mut buf, *version_idx);
            crate::storage::value_codec::write_row(&mut buf, row);
        }
        MvccWalRecord::Delete {
            table,
            txn_id,
            version_idx,
        } => {
            buf.push(TAG_DELETE);
            write_str(&mut buf, table);
            write_u64(&mut buf, *txn_id);
            write_u32(&mut buf, *version_idx);
        }
        MvccWalRecord::Update {
            table,
            txn_id,
            old_version_idx,
            new_version_idx,
            new_row,
        } => {
            buf.push(TAG_UPDATE);
            write_str(&mut buf, table);
            write_u64(&mut buf, *txn_id);
            write_u32(&mut buf, *old_version_idx);
            write_u32(&mut buf, *new_version_idx);
            crate::storage::value_codec::write_row(&mut buf, new_row);
        }
        MvccWalRecord::Begin { txn_id } => {
            buf.push(TAG_BEGIN);
            write_u64(&mut buf, *txn_id);
        }
        MvccWalRecord::Commit { txn_id } => {
            buf.push(TAG_COMMIT);
            write_u64(&mut buf, *txn_id);
        }
        MvccWalRecord::Abort { txn_id } => {
            buf.push(TAG_ABORT);
            write_u64(&mut buf, *txn_id);
        }
        MvccWalRecord::XactCommit { xact } => {
            buf.push(TAG_XACT_COMMIT);
            write_u64(&mut buf, *xact);
        }
        MvccWalRecord::Checkpoint => {
            buf.push(TAG_CHECKPOINT);
        }
    }
    buf
}

// ── Primitive helpers ────────────────────────────────────────────────────────

fn write_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}
fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    write_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

fn read_u32_val(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_u64_val(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_str(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32_val(data, pos)? as usize;
    if *pos + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .ok()?
        .to_string();
    *pos += len;
    Some(s)
}

fn datatype_to_u8(dt: &DataType) -> u8 {
    match dt {
        DataType::Bool => 0,
        DataType::Int32 => 1,
        DataType::Int64 => 2,
        DataType::Float64 => 3,
        DataType::Text => 4,
        DataType::Bytea => 5,
        DataType::Numeric => 6,
        DataType::Uuid => 7,
        DataType::Date => 8,
        DataType::Timestamp => 9,
        DataType::TimestampTz => 10,
        DataType::Interval => 11,
        DataType::Jsonb => 12,
        DataType::Vector(_) => 13,
        DataType::Array(_) => 14,
        DataType::UserDefined(_) => 15,
    }
}

/// Decode a schema type tag. Unknown codes return None (NU-15): silently
/// substituting TEXT for a corrupt/unknown type code reconstructed a
/// DIFFERENT schema than the one that was written.
fn u8_to_datatype(v: u8) -> Option<DataType> {
    let dt = match v {
        0 => DataType::Bool,
        1 => DataType::Int32,
        2 => DataType::Int64,
        3 => DataType::Float64,
        4 => DataType::Text,
        5 => DataType::Bytea,
        6 => DataType::Numeric,
        7 => DataType::Uuid,
        8 => DataType::Date,
        9 => DataType::Timestamp,
        10 => DataType::TimestampTz,
        11 => DataType::Interval,
        12 => DataType::Jsonb,
        13 => DataType::Vector(0),
        14 => DataType::Array(Box::new(DataType::Text)),
        15 => DataType::UserDefined(String::new()),
        _ => return None,
    };
    Some(dt)
}

/// Simple CRC32C (Castagnoli) for WAL record integrity.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

// ── Replay ───────────────────────────────────────────────────────────────────

/// Ceiling on a single framed payload. A length field beyond this is treated
/// as corruption, not as a torn tail: real records are kilobytes at most, so
/// a multi-gigabyte "length" can only be a damaged frame.
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

/// Why replay stopped early, when it did.
#[derive(Debug)]
pub enum ReplayStop {
    /// The file ended exactly on a frame boundary — a clean log.
    CleanEof,
    /// The final frame is incomplete (torn write from a crash mid-append).
    /// Everything before it parsed and is recovered; the torn frame is
    /// dropped by the next compaction. This is the explicit torn-tail
    /// policy (NU-04): accepted, because the frame never had a durable
    /// commit decision behind it.
    TornTail { at: usize },
}

/// Replay WAL data to recover committed state.
///
/// Fail-closed corruption policy (NU-04): a CRC mismatch, an impossible
/// length field, an unknown record tag, or undecodable payload is CORRUPTION
/// and returns Err — startup fails with the byte offset and the original
/// file is left untouched for diagnosis (compaction only runs after a
/// successful open). The previous behavior silently accepted the longest
/// parseable prefix and then compacted it over the damaged suffix,
/// permanently discarding the evidence and any committed records after the
/// damage.
fn replay(data: &[u8]) -> Result<(MvccWalState, ReplayStop), String> {
    let mut pos = 0usize;
    let mut records: Vec<MvccWalRecord> = Vec::new();

    // Phase 1: Parse all records
    loop {
        if pos == data.len() {
            return finish_replay(records, ReplayStop::CleanEof);
        }
        let frame_start = pos;
        let Some(length_bytes) = data.get(pos..pos + 4) else {
            // Fewer than 4 bytes left: a length header cut off mid-write.
            return finish_replay(records, ReplayStop::TornTail { at: frame_start });
        };
        let len = u32::from_le_bytes([length_bytes[0], length_bytes[1], length_bytes[2], length_bytes[3]])
            as usize;
        pos += 4;
        if len == 0 || len > MAX_PAYLOAD {
            return Err(format!(
                "MVCC WAL corruption: impossible record length {len} at byte {frame_start}"
            ));
        }
        if pos + len + 4 > data.len() {
            // The declared frame extends past EOF: a torn write.
            return finish_replay(records, ReplayStop::TornTail { at: frame_start });
        }
        let payload = &data[pos..pos + len];
        pos += len;
        let crc_bytes = &data[pos..pos + 4];
        pos += 4;
        let stored_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        let computed_crc = crc32c(payload);
        if stored_crc != computed_crc {
            return Err(format!(
                "MVCC WAL corruption: CRC mismatch at byte {frame_start} \
                 (stored {stored_crc:#x}, computed {computed_crc:#x})"
            ));
        }

        match decode_record(payload) {
            Some(rec) => records.push(rec),
            None => {
                return Err(format!(
                    "MVCC WAL corruption: undecodable record (tag {:#x}) at byte {frame_start}",
                    payload.first().copied().unwrap_or(0)
                ));
            }
        }
    }
}

/// Phase 2+3 of replay, shared by every stop kind.
fn finish_replay(
    records: Vec<MvccWalRecord>,
    stop: ReplayStop,
) -> Result<(MvccWalState, ReplayStop), String> {
    // Identify committed transactions, validating the decision log (NU-23):
    // a transaction with BOTH a Commit and an Abort record is corrupt input
    // (a writer/retry bug — replay used to silently treat it as committed),
    // and a terminal record for the reserved autocommit id 0 contradicts the
    // implicit-autocommit contract. Identical duplicate markers stay
    // idempotent. Contradictions fail recovery with the offending id; the
    // file is left untouched for diagnosis, like every other corruption.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Decision {
        Commit,
        Abort,
    }
    fn record_decision(
        decisions: &mut std::collections::HashMap<u64, Decision>,
        txn_id: u64,
        next: Decision,
    ) -> Result<(), String> {
        if txn_id == 0 {
            return Err(format!(
                "MVCC WAL corruption: terminal {:?} record for reserved autocommit id 0",
                next
            ));
        }
        match decisions.get(&txn_id) {
            Some(&previous) if previous != next => Err(format!(
                "MVCC WAL corruption: conflicting terminal decisions for txn {txn_id} \
                 ({previous:?} then {next:?})"
            )),
            Some(_) => Ok(()), // identical duplicate marker: idempotent
            None => {
                decisions.insert(txn_id, next);
                Ok(())
            }
        }
    }
    let mut decisions: std::collections::HashMap<u64, Decision> =
        std::collections::HashMap::new();
    let mut committed_xacts: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for rec in &records {
        match rec {
            MvccWalRecord::Commit { txn_id } => {
                record_decision(&mut decisions, *txn_id, Decision::Commit)?;
            }
            MvccWalRecord::Abort { txn_id } => {
                record_decision(&mut decisions, *txn_id, Decision::Abort)?;
            }
            MvccWalRecord::XactCommit { xact } => {
                committed_xacts.insert(*xact);
            }
            _ => {}
        }
    }
    let committed: std::collections::HashSet<u64> = decisions
        .into_iter()
        .filter_map(|(txn_id, decision)| match decision {
            Decision::Commit => Some(txn_id),
            Decision::Abort => None,
        })
        .collect();

    // Replay committed operations (and auto-commits where txn_id=0).
    // Rows are keyed by the engine's stable per-row VERSION INDEX, so DELETE and
    // UPDATE address the exact row by identity — no fragile scan-position
    // arithmetic. A BTreeMap keeps rows in version order (the scan order); the
    // final ordering is irrelevant to callers, which re-sort, but it is
    // deterministic. An uncommitted transaction's records are simply never
    // applied, so its writes are rolled back on recovery.
    let mut columns: HashMap<String, Vec<(String, DataType)>> = HashMap::new();
    let mut rowmaps: HashMap<String, std::collections::BTreeMap<u32, Vec<Value>>> = HashMap::new();

    for rec in &records {
        let committed_rec = |txn_id: &u64| *txn_id == 0 || committed.contains(txn_id);
        match rec {
            MvccWalRecord::CreateTable {
                name,
                columns: cols,
            } => {
                columns.insert(name.clone(), cols.clone());
                rowmaps.insert(name.clone(), std::collections::BTreeMap::new());
            }
            MvccWalRecord::DropTable { name } => {
                columns.remove(name);
                rowmaps.remove(name);
            }
            MvccWalRecord::Insert {
                table,
                txn_id,
                version_idx,
                row,
            } => {
                if committed_rec(txn_id)
                    && let Some(m) = rowmaps.get_mut(table)
                {
                    m.insert(*version_idx, row.clone());
                }
            }
            MvccWalRecord::Delete {
                table,
                txn_id,
                version_idx,
            } => {
                if committed_rec(txn_id)
                    && let Some(m) = rowmaps.get_mut(table)
                {
                    m.remove(version_idx);
                }
            }
            MvccWalRecord::Update {
                table,
                txn_id,
                old_version_idx,
                new_version_idx,
                new_row,
            } => {
                if committed_rec(txn_id)
                    && let Some(m) = rowmaps.get_mut(table)
                {
                    m.remove(old_version_idx);
                    m.insert(*new_version_idx, new_row.clone());
                }
            }
            MvccWalRecord::Checkpoint => {
                // After a checkpoint, previous records can be ignored.
                // In a future version, truncate records before the checkpoint.
            }
            _ => {} // Begin, Commit, Abort handled above
        }
    }

    let tables: HashMap<String, RecoveredTable> = columns
        .into_iter()
        .map(|(name, cols)| {
            let rows = rowmaps
                .remove(&name)
                .map(|m| m.into_values().collect())
                .unwrap_or_default();
            (
                name,
                RecoveredTable {
                    columns: cols,
                    rows,
                },
            )
        })
        .collect();

    Ok((
        MvccWalState {
            tables,
            committed_xacts,
        },
        stop,
    ))
}

/// Decode a payload to a record, or None when it is not a well-formed
/// record of a KNOWN shape (NU-04/NU-15): unknown tags, unknown type codes,
/// and trailing undecoded bytes are all rejected rather than best-effort
/// coerced — a value_codec read that stops early used to leave silently
/// ignored bytes, and unknown schema-type tags used to degrade to TEXT.
fn decode_record(data: &[u8]) -> Option<MvccWalRecord> {
    let mut pos = 0usize;
    let tag = *data.get(pos)?;
    pos += 1;
    let record = match tag {
        TAG_CREATE_TABLE => {
            let name = read_str(data, &mut pos)?;
            let count = read_u32_val(data, &mut pos)? as usize;
            let mut columns = Vec::with_capacity(super::wal_util::bounded_capacity(count));
            for _ in 0..count {
                let col_name = read_str(data, &mut pos)?;
                let col_type = *data.get(pos)?;
                pos += 1;
                columns.push((col_name, u8_to_datatype(col_type)?));
            }
            MvccWalRecord::CreateTable { name, columns }
        }
        TAG_DROP_TABLE => {
            let name = read_str(data, &mut pos)?;
            MvccWalRecord::DropTable { name }
        }
        TAG_INSERT => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let version_idx = read_u32_val(data, &mut pos)?;
            let row = crate::storage::value_codec::read_row(data, &mut pos)?;
            MvccWalRecord::Insert {
                table,
                txn_id,
                version_idx,
                row,
            }
        }
        TAG_DELETE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let version_idx = read_u32_val(data, &mut pos)?;
            MvccWalRecord::Delete {
                table,
                txn_id,
                version_idx,
            }
        }
        TAG_UPDATE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let old_version_idx = read_u32_val(data, &mut pos)?;
            let new_version_idx = read_u32_val(data, &mut pos)?;
            let new_row = crate::storage::value_codec::read_row(data, &mut pos)?;
            MvccWalRecord::Update {
                table,
                txn_id,
                old_version_idx,
                new_version_idx,
                new_row,
            }
        }
        TAG_BEGIN => {
            let txn_id = read_u64_val(data, &mut pos)?;
            MvccWalRecord::Begin { txn_id }
        }
        TAG_COMMIT => {
            let txn_id = read_u64_val(data, &mut pos)?;
            MvccWalRecord::Commit { txn_id }
        }
        TAG_ABORT => {
            let txn_id = read_u64_val(data, &mut pos)?;
            MvccWalRecord::Abort { txn_id }
        }
        TAG_XACT_COMMIT => {
            let xact = read_u64_val(data, &mut pos)?;
            MvccWalRecord::XactCommit { xact }
        }
        TAG_CHECKPOINT => MvccWalRecord::Checkpoint,
        _ => return None,
    };
    if pos != data.len() {
        // Trailing bytes the decoder does not know how to interpret.
        return None;
    }
    Some(record)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_create_insert_commit() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: Write records
        {
            let (wal, state) = MvccWal::open(dir.path()).unwrap();
            assert!(state.tables.is_empty());

            wal.log(&MvccWalRecord::CreateTable {
                name: "users".into(),
                columns: vec![
                    ("id".into(), DataType::Int64),
                    ("name".into(), DataType::Text),
                ],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "users".into(),
                txn_id: 1,
                version_idx: 0,
                row: vec![Value::Int64(1), Value::Text("Alice".into())],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "users".into(),
                txn_id: 1,
                version_idx: 1,
                row: vec![Value::Int64(2), Value::Text("Bob".into())],
            })
            .unwrap();
            wal.log_commit(1, None).unwrap();
            drop(wal);
        }

        // Phase 2: Recover
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        let users = state.tables.get("users").unwrap();
        assert_eq!(users.rows.len(), 2);
        assert_eq!(users.rows[0][1], Value::Text("Alice".into()));
        assert_eq!(users.rows[1][1], Value::Text("Bob".into()));
    }

    #[test]
    fn test_aborted_txn_not_recovered() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int32)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_idx: 0,
                row: vec![Value::Int32(10)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Abort { txn_id: 1 }).unwrap();
            drop(wal);
        }

        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").unwrap();
        assert_eq!(t.rows.len(), 0); // Aborted insert should NOT be present
    }

    #[test]
    fn test_uncommitted_txn_not_recovered() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int32)],
            })
            .unwrap();
            // Begin but never commit/abort
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_idx: 0,
                row: vec![Value::Int32(42)],
            })
            .unwrap();
            drop(wal);
        }

        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").unwrap();
        assert_eq!(t.rows.len(), 0); // In-flight insert should NOT be present
    }

    #[test]
    fn test_crc_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int32)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_idx: 0,
                row: vec![Value::Int32(99)],
            })
            .unwrap();
            wal.log_commit(1, None).unwrap();
            drop(wal);
        }

        // Corrupt a byte in the middle of the WAL
        let path = dir.path().join("mvcc.wal");
        let original = std::fs::read(&path).unwrap();
        let mut data = original.clone();
        if data.len() > 20 {
            data[20] ^= 0xFF;
        }
        std::fs::write(&path, &data).unwrap();

        // Fail closed (NU-04): mid-file corruption is an error, the file is
        // preserved for diagnosis, and no state is silently recovered.
        let result = MvccWal::open(dir.path());
        match result {
            Err(err) => {
                assert!(
                    err.to_string().contains("corruption"),
                    "error must name corruption: {err}"
                );
                // The damaged file is left byte-for-byte intact.
                assert_eq!(std::fs::read(&path).unwrap(), data);
            }
            Ok(_) => panic!("corrupted WAL was accepted"),
        }
        // Restore the pristine log to prove it still opens cleanly.
        std::fs::write(&path, original).unwrap();
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(state.tables.get("t").map(|t| t.rows.len()), Some(1));
    }

    #[test]
    fn torn_tail_recovers_the_prefix() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_idx: 0,
                row: vec![Value::Int64(1)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_idx: 1,
                row: vec![Value::Int64(2)],
            })
            .unwrap();
            wal.sync().unwrap();
        }
        // Truncate mid-frame: the FINAL insert loses its tail bytes and is
        // dropped; everything before it was fsynced and must survive.
        let path = dir.path().join("mvcc.wal");
        let full = std::fs::read(&path).unwrap();
        std::fs::write(&path, &full[..full.len() - 3]).unwrap();

        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").expect("table survived");
        assert_eq!(t.rows.len(), 1, "complete prefix records must be recovered");
        assert_eq!(t.rows[0], vec![Value::Int64(1)]);
    }

    #[test]
    fn test_drop_table_removes_data() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "temp".into(),
                columns: vec![("x".into(), DataType::Int32)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "temp".into(),
                txn_id: 0,
                version_idx: 0,
                row: vec![Value::Int32(1)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::DropTable {
                name: "temp".into(),
            })
            .unwrap();
            drop(wal);
        }

        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        assert!(!state.tables.contains_key("temp"));
    }

    /// NU-21: the writer must refuse a record its own replay would reject
    /// as oversized, before writing a single byte — an accepted append that
    /// bricks every later open is a writer/replay contract mismatch.
    #[test]
    fn oversized_record_is_rejected_and_leaves_file_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = MvccWal::open(dir.path()).unwrap();
        wal.log(&MvccWalRecord::CreateTable {
            name: "t".into(),
            columns: vec![("x".into(), DataType::Bytea)],
        })
        .unwrap();
        wal.sync().unwrap();
        let before = std::fs::read(dir.path().join("mvcc.wal")).unwrap();

        let oversized = MvccWalRecord::Insert {
            table: "t".into(),
            txn_id: 0,
            version_idx: 0,
            row: vec![Value::Bytea(vec![0u8; MAX_PAYLOAD + 1])],
        };
        let err = wal.log(&oversized).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        // Rejected before the first byte: the file is byte-for-byte intact
        // and still opens cleanly.
        assert_eq!(std::fs::read(dir.path().join("mvcc.wal")).unwrap(), before);
        drop(wal);
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(state.tables.get("t").map(|t| t.rows.len()), Some(0));
    }

    /// NU-23: a transaction carrying BOTH Commit and Abort records is
    /// contradictory input; recovery must fail closed instead of silently
    /// treating the transaction as committed.
    #[test]
    fn contradictory_commit_and_abort_fails_recovery() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int32)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_idx: 0,
                row: vec![Value::Int32(7)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Commit { txn_id: 1 }).unwrap();
            // Written directly (not via log_commit): the contradictory
            // terminal marker a buggy writer/retry sequence would emit.
            wal.log(&MvccWalRecord::Abort { txn_id: 1 }).unwrap();
            drop(wal);
        }
        let result = MvccWal::open(dir.path());
        match result {
            Err(err) => assert!(
                err.to_string().contains("conflicting terminal decisions"),
                "error must name the contradiction: {err}"
            ),
            Ok(_) => panic!("contradictory terminal records were accepted"),
        }
    }

    /// NU-23: identical duplicate terminal markers stay idempotent — the
    /// same Commit record twice is a benign retry, not corruption.
    #[test]
    fn duplicate_identical_commit_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int32)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_idx: 0,
                row: vec![Value::Int32(7)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Commit { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Commit { txn_id: 1 }).unwrap();
            drop(wal);
        }
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(state.tables.get("t").map(|t| t.rows.len()), Some(1));
    }
}

// ── S63: coordinating-transaction markers survive reclaim ─────────────────

#[cfg(test)]
mod xact_marker_tests {
    use super::*;

    /// A commit's XactCommit marker is recovered into the committed set.
    #[test]
    fn xact_marker_is_recovered() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
            })
            .unwrap();
            wal.log_commit(7, Some(42)).unwrap();
        }
        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        assert!(
            state.committed_xacts.contains(&42),
            "the coordinating id must be recovered alongside the commit"
        );
    }

    /// Compaction runs on EVERY reopen and rewrites the log as txn-0
    /// auto-commits, which would erase every Commit — and with them every
    /// XactCommit marker. The markers must be rewritten into the baseline or
    /// the second restart loses the only proof those transactions committed
    /// (and the S6 filter discards their specialty records).
    #[test]
    fn xact_markers_survive_compaction_across_two_reopens() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_idx: 0,
                row: vec![Value::Int64(5)],
            })
            .unwrap();
            wal.log_commit(1, Some(9)).unwrap();
            drop(wal);
        }
        // Reopen 1: replay, then compact (the with_wal sequence).
        let (wal, state) = MvccWal::open(dir.path()).unwrap();
        assert!(state.committed_xacts.contains(&9));
        wal.compact(&state).unwrap();
        drop(wal);
        // Reopen 2: the compacted baseline must still answer for xact 9.
        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        assert!(
            state.committed_xacts.contains(&9),
            "compaction erased the XactCommit marker: the second restart would \
             discard a committed transaction's specialty records"
        );
        assert_eq!(
            state.tables.get("t").map(|t| t.rows.len()),
            Some(1),
            "the baseline rows are unchanged by marker preservation"
        );
    }
}

#[cfg(test)]
mod crash_safety_tests {
    use super::*;

    /// Compaction must never leave the live WAL truncated.
    ///
    /// Regression pin for a total-data-loss defect: `compact` used to
    /// `truncate()` the live WAL and then rewrite it, so a crash in that window
    /// destroyed the only durable copy. Compaction runs on every reopen of a
    /// populated database, which made a power loss during startup lose a
    /// database whose every commit had been fsynced.
    ///
    /// This simulates the crash without a subprocess: it stages the compaction
    /// exactly as `compact` does, then abandons it before the rename — the
    /// worst instant — and asserts the live WAL still replays every record.
    #[test]
    fn abandoned_compaction_leaves_the_live_wal_intact() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = MvccWal::open(dir.path()).unwrap();

        wal.log(&MvccWalRecord::CreateTable {
            name: "t".into(),
            columns: vec![("id".into(), DataType::Int64)],
        })
        .unwrap();
        for i in 0..5u32 {
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_idx: i,
                row: vec![Value::Int64(i as i64)],
            })
            .unwrap();
        }
        wal.sync().unwrap();

        // Simulate a compaction that died after staging but before the swap.
        let staged = dir.path().join("mvcc.wal.compacting");
        std::fs::write(&staged, b"partial garbage").unwrap();

        // Reopen: the live WAL must still hold all 5 rows, and the abandoned
        // staging file must be discarded rather than trusted.
        drop(wal);
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        let tbl = state.tables.get("t").expect("table survived");
        assert_eq!(
            tbl.rows.len(),
            5,
            "abandoned compaction lost committed rows"
        );
        assert!(
            !staged.exists(),
            "stale staging file was left behind for a later compaction to trip over"
        );
    }

    /// A completed compaction must be durable and replay to the same state.
    #[test]
    fn completed_compaction_preserves_state_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = MvccWal::open(dir.path()).unwrap();
        wal.log(&MvccWalRecord::CreateTable {
            name: "t".into(),
            columns: vec![("id".into(), DataType::Int64)],
        })
        .unwrap();
        for i in 0..3u32 {
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_idx: i,
                row: vec![Value::Int64(i as i64)],
            })
            .unwrap();
        }
        wal.sync().unwrap();
        let (_, state) = MvccWal::open(dir.path()).unwrap();

        wal.compact(&state).unwrap();
        assert!(
            !dir.path().join("mvcc.wal.compacting").exists(),
            "compaction left its staging file in place"
        );

        drop(wal);
        let (_wal2, after) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(
            after.tables.get("t").map(|t| t.rows.len()),
            Some(3),
            "compaction changed the recovered row set"
        );
    }
}
