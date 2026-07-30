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
use std::path::{Path, PathBuf};

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
    Checkpoint,
}

/// State recovered from replaying the MVCC WAL.
#[derive(Debug, Default)]
pub struct MvccWalState {
    /// Recovered tables: table_name → (columns, rows).
    pub tables: HashMap<String, RecoveredTable>,
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

/// Write one length-prefixed, CRC-suffixed record onto any writer. Shares the
/// exact framing `MvccWal::log` uses so a staged file replays identically.
fn write_framed<W: Write>(w: &mut W, record: &MvccWalRecord) -> io::Result<()> {
    let payload = encode_record(record);
    let crc = crc32c(&payload);
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(&payload)?;
    w.write_all(&crc.to_le_bytes())
}

impl MvccWal {
    /// Open or create the WAL file.  Returns (wal, recovered_state).
    pub fn open(dir: &Path) -> io::Result<(Self, MvccWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("mvcc.wal");
        let state = if path.exists() {
            let mut data = Vec::new();
            File::open(&path)?.read_to_end(&mut data)?;
            replay(&data)
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
        let payload = encode_record(record);
        let crc = crc32c(&payload);
        let len = payload.len() as u32;
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

    /// Log a COMMIT and immediately fsync.
    pub fn log_commit(&self, txn_id: u64) -> io::Result<()> {
        crate::storage::crashpoint::reach("wal.before_commit_record");
        self.log(&MvccWalRecord::Commit { txn_id })?;
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

fn u8_to_datatype(v: u8) -> DataType {
    match v {
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
        _ => DataType::Text,
    }
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

/// Replay WAL data to recover committed state.
fn replay(data: &[u8]) -> MvccWalState {
    let mut pos = 0usize;
    let mut records: Vec<MvccWalRecord> = Vec::new();

    // Phase 1: Parse all records
    while pos + 4 <= data.len() {
        let len =
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        if pos + len + 4 > data.len() {
            break;
        } // truncated
        let payload = &data[pos..pos + len];
        pos += len;
        let stored_crc =
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let computed_crc = crc32c(payload);
        if stored_crc != computed_crc {
            break;
        } // corrupt

        if let Some(rec) = decode_record(payload) {
            records.push(rec);
        } else {
            break;
        }
    }

    // Phase 2: Identify committed transactions
    let mut committed: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut aborted: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for rec in &records {
        match rec {
            MvccWalRecord::Commit { txn_id } => {
                committed.insert(*txn_id);
            }
            MvccWalRecord::Abort { txn_id } => {
                aborted.insert(*txn_id);
            }
            _ => {}
        }
    }

    // Phase 3: Replay committed operations (and auto-commits where txn_id=0).
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

    MvccWalState { tables }
}

fn decode_record(data: &[u8]) -> Option<MvccWalRecord> {
    let mut pos = 0usize;
    let tag = *data.get(pos)?;
    pos += 1;
    match tag {
        TAG_CREATE_TABLE => {
            let name = read_str(data, &mut pos)?;
            let count = read_u32_val(data, &mut pos)? as usize;
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                let col_name = read_str(data, &mut pos)?;
                let col_type = *data.get(pos)?;
                pos += 1;
                columns.push((col_name, u8_to_datatype(col_type)));
            }
            Some(MvccWalRecord::CreateTable { name, columns })
        }
        TAG_DROP_TABLE => {
            let name = read_str(data, &mut pos)?;
            Some(MvccWalRecord::DropTable { name })
        }
        TAG_INSERT => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let version_idx = read_u32_val(data, &mut pos)?;
            let row = crate::storage::value_codec::read_row(data, &mut pos)?;
            Some(MvccWalRecord::Insert {
                table,
                txn_id,
                version_idx,
                row,
            })
        }
        TAG_DELETE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let version_idx = read_u32_val(data, &mut pos)?;
            Some(MvccWalRecord::Delete {
                table,
                txn_id,
                version_idx,
            })
        }
        TAG_UPDATE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let old_version_idx = read_u32_val(data, &mut pos)?;
            let new_version_idx = read_u32_val(data, &mut pos)?;
            let new_row = crate::storage::value_codec::read_row(data, &mut pos)?;
            Some(MvccWalRecord::Update {
                table,
                txn_id,
                old_version_idx,
                new_version_idx,
                new_row,
            })
        }
        TAG_BEGIN => {
            let txn_id = read_u64_val(data, &mut pos)?;
            Some(MvccWalRecord::Begin { txn_id })
        }
        TAG_COMMIT => {
            let txn_id = read_u64_val(data, &mut pos)?;
            Some(MvccWalRecord::Commit { txn_id })
        }
        TAG_ABORT => {
            let txn_id = read_u64_val(data, &mut pos)?;
            Some(MvccWalRecord::Abort { txn_id })
        }
        TAG_CHECKPOINT => Some(MvccWalRecord::Checkpoint),
        _ => None,
    }
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
            wal.log_commit(1).unwrap();
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
            wal.log_commit(1).unwrap();
            drop(wal);
        }

        // Corrupt a byte in the middle of the WAL
        {
            let path = dir.path().join("mvcc.wal");
            let mut data = std::fs::read(&path).unwrap();
            if data.len() > 20 {
                data[20] ^= 0xFF;
            }
            std::fs::write(&path, data).unwrap();
        }

        // Recover — should stop at corrupted record
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        // Depending on which record was corrupted, table may or may not exist
        // but it should NOT panic
        let _ = state.tables.get("t");
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
        assert_eq!(tbl.rows.len(), 5, "abandoned compaction lost committed rows");
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
