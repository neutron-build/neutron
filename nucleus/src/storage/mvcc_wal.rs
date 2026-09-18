//! Write-ahead log for the MVCC storage adapter.
//!
//! Provides crash-safe durability by logging all mutations (DDL + DML)
//! as logical records.  On recovery, committed transactions are replayed
//! in order while aborted/in-flight transactions are skipped.
//!
//! ## Format v2 (current)
//!
//! Every frame carries a versioned, checksummed header so a corrupted
//! length is distinguishable from a genuine crash-torn tail (NU-04
//! remainder):
//! ```text
//! [magic: u32 "NUW2"] [version: u16 = 2] [len: u32]
//! [hcrc: u32 = crc32c(magic|version|len)] [payload: len bytes]
//! [pcrc: u32 = crc32c(payload)]
//! ```
//! A crash mid-append can only ever leave a PREFIX of the header or a
//! truncated payload; a complete 14-byte header whose own CRC fails is
//! therefore damage, not a torn tail, and fails closed. Payloads carry
//! 64-bit stable version ids (never narrowed), lossless type
//! descriptors for parameterized columns, and the atomic cross-model
//! `CommitV2` record (txn + all enlistment ids in one frame, NU-08).
//!
//! ## Format v1 (legacy, read-only)
//!
//! ```text
//! [record_len: u32 LE] [tag: u8] [payload ...] [crc32: u32 LE]
//! ```
//! A v1 log is detected on open (its first four bytes are a length, and
//! `MAGIC` as a length exceeds the replay ceiling), fully replayed, and
//! rewritten as a v2 baseline — the original is preserved as
//! `mvcc.wal.v1` until the next clean v2 open retires it. v1 is never
//! appended to (no dual-format limbo). An old binary reading a v2 log
//! rejects it as an impossible record length and leaves the file
//! untouched: fail-closed in both directions.

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
/// alongside this WAL's own txn id. Written by pre-v2 commits next to the
/// `Commit` record (the two-frame window `CommitV2` closes for new writes)
/// and rewritten by `compact` so the marker set survives log reclaim.
const TAG_XACT_COMMIT: u8 = 0x13;
/// v2 atomic cross-model commit (NU-08): one frame carrying the SQL txn id
/// and every enlisted coordinating id, checksummed as a unit. A crash can
/// no longer leave "SQL committed" durable while the enlistment marker
/// torn — the whole decision is present or absent.
const TAG_COMMIT_V2: u8 = 0x14;
const TAG_CHECKPOINT: u8 = 0x20;

// ── v2 framing constants ─────────────────────────────────────────────────────

/// v2 frame magic. On disk the first four bytes of every v2 log read
/// "NUW2". As a little-endian u32 it is ~844 million — far above the
/// 64 MiB replay ceiling, so it can never be confused with a v1 record
/// length, and a v1-only reader offered a v2 log rejects it as an
/// impossible length instead of misinterpreting it.
const MAGIC: u32 = 0x3257_554E;
/// The frame version this writer emits. A reader that sees a higher
/// version in a valid header refuses the log with an explicit
/// "newer format" error rather than guessing at the layout.
const FORMAT_VERSION: u16 = 2;
/// magic(4) + version(2) + len(4) + hcrc(4).
const V2_HEADER_SIZE: usize = 14;
// ── Public API ───────────────────────────────────────────────────────────────

/// A logical WAL record for the MVCC engine.
#[derive(Debug, Clone)]
pub enum MvccWalRecord {
    CreateTable {
        name: String,
        columns: Vec<(String, DataType)>,
        /// Durable version-id floor for the table (cluster 1): the next id
        /// the writer may mint. Recorded so recovery can validate that the
        /// identity space never rewinds, and so ids from earlier log
        /// generations are never reused for a different row.
        next_version_id: u64,
    },
    DropTable {
        name: String,
    },
    Insert {
        table: String,
        txn_id: u64,
        /// Engine version id assigned to this row — a stable 64-bit durable
        /// identity (cluster 1). Replay keys rows by it so DELETE/UPDATE
        /// address the exact row regardless of scan order, and recovery
        /// rejects a committed INSERT onto an already-live id (id reuse is
        /// a writer bug, not a recoverable state).
        version_id: u64,
        row: Vec<Value>,
    },
    Delete {
        table: String,
        txn_id: u64,
        /// Version id of the deleted row (NOT a scan position).
        version_id: u64,
    },
    Update {
        table: String,
        txn_id: u64,
        /// Version id of the superseded row.
        old_version_id: u64,
        /// Version id of the new row version the engine appended.
        new_version_id: u64,
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
    /// v2 atomic commit (NU-08): `txn_id` committed AND every id in
    /// `xacts` is a committed coordinating transaction — one frame, one
    /// checksum, one fsync decision. Subsumes the `Commit` + `XactCommit`
    /// pair; the pair's two-frame window is closed for new writes.
    CommitV2 {
        txn_id: u64,
        xacts: Vec<u64>,
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
    /// Committed rows with their durable version ids, in id order.
    /// The id is part of the recovered state (cluster 1): reconstruction
    /// re-seats each row at exactly this id so identities survive restarts.
    pub rows: Vec<(u64, Vec<Value>)>,
    /// The next version id the engine may mint for this table — never lower
    /// than every id that ever appeared in this log, live or dead.
    pub next_version_id: u64,
}

/// Append-only WAL for MVCC durability (v2 writer).
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

/// Write one v2 frame onto any writer. Append and compaction share this
/// path (and `encode_checked`), so an appender and a rewritten baseline
/// can never diverge on the framing or size contract (NU-21).
fn write_framed<W: Write>(w: &mut W, record: &MvccWalRecord) -> io::Result<()> {
    let payload = encode_checked(record)?;
    write_v2_frame(w, &payload)
}

/// Lay down one v2 frame: checksummed header (magic, version, length)
/// followed by the payload and its own CRC. The header CRC is what makes
/// a corrupted length provable — see the module docs.
fn write_v2_frame<W: Write>(w: &mut W, payload: &[u8]) -> io::Result<()> {
    let mut header = [0u8; V2_HEADER_SIZE - 4];
    header[..4].copy_from_slice(&MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    header[6..10].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    let hcrc = crc32c(&header);
    w.write_all(&header)?;
    w.write_all(&hcrc.to_le_bytes())?;
    w.write_all(payload)?;
    let pcrc = crc32c(payload);
    w.write_all(&pcrc.to_le_bytes())
}

impl MvccWal {
    /// Open or create the WAL file.  Returns (wal, recovered_state).
    ///
    /// Corruption is fatal (NU-04): a mid-file CRC mismatch, an impossible
    /// or damaged header, or an undecodable record surfaces as
    /// `InvalidData` with the byte offset, and the file is left exactly as
    /// found — repair is an operator decision. A torn FINAL frame (crash
    /// mid-append, nothing durable behind it) is accepted with a warning
    /// and recovers the prefix; a v2 log is truncated to the last valid
    /// frame so appends never land behind a torn tail.
    ///
    /// A legacy (v1) log is replayed in full and immediately rewritten as
    /// a v2 baseline (upgrade-on-open); the original is preserved as
    /// `mvcc.wal.v1` until the next clean v2 open retires it — the engine
    /// has no clean-shutdown hook, so a full clean replay of the upgraded
    /// log is the proof point that the backup is no longer needed.
    pub fn open(dir: &std::path::Path) -> io::Result<(Self, MvccWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("mvcc.wal");
        let mut was_legacy = false;
        let state = if path.exists() {
            let mut data = Vec::new();
            File::open(&path)?.read_to_end(&mut data)?;
            if is_v2(&data) {
                match replay_v2(&data) {
                    Ok((state, ReplayStop::TornTail { at })) => {
                        let recovered: usize = state.tables.values().map(|t| t.rows.len()).sum();
                        eprintln!(
                            "nucleus: MVCC WAL (v2) has a torn final frame at byte {at} \
                             (crash during append); recovering {recovered} committed rows \
                             and truncating the tail"
                        );
                        // Repair now: a later append must never land behind
                        // torn bytes the next replay would stop at. The torn
                        // frame never had a durable decision behind it.
                        let file = OpenOptions::new().write(true).open(&path)?;
                        file.set_len(at as u64)?;
                        file.sync_all()?;
                        state
                    }
                    Ok((state, ReplayStop::CleanEof)) => {
                        // The v2 log replayed cleanly end to end: retire a
                        // leftover upgrade backup, if any. Its v1 original
                        // has now been superseded by a log that provably
                        // opens.
                        let _ = std::fs::remove_file(v1_backup_path(&path));
                        state
                    }
                    Err(msg) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{msg} — the WAL is left unmodified; inspect it before retrying"),
                        ));
                    }
                }
            } else {
                match replay_v1(&data) {
                    Ok((state, _stop)) => {
                        // Legacy log: replay is done, now upgrade. Preserve
                        // the original before anything touches it. A torn v1
                        // tail is the accepted prefix case (`_stop`), and the
                        // rewrite below drops it cleanly.
                        was_legacy = true;
                        let backup = v1_backup_path(&path);
                        if let Err(e) = std::fs::copy(&path, &backup) {
                            return Err(std::io::Error::other(format!(
                                "legacy WAL upgrade: could not preserve the original at {}: {e}",
                                backup.display()
                            )));
                        }
                        let upgraded_rows: usize =
                            state.tables.values().map(|t| t.rows.len()).sum();
                        eprintln!(
                            "nucleus: MVCC WAL is format v1; replayed {upgraded_rows} committed \
                             rows, upgrading to v2 (original preserved at {})",
                            backup.display()
                        );
                        state
                    }
                    Err(msg) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{msg} — the WAL is left unmodified; inspect it before retrying"),
                        ));
                    }
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
        let wal = Self {
            path,
            writer: Mutex::new(BufWriter::new(file)),
            sync: crate::storage::wal_util::WalSync::new(),
        };
        // A legacy log was replayed above; rewrite it as the v2 baseline NOW,
        // inside open, so no caller can end up appending v2 frames after v1
        // bytes (no dual-format limbo). compact() stages + fsyncs + atomically
        // renames; on failure the original v1 log is still intact (and the
        // .v1 backup exists).
        if was_legacy {
            wal.compact(&state)?;
        }
        Ok((wal, state))
    }

    /// Log a record and flush to OS buffer.
    pub fn log(&self, record: &MvccWalRecord) -> io::Result<()> {
        // Size-check before the first byte goes out (NU-21): an accepted
        // record that replay rejects would brick every later open.
        let payload = encode_checked(record)?;
        let mut w = self.writer.lock();
        crate::storage::crashpoint::io_fault_check!("wal.append");
        crate::storage::crashpoint::reach("wal.before_append");
        write_v2_frame(&mut *w, &payload)?;
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

    /// Log a commit decision and immediately fsync (NU-08). With
    /// enlistments this writes ONE `CommitV2` frame — the SQL txn id and
    /// every coordinating id, checksummed as a unit — so a crash between
    /// decision bytes can no longer split "SQL committed" from
    /// "specialty writes keepable". Without enlistments it is the plain
    /// `Commit` frame.
    pub fn log_commit(&self, txn_id: u64, xacts: &[u64]) -> io::Result<()> {
        crate::storage::crashpoint::reach("wal.before_commit_record");
        if xacts.is_empty() {
            self.log(&MvccWalRecord::Commit { txn_id })?;
        } else {
            self.log(&MvccWalRecord::CommitV2 {
                txn_id,
                xacts: xacts.to_vec(),
            })?;
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

    /// Rewrite the WAL as a clean v2 baseline for a recovered state: one
    /// `CreateTable` (with the table's version-id floor) plus auto-committed
    /// `Insert`s — each row at its ORIGINAL durable version id — and the
    /// surviving `XactCommit` markers.
    ///
    /// Preserving ids across the rewrite is what makes the identity space
    /// stable across restarts (cluster 1): a row recovered from an old log
    /// keeps the id it had, and the recorded floor keeps the next minted id
    /// above every id the log ever contained. This is also what un-blocks
    /// in-memory GC compaction (NU-01's deferred half): the WAL speaks
    /// stable ids, not vector positions, so a future compaction can
    /// renumber positions behind an id map without touching the format.
    /// (Compaction itself remains deliberately unimplemented.)
    ///
    /// Called on open right after replay so that the WAL stays compact and
    /// a legacy log is upgraded in the same breath. The caller
    /// reconstructs the engine from the SAME `state` in the same per-table
    /// row order, so the engine's version ids match these baseline records
    /// exactly.
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
                        next_version_id: tbl.next_version_id,
                    },
                )?;
                for (version_id, row) in &tbl.rows {
                    write_framed(
                        &mut w,
                        &MvccWalRecord::Insert {
                            table: name.clone(),
                            txn_id: 0,
                            version_id: *version_id,
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

/// Where the pre-upgrade original of a legacy log is kept.
fn v1_backup_path(path: &std::path::Path) -> PathBuf {
    path.with_extension("wal.v1")
}

/// Does `data` begin with a v2 frame? The magic is far above the v1 replay
/// ceiling, so a v1 record length can never alias it.
fn is_v2(data: &[u8]) -> bool {
    data.len() >= 4 && data[..4] == MAGIC.to_le_bytes()
}

// ── Encoding (v2 payloads) ───────────────────────────────────────────────────

/// Lossless type descriptor (cluster 3 / NU-15): parameterized types carry
/// their parameters. `[code: u8]` alone for simple types; `Vector` appends
/// a u32 dimension, `Array` a recursive descriptor, `UserDefined` the type
/// name. Unknown codes still fail closed on decode.
fn write_type_desc(buf: &mut Vec<u8>, dt: &DataType) {
    match dt {
        DataType::Vector(dim) => {
            buf.push(TYPE_VECTOR);
            write_u32(buf, *dim as u32);
        }
        DataType::Array(inner) => {
            buf.push(TYPE_ARRAY);
            write_type_desc(buf, inner);
        }
        DataType::UserDefined(name) => {
            buf.push(TYPE_USER_DEFINED);
            write_str(buf, name);
        }
        other => buf.push(datatype_to_u8(other)),
    }
}

fn read_type_desc(data: &[u8], pos: &mut usize) -> Option<DataType> {
    let code = *data.get(*pos)?;
    *pos += 1;
    let dt = match code {
        TYPE_VECTOR => {
            let dim = read_u32_val(data, pos)?;
            DataType::Vector(dim as usize)
        }
        TYPE_ARRAY => {
            let inner = read_type_desc(data, pos)?;
            DataType::Array(Box::new(inner))
        }
        TYPE_USER_DEFINED => {
            let name = read_str(data, pos)?;
            DataType::UserDefined(name)
        }
        code => u8_to_datatype(code)?,
    };
    Some(dt)
}

fn encode_record(rec: &MvccWalRecord) -> Vec<u8> {
    let mut buf = Vec::new();
    match rec {
        MvccWalRecord::CreateTable {
            name,
            columns,
            next_version_id,
        } => {
            buf.push(TAG_CREATE_TABLE);
            write_str(&mut buf, name);
            write_u32(&mut buf, columns.len() as u32);
            for (col_name, col_type) in columns {
                write_str(&mut buf, col_name);
                write_type_desc(&mut buf, col_type);
            }
            write_u64(&mut buf, *next_version_id);
        }
        MvccWalRecord::DropTable { name } => {
            buf.push(TAG_DROP_TABLE);
            write_str(&mut buf, name);
        }
        MvccWalRecord::Insert {
            table,
            txn_id,
            version_id,
            row,
        } => {
            buf.push(TAG_INSERT);
            write_str(&mut buf, table);
            write_u64(&mut buf, *txn_id);
            write_u64(&mut buf, *version_id);
            crate::storage::value_codec::write_row(&mut buf, row);
        }
        MvccWalRecord::Delete {
            table,
            txn_id,
            version_id,
        } => {
            buf.push(TAG_DELETE);
            write_str(&mut buf, table);
            write_u64(&mut buf, *txn_id);
            write_u64(&mut buf, *version_id);
        }
        MvccWalRecord::Update {
            table,
            txn_id,
            old_version_id,
            new_version_id,
            new_row,
        } => {
            buf.push(TAG_UPDATE);
            write_str(&mut buf, table);
            write_u64(&mut buf, *txn_id);
            write_u64(&mut buf, *old_version_id);
            write_u64(&mut buf, *new_version_id);
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
        MvccWalRecord::CommitV2 { txn_id, xacts } => {
            buf.push(TAG_COMMIT_V2);
            write_u64(&mut buf, *txn_id);
            write_u32(&mut buf, xacts.len() as u32);
            for xact in xacts {
                write_u64(&mut buf, *xact);
            }
        }
        MvccWalRecord::Checkpoint => {
            buf.push(TAG_CHECKPOINT);
        }
    }
    buf
}

// ── Primitive helpers ────────────────────────────────────────────────────────

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

const TYPE_BOOL: u8 = 0;
const TYPE_INT32: u8 = 1;
const TYPE_INT64: u8 = 2;
const TYPE_FLOAT64: u8 = 3;
const TYPE_TEXT: u8 = 4;
const TYPE_BYTEA: u8 = 5;
const TYPE_NUMERIC: u8 = 6;
const TYPE_UUID: u8 = 7;
const TYPE_DATE: u8 = 8;
const TYPE_TIMESTAMP: u8 = 9;
const TYPE_TIMESTAMPTZ: u8 = 10;
const TYPE_INTERVAL: u8 = 11;
const TYPE_JSONB: u8 = 12;
const TYPE_VECTOR: u8 = 13;
const TYPE_ARRAY: u8 = 14;
const TYPE_USER_DEFINED: u8 = 15;

fn datatype_to_u8(dt: &DataType) -> u8 {
    match dt {
        DataType::Bool => TYPE_BOOL,
        DataType::Int32 => TYPE_INT32,
        DataType::Int64 => TYPE_INT64,
        DataType::Float64 => TYPE_FLOAT64,
        DataType::Text => TYPE_TEXT,
        DataType::Bytea => TYPE_BYTEA,
        DataType::Numeric => TYPE_NUMERIC,
        DataType::Uuid => TYPE_UUID,
        DataType::Date => TYPE_DATE,
        DataType::Timestamp => TYPE_TIMESTAMP,
        DataType::TimestampTz => TYPE_TIMESTAMPTZ,
        DataType::Interval => TYPE_INTERVAL,
        DataType::Jsonb => TYPE_JSONB,
        DataType::Vector(_) => TYPE_VECTOR,
        DataType::Array(_) => TYPE_ARRAY,
        DataType::UserDefined(_) => TYPE_USER_DEFINED,
    }
}

/// Decode a schema type code. Unknown codes return None (NU-15): silently
/// substituting TEXT for a corrupt/unknown type code reconstructed a
/// DIFFERENT schema than the one that was written. Parameterized codes
/// (13/14/15) WITHOUT their parameters decode only on the legacy path —
/// here they are errors, because a bare 13/14/15 in a v2 payload is a
/// malformed descriptor.
fn u8_to_datatype(v: u8) -> Option<DataType> {
    let dt = match v {
        TYPE_BOOL => DataType::Bool,
        TYPE_INT32 => DataType::Int32,
        TYPE_INT64 => DataType::Int64,
        TYPE_FLOAT64 => DataType::Float64,
        TYPE_TEXT => DataType::Text,
        TYPE_BYTEA => DataType::Bytea,
        TYPE_NUMERIC => DataType::Numeric,
        TYPE_UUID => DataType::Uuid,
        TYPE_DATE => DataType::Date,
        TYPE_TIMESTAMP => DataType::Timestamp,
        TYPE_TIMESTAMPTZ => DataType::TimestampTz,
        TYPE_INTERVAL => DataType::Interval,
        TYPE_JSONB => DataType::Jsonb,
        _ => return None,
    };
    Some(dt)
}

/// Legacy one-byte decode (v1 logs only): parameterized types come back
/// with their parameters defaulted — the information was never recorded.
/// This is the recorded NU-15 loss for pre-v2 logs; v2 descriptors are
/// lossless.
fn u8_to_datatype_legacy(v: u8) -> Option<DataType> {
    let dt = match v {
        TYPE_VECTOR => DataType::Vector(0),
        TYPE_ARRAY => DataType::Array(Box::new(DataType::Text)),
        TYPE_USER_DEFINED => DataType::UserDefined(String::new()),
        v => u8_to_datatype(v)?,
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
    /// Everything before it parsed and is recovered; v2 open truncates the
    /// torn frame away. This is the explicit torn-tail policy (NU-04):
    /// accepted, because the frame never had a durable commit decision
    /// behind it.
    TornTail { at: usize },
}

/// Detect + dispatch (used by binary probes; open() dispatches inline so
/// each arm can apply its own torn-tail policy).
#[allow(dead_code)]
fn replay(data: &[u8]) -> Result<(MvccWalState, ReplayStop), String> {
    if is_v2(data) {
        replay_v2(data)
    } else {
        replay_v1(data)
    }
}

/// Replay a v2 log.
fn replay_v2(data: &[u8]) -> Result<(MvccWalState, ReplayStop), String> {
    let mut pos = 0usize;
    let mut records: Vec<MvccWalRecord> = Vec::new();

    loop {
        if pos == data.len() {
            return finish_replay(records, ReplayStop::CleanEof);
        }
        let frame_start = pos;
        // A crash leaves a prefix; fewer bytes than a full header is a torn
        // tail, whatever their contents.
        let Some(header) = data.get(pos..pos + V2_HEADER_SIZE) else {
            return finish_replay(records, ReplayStop::TornTail { at: frame_start });
        };
        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let version = u16::from_le_bytes([header[4], header[5]]);
        let len = u32::from_le_bytes([header[6], header[7], header[8], header[9]]) as usize;
        let stored_hcrc =
            u32::from_le_bytes([header[10], header[11], header[12], header[13]]);
        if magic != MAGIC {
            // A v1 frame inside a v2 log (or garbage). Neither is a crash
            // artifact: crash truncates, it does not substitute bytes.
            return Err(format!(
                "MVCC WAL corruption: non-v2 frame at byte {frame_start} inside a v2 log \
                 (mixed-format logs are not appendable)"
            ));
        }
        if version != FORMAT_VERSION {
            return Err(format!(
                "MVCC WAL: frame at byte {frame_start} has format version {version}; \
                 this reader supports up to {FORMAT_VERSION} — the log was written by a \
                 newer engine"
            ));
        }
        let computed_hcrc = crc32c(&header[..V2_HEADER_SIZE - 4]);
        if stored_hcrc != computed_hcrc {
            // The header is COMPLETE on disk (14 bytes exist) but its own
            // checksum fails. A crash mid-write cannot produce this — the
            // hcrc bytes are the last thing the append writes before the
            // payload, and a torn append leaves a SHORT header, handled
            // above. This is the NU-04 remainder closed: a damaged length
            // (whatever value it took, plausible or not) is corruption,
            // distinguishable from a genuine torn tail.
            return Err(format!(
                "MVCC WAL corruption: header CRC mismatch at byte {frame_start} \
                 (stored {stored_hcrc:#x}, computed {computed_hcrc:#x}) — a complete \
                 header with a bad checksum is damage, not a torn tail"
            ));
        }
        if len == 0 || len > MAX_PAYLOAD {
            return Err(format!(
                "MVCC WAL corruption: impossible payload length {len} at byte {frame_start} \
                 with a valid header CRC"
            ));
        }
        let payload_end = frame_start + V2_HEADER_SIZE + len;
        let frame_end = payload_end + 4;
        if frame_end > data.len() {
            // Valid header, physically incomplete payload: the ordinary
            // crash-torn tail.
            return finish_replay(records, ReplayStop::TornTail { at: frame_start });
        }
        let payload = &data[frame_start + V2_HEADER_SIZE..payload_end];
        let crc_bytes = &data[payload_end..frame_end];
        let stored_pcrc =
            u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        let computed_pcrc = crc32c(payload);
        if stored_pcrc != computed_pcrc {
            return Err(format!(
                "MVCC WAL corruption: payload CRC mismatch at byte {frame_start} \
                 (stored {stored_pcrc:#x}, computed {computed_pcrc:#x})"
            ));
        }

        match decode_record_v2(payload) {
            Some(rec) => records.push(rec),
            None => {
                return Err(format!(
                    "MVCC WAL corruption: undecodable record (tag {:#x}) at byte {frame_start}",
                    payload.first().copied().unwrap_or(0)
                ));
            }
        }
        pos = frame_end;
    }
}

/// Replay a legacy (v1) log. Read-only path kept for upgrade-on-open; the
/// writer never emits v1 frames.
fn replay_v1(data: &[u8]) -> Result<(MvccWalState, ReplayStop), String> {
    let mut pos = 0usize;
    let mut records: Vec<MvccWalRecord> = Vec::new();

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
            // This includes a v2 log offered to the v1 path: the magic reads
            // as a ~844 MB length, far above the ceiling — the old reader's
            // honest, fail-closed rejection.
            return Err(format!(
                "MVCC WAL corruption: impossible record length {len} at byte {frame_start}{}",
                if len == MAGIC as usize {
                    " (the value is the v2 frame magic — this log is format v2, \
                     written by a newer engine)"
                } else {
                    ""
                }
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

        match decode_record_v1(payload) {
            Some(rec) => records.push(rec),
            None => {
                return Err(format!(
                    "MVCC WAL corruption: undecodable record (tag {:#x}) at byte {frame_start}{}",
                    payload.first().copied().unwrap_or(0),
                    if payload.first() == Some(&TAG_COMMIT_V2) {
                        " (tag 0x14 is CommitV2 — this record was written by a \
                         v2-format engine and cannot be read by a v1 decoder)"
                    } else {
                        ""
                    }
                ));
            }
        }
    }
}

/// Phase 2+3 of replay, shared by both format readers.
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
            MvccWalRecord::CommitV2 { txn_id, xacts } => {
                record_decision(&mut decisions, *txn_id, Decision::Commit)?;
                committed_xacts.extend(xacts.iter().copied());
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
    // Rows are keyed by the engine's stable per-row VERSION ID, so DELETE and
    // UPDATE address the exact row by identity — no fragile scan-position
    // arithmetic. A BTreeMap keeps rows in id order (the scan order); the
    // final ordering is irrelevant to callers, which re-sort, but it is
    // deterministic. An uncommitted transaction's records are simply never
    // applied, so its writes are rolled back on recovery.
    //
    // ID-space validation (cluster 1): a committed INSERT onto an id that is
    // currently LIVE is corruption — the writer minted (or reused) an id
    // already assigned to a different live row. Re-INSERTING a dead id is
    // legal (savepoint-rollback compensation resurrects deleted versions).
    let mut columns: HashMap<String, Vec<(String, DataType)>> = HashMap::new();
    let mut rowmaps: HashMap<String, std::collections::BTreeMap<u64, Vec<Value>>> = HashMap::new();
    let mut floors: HashMap<String, u64> = HashMap::new();

    let observe_id = |floors: &mut HashMap<String, u64>, table: &str, id: u64| {
        let floor = floors.entry(table.to_string()).or_insert(0);
        if id >= *floor {
            *floor = id + 1;
        }
    };

    for rec in &records {
        let committed_rec = |txn_id: &u64| *txn_id == 0 || committed.contains(txn_id);
        match rec {
            MvccWalRecord::CreateTable {
                name,
                columns: cols,
                next_version_id,
            } => {
                columns.insert(name.clone(), cols.clone());
                rowmaps.insert(name.clone(), std::collections::BTreeMap::new());
                floors.insert(name.clone(), *next_version_id);
            }
            MvccWalRecord::DropTable { name } => {
                columns.remove(name);
                rowmaps.remove(name);
                floors.remove(name);
            }
            MvccWalRecord::Insert {
                table,
                txn_id,
                version_id,
                row,
            } => {
                observe_id(&mut floors, table, *version_id);
                if committed_rec(txn_id)
                    && let Some(m) = rowmaps.get_mut(table)
                {
                    if m.contains_key(version_id) {
                        return Err(format!(
                            "MVCC WAL corruption: committed INSERT reuses live version id \
                             {version_id} in table {table} — the durable identity space was \
                             reused for two live rows"
                        ));
                    }
                    m.insert(*version_id, row.clone());
                }
            }
            MvccWalRecord::Delete {
                table,
                txn_id,
                version_id,
            } => {
                observe_id(&mut floors, table, *version_id);
                if committed_rec(txn_id)
                    && let Some(m) = rowmaps.get_mut(table)
                {
                    m.remove(version_id);
                }
            }
            MvccWalRecord::Update {
                table,
                txn_id,
                old_version_id,
                new_version_id,
                new_row,
            } => {
                observe_id(&mut floors, table, *old_version_id);
                observe_id(&mut floors, table, *new_version_id);
                if committed_rec(txn_id)
                    && let Some(m) = rowmaps.get_mut(table)
                {
                    m.remove(old_version_id);
                    if m.contains_key(new_version_id) {
                        return Err(format!(
                            "MVCC WAL corruption: committed UPDATE targets live version id \
                             {new_version_id} in table {table} — the durable identity space \
                             was reused for two live rows"
                        ));
                    }
                    m.insert(*new_version_id, new_row.clone());
                }
            }
            MvccWalRecord::Checkpoint => {
                // After a checkpoint, previous records can be ignored.
                // In a future version, truncate records before the checkpoint.
            }
            _ => {} // Begin, Commit, Abort, XactCommit handled above
        }
    }

    let tables: HashMap<String, RecoveredTable> = columns
        .into_iter()
        .map(|(name, cols)| {
            let (rows, next_version_id) = match rowmaps.remove(&name) {
                Some(m) => {
                    let floor = floors.get(&name).copied().unwrap_or(0);
                    let rows: Vec<(u64, Vec<Value>)> = m.into_iter().collect();
                    (rows, floor)
                }
                None => (
                    Vec::new(),
                    floors.get(&name).copied().unwrap_or(0),
                ),
            };
            (
                name,
                RecoveredTable {
                    columns: cols,
                    rows,
                    next_version_id,
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

/// Decode a v2 payload to a record, or None when it is not a well-formed
/// record of a KNOWN shape (NU-04/NU-15): unknown tags, unknown type codes,
/// and trailing undecoded bytes are all rejected rather than best-effort
/// coerced.
fn decode_record_v2(data: &[u8]) -> Option<MvccWalRecord> {
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
                let col_type = read_type_desc(data, &mut pos)?;
                columns.push((col_name, col_type));
            }
            let next_version_id = read_u64_val(data, &mut pos)?;
            MvccWalRecord::CreateTable {
                name,
                columns,
                next_version_id,
            }
        }
        TAG_DROP_TABLE => {
            let name = read_str(data, &mut pos)?;
            MvccWalRecord::DropTable { name }
        }
        TAG_INSERT => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let version_id = read_u64_val(data, &mut pos)?;
            let row = crate::storage::value_codec::read_row(data, &mut pos)?;
            MvccWalRecord::Insert {
                table,
                txn_id,
                version_id,
                row,
            }
        }
        TAG_DELETE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let version_id = read_u64_val(data, &mut pos)?;
            MvccWalRecord::Delete {
                table,
                txn_id,
                version_id,
            }
        }
        TAG_UPDATE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let old_version_id = read_u64_val(data, &mut pos)?;
            let new_version_id = read_u64_val(data, &mut pos)?;
            let new_row = crate::storage::value_codec::read_row(data, &mut pos)?;
            MvccWalRecord::Update {
                table,
                txn_id,
                old_version_id,
                new_version_id,
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
        TAG_COMMIT_V2 => {
            let txn_id = read_u64_val(data, &mut pos)?;
            let count = read_u32_val(data, &mut pos)? as usize;
            let mut xacts = Vec::with_capacity(super::wal_util::bounded_capacity(count));
            for _ in 0..count {
                xacts.push(read_u64_val(data, &mut pos)?);
            }
            MvccWalRecord::CommitV2 { txn_id, xacts }
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

/// Decode a v1 payload (frozen legacy layouts): u32 version indices widened
/// to the u64 id space, one-byte schema codes with parameterized types
/// defaulted (the recorded NU-15 loss — the parameters were never written),
/// no `CreateTable` floor, no `CommitV2` (tag 0x14 fails closed with an
/// explicit newer-writer message).
fn decode_record_v1(data: &[u8]) -> Option<MvccWalRecord> {
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
                columns.push((col_name, u8_to_datatype_legacy(col_type)?));
            }
            MvccWalRecord::CreateTable {
                name,
                columns,
                next_version_id: 0,
            }
        }
        TAG_DROP_TABLE => {
            let name = read_str(data, &mut pos)?;
            MvccWalRecord::DropTable { name }
        }
        TAG_INSERT => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let version_id = read_u32_val(data, &mut pos)? as u64;
            let row = crate::storage::value_codec::read_row(data, &mut pos)?;
            MvccWalRecord::Insert {
                table,
                txn_id,
                version_id,
                row,
            }
        }
        TAG_DELETE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let version_id = read_u32_val(data, &mut pos)? as u64;
            MvccWalRecord::Delete {
                table,
                txn_id,
                version_id,
            }
        }
        TAG_UPDATE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let old_version_id = read_u32_val(data, &mut pos)? as u64;
            let new_version_id = read_u32_val(data, &mut pos)? as u64;
            let new_row = crate::storage::value_codec::read_row(data, &mut pos)?;
            MvccWalRecord::Update {
                table,
                txn_id,
                old_version_id,
                new_version_id,
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
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "users".into(),
                txn_id: 1,
                version_id: 0,
                row: vec![Value::Int64(1), Value::Text("Alice".into())],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "users".into(),
                txn_id: 1,
                version_id: 1,
                row: vec![Value::Int64(2), Value::Text("Bob".into())],
            })
            .unwrap();
            wal.log_commit(1, &[]).unwrap();
            drop(wal);
        }

        // Phase 2: Recover
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        let users = state.tables.get("users").unwrap();
        assert_eq!(users.rows.len(), 2);
        assert_eq!(users.rows[0].0, 0);
        assert_eq!(users.rows[0].1[1], Value::Text("Alice".into()));
        assert_eq!(users.rows[1].0, 1);
        assert_eq!(users.rows[1].1[1], Value::Text("Bob".into()));
    }

    #[test]
    fn test_aborted_txn_not_recovered() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int32)],
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_id: 0,
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
                next_version_id: 0,
            })
            .unwrap();
            // Begin but never commit/abort
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_id: 0,
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
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_id: 0,
                row: vec![Value::Int32(99)],
            })
            .unwrap();
            wal.log_commit(1, &[]).unwrap();
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
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: 0,
                row: vec![Value::Int64(1)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: 1,
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
        assert_eq!(t.rows[0].1, vec![Value::Int64(1)]);
    }

    #[test]
    fn test_drop_table_removes_data() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "temp".into(),
                columns: vec![("x".into(), DataType::Int32)],
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "temp".into(),
                txn_id: 0,
                version_id: 0,
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
            next_version_id: 0,
        })
        .unwrap();
        wal.sync().unwrap();
        let before = std::fs::read(dir.path().join("mvcc.wal")).unwrap();

        let oversized = MvccWalRecord::Insert {
            table: "t".into(),
            txn_id: 0,
            version_id: 0,
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
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_id: 0,
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
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_id: 0,
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
                next_version_id: 0,
            })
            .unwrap();
            wal.log_commit(7, &[42]).unwrap();
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
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_id: 0,
                row: vec![Value::Int64(5)],
            })
            .unwrap();
            wal.log_commit(1, &[9]).unwrap();
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
            next_version_id: 0,
        })
        .unwrap();
        for i in 0..5u64 {
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: i,
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
            next_version_id: 0,
        })
        .unwrap();
        for i in 0..3u64 {
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: i,
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

// ── v2 framing, schema codec, and identity-space tests ──────────────────────

#[cfg(test)]
mod format_v2_tests {
    use super::*;

    fn write_samples(wal: &MvccWal) {
        wal.log(&MvccWalRecord::CreateTable {
            name: "t".into(),
            columns: vec![("x".into(), DataType::Int64)],
            next_version_id: 0,
        })
        .unwrap();
        for i in 0..3u64 {
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: i,
                row: vec![Value::Int64(i as i64)],
            })
            .unwrap();
        }
        wal.sync().unwrap();
    }

    /// The written file is v2: magic first, and every writer frame carries
    /// the format version.
    #[test]
    fn writer_emits_v2_frames() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            write_samples(&wal);
        }
        let data = std::fs::read(dir.path().join("mvcc.wal")).unwrap();
        assert!(is_v2(&data), "file must start with the v2 magic");
        let version = u16::from_le_bytes([data[4], data[5]]);
        assert_eq!(version, FORMAT_VERSION);
    }

    /// A corrupted-but-plausible LENGTH extending past EOF was v1's
    /// unresolvable case (classified torn tail). In v2 the length lives in a
    /// checksummed header, so the same damage is provably corruption — even
    /// in the FINAL frame — and fails closed with the file untouched.
    /// This is the cluster-4 / NU-04-remainder fix.
    #[test]
    fn corrupted_length_is_corruption_not_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            write_samples(&wal);
        }
        let path = dir.path().join("mvcc.wal");
        let mut data = std::fs::read(&path).unwrap();
        // Flip a bit in the FINAL frame's length field (header offset 6).
        // The frame's declared length now disagrees with its header CRC.
        // Locate the final frame precisely: walk frames like replay does.
        let mut pos = 0usize;
        let mut last_start = 0usize;
        while pos + V2_HEADER_SIZE <= data.len() {
            let len =
                u32::from_le_bytes([data[pos + 6], data[pos + 7], data[pos + 8], data[pos + 9]])
                    as usize;
            last_start = pos;
            pos += V2_HEADER_SIZE + len + 4;
        }
        data[last_start + 6] ^= 0x08;
        std::fs::write(&path, &data).unwrap();

        let result = MvccWal::open(dir.path());
        match result {
            Err(err) => {
                assert!(
                    err.to_string().contains("header CRC mismatch"),
                    "error must name the header CRC: {err}"
                );
                // Fail-closed: the damaged file is left untouched.
                assert_eq!(std::fs::read(&path).unwrap(), data);
            }
            Ok(_) => panic!("corrupted length was classified as a torn tail"),
        }
    }

    /// A mid-file frame with a damaged header is corruption, and recovery
    /// refuses rather than recovering a prefix behind the damage.
    #[test]
    fn midfile_header_damage_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            write_samples(&wal);
        }
        let path = dir.path().join("mvcc.wal");
        let mut data = std::fs::read(&path).unwrap();
        // First frame starts at 0; the second frame's header begins right
        // after it. Damage the second frame's magic byte.
        let first_len =
            u32::from_le_bytes([data[6], data[7], data[8], data[9]]) as usize;
        let second = V2_HEADER_SIZE + first_len + 4;
        data[second] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = MvccWal::open(dir.path());
        match result {
            Err(err) => assert!(
                err.to_string().contains("non-v2 frame"),
                "error must name the mixed/damaged frame: {err}"
            ),
            Ok(_) => panic!("damaged interior frame was accepted"),
        }
    }

    /// A frame version above this reader's is refused with an explicit
    /// newer-format error, not a guess at the layout.
    #[test]
    fn newer_format_version_is_rejected_explicitly() {
        let mut payload = vec![TAG_CHECKPOINT];
        let mut buf = Vec::new();
        let mut header = [0u8; V2_HEADER_SIZE - 4];
        header[..4].copy_from_slice(&MAGIC.to_le_bytes());
        header[4..6].copy_from_slice(&99u16.to_le_bytes()); // future version
        header[6..10].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        std::io::Write::write_all(&mut buf, &header).unwrap();
        let hcrc = crc32c(&header);
        std::io::Write::write_all(&mut buf, &hcrc.to_le_bytes()).unwrap();
        payload.clear();
        let pcrc = crc32c(&payload);
        std::io::Write::write_all(&mut buf, &payload).unwrap();
        std::io::Write::write_all(&mut buf, &pcrc.to_le_bytes()).unwrap();

        let err = replay_v2(&buf).unwrap_err();
        assert!(
            err.contains("format version 99"),
            "error must name the version: {err}"
        );
    }

    /// The v1 reader's honest rejection of a v2 log: the magic parses as a
    /// length far above the ceiling. The error names the situation.
    #[test]
    fn legacy_reader_rejects_v2_log_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            write_samples(&wal);
        }
        let data = std::fs::read(dir.path().join("mvcc.wal")).unwrap();
        let err = replay_v1(&data).unwrap_err();
        assert!(
            err.contains("impossible record length")
                && err.contains("format v2"),
            "error must name the impossible length and the v2 magic: {err}"
        );
    }

    /// A v1 frame appended after v2 frames is a mixed-format log: refused,
    /// never best-effort parsed.
    #[test]
    fn mixed_format_log_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            write_samples(&wal);
        }
        let path = dir.path().join("mvcc.wal");
        let mut data = std::fs::read(&path).unwrap();
        // A syntactically valid v1 frame (a Commit record) — at 17 bytes it
        // is longer than a v2 header, so the magic check engages rather
        // than the torn-tail path.
        let v1_payload = {
            let mut p = vec![TAG_COMMIT];
            p.extend_from_slice(&7u64.to_le_bytes());
            p
        };
        let crc = crc32c(&v1_payload);
        data.extend_from_slice(&(v1_payload.len() as u32).to_le_bytes());
        data.extend_from_slice(&v1_payload);
        data.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let result = MvccWal::open(dir.path());
        match result {
            Err(err) => assert!(
                err.to_string().contains("non-v2 frame"),
                "error must name the mixed format: {err}"
            ),
            Ok(_) => panic!("mixed-format log was accepted"),
        }
    }

    /// A v2 torn tail is truncated at open, so appends never land behind
    /// bytes the next replay would stop at.
    #[test]
    fn v2_torn_tail_is_repaired_at_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            write_samples(&wal);
        }
        let path = dir.path().join("mvcc.wal");
        let full = std::fs::read(&path).unwrap();
        // Walk to the start of the last frame, keep everything before it,
        // plus a few header bytes of the last frame.
        let mut pos = 0usize;
        let mut last_start = 0usize;
        while pos + V2_HEADER_SIZE <= full.len() {
            let len = u32::from_le_bytes([
                full[pos + 6],
                full[pos + 7],
                full[pos + 8],
                full[pos + 9],
            ]) as usize;
            last_start = pos;
            pos += V2_HEADER_SIZE + len + 4;
        }
        std::fs::write(&path, &full[..last_start + 7]).unwrap();

        let (wal, state) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(state.tables.get("t").map(|t| t.rows.len()), Some(2));
        // The file was truncated back to the valid prefix: appending works
        // and a second open replays everything.
        wal.log(&MvccWalRecord::Insert {
            table: "t".into(),
            txn_id: 0,
            version_id: 2,
            row: vec![Value::Int64(2)],
        })
        .unwrap();
        wal.sync().unwrap();
        drop(wal);
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(state.tables.get("t").map(|t| t.rows.len()), Some(3));
    }
}

#[cfg(test)]
mod commit_v2_tests {
    use super::*;

    /// CommitV2 is ONE frame carrying the txn and every enlistment id, and
    /// replay recovers both halves from it (cluster 2 / NU-08).
    #[test]
    fn commit_v2_is_one_atomic_frame() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 5 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 5,
                version_id: 0,
                row: vec![Value::Int64(1)],
            })
            .unwrap();
            wal.log_commit(5, &[9, 10]).unwrap();
            drop(wal);
        }
        // Exactly ONE frame was appended for the decision: count frames.
        let data = std::fs::read(dir.path().join("mvcc.wal")).unwrap();
        let mut pos = 0usize;
        let mut frames = 0usize;
        let mut saw_commit_v2 = false;
        while pos < data.len() {
            let len = u32::from_le_bytes([
                data[pos + 6],
                data[pos + 7],
                data[pos + 8],
                data[pos + 9],
            ]) as usize;
            let payload = &data[pos + V2_HEADER_SIZE..pos + V2_HEADER_SIZE + len];
            if payload.first() == Some(&TAG_COMMIT_V2) {
                saw_commit_v2 = true;
            }
            pos += V2_HEADER_SIZE + len + 4;
            frames += 1;
        }
        // CreateTable + Begin + Insert + CommitV2 = 4 frames, one decision.
        assert_eq!(frames, 4, "the decision must be a single frame");
        assert!(saw_commit_v2);

        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(state.tables.get("t").map(|t| t.rows.len()), Some(1));
        assert!(
            state.committed_xacts.contains(&9) && state.committed_xacts.contains(&10),
            "CommitV2 must recover every enlistment id"
        );
    }

    /// The closed two-record window: a torn commit decision leaves the
    /// transaction AND its enlistments entirely absent. Under v1's
    /// Commit + XactCommit pair, a crash between the records could leave
    /// "SQL committed" durable while the specialty filter lost its marker.
    #[test]
    fn torn_commit_v2_leaves_txn_and_enlistments_absent() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 5 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 5,
                version_id: 0,
                row: vec![Value::Int64(1)],
            })
            .unwrap();
            wal.sync().unwrap();
            // Append the CommitV2 frame's bytes by hand, then cut it short.
            let payload = encode_record(&MvccWalRecord::CommitV2 {
                txn_id: 5,
                xacts: vec![9],
            });
            let mut frame = Vec::new();
            write_v2_frame(&mut frame, &payload).unwrap();
            let path = dir.path().join("mvcc.wal");
            let mut data = std::fs::read(&path).unwrap();
            data.extend_from_slice(&frame[..frame.len() - 5]); // torn mid-payload
            std::fs::write(&path, &data).unwrap();
        }
        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(
            state.tables.get("t").map(|t| t.rows.len()),
            Some(0),
            "a torn decision frame must leave the txn uncommitted"
        );
        assert!(
            !state.committed_xacts.contains(&9),
            "a torn decision frame must leave the enlistments unproven — the \
             two-record window is closed because there is only one record"
        );
    }

    /// A plain Commit (no enlistments) still round-trips.
    #[test]
    fn plain_commit_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
                next_version_id: 0,
            })
            .unwrap();
            wal.log_commit(3, &[]).unwrap();
        }
        let (_wal, _state) = MvccWal::open(dir.path()).unwrap();
        // Reaching here means the plain Commit frame replayed cleanly.
    }

    /// The v1 decoder names tag 0x14 as a newer-writer record rather than a
    /// generic undecodable tag — an operator can tell format skew from
    /// random corruption (old-reader rejection policy, cluster 2).
    #[test]
    fn v1_decoder_names_commit_v2_explicitly() {
        // Hand-craft a v1 frame whose payload is tag 0x14 + txn id.
        let payload = {
            let mut p = vec![TAG_COMMIT_V2];
            p.extend_from_slice(&7u64.to_le_bytes());
            p
        };
        let crc = crc32c(&payload);
        let mut frame = Vec::new();
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&crc.to_le_bytes());

        let err = replay_v1(&frame).unwrap_err();
        assert!(
            err.contains("CommitV2"),
            "error must name CommitV2: {err}"
        );
    }
}

#[cfg(test)]
mod schema_codec_tests {
    use super::*;

    /// Lossless parameterized type descriptors (cluster 3 / NU-15): vector
    /// dims, array element types (recursively), and UDT names round-trip
    /// exactly through a write-replay cycle.
    #[test]
    fn parameterized_schema_roundtrips_losslessly() {
        let dir = tempfile::tempdir().unwrap();
        let columns = vec![
            ("id".into(), DataType::Int64),
            (
                "nested".into(),
                DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int32)))),
            ),
            ("emb".into(), DataType::Vector(1536)),
            ("mood".into(), DataType::UserDefined("mood".into())),
            ("plain".into(), DataType::TimestampTz),
        ];
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: columns.clone(),
                next_version_id: 0,
            })
            .unwrap();
            wal.sync().unwrap();
        }
        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").expect("table survived");
        assert_eq!(t.columns, columns, "schema must round-trip losslessly");
    }

    /// The descriptor codec itself: every parameterized shape, including
    /// nesting, encodes and decodes to the identical type.
    #[test]
    fn type_descriptors_roundtrip() {
        for dt in [
            DataType::Bool,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Text,
            DataType::Bytea,
            DataType::Numeric,
            DataType::Uuid,
            DataType::Date,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Interval,
            DataType::Jsonb,
            DataType::Vector(0),
            DataType::Vector(7),
            DataType::Array(Box::new(DataType::Text)),
            DataType::Array(Box::new(DataType::Array(Box::new(
                DataType::Vector(3),
            )))),
            DataType::UserDefined("color".into()),
        ] {
            let mut buf = Vec::new();
            write_type_desc(&mut buf, &dt);
            let mut pos = 0;
            let out = read_type_desc(&buf, &mut pos).expect("decodes");
            assert_eq!(pos, buf.len(), "descriptor consumed exactly");
            assert_eq!(out, dt, "descriptor did not round-trip: {dt:?}");
        }
    }

    /// Unknown type codes still fail closed (NU-15): no silent TEXT.
    #[test]
    fn unknown_type_code_fails_closed() {
        assert!(read_type_desc(&[200u8], &mut 0).is_none());
        // Bare parameterized codes without their parameters are malformed
        // in v2 descriptors.
        assert!(read_type_desc(&[TYPE_VECTOR], &mut 0).is_none());
        assert!(read_type_desc(&[TYPE_ARRAY], &mut 0).is_none());
        assert!(read_type_desc(&[TYPE_USER_DEFINED], &mut 0).is_none());
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    /// Cluster 1: a committed INSERT that reuses a LIVE version id is
    /// corruption — the durable identity space was double-assigned.
    #[test]
    fn committed_insert_reusing_live_id_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: 4,
                row: vec![Value::Int64(1)],
            })
            .unwrap();
            // Same id, still live, committed again: a mint-past-floor bug.
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: 4,
                row: vec![Value::Int64(2)],
            })
            .unwrap();
            wal.sync().unwrap();
        }
        let result = MvccWal::open(dir.path());
        match result {
            Err(err) => assert!(
                err.to_string().contains("reuses live version id"),
                "error must name the identity reuse: {err}"
            ),
            Ok(_) => panic!("live version-id reuse was accepted"),
        }
    }

    /// Re-INSERTING a dead id is legal: savepoint-rollback compensation
    /// resurrects deleted versions by id.
    #[test]
    fn insert_of_dead_id_is_resurrection_not_corruption() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: 4,
                row: vec![Value::Int64(1)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Delete {
                table: "t".into(),
                txn_id: 0,
                version_id: 4,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: 4,
                row: vec![Value::Int64(9)],
            })
            .unwrap();
            wal.sync().unwrap();
        }
        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").unwrap();
        assert_eq!(t.rows.len(), 1);
        assert_eq!(t.rows[0], (4, vec![Value::Int64(9)]));
    }

    /// The recovered floor advances past every id the log ever contained —
    /// live, dead, or belonging to an aborted txn — so the next minted id
    /// can never collide with one of them.
    #[test]
    fn floor_advances_past_every_observed_id() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 1,
                version_id: 2,
                row: vec![Value::Int64(1)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::Abort { txn_id: 1 }).unwrap();
            // Even the aborted txn's id 2 burns the floor forward.
            wal.sync().unwrap();
        }
        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").unwrap();
        assert_eq!(t.rows.len(), 0);
        assert_eq!(t.next_version_id, 3, "floor must pass the aborted id");
    }

    /// Compaction preserves row ids and the floor, so identities survive
    /// restarts: reopen twice and the same rows keep the same ids, with the
    /// floor never rewinding.
    #[test]
    fn compaction_preserves_ids_and_floor_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
                next_version_id: 0,
            })
            .unwrap();
            // Sparse ids, as a preserved-id baseline produces.
            for id in [0u64, 2, 5] {
                wal.log(&MvccWalRecord::Insert {
                    table: "t".into(),
                    txn_id: 0,
                    version_id: id,
                    row: vec![Value::Int64(id as i64)],
                })
                .unwrap();
            }
            wal.sync().unwrap();
        }
        let ids_after_restart = {
            let (wal, state) = MvccWal::open(dir.path()).unwrap();
            wal.compact(&state).unwrap();
            drop(wal);
            let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
            let t = state.tables.get("t").unwrap();
            let ids: Vec<u64> = t.rows.iter().map(|(id, _)| *id).collect();
            assert_eq!(t.next_version_id, 6);
            ids
        };
        assert_eq!(ids_after_restart, vec![0, 2, 5]);
        // And once more: still stable.
        let (_wal3, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").unwrap();
        let ids: Vec<u64> = t.rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![0, 2, 5]);
        assert_eq!(t.next_version_id, 6);
    }

    /// A DROP + CREATE of the same name starts a fresh identity space —
    /// the new table's floor is its own.
    #[test]
    fn drop_then_create_resets_the_identity_space() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int64)],
                next_version_id: 0,
            })
            .unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(),
                txn_id: 0,
                version_id: 3,
                row: vec![Value::Int64(1)],
            })
            .unwrap();
            wal.log(&MvccWalRecord::DropTable { name: "t".into() }).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("y".into(), DataType::Text)],
                next_version_id: 0,
            })
            .unwrap();
            wal.sync().unwrap();
        }
        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").unwrap();
        assert_eq!(t.rows.len(), 0);
        assert_eq!(t.next_version_id, 0, "recreated table starts fresh");
    }
}

// ── Legacy (v1) upgrade-on-open: frozen corpus tests ────────────────────────
//
// The corpus bytes below were generated against the pre-v2 writer (at commit
// 6dcefaab, before any v2 write landed), so these tests exercise the REAL
// legacy format — not a re-derivation from the current encoder that could
// drift in lockstep with a bug.

#[cfg(test)]
mod legacy_upgrade_tests {
    use super::*;

/// Frozen legacy (v1) corpus: 597 bytes emitted by the pre-v2 writer at
/// commit 6dcefaab (generated against the then-current code BEFORE any v2
/// write landed). Covers: CreateTable with parameterized columns (Array,
/// Vector), explicit-txn inserts + commit, an autocommit insert, a committed
/// txn doing Update + Delete + log_commit(txn, Some(77)) (Commit + XactCommit
/// pair), and an aborted txn whose insert must not survive.
const LEGACY_V1_CORPUS: &[u8] = &[
    0x2F, 0x00, 0x00, 0x00, 0x01, 0x05, 0x00, 0x00, 0x00, 0x75, 0x73, 0x65, 0x72, 0x73, 0x04, 0x00,
    0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x69, 0x64, 0x02, 0x04, 0x00, 0x00, 0x00, 0x6E, 0x61, 0x6D,
    0x65, 0x04, 0x04, 0x00, 0x00, 0x00, 0x74, 0x61, 0x67, 0x73, 0x0E, 0x03, 0x00, 0x00, 0x00, 0x65,
    0x6D, 0x62, 0x0D, 0x06, 0xD9, 0xD4, 0x2D, 0x09, 0x00, 0x00, 0x00, 0x10, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x5B, 0x86, 0xD2, 0x87, 0x48, 0x00, 0x00, 0x00, 0x03, 0x05, 0x00, 0x00,
    0x00, 0x75, 0x73, 0x65, 0x72, 0x73, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
    0x05, 0x00, 0x00, 0x00, 0x41, 0x6C, 0x69, 0x63, 0x65, 0x0F, 0x01, 0x00, 0x00, 0x00, 0x02, 0x07,
    0x00, 0x00, 0x00, 0x0D, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3F, 0x00, 0x00, 0x80, 0x3F,
    0x00, 0x00, 0x00, 0xC0, 0xBF, 0x3E, 0x33, 0x8C, 0x41, 0x00, 0x00, 0x00, 0x03, 0x05, 0x00, 0x00,
    0x00, 0x75, 0x73, 0x65, 0x72, 0x73, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
    0x03, 0x00, 0x00, 0x00, 0x42, 0x6F, 0x62, 0x0F, 0x00, 0x00, 0x00, 0x00, 0x0D, 0x03, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x82, 0xB5, 0x24,
    0xDB, 0x09, 0x00, 0x00, 0x00, 0x11, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x50,
    0xEC, 0x73, 0x4D, 0x00, 0x00, 0x00, 0x03, 0x05, 0x00, 0x00, 0x00, 0x75, 0x73, 0x65, 0x72, 0x73,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
    0x03, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x05, 0x00, 0x00, 0x00, 0x43, 0x61,
    0x72, 0x6F, 0x6C, 0x0F, 0x02, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x02, 0x02, 0x00,
    0x00, 0x00, 0x0D, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x80, 0x3F, 0x00,
    0x00, 0x80, 0x3F, 0x34, 0x94, 0xB6, 0xAD, 0x09, 0x00, 0x00, 0x00, 0x10, 0x02, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x32, 0x01, 0x96, 0x5C, 0x48, 0x00, 0x00, 0x00, 0x05, 0x05, 0x00, 0x00,
    0x00, 0x75, 0x73, 0x65, 0x72, 0x73, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x05, 0x06, 0x00, 0x00, 0x00, 0x41, 0x6C, 0x69, 0x63, 0x69, 0x61, 0x0F, 0x00,
    0x00, 0x00, 0x00, 0x0D, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x41, 0x00, 0x00, 0x10, 0x41,
    0x00, 0x00, 0x10, 0x41, 0x57, 0x89, 0x68, 0x9C, 0x16, 0x00, 0x00, 0x00, 0x04, 0x05, 0x00, 0x00,
    0x00, 0x75, 0x73, 0x65, 0x72, 0x73, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x21, 0xD0, 0xFE, 0xF3, 0x09, 0x00, 0x00, 0x00, 0x11, 0x02, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x7A, 0xD7, 0xA8, 0xA8, 0x09, 0x00, 0x00, 0x00, 0x13, 0x4D, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0xF7, 0xC7, 0x5B, 0xE7, 0x09, 0x00, 0x00, 0x00, 0x10, 0x03, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0x7C, 0xAA, 0x15, 0x43, 0x00, 0x00, 0x00, 0x03, 0x05, 0x00,
    0x00, 0x00, 0x75, 0x73, 0x65, 0x72, 0x73, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
    0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x03, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x05, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x65, 0x76, 0x65, 0x72, 0x0F, 0x00, 0x00, 0x00, 0x00, 0x0D,
    0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x64, 0xBF, 0x14, 0x5B, 0x09, 0x00, 0x00, 0x00, 0x12, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x74, 0xA6, 0x3B, 0xF8,
];

    /// Expected recovery from the corpus: txn 1 committed (Alice@0, Bob@1),
    /// autocommit Carol@2, txn 2 committed (Update 0->3 "Alicia", Delete 1),
    /// txn 3 aborted (Never@4 must not survive). Live rows: 2 and 3.
    /// Floor 5 (ids 0..4 were all observed). XactCommit 77 recovered.
    /// Parameterized columns come back defaulted — the parameters were never
    /// recorded in v1 (the recorded NU-15 loss; v2 is lossless).
    #[test]
    fn legacy_corpus_upgrades_to_v2_with_exact_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mvcc.wal");
        std::fs::write(&path, LEGACY_V1_CORPUS).unwrap();

        let (_wal, state) = MvccWal::open(dir.path()).unwrap();

        let users = state.tables.get("users").expect("table survived");
        assert_eq!(users.rows.len(), 2, "Alice-updated + Carol survive; Bob is deleted");
        assert_eq!(users.rows[0].0, 2);
        assert_eq!(users.rows[0].1[1], Value::Text("Carol".into()));
        assert_eq!(users.rows[1].0, 3);
        assert_eq!(users.rows[1].1[1], Value::Text("Alicia".into()));
        assert_eq!(users.next_version_id, 5, "floor passes every observed id");
        assert!(
            state.committed_xacts.contains(&77),
            "the v1 Commit+XactCommit pair must upgrade into the committed set"
        );
        // Legacy schema decode: parameters were never written.
        let tags = &users.columns[2].1;
        assert_eq!(*tags, DataType::Array(Box::new(DataType::Text)));
        let emb = &users.columns[3].1;
        assert_eq!(*emb, DataType::Vector(0));

        // The live log was rewritten as v2; the original is preserved.
        assert!(is_v2(&std::fs::read(&path).unwrap()), "upgraded log is v2");
        let backup = v1_backup_path(&path);
        assert_eq!(
            std::fs::read(&backup).unwrap(),
            LEGACY_V1_CORPUS,
            "the pre-upgrade original is preserved byte-for-byte"
        );
    }

    /// Upgrade idempotence: the second open reads the v2 log (no re-upgrade),
    /// recovers the SAME ids, retires the backup on the clean open, and the
    /// third open is an ordinary v2 open.
    #[test]
    fn upgrade_is_idempotent_and_backup_retires_on_clean_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mvcc.wal");
        std::fs::write(&path, LEGACY_V1_CORPUS).unwrap();

        let ids_and_floor = |state: &MvccWalState| {
            let users = state.tables.get("users").unwrap();
            (
                users.rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                users.next_version_id,
            )
        };

        let (wal, state) = MvccWal::open(dir.path()).unwrap();
        let first = ids_and_floor(&state);
        drop(wal);

        // Second open: v2, clean -> backup retired, identity stable.
        let (wal, state) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(ids_and_floor(&state), first, "ids and floor must be stable");
        assert!(
            !v1_backup_path(&path).exists(),
            "a clean v2 open retires the upgrade backup"
        );
        assert!(is_v2(&std::fs::read(&path).unwrap()));
        drop(wal);

        // Third open: ordinary v2 open, still stable.
        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        assert_eq!(ids_and_floor(&state), first);
    }

    /// A torn v1 tail upgrades its prefix; the torn original is preserved
    /// as the backup. Truncating inside frame 7 (the Update) leaves txn 2
    /// without a Commit, so Alice/Bob/Carol survive unmodified and no
    /// XactCommit is recovered.
    #[test]
    fn torn_legacy_tail_upgrades_the_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mvcc.wal");
        std::fs::write(&path, &LEGACY_V1_CORPUS[..350]).unwrap();

        let (_wal, state) = MvccWal::open(dir.path()).unwrap();
        let users = state.tables.get("users").expect("table survived");
        let names: Vec<&Value> = users.rows.iter().map(|(_, r)| &r[1]).collect();
        assert_eq!(
            names,
            vec![
                &Value::Text("Alice".into()),
                &Value::Text("Bob".into()),
                &Value::Text("Carol".into()),
            ],
            "the torn-away txn-2 records must not apply"
        );
        assert!(state.committed_xacts.is_empty());
        // The torn original is preserved, not silently discarded.
        assert_eq!(
            std::fs::read(v1_backup_path(&path)).unwrap(),
            &LEGACY_V1_CORPUS[..350]
        );
    }

    /// Legacy corruption still fails closed with the file untouched: the
    /// upgrade path inherits NU-04's policy exactly.
    #[test]
    fn corrupted_legacy_log_fails_closed_before_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mvcc.wal");
        let mut data = LEGACY_V1_CORPUS.to_vec();
        data[20] ^= 0xFF; // inside the first CreateTable payload
        std::fs::write(&path, &data).unwrap();

        let result = MvccWal::open(dir.path());
        match result {
            Err(err) => {
                assert!(err.to_string().contains("corruption"), "{err}");
                assert_eq!(std::fs::read(&path).unwrap(), data);
                assert!(
                    !v1_backup_path(&path).exists(),
                    "no backup is taken for a log that failed to replay"
                );
            }
            Ok(_) => panic!("corrupted legacy WAL was accepted"),
        }
    }
}

