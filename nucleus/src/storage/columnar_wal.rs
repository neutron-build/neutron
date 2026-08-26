//! Write-ahead log for the columnar storage engine.
//!
//! Provides crash-recovery by recording all table mutations to an append-only
//! log file (`columnar.wal`). On restart the log is replayed from top to bottom
//! to reconstruct in-memory state.
//!
//! ## Log entry binary format
//! ```text
//! [entry_type: u8]
//! [name_len: u32 LE]  [name_bytes: name_len]
//! [payload_len: u32 LE] [payload: payload_len]
//! ```
//!
//! ## Entry types
//! | Tag  | Name         | Payload                                        |
//! |------|--------------|------------------------------------------------|
//! | 0x01 | CREATE_TABLE | (empty)                                        |
//! | 0x02 | DROP_TABLE   | (empty)                                        |
//! | 0x03 | INSERT_ROWS  | n_rows(u32) + rows…                            |
//! | 0x04 | SNAPSHOT     | n_tables(u32) + (name_len + name + n_rows + rows…)… |
//! | 0x05 | INSERT_ROWS_NAMED | n_cols(u32) + col names + n_rows(u32) + rows… |
//! | 0x06 | SNAPSHOT_NAMED | n_tables(u32) + (name + n_cols + col names + n_rows + rows…)… |
//! | 0x07 | CREATE_TABLE_XACT | xact(u64 LE) — S63 twin of 0x01              |
//! | 0x08 | DROP_TABLE_XACT   | xact(u64 LE) — S63 twin of 0x02              |
//! | 0x09 | INSERT_ROWS_NAMED_XACT | xact(u64 LE) — S63 twin of 0x05         |
//!
//! A SNAPSHOT resets all table state. After `checkpoint()` the file is
//! truncated to a single SNAPSHOT entry so the log stays small.
//!
//! ## Transaction-tagged records (S63)
//!
//! Tags `0x07`-`0x09` are the `_XACT` twins of the mutation records, each
//! carrying the coordinating transaction id (`u64 LE`) between the tag and
//! the twin's body. Replay keeps a tagged record only if its id is
//! `XACT_AUTOCOMMIT` (0 — written outside any explicit transaction, whose
//! durability point is this log's own fsync) or appears in the committed set
//! recovered from the SQL side; everything else was written inside a
//! transaction that never committed and is discarded. The untagged tags keep
//! their keep-unconditionally meaning, so pre-S63 logs replay unchanged. A
//! SNAPSHOT is committed by construction (the S7 checkpoint gate keeps one
//! from folding an open transaction's writes) and always replays.
//!
//! The per-table storage engines (`WITH (engine='columnar')`) write through
//! the `StorageEngine` trait, which carries no transaction identity, so
//! their records stay UNTAGGED — legacy keep-always semantics, the same
//! behaviour they had before S63. Only the columnar MODEL store (driven by
//! the executor's `COLUMNAR_INSERT`) tags its records today.
//!
//! ## Why the `_NAMED` variants exist
//!
//! 0x03 and 0x04 record a table's rows and nothing else. `ColumnarStore`'s
//! tables have *named* columns (`COLUMNAR_INSERT('t','metric',99)`), so
//! replaying them rebuilt every table with columns renamed `"0"`, `"1"`, … —
//! the rows were all there and `COLUMNAR_COUNT` was right, while
//! `COLUMNAR_SUM`/`AVG`/`MIN`/`MAX`, which look a column up by name, returned
//! 0 on a database that had just been restarted or restored. Silently, and
//! for every columnar table written through SQL.
//!
//! `ColumnarStorageEngine` is unaffected because it names its columns "0",
//! "1", … positionally by convention, which is also why the older entries
//! carry no names: they were written by that path first.
//!
//! Both writers now emit the `_NAMED` forms, with an empty name list meaning
//! "positional" — so the engine's behaviour is unchanged and old logs still
//! replay through the 0x03/0x04 arms. The names are repeated in every insert
//! entry rather than being declared once, so a truncated log still interprets
//! whatever entries survive; checkpointing is what keeps that from growing.
//! An OLDER binary reading a newer log skips the unknown tags and loses the
//! rows in them — downgrade requires a checkpoint on the old version first.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::executor::enlistment::XACT_AUTOCOMMIT;
use crate::storage::wal::GroupCommitter;
use crate::types::{Row, Value};

// ─── Entry type tags ──────────────────────────────────────────────────────────

const ENTRY_CREATE_TABLE: u8 = 0x01;
const ENTRY_DROP_TABLE: u8 = 0x02;
const ENTRY_INSERT_ROWS: u8 = 0x03;
const ENTRY_SNAPSHOT: u8 = 0x04;
const ENTRY_INSERT_ROWS_NAMED: u8 = 0x05;
const ENTRY_SNAPSHOT_NAMED: u8 = 0x06;
/// S63: CREATE_TABLE carrying the coordinating transaction id.
const ENTRY_CREATE_TABLE_XACT: u8 = 0x07;
/// S63: DROP_TABLE carrying the coordinating transaction id.
const ENTRY_DROP_TABLE_XACT: u8 = 0x08;
/// S63: INSERT_ROWS_NAMED carrying the coordinating transaction id.
const ENTRY_INSERT_ROWS_NAMED_XACT: u8 = 0x09;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Recovered table state from a WAL replay.
pub struct WalState {
    /// `(table_name, rows)` pairs — order unspecified.
    pub tables: Vec<(String, Vec<Row>)>,
    /// `(table_name, column_names)` for every table whose log recorded them.
    /// Absent for a table written by a positional caller, or by any writer
    /// predating the `_NAMED` entries.
    pub columns: Vec<(String, Vec<String>)>,
    /// The highest coordinating transaction id seen on a tagged record,
    /// whether that record was kept or discarded. Seeds the XactId
    /// high-water mark at executor construction (S63).
    pub max_xact_id: u64,
}

/// Append-only columnar WAL.
pub struct ColumnarWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Monotone append counter, incremented under the writer lock — serves as
    /// this WAL's LSN for durable-coverage tracking.
    appends: AtomicU64,
    /// Highest append-counter value covered by a COMPLETED fsync. Updated
    /// only after the fsync returns, so `synced >= mark` is a durable claim.
    synced: AtomicU64,
    /// Group-commit coordinator so concurrent commit-time syncs share fsyncs.
    committer: GroupCommitter,
    /// The writer holds an inode a checkpoint's rename displaced: it is
    /// unlinked, so appends to it "succeed" into a file no future recovery
    /// reads while `group_sync`/`is_dirty` report healthy. Set when a
    /// checkpoint replaced the log but its reopen failed; cleared by the next
    /// successful reattach (or checkpoint reopen). See `reattach_if_stranded`.
    stranded: AtomicBool,
    /// The highest coordinating transaction id recovered at open (S63).
    max_xact_id: u64,
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint_named`.
    #[cfg(test)]
    fail_reopen_once: AtomicBool,
}

impl ColumnarWal {
    /// Open or create the WAL file in `dir`, replaying with an EMPTY
    /// committed set so every tagged record keeps — the pre-S63 contract.
    /// The executor opens through [`ColumnarWal::open_with_committed`]
    /// instead, passing the coordinating transaction ids the SQL side
    /// durably committed so the S63 replay filter can discard the rest.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty (no tables). Corrupt trailing bytes are silently ignored
    /// (best-effort recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, WalState)> {
        Self::open_with_committed(dir, &HashSet::new())
    }

    /// Open or create the WAL file in `dir` whose replay is filtered by the
    /// S63 committed set: a tagged record whose coordinating transaction id
    /// is neither `XACT_AUTOCOMMIT` nor in `committed` was written inside a
    /// transaction that never committed, and is discarded.
    pub fn open_with_committed(
        dir: &Path,
        committed: &HashSet<u64>,
    ) -> io::Result<(Self, WalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("columnar.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data, committed)
        } else {
            WalState {
                tables: Vec::new(),
                columns: Vec::new(),
                max_xact_id: 0,
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let max_xact_id = state.max_xact_id;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                appends: AtomicU64::new(0),
                synced: AtomicU64::new(0),
                committer: GroupCommitter::new(),
                stranded: AtomicBool::new(false),
                max_xact_id,
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

    /// Whether appends exist that no completed fsync covers yet.
    pub fn is_dirty(&self) -> bool {
        self.synced.load(Ordering::Acquire) < self.appends.load(Ordering::Acquire)
    }

    /// Fsync the log and record the append mark the sync covered.
    /// Returns that mark. The mark is captured under the writer lock, where
    /// appends also increment it, so every append at or below it is flushed
    /// and fsynced by this call.
    fn sync_covering(&self) -> io::Result<u64> {
        let mut w = self.writer.lock();
        let covered = self.appends.load(Ordering::Acquire);
        w.flush()?;
        w.get_ref().sync_all()?;
        self.synced.fetch_max(covered, Ordering::AcqRel);
        Ok(covered)
    }

    /// Fsync the log to stable storage. Appends only `write()` into the OS
    /// page cache; a commit ack requires this.
    pub fn sync(&self) -> io::Result<()> {
        self.sync_covering().map(|_| ())
    }

    /// Group-commit sync: concurrent committers share fsyncs, but each caller
    /// only returns once a completed sync covers every append made before
    /// this call.
    pub fn group_sync(&self) -> io::Result<()> {
        let mark = self.appends.load(Ordering::Acquire);
        if self.synced.load(Ordering::Acquire) >= mark {
            return Ok(());
        }
        self.committer.sync_up_to(mark, || self.sync_covering())
    }

    /// Log a CREATE TABLE operation.
    ///
    /// `xact` is the coordinating transaction id the record is tagged with:
    /// `Some(XACT_AUTOCOMMIT)` for a write outside any explicit transaction,
    /// `Some(id)` inside one, `None` to write the legacy untagged record
    /// (kept unconditionally on replay — the pre-S63 compatibility rule).
    pub fn log_create_table(&self, xact: Option<u64>, table: &str) -> io::Result<()> {
        self.append(
            ENTRY_CREATE_TABLE,
            ENTRY_CREATE_TABLE_XACT,
            xact,
            table,
            &[],
        )
    }

    /// Log a DROP TABLE operation. `xact` mirrors [`ColumnarWal::log_create_table`].
    pub fn log_drop_table(&self, xact: Option<u64>, table: &str) -> io::Result<()> {
        self.append(ENTRY_DROP_TABLE, ENTRY_DROP_TABLE_XACT, xact, table, &[])
    }

    /// Log a batch of newly inserted rows whose columns are positional.
    pub fn log_insert_rows(&self, table: &str, rows: &[Row]) -> io::Result<()> {
        self.log_insert_rows_named(None, table, &[], rows)
    }

    /// Log a batch of newly inserted rows together with their column names.
    ///
    /// An empty `columns` means the caller names columns positionally, which
    /// is `ColumnarStorageEngine`'s convention. Anything that puts real names
    /// on a batch must pass them, or the names do not survive a restart —
    /// see this module's header. `xact` mirrors
    /// [`ColumnarWal::log_create_table`].
    pub fn log_insert_rows_named(
        &self,
        xact: Option<u64>,
        table: &str,
        columns: &[String],
        rows: &[Row],
    ) -> io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::new();
        encode_names(columns, &mut payload);
        payload.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for row in rows {
            encode_row(row, &mut payload);
        }
        self.append(
            ENTRY_INSERT_ROWS_NAMED,
            ENTRY_INSERT_ROWS_NAMED_XACT,
            xact,
            table,
            &payload,
        )
    }

    /// Write the complete current state of all tables as a single SNAPSHOT
    /// entry and truncate the log to just that entry.
    ///
    /// `tables` is a slice of `(table_name, all_rows)` covering every table
    /// that the engine currently knows about.
    pub fn checkpoint(&self, tables: &[(&str, Vec<Row>)]) -> io::Result<()> {
        let named: Vec<(&str, Vec<String>, &[Row])> = tables
            .iter()
            .map(|(name, rows)| (*name, Vec::new(), rows.as_slice()))
            .collect();
        self.checkpoint_named(&named)
    }

    /// `checkpoint`, preserving each table's column names.
    pub fn checkpoint_named(&self, tables: &[(&str, Vec<String>, &[Row])]) -> io::Result<()> {
        // Build snapshot payload.
        let mut payload = Vec::new();
        payload.extend_from_slice(&(tables.len() as u32).to_le_bytes());
        for (name, columns, rows) in tables {
            let nb = name.as_bytes();
            payload.extend_from_slice(&(nb.len() as u32).to_le_bytes());
            payload.extend_from_slice(nb);
            encode_names(columns, &mut payload);
            payload.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            for row in rows.iter() {
                encode_row(row, &mut payload);
            }
        }

        // Hold the writer lock across truncate + rewrite + swap so no append
        // can interleave: an entry appended between the truncate and the
        // writer swap would be destroyed without being in the snapshot.
        let mut writer = self.writer.lock();
        writer.flush()?;

        // Serialize the complete new log body, then replace atomically (temp file +
        // fsync + rename) so a crash between the truncate and the snapshot rewrite
        // can't leave a truncated or empty file.
        let mut contents: Vec<u8> = Vec::new();
        write_entry(
            &mut contents,
            ENTRY_SNAPSHOT_NAMED,
            ENTRY_SNAPSHOT_NAMED,
            None,
            "",
            &payload,
        )?;
        crate::storage::wal_util::atomic_replace_wal(&self.path, &contents)?;

        // The reopen is the hazardous half: the rename above already unlinked
        // the inode `writer` holds, so a failure here leaves the writer
        // pointing at a file no future recovery reads.
        #[cfg(test)]
        let injected: Option<io::Error> = self
            .fail_reopen_once
            .swap(false, Ordering::AcqRel)
            .then(|| io::Error::other("injected columnar WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = if let Some(e) = injected {
            Err(e)
        } else if let Some(e) = crate::storage::crashpoint::io_fault("columnar.wal_reopen") {
            Err(e)
        } else {
            OpenOptions::new().append(true).open(&self.path)
        };
        let file = match file {
            Ok(f) => f,
            Err(e) => {
                // The rename already happened, so the handle in `writer` is
                // now an unlinked inode. Mark the writer stranded: appends
                // must reattach (or fail loudly), never write through it.
                self.stranded.store(true, Ordering::Release);
                return Err(e);
            }
        };
        // Re-open in append mode for future writes, and count the snapshot
        // as a covered append so coverage marks stay consistent.
        *writer = BufWriter::new(file);
        self.stranded.store(false, Ordering::Release);
        let mark = self.appends.fetch_add(1, Ordering::AcqRel) + 1;
        self.synced.fetch_max(mark, Ordering::AcqRel);
        Ok(())
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    fn append(
        &self,
        plain: u8,
        xact_tagged: u8,
        xact: Option<u64>,
        name: &str,
        payload: &[u8],
    ) -> io::Result<()> {
        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        write_entry(&mut *w, plain, xact_tagged, xact, name, payload)?;
        w.flush()?;
        // Counted under the writer lock so sync_covering's mark is exact.
        self.appends.fetch_add(1, Ordering::AcqRel);
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
        if let Some(e) = crate::storage::crashpoint::io_fault("columnar.wal_reopen") {
            return Err(e);
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "columnar WAL writer is stranded: a checkpoint replaced {} but its \
                         reopen failed; refusing to append to the unlinked old file ({e})",
                        self.path.display()
                    ),
                )
            })?;
        *w = BufWriter::new(file);
        self.stranded.store(false, Ordering::Release);
        Ok(())
    }
}

// ─── Binary encoding ──────────────────────────────────────────────────────────

fn write_entry<W: Write>(
    w: &mut W,
    plain: u8,
    xact_tagged: u8,
    xact: Option<u64>,
    name: &str,
    payload: &[u8],
) -> io::Result<()> {
    let nb = name.as_bytes();
    w.write_all(&[plain_or_tag(plain, xact_tagged, xact)])?;
    if let Some(x) = xact {
        w.write_all(&x.to_le_bytes())?;
    }
    w.write_all(&(nb.len() as u32).to_le_bytes())?;
    w.write_all(nb)?;
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(payload)
}

/// The tag byte for one record: the `_XACT` twin when `xact` is `Some`, the
/// legacy untagged tag when `None`.
fn plain_or_tag(plain: u8, xact_tagged: u8, xact: Option<u64>) -> u8 {
    match xact {
        Some(_) => xact_tagged,
        None => plain,
    }
}

fn encode_names(names: &[String], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(names.len() as u32).to_le_bytes());
    for name in names {
        let b = name.as_bytes();
        buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
        buf.extend_from_slice(b);
    }
}

fn encode_row(row: &Row, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(row.len() as u32).to_le_bytes());
    for val in row {
        encode_value(val, buf);
    }
}

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
        Value::Numeric(s) => {
            buf.push(10);
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Date(d) => {
            buf.push(7);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Timestamp(t) => {
            buf.push(8);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::TimestampTz(t) => {
            buf.push(9);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Interval {
            months,
            days,
            microseconds,
        } => {
            buf.push(11);
            buf.extend_from_slice(&months.to_le_bytes());
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&microseconds.to_le_bytes());
        }
        other => {
            // Fallback: encode as Text (lossy for exotic types — sufficient for
            // columnar analytical workloads that don't use JSON/UUID/Array).
            let s = format!("{other}");
            let b = s.as_bytes();
            buf.push(5);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
}

// ─── Replay ───────────────────────────────────────────────────────────────────

/// Replay all entries in `data` to reconstruct table state.
///
/// SNAPSHOT entries reset all state to their embedded snapshot, so only the
/// *last* SNAPSHOT (and subsequent incremental entries) matter in practice.
///
/// `committed` is the set of coordinating transaction ids that durably
/// committed on the SQL side (S63). A tagged record whose id is neither
/// `XACT_AUTOCOMMIT` nor in it was written inside a transaction that never
/// committed, and is discarded — its body is still parsed past, and ids feed
/// `max_xact_id` whether kept or discarded, so the caller can seed the XactId
/// high-water mark.
fn replay(data: &[u8], committed: &HashSet<u64>) -> WalState {
    let mut tables: std::collections::HashMap<String, Vec<Row>> = std::collections::HashMap::new();
    let mut columns: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    let mut pos = 0usize;
    let mut max_xact_id: u64 = 0;

    while pos < data.len() {
        // entry_type
        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        // The tagged records parse their id, then share the body parse with
        // the untagged twin. `keep_tagged` is the S63 filter in one
        // expression: an autocommit record is durable by its own fsync, a
        // committed id was vouched for by a durable COMMIT record, anything
        // else never happened.
        let mut keep_tagged = true;
        if matches!(
            entry_type,
            ENTRY_CREATE_TABLE_XACT | ENTRY_DROP_TABLE_XACT | ENTRY_INSERT_ROWS_NAMED_XACT
        ) {
            let Some(xact) = read_u64(data, &mut pos) else {
                break;
            };
            max_xact_id = max_xact_id.max(xact);
            keep_tagged = xact == XACT_AUTOCOMMIT || committed.contains(&xact);
        }

        // name
        let Some(name_len) = read_u32(data, &mut pos) else {
            break;
        };
        let name_len = name_len as usize;
        if pos + name_len > data.len() {
            break;
        }
        let name = match std::str::from_utf8(&data[pos..pos + name_len]) {
            Ok(s) => s.to_string(),
            Err(_) => break,
        };
        pos += name_len;

        // payload
        let Some(payload_len) = read_u32(data, &mut pos) else {
            break;
        };
        let payload_len = payload_len as usize;
        if pos + payload_len > data.len() {
            break;
        }
        let payload = &data[pos..pos + payload_len];
        pos += payload_len;

        match entry_type {
            ENTRY_CREATE_TABLE | ENTRY_CREATE_TABLE_XACT => {
                if keep_tagged {
                    tables.entry(name).or_default();
                }
            }
            ENTRY_DROP_TABLE | ENTRY_DROP_TABLE_XACT => {
                if keep_tagged {
                    tables.remove(&name);
                }
            }
            ENTRY_INSERT_ROWS => {
                let rows = decode_rows(payload);
                tables.entry(name).or_default().extend(rows);
            }
            ENTRY_INSERT_ROWS_NAMED | ENTRY_INSERT_ROWS_NAMED_XACT => {
                let mut pos = 0usize;
                let names = decode_names(payload, &mut pos);
                let rows = decode_rows_at(payload, &mut pos);
                if keep_tagged {
                    if !names.is_empty() {
                        columns.insert(name.clone(), names);
                    }
                    tables.entry(name).or_default().extend(rows);
                }
            }
            ENTRY_SNAPSHOT => {
                tables.clear();
                columns.clear();
                decode_snapshot_into(payload, &mut tables, &mut columns, false);
            }
            ENTRY_SNAPSHOT_NAMED => {
                tables.clear();
                columns.clear();
                decode_snapshot_into(payload, &mut tables, &mut columns, true);
            }
            _ => {} // Unknown entry types are skipped.
        }
    }

    WalState {
        tables: tables.into_iter().collect(),
        columns: columns.into_iter().collect(),
        max_xact_id,
    }
}

fn decode_rows(data: &[u8]) -> Vec<Row> {
    let mut pos = 0;
    decode_rows_at(data, &mut pos)
}

/// Column names, as written by `encode_names`.
fn decode_names(data: &[u8], pos: &mut usize) -> Vec<String> {
    let Some(n) = read_u32(data, pos) else {
        return Vec::new();
    };
    let n = n as usize;
    let mut names = Vec::with_capacity(super::wal_util::bounded_capacity(n));
    for _ in 0..n {
        let Some(len) = read_u32(data, pos) else {
            break;
        };
        let len = len as usize;
        if *pos + len > data.len() {
            break;
        }
        match std::str::from_utf8(&data[*pos..*pos + len]) {
            Ok(s) => names.push(s.to_string()),
            Err(_) => break,
        }
        *pos += len;
    }
    names
}

fn decode_rows_at(data: &[u8], pos: &mut usize) -> Vec<Row> {
    let n = match read_u32(data, pos) {
        Some(n) => n as usize,
        None => return vec![],
    };
    let mut rows = Vec::with_capacity(super::wal_util::bounded_capacity(n));
    for _ in 0..n {
        match decode_row(data, pos) {
            Some(r) => rows.push(r),
            None => break,
        }
    }
    rows
}

fn decode_snapshot_into(
    data: &[u8],
    tables: &mut std::collections::HashMap<String, Vec<Row>>,
    columns: &mut std::collections::HashMap<String, Vec<String>>,
    named: bool,
) {
    let mut pos = 0;
    let n_tables = match read_u32(data, &mut pos) {
        Some(n) => n as usize,
        None => return,
    };
    for _ in 0..n_tables {
        // table name
        let name_len = match read_u32(data, &mut pos) {
            Some(n) => n as usize,
            None => return,
        };
        if pos + name_len > data.len() {
            return;
        }
        let name = match std::str::from_utf8(&data[pos..pos + name_len]) {
            Ok(s) => s.to_string(),
            Err(_) => return,
        };
        pos += name_len;
        // column names, in the 0x06 form only
        if named {
            let names = decode_names(data, &mut pos);
            if !names.is_empty() {
                columns.insert(name.clone(), names);
            }
        }
        // rows
        let n_rows = match read_u32(data, &mut pos) {
            Some(n) => n as usize,
            None => return,
        };
        // `n_rows` comes off disk: a corrupt length must not reserve it. An
        // unbounded `with_capacity` here aborts the process on Linux rather
        // than returning an error, and silently succeeds on an overcommitting
        // macOS, so this is not visible from a local run.
        let mut rows = Vec::with_capacity(super::wal_util::bounded_capacity(n_rows));
        for _ in 0..n_rows {
            match decode_row(data, &mut pos) {
                Some(r) => rows.push(r),
                None => return,
            }
        }
        tables.insert(name, rows);
    }
}

fn decode_row(data: &[u8], pos: &mut usize) -> Option<Row> {
    let col_count = read_u32(data, pos)? as usize;
    // `col_count` comes off disk, like the three counts already wrapped above.
    // Every value costs at least its 1-byte tag, so the bytes remaining bound
    // how many can really follow; an unbounded reservation ABORTS on Linux.
    let mut row = Vec::with_capacity(col_count.min(data.len().saturating_sub(*pos)));
    for _ in 0..col_count {
        row.push(decode_value(data, pos)?);
    }
    Some(row)
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
        7 => Some(Value::Date(read_i32(data, pos)?)),
        8 => Some(Value::Timestamp(read_i64(data, pos)?)),
        9 => Some(Value::TimestampTz(read_i64(data, pos)?)),
        10 => {
            let len = read_u32(data, pos)? as usize;
            if *pos + len > data.len() {
                return None;
            }
            let value = std::str::from_utf8(&data[*pos..*pos + len])
                .ok()?
                .to_string();
            *pos += len;
            Some(Value::Numeric(value))
        }
        11 => Some(Value::Interval {
            months: read_i32(data, pos)?,
            days: read_i32(data, pos)?,
            microseconds: read_i64(data, pos)?,
        }),
        _ => None, // Unknown tag — stop decoding row.
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

fn read_i64(data: &[u8], pos: &mut usize) -> Option<i64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(i64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    read_i64(data, pos).map(|v| v as u64)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    read_i64(data, pos).map(|v| f64::from_bits(v as u64))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn int_row(id: i64, v: f64) -> Row {
        vec![Value::Int64(id), Value::Float64(v)]
    }

    // ── S63: the recovery filter ──────────────────────────────────────────

    /// One log exercising every filter decision at once: legacy and
    /// autocommit records keep, committed ids keep, unknown ids discard —
    /// and a discarded record in the MIDDLE does not stop the records after
    /// it (they are length-framed, so parsing past is exact). Also asserts
    /// the max surviving tagged id (kept or not) is reported for the XactId
    /// floor.
    #[test]
    fn tagged_records_filter_on_the_committed_set() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = ColumnarWal::open(dir.path()).unwrap();
            // Legacy untagged INSERT (pre-S63 log): keep unconditionally.
            wal.log_insert_rows("legacy", &[int_row(1, 1.0)]).unwrap();
            wal.log_insert_rows_named(Some(XACT_AUTOCOMMIT), "auto", &[], &[int_row(1, 1.0)])
                .unwrap();
            wal.log_insert_rows_named(Some(7), "committed", &[], &[int_row(1, 1.0)])
                .unwrap();
            wal.log_insert_rows_named(Some(8), "never", &[], &[int_row(1, 1.0)])
                .unwrap();
            // An abandoned transaction's DROP and CREATE must not reach the
            // state that survived it either.
            wal.log_drop_table(Some(9), "auto").unwrap();
            wal.log_create_table(Some(9), "ghost").unwrap();
            wal.log_insert_rows_named(Some(10), "late", &[], &[int_row(1, 1.0)])
                .unwrap();
        }

        let committed: HashSet<u64> = [7u64, 10].into_iter().collect();
        let (_wal, state) = ColumnarWal::open_with_committed(dir.path(), &committed).unwrap();
        assert_eq!(
            state.max_xact_id, 10,
            "discarded records still feed the floor"
        );
        let count = |name: &str| {
            state
                .tables
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, rows)| rows.len())
                .unwrap_or(0)
        };
        assert_eq!(count("legacy"), 1, "untagged records keep unconditionally");
        assert_eq!(count("auto"), 1, "autocommit records carry id 0 and keep");
        assert_eq!(count("committed"), 1, "a committed id was vouched for");
        assert_eq!(
            count("never"),
            0,
            "id 8 never committed; its rows must be discarded, not replayed"
        );
        assert_eq!(
            count("auto"),
            1,
            "the abandoned DROP (id 9) must not remove a surviving table"
        );
        assert_eq!(
            count("ghost"),
            0,
            "the abandoned CREATE (id 9) must not reach recovery"
        );
        assert_eq!(
            count("late"),
            1,
            "the record AFTER a discarded one must still land"
        );
    }

    #[test]
    fn group_sync_covers_prior_appends_and_clears_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _state) = ColumnarWal::open(dir.path()).unwrap();
        assert!(!wal.is_dirty(), "fresh WAL must start clean");

        wal.log_create_table(None, "t").unwrap();
        wal.log_insert_rows("t", &[int_row(1, 1.0)]).unwrap();
        assert!(wal.is_dirty(), "appends must mark the WAL dirty");

        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "a completed sync covers prior appends");

        wal.log_insert_rows("t", &[int_row(2, 2.0)]).unwrap();
        assert!(wal.is_dirty(), "new appends after a sync are uncovered");
    }

    #[test]
    fn checkpoint_counts_as_covered() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _state) = ColumnarWal::open(dir.path()).unwrap();
        wal.log_create_table(None, "t").unwrap();
        wal.log_insert_rows("t", &[int_row(1, 1.0)]).unwrap();
        assert!(wal.is_dirty());
        wal.checkpoint(&[("t", vec![int_row(1, 1.0)])]).unwrap();
        assert!(
            !wal.is_dirty(),
            "checkpoint fsyncs the snapshot — nothing left to force"
        );
    }

    /// S31-14: a checkpoint whose reopen fails must not leave the writer
    /// appending into the unlinked inode the rename displaced. Those appends
    /// report success while no future recovery can ever read them, so an
    /// acknowledged row silently vanishes at restart. The discriminator is
    /// durability: the post-failure insert must land in the replaced file.
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = ColumnarWal::open(dir.path()).unwrap();
            wal.log_create_table(None, "t").unwrap();
            wal.log_insert_rows("t", &[int_row(1, 1.0)]).unwrap();
            wal.fail_reopen_once.store(true, Ordering::SeqCst);
            wal.checkpoint(&[("t", vec![int_row(1, 1.0)])])
                .expect_err("the injected reopen failure must fail the checkpoint");
            wal.log_insert_rows("t", &[int_row(2, 2.0)])
                .expect("a later append must reattach, not strand");
        }
        let (_wal2, state) = ColumnarWal::open(dir.path()).unwrap();
        let t = state.tables.iter().find(|(n, _)| n == "t").unwrap();
        assert_eq!(
            t.1.len(),
            2,
            "the post-checkpoint-failure insert went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
    }

    #[test]
    fn test_create_insert_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = ColumnarWal::open(dir.path()).unwrap();
        assert!(state.tables.is_empty());

        wal.log_create_table(None, "t").unwrap();
        wal.log_insert_rows("t", &[int_row(1, 1.0), int_row(2, 2.0)])
            .unwrap();
        drop(wal);

        // Reopen — should see 2 rows.
        let (_wal2, state2) = ColumnarWal::open(dir.path()).unwrap();
        let t = state2.tables.iter().find(|(n, _)| n == "t").unwrap();
        assert_eq!(t.1.len(), 2);
        assert_eq!(t.1[0][0], Value::Int64(1));
    }

    #[test]
    fn test_drop_table_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = ColumnarWal::open(dir.path()).unwrap();
        wal.log_create_table(None, "t").unwrap();
        wal.log_insert_rows("t", &[int_row(1, 1.0)]).unwrap();
        wal.log_drop_table(None, "t").unwrap();
        drop(wal);

        let (_wal2, state) = ColumnarWal::open(dir.path()).unwrap();
        assert!(state.tables.iter().all(|(n, _)| n != "t"));
    }

    #[test]
    fn test_checkpoint_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = ColumnarWal::open(dir.path()).unwrap();
        wal.log_create_table(None, "t").unwrap();
        let five: Vec<Row> = (1..=5).map(|i| int_row(i, i as f64)).collect();
        wal.log_insert_rows("t", &five).unwrap();
        // Checkpoint with 5 rows.
        let rows: Vec<Row> = (1..=5).map(|i| int_row(i, i as f64)).collect();
        wal.checkpoint(&[("t", rows)]).unwrap();
        // Insert 2 more rows after checkpoint.
        wal.log_insert_rows("t", &[int_row(6, 6.0), int_row(7, 7.0)])
            .unwrap();
        drop(wal);

        let (_wal2, state) = ColumnarWal::open(dir.path()).unwrap();
        let t = state.tables.iter().find(|(n, _)| n == "t").unwrap();
        assert_eq!(t.1.len(), 7);
    }

    #[test]
    fn test_empty_wal_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = ColumnarWal::open(dir.path()).unwrap();
        assert!(state.tables.is_empty());
    }

    #[test]
    fn test_multiple_tables() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = ColumnarWal::open(dir.path()).unwrap();
        wal.log_create_table(None, "a").unwrap();
        wal.log_create_table(None, "b").unwrap();
        wal.log_insert_rows("a", &[int_row(1, 1.0), int_row(2, 2.0)])
            .unwrap();
        wal.log_insert_rows("b", &[int_row(10, 10.0)]).unwrap();
        drop(wal);

        let (_w, state) = ColumnarWal::open(dir.path()).unwrap();
        let a = state.tables.iter().find(|(n, _)| n == "a").unwrap();
        let b = state.tables.iter().find(|(n, _)| n == "b").unwrap();
        assert_eq!(a.1.len(), 2);
        assert_eq!(b.1.len(), 1);
    }
}
