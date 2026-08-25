//! Write-ahead log for Streams (Redis-style append-only logs).
//!
//! Provides crash-recovery by recording all stream mutations to an append-only
//! log file (`streams.wal`). On restart the log is replayed from top to bottom
//! to reconstruct in-memory Stream state.
//!
//! ## Log entry binary format
//! ```text
//! XADD:      [0x01] [stream_name_len: u32 LE] [stream_name: bytes]
//!            [ms: u64 LE] [seq: u64 LE]
//!            [n_fields: u32 LE] [per field: key_len(u32) + key + val_len(u32) + val]
//! SNAPSHOT:  [0x02] [n_streams: u32 LE]
//!            [per stream: name_len(u32) + name + n_entries(u32)
//!             + per entry: ms(u64) + seq(u64) + n_fields(u32)
//!             + per field: key_len(u32) + key + val_len(u32) + val]
//! SNAPSHOT2: [0x03] [n_streams: u32 LE]
//!            [per stream: name_len(u32) + name
//!             + has_max_len(u8) + max_len(u64)
//!             + n_entries(u32) + entries (as SNAPSHOT)
//!             + n_groups(u32) + per group:
//!                 name_len(u32) + name
//!                 + last_delivered_ms(u64) + last_delivered_seq(u64)
//!                 + n_consumers(u32) + per consumer: len(u32) + name
//!                 + n_pending_consumers(u32) + per: len(u32) + name
//!                     + n_ids(u32) + per id: ms(u64) + seq(u64)]
//! XGROUP:    [0x04] [stream_name] [group_name] [start_ms: u64] [start_seq: u64]
//! XREADGROUP:[0x05] [stream_name] [group_name] [consumer]
//!            [last_delivered_ms: u64] [last_delivered_seq: u64]
//!            [n_ids: u32] [per id: ms(u64) + seq(u64)]
//! XACK:      [0x06] [stream_name] [group_name]
//!            [n_ids: u32] [per id: ms(u64) + seq(u64)]
//! XACK_V2:   [0x0B] [stream_name] [group_name]
//!            [n_consumers: u32] [per consumer: len(u32) + name
//!             + n_ids(u32) + per id: ms(u64) + seq(u64)]
//! ```
//!
//! A SNAPSHOT (either version) resets all state. After `checkpoint()` the file
//! is truncated to a single SNAPSHOT2 entry so the log stays small.
//!
//! ## Format evolution (S31-05)
//!
//! Opcodes `0x03`-`0x06` were added after `0x01`/`0x02` shipped, so consumer
//! groups, cursors, pending lists and acks — the one part of stream state a
//! client cannot reconstruct — survive a restart. Compatibility is by
//! **addition only**: `0x01` and `0x02` keep their exact byte layouts, so a
//! log written before groups existed still replays (a `0x02` snapshot simply
//! recovers no groups, which is what that file actually recorded). Nothing
//! reads a version header, because there is none and adding one would break
//! exactly those old files.
//!
//! ## Transaction-tagged records (S63)
//!
//! Opcodes `0x07`-`0x0A` are the `_XACT` twins of the four mutation records,
//! each carrying the coordinating transaction id (`u64 LE`) between the tag
//! and the twin's body. Replay keeps a tagged record only if its id is
//! `XACT_AUTOCOMMIT` (0 — written outside any explicit transaction, whose
//! durability point is this log's own fsync) or appears in the committed set
//! recovered from the SQL side; everything else was written inside a
//! transaction that never committed and is discarded — absence of a commit
//! record means discard, always. The untagged opcodes keep their keep-
//! unconditionally meaning, so pre-S63 logs replay unchanged.
//!
//! The truncation contract in `replay` is preserved too: every new arm either
//! applies whole or abandons the record at `entry_start`, so a torn tail is
//! still truncated to the last valid boundary rather than half-applied. This
//! log carries no checksum, so replay stopping remains the only detection
//! there is — which is also why every count read off disk is bounded by
//! `bounded_by_remaining` before it reaches `Vec::with_capacity`.
//!
//! ## Owner-tagged acks (S31-15)
//!
//! `ENTRY_XACK_V2` (`0x0B`) and its `_XACT` twin (`0x0C`) extend the ack
//! record with the PEL owner of each acked id, grouped per consumer. The
//! owner is known only before the ack removes the entry, and a statement
//! that cannot log it must not perform it, so the executor collects owners,
//! logs this record, and only then removes from the PELs. Replay removes
//! each consumer's ids from exactly that consumer's pending list; the v1
//! record (which recorded no owners) keeps its remove-from-everyone
//! meaning, so pre-S31-15 logs replay unchanged.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use super::{ConsumerGroup, Stream, StreamEntryId};
use crate::executor::enlistment::XACT_AUTOCOMMIT;

// ---- Entry type tags --------------------------------------------------------

const ENTRY_XADD: u8 = 0x01;
const ENTRY_SNAPSHOT: u8 = 0x02;
/// Snapshot carrying consumer groups and `max_len` as well as entries.
const ENTRY_SNAPSHOT_V2: u8 = 0x03;
const ENTRY_XGROUP_CREATE: u8 = 0x04;
const ENTRY_XREADGROUP: u8 = 0x05;
const ENTRY_XACK: u8 = 0x06;
/// S63: XADD carrying the coordinating transaction id. Body after the id is
/// byte-identical to [`ENTRY_XADD`].
const ENTRY_XADD_XACT: u8 = 0x07;
/// S63: consumer-group create carrying the coordinating transaction id.
const ENTRY_XGROUP_CREATE_XACT: u8 = 0x08;
/// S63: group delivery (cursor advance + PEL additions) carrying the
/// coordinating transaction id.
const ENTRY_XREADGROUP_XACT: u8 = 0x09;
/// S63: acknowledgement carrying the coordinating transaction id.
const ENTRY_XACK_XACT: u8 = 0x0A;
/// S31-15: acknowledgement that additionally records, per consumer, the ids
/// that consumer's pending list owned at ack time — the one fact the removal
/// itself destroys. Body layout otherwise follows [`ENTRY_XACK`] with the flat
/// id list replaced by the per-consumer grouping.
const ENTRY_XACK_V2: u8 = 0x0B;
/// S31-15: owner-carrying acknowledgement with the coordinating transaction
/// id. Body after the id is byte-identical to [`ENTRY_XACK_V2`].
const ENTRY_XACK_V2_XACT: u8 = 0x0C;

// ---- Public types -----------------------------------------------------------

/// One stream entry: its ID plus ordered field/value pairs.
pub type StreamEntry = (StreamEntryId, Vec<(String, String)>);

/// Per-stream recovered entries, keyed by stream name.
pub type StreamsMap = HashMap<String, Vec<StreamEntry>>;

/// Per-stream recovered consumer groups: `stream_name -> group_name -> group`.
pub type StreamGroupsMap = HashMap<String, HashMap<String, ConsumerGroup>>;

/// Recovered streams state from WAL replay.
#[derive(Default)]
pub struct StreamsWalState {
    /// `stream_name -> Vec<(entry_id, fields)>` in order.
    pub streams: StreamsMap,
    /// `stream_name -> group_name -> group` (cursor, consumers, pending list).
    ///
    /// A stream can appear here and not in `streams` (a group created on a
    /// stream that has no entries yet) and vice versa.
    pub groups: StreamGroupsMap,
    /// `stream_name -> max_len` for streams that carry a cap.
    pub max_len: HashMap<String, usize>,
    /// The highest coordinating transaction id seen on a tagged record,
    /// whether that record was kept or discarded. Seeds the XactId
    /// high-water mark at executor construction (S63): a reopened process
    /// must never mint an id that a surviving tagged record already carries,
    /// or the recovery filter could resurrect stale records by matching a
    /// fresh transaction against them.
    pub max_xact_id: u64,
}

/// Append-only Streams WAL.
pub struct StreamsWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Group-commit fsync coordinator (durability of the un-checkpointed tail).
    syncer: crate::storage::wal_util::WalSync,
    /// The writer holds an inode a checkpoint's rename displaced: it is
    /// unlinked, so appends to it "succeed" into a file no recovery reads
    /// while `group_sync`/`is_dirty` report healthy. Set when a checkpoint
    /// replaced the log but its reopen failed; cleared by the next successful
    /// reattach (or checkpoint reopen). See `reattach_if_stranded`.
    stranded: std::sync::atomic::AtomicBool,
    /// Test-only append fault switch; see `append`.
    #[cfg(test)]
    fail_appends: std::sync::atomic::AtomicBool,
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint`.
    #[cfg(test)]
    fail_reopen_once: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl StreamsWal {
    /// Make every subsequent `append` fail with ENOSPC. Test-only.
    pub fn set_fail_appends(&self, on: bool) {
        self.fail_appends
            .store(on, std::sync::atomic::Ordering::SeqCst);
    }
}

impl StreamsWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// `committed` is the set of coordinating transaction ids that durably
    /// committed on the SQL side (S63); a tagged record whose id is neither in
    /// it nor `XACT_AUTOCOMMIT` is discarded — its transaction never
    /// committed, and absence of a commit record means discard, always.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. A torn or corrupt tail ends replay and is truncated
    /// away, so subsequent appends land on a valid boundary (they would
    /// otherwise sit behind garbage and be lost to every future replay — this
    /// log carries no checksum, so replay stopping is the only detection there
    /// is). Same treatment as `blob/wal.rs::open`.
    pub fn open(dir: &Path, committed: &HashSet<u64>) -> io::Result<(Self, StreamsWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("streams.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            let (state, valid_end) = replay(&data, committed);
            if valid_end < data.len() {
                eprintln!(
                    "streams WAL: truncating {} torn/corrupt trailing bytes",
                    data.len() - valid_end
                );
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(valid_end as u64)?;
                f.sync_all()?;
            }
            state
        } else {
            StreamsWalState::default()
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: crate::storage::wal_util::WalSync::new(),
                stranded: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                fail_appends: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                fail_reopen_once: std::sync::atomic::AtomicBool::new(false),
            },
            state,
        ))
    }

    /// Log an XADD operation (stream append).
    ///
    /// `xact` is the coordinating transaction id the record is tagged with:
    /// `Some(XACT_AUTOCOMMIT)` for a write outside any explicit transaction,
    /// `Some(id)` inside one, `None` to write the legacy untagged record
    /// (kept unconditionally on replay — the pre-S63 compatibility rule).
    pub fn log_xadd(
        &self,
        xact: Option<u64>,
        stream_name: &str,
        entry_id: &StreamEntryId,
        fields: &[(String, String)],
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        match xact {
            Some(x) => {
                buf.push(ENTRY_XADD_XACT);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            None => buf.push(ENTRY_XADD),
        }

        // stream name
        write_str(&mut buf, stream_name);

        // entry ID (ms + seq)
        buf.extend_from_slice(&entry_id.ms.to_le_bytes());
        buf.extend_from_slice(&entry_id.seq.to_le_bytes());

        // fields
        buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
        for (k, v) in fields {
            write_str(&mut buf, k);
            write_str(&mut buf, v);
        }

        self.append(&buf)
    }

    /// Log the creation (or idempotent re-creation) of a consumer group.
    ///
    /// Group state is the one part of a stream a client cannot rebuild for
    /// itself: entries are re-readable, a cursor is not. Before this existed
    /// (S31-05) a restart dropped every group, and `STREAM_XREADGROUP` against
    /// the vanished group returned an empty result — indistinguishable from
    /// "caught up", so a consumer silently skipped everything it had not yet
    /// processed instead of failing.
    ///
    /// `xact` mirrors [`StreamsWal::log_xadd`].
    pub fn log_xgroup_create(
        &self,
        xact: Option<u64>,
        stream_name: &str,
        group: &str,
        start_id: &StreamEntryId,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        match xact {
            Some(x) => {
                buf.push(ENTRY_XGROUP_CREATE_XACT);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            None => buf.push(ENTRY_XGROUP_CREATE),
        }
        write_str(&mut buf, stream_name);
        write_str(&mut buf, group);
        buf.extend_from_slice(&start_id.ms.to_le_bytes());
        buf.extend_from_slice(&start_id.seq.to_le_bytes());
        self.append(&buf)
    }

    /// Log a group delivery: the advanced cursor, the consumer that claimed the
    /// entries, and the ids added to that consumer's pending list (the PEL).
    ///
    /// `xact` mirrors [`StreamsWal::log_xadd`].
    pub fn log_xreadgroup(
        &self,
        xact: Option<u64>,
        stream_name: &str,
        group: &str,
        consumer: &str,
        last_delivered: &StreamEntryId,
        delivered: &[StreamEntryId],
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        match xact {
            Some(x) => {
                buf.push(ENTRY_XREADGROUP_XACT);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            None => buf.push(ENTRY_XREADGROUP),
        }
        write_str(&mut buf, stream_name);
        write_str(&mut buf, group);
        write_str(&mut buf, consumer);
        buf.extend_from_slice(&last_delivered.ms.to_le_bytes());
        buf.extend_from_slice(&last_delivered.seq.to_le_bytes());
        write_ids(&mut buf, delivered);
        self.append(&buf)
    }

    /// Log an acknowledgement: the ids leave the group's pending list.
    ///
    /// Legacy v1 record: it names no owner, so replay removes the ids from
    /// every consumer's pending list. New writers should prefer
    /// [`StreamsWal::log_xack_owned`], which records the owner per id; this
    /// one is kept so a pre-S31-15 log keeps replaying (and keeps being
    /// testable) byte-for-byte.
    ///
    /// `xact` mirrors [`StreamsWal::log_xadd`].
    pub fn log_xack(
        &self,
        xact: Option<u64>,
        stream_name: &str,
        group: &str,
        ids: &[StreamEntryId],
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        match xact {
            Some(x) => {
                buf.push(ENTRY_XACK_XACT);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            None => buf.push(ENTRY_XACK),
        }
        write_str(&mut buf, stream_name);
        write_str(&mut buf, group);
        write_ids(&mut buf, ids);
        self.append(&buf)
    }

    /// Log an acknowledgement recording, per consumer, the ids that
    /// consumer's pending list owned at ack time (S31-15).
    ///
    /// The owner is only knowable before the ack removes the entry, which is
    /// why the caller collects `owners` first, logs this record, and only
    /// then mutates the PELs — on an append failure the statement fails and
    /// nothing has to be restored. Replay removes each consumer's ids from
    /// exactly that consumer's pending list.
    ///
    /// `xact` mirrors [`StreamsWal::log_xadd`].
    pub fn log_xack_owned(
        &self,
        xact: Option<u64>,
        stream_name: &str,
        group: &str,
        owners: &[(String, Vec<StreamEntryId>)],
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        match xact {
            Some(x) => {
                buf.push(ENTRY_XACK_V2_XACT);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            None => buf.push(ENTRY_XACK_V2),
        }
        write_str(&mut buf, stream_name);
        write_str(&mut buf, group);
        buf.extend_from_slice(&(owners.len() as u32).to_le_bytes());
        for (consumer, ids) in owners {
            write_str(&mut buf, consumer);
            write_ids(&mut buf, ids);
        }
        self.append(&buf)
    }

    /// Append one complete, self-contained record.
    ///
    /// Records are built in full in memory first so a single `write_all` puts
    /// the whole record in the page cache: a partial record can only ever be a
    /// torn *tail*, which `open` truncates, never a hole in the middle.
    fn append(&self, buf: &[u8]) -> io::Result<()> {
        if let Some(e) = crate::storage::crashpoint::io_fault("streams.wal_append") {
            return Err(e);
        }
        // In-process arming for unit tests. The `NUCLEUS_IOFAULT` machinery
        // above reads its environment through a `OnceLock` initialised by
        // whichever call site runs first, so it cannot be armed from inside a
        // shared test binary — only from a freshly spawned process.
        #[cfg(test)]
        if self.fail_appends.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "injected streams WAL append failure",
            ));
        }
        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        w.write_all(buf)?;
        w.flush()?;
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
        if !self.stranded.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        if let Some(e) = crate::storage::crashpoint::io_fault("streams.wal_reopen") {
            return Err(e);
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "streams WAL writer is stranded: a checkpoint replaced {} but its \
                         reopen failed; refusing to append to the unlinked old file ({e})",
                        self.path.display()
                    ),
                )
            })?;
        *w = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Flush + `fsync` the log, capturing (under the writer lock) the highest
    /// append LSN the fsync covers.
    fn sync_covering(&self) -> io::Result<u64> {
        let mut w = self.writer.lock();
        let covered = self.syncer.current();
        w.flush()?;
        w.get_ref().sync_all()?;
        Ok(covered)
    }

    /// Group-commit sync: durable coverage of every append made before this
    /// call; concurrent committers share fsyncs.
    pub fn group_sync(&self) -> io::Result<()> {
        self.syncer.group_sync(|| self.sync_covering())
    }

    /// Whether appends exist that no completed fsync covers yet.
    pub fn is_dirty(&self) -> bool {
        self.syncer.is_dirty()
    }

    /// Write a full snapshot and truncate the log to just that snapshot.
    ///
    /// `streams` maps stream name to its current state — entries, consumer
    /// groups and `max_len`. Everything the in-memory `Stream` holds is written,
    /// so the snapshot is a complete replacement for the log it replaces; a
    /// checkpoint that dropped group state would silently un-persist the very
    /// records `log_xgroup_create`/`log_xreadgroup`/`log_xack` just wrote.
    ///
    /// Written with the `SNAPSHOT2` opcode. Iteration is sorted throughout so
    /// checkpointing unchanged state produces identical bytes.
    pub fn checkpoint(&self, streams: &HashMap<String, Stream>) -> io::Result<()> {
        let mut payload = Vec::new();

        // n_streams
        payload.extend_from_slice(&(streams.len() as u32).to_le_bytes());

        let mut names: Vec<&String> = streams.keys().collect();
        names.sort();
        for name in names {
            let stream = &streams[name];
            // stream name
            write_str(&mut payload, name);

            // max_len (flagged, so `Some(0)` and `None` stay distinct)
            match stream.max_len {
                Some(n) => {
                    payload.push(1);
                    payload.extend_from_slice(&(n as u64).to_le_bytes());
                }
                None => {
                    payload.push(0);
                    payload.extend_from_slice(&0u64.to_le_bytes());
                }
            }

            // n_entries
            payload.extend_from_slice(&(stream.entries.len() as u32).to_le_bytes());

            for entry in &stream.entries {
                // entry ID
                payload.extend_from_slice(&entry.id.ms.to_le_bytes());
                payload.extend_from_slice(&entry.id.seq.to_le_bytes());

                // fields
                payload.extend_from_slice(&(entry.fields.len() as u32).to_le_bytes());
                for (k, v) in &entry.fields {
                    write_str(&mut payload, k);
                    write_str(&mut payload, v);
                }
            }

            // consumer groups
            payload.extend_from_slice(&(stream.groups.len() as u32).to_le_bytes());
            let mut group_names: Vec<&String> = stream.groups.keys().collect();
            group_names.sort();
            for gname in group_names {
                let g = &stream.groups[gname];
                write_str(&mut payload, &g.name);
                payload.extend_from_slice(&g.last_delivered_id.ms.to_le_bytes());
                payload.extend_from_slice(&g.last_delivered_id.seq.to_le_bytes());

                let mut consumers: Vec<&String> = g.consumers.iter().collect();
                consumers.sort();
                payload.extend_from_slice(&(consumers.len() as u32).to_le_bytes());
                for c in consumers {
                    write_str(&mut payload, c);
                }

                let mut pending: Vec<(&String, &Vec<StreamEntryId>)> = g.pending.iter().collect();
                pending.sort_by(|a, b| a.0.cmp(b.0));
                payload.extend_from_slice(&(pending.len() as u32).to_le_bytes());
                for (consumer, ids) in pending {
                    write_str(&mut payload, consumer);
                    write_ids(&mut payload, ids);
                }
            }
        }

        // Serialize the complete new log body (SNAPSHOT2 tag + payload).
        let mut contents = Vec::with_capacity(payload.len() + 1);
        contents.push(ENTRY_SNAPSHOT_V2);
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
            .swap(false, std::sync::atomic::Ordering::AcqRel)
            .then(|| io::Error::other("injected streams WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = if let Some(e) = injected {
            Err(e)
        } else if let Some(e) = crate::storage::crashpoint::io_fault("streams.wal_reopen") {
            Err(e)
        } else {
            OpenOptions::new().append(true).open(&self.path)
        };
        let file = match file {
            Ok(f) => f,
            Err(e) => {
                // The rename already happened, so the handle in `w` is now an
                // unlinked inode. Mark the writer stranded: appends must
                // reattach (or fail loudly), never write through it.
                self.stranded
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(e);
            }
        };
        *w = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
        // The snapshot was fsync'd by `atomic_replace_wal`; count it as covered.
        let mark = self.syncer.on_append();
        self.syncer.mark_synced(mark);
        Ok(())
    }
}

/// Reconstruct in-memory Streams from recovered WAL state.
///
/// Call this after `StreamsWal::open()` to rebuild the `HashMap<String, Stream>`.
pub fn rebuild_streams(state: &StreamsWalState) -> HashMap<String, Stream> {
    let mut result: HashMap<String, Stream> = HashMap::new();
    for (name, entries) in &state.streams {
        let mut stream = Stream::new();
        // Set the cap before replaying, so the recovered stream trims exactly
        // where the live one did rather than exceeding its own max_len.
        stream.max_len = state.max_len.get(name).copied();
        for (id, fields) in entries {
            stream.xadd_with_id(id.clone(), fields.clone());
        }
        result.insert(name.clone(), stream);
    }
    // A group can exist on a stream with no entries (created, never written to,
    // or drained by a trim), so groups drive their own pass rather than riding
    // along with entries.
    for (name, groups) in &state.groups {
        let stream = result.entry(name.clone()).or_insert_with(|| {
            let mut s = Stream::new();
            s.max_len = state.max_len.get(name).copied();
            s
        });
        stream.groups = groups.clone();
    }
    result
}

// ---- Binary encoding helpers ------------------------------------------------

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

fn write_ids(buf: &mut Vec<u8>, ids: &[StreamEntryId]) {
    buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for id in ids {
        buf.extend_from_slice(&id.ms.to_le_bytes());
        buf.extend_from_slice(&id.seq.to_le_bytes());
    }
}

// ---- Replay -----------------------------------------------------------------

/// Replay all entries in `data`. Returns the recovered state and the byte
/// offset of the first torn/corrupt entry (== `data.len()` when fully valid).
///
/// `committed` is the set of coordinating transaction ids that durably
/// committed on the SQL side. A `_XACT` record whose id is `XACT_AUTOCOMMIT`
/// or in the set is applied exactly like its untagged twin; any other id is
/// parsed (so the next record's boundary is still found — this format is not
/// length-framed) and then discarded: its transaction never committed, and
/// absence of a commit record means discard, always. Ids seen on tagged
/// records feed `max_xact_id` whether or not the record is kept, so the
/// caller can seed the id high-water mark.
///
/// No arm here half-applies: the XADD arm pushes only after every field parses,
/// and the SNAPSHOT arm builds a temporary map and swaps it in only on success.
/// So the state accumulated when an entry is abandoned already equals a replay
/// of the clean prefix, and `entry_start` is the truncation point.
fn replay(data: &[u8], committed: &HashSet<u64>) -> (StreamsWalState, usize) {
    let mut streams: StreamsMap = HashMap::new();
    let mut groups: StreamGroupsMap = HashMap::new();
    let mut max_len: HashMap<String, usize> = HashMap::new();
    let mut max_xact_id: u64 = 0;
    let mut pos = 0usize;

    while pos < data.len() {
        let entry_start = pos;
        macro_rules! torn {
            () => {{
                return (
                    StreamsWalState {
                        streams,
                        groups,
                        max_len,
                        max_xact_id,
                    },
                    entry_start,
                );
            }};
        }

        let Some(&entry_type) = data.get(pos) else {
            torn!();
        };
        pos += 1;

        // The tagged arms parse their id, then share the body parse with the
        // untagged twin. `keep_tagged` is the S63 filter in one expression: an
        // autocommit record is durable by its own fsync, a committed id was
        // vouched for by a durable COMMIT record, anything else never
        // happened. Parsing continues either way — the record must be fully
        // consumed to find the next one, since nothing length-frames these.
        let mut keep_tagged = true;
        if matches!(
            entry_type,
            ENTRY_XADD_XACT
                | ENTRY_XGROUP_CREATE_XACT
                | ENTRY_XREADGROUP_XACT
                | ENTRY_XACK_XACT
                | ENTRY_XACK_V2_XACT
        ) {
            let Some(xact) = read_u64(data, &mut pos) else {
                torn!();
            };
            max_xact_id = max_xact_id.max(xact);
            keep_tagged = xact == XACT_AUTOCOMMIT || committed.contains(&xact);
        }

        match entry_type {
            ENTRY_XADD | ENTRY_XADD_XACT => {
                let Some(stream_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(ms) = read_u64(data, &mut pos) else {
                    torn!();
                };
                let Some(seq) = read_u64(data, &mut pos) else {
                    torn!();
                };
                let Some(n_fields) = read_u32(data, &mut pos) else {
                    torn!();
                };
                // `n_fields` comes off disk and this WAL carries NO checksum, so
                // nothing rejects a corrupt length before it reaches here. An
                // unbounded `with_capacity` ABORTS the process on Linux (an
                // allocation failure is not an `Err`) and silently succeeds on an
                // overcommitting macOS. Each field costs at least two 4-byte
                // length prefixes, so the bytes remaining are an exact bound.
                let mut fields =
                    Vec::with_capacity(bounded_by_remaining(data, pos, n_fields as usize, 8));
                let mut ok = true;
                for _ in 0..n_fields {
                    let Some(k) = read_string(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(v) = read_string(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    fields.push((k, v));
                }
                if !ok {
                    torn!();
                }
                if keep_tagged {
                    streams
                        .entry(stream_name)
                        .or_default()
                        .push((StreamEntryId::new(ms, seq), fields));
                }
            }
            ENTRY_SNAPSHOT | ENTRY_SNAPSHOT_V2 => {
                // Parse into a temporary map and only swap it in once the snapshot
                // parses completely. Clearing first meant a corrupt/truncated
                // snapshot wiped all already-recovered state before failing.
                //
                // `ENTRY_SNAPSHOT` (0x02) is the pre-groups layout and is still
                // accepted verbatim: a log written before S31-05 must replay.
                // It recovers no groups and no max_len, which is exactly what
                // that file recorded — and a snapshot resets ALL state, so the
                // group maps are replaced (emptied), not merged into.
                let with_groups = entry_type == ENTRY_SNAPSHOT_V2;
                let mut snapshot = StreamsWalState::default();
                if replay_snapshot(data, &mut pos, &mut snapshot, with_groups) {
                    streams = snapshot.streams;
                    groups = snapshot.groups;
                    max_len = snapshot.max_len;
                } else {
                    torn!();
                }
            }
            ENTRY_XGROUP_CREATE | ENTRY_XGROUP_CREATE_XACT => {
                let Some(stream_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(group_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(ms) = read_u64(data, &mut pos) else {
                    torn!();
                };
                let Some(seq) = read_u64(data, &mut pos) else {
                    torn!();
                };
                if !keep_tagged {
                    continue;
                }
                // Last-wins, matching `Stream::xgroup_recreate`. Replay must
                // apply this record unconditionally: it exists only because a
                // live create succeeded, and the only way a second record for
                // a live group can now be written is an explicit recreate
                // (S31-11), which did reset the cursor and drop the PEL.
                groups.entry(stream_name).or_default().insert(
                    group_name.clone(),
                    ConsumerGroup {
                        name: group_name,
                        last_delivered_id: StreamEntryId::new(ms, seq),
                        pending: HashMap::new(),
                        consumers: HashSet::new(),
                    },
                );
            }
            ENTRY_XREADGROUP | ENTRY_XREADGROUP_XACT => {
                let Some(stream_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(group_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(consumer) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(ms) = read_u64(data, &mut pos) else {
                    torn!();
                };
                let Some(seq) = read_u64(data, &mut pos) else {
                    torn!();
                };
                let Some(ids) = read_ids(data, &mut pos) else {
                    torn!();
                };
                if !keep_tagged {
                    continue;
                }
                // A delivery against a group the log never created cannot be
                // applied to anything; skip it rather than inventing a group
                // with a cursor nobody chose.
                if let Some(g) = groups
                    .get_mut(&stream_name)
                    .and_then(|m| m.get_mut(&group_name))
                {
                    g.last_delivered_id = StreamEntryId::new(ms, seq);
                    g.consumers.insert(consumer.clone());
                    g.pending.entry(consumer).or_default().extend(ids);
                }
            }
            ENTRY_XACK | ENTRY_XACK_XACT => {
                let Some(stream_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(group_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(ids) = read_ids(data, &mut pos) else {
                    torn!();
                };
                if !keep_tagged {
                    continue;
                }
                if let Some(g) = groups
                    .get_mut(&stream_name)
                    .and_then(|m| m.get_mut(&group_name))
                {
                    for pending in g.pending.values_mut() {
                        pending.retain(|id| !ids.contains(id));
                    }
                }
            }
            ENTRY_XACK_V2 | ENTRY_XACK_V2_XACT => {
                let Some(stream_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(group_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(n_consumers) = read_u32(data, &mut pos) else {
                    torn!();
                };
                // Off-disk count, unchecksummed file: bound it by the bytes
                // actually present. A consumer section costs at least its two
                // 4-byte length prefixes (name and id count).
                let mut owners =
                    Vec::with_capacity(bounded_by_remaining(data, pos, n_consumers as usize, 8));
                let mut ok = true;
                for _ in 0..n_consumers {
                    let Some(consumer) = read_string(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(ids) = read_ids(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    owners.push((consumer, ids));
                }
                if !ok {
                    torn!();
                }
                if !keep_tagged {
                    continue;
                }
                if let Some(g) = groups
                    .get_mut(&stream_name)
                    .and_then(|m| m.get_mut(&group_name))
                {
                    for (consumer, ids) in owners {
                        if let Some(pending) = g.pending.get_mut(&consumer) {
                            pending.retain(|id| !ids.contains(id));
                        }
                    }
                }
            }
            _ => {
                // Unknown entry type -- corrupt data; keep the clean prefix.
                torn!();
            }
        }
    }

    (
        StreamsWalState {
            streams,
            groups,
            max_len,
            max_xact_id,
        },
        pos,
    )
}

/// Parse a snapshot body into `out`.
///
/// `with_groups` selects the layout: `false` is the original `ENTRY_SNAPSHOT`
/// (0x02) body, `true` the `ENTRY_SNAPSHOT_V2` (0x03) body that additionally
/// carries `max_len` and consumer groups per stream. The two differ only by
/// added fields, so the old parse is the new one with those reads skipped.
fn replay_snapshot(
    data: &[u8],
    pos: &mut usize,
    out: &mut StreamsWalState,
    with_groups: bool,
) -> bool {
    let Some(n_streams) = read_u32(data, pos) else {
        return false;
    };
    for _ in 0..n_streams as usize {
        let Some(name) = read_string(data, pos) else {
            return false;
        };
        if with_groups {
            let Some(&flag) = data.get(*pos) else {
                return false;
            };
            *pos += 1;
            let Some(cap) = read_u64(data, pos) else {
                return false;
            };
            if flag == 1 {
                out.max_len.insert(name.clone(), cap as usize);
            }
        }
        let Some(n_entries) = read_u32(data, pos) else {
            return false;
        };
        // Off-disk count, unchecksummed file: bound it by the bytes actually
        // present. An entry costs at least ms(8) + seq(8) + n_fields(4).
        let mut entries =
            Vec::with_capacity(bounded_by_remaining(data, *pos, n_entries as usize, 20));
        for _ in 0..n_entries as usize {
            let Some(ms) = read_u64(data, pos) else {
                return false;
            };
            let Some(seq) = read_u64(data, pos) else {
                return false;
            };
            let Some(n_fields) = read_u32(data, pos) else {
                return false;
            };
            // Same: each field costs at least two 4-byte length prefixes.
            let mut fields =
                Vec::with_capacity(bounded_by_remaining(data, *pos, n_fields as usize, 8));
            for _ in 0..n_fields as usize {
                let Some(k) = read_string(data, pos) else {
                    return false;
                };
                let Some(v) = read_string(data, pos) else {
                    return false;
                };
                fields.push((k, v));
            }
            entries.push((StreamEntryId::new(ms, seq), fields));
        }
        if with_groups {
            let Some(n_groups) = read_u32(data, pos) else {
                return false;
            };
            let mut stream_groups: HashMap<String, ConsumerGroup> = HashMap::new();
            for _ in 0..n_groups as usize {
                let Some(gname) = read_string(data, pos) else {
                    return false;
                };
                let Some(last_ms) = read_u64(data, pos) else {
                    return false;
                };
                let Some(last_seq) = read_u64(data, pos) else {
                    return false;
                };
                let Some(n_consumers) = read_u32(data, pos) else {
                    return false;
                };
                let mut consumers = HashSet::new();
                for _ in 0..n_consumers as usize {
                    let Some(c) = read_string(data, pos) else {
                        return false;
                    };
                    consumers.insert(c);
                }
                let Some(n_pending) = read_u32(data, pos) else {
                    return false;
                };
                let mut pending: HashMap<String, Vec<StreamEntryId>> = HashMap::new();
                for _ in 0..n_pending as usize {
                    let Some(c) = read_string(data, pos) else {
                        return false;
                    };
                    let Some(ids) = read_ids(data, pos) else {
                        return false;
                    };
                    pending.insert(c, ids);
                }
                stream_groups.insert(
                    gname.clone(),
                    ConsumerGroup {
                        name: gname,
                        last_delivered_id: StreamEntryId::new(last_ms, last_seq),
                        pending,
                        consumers,
                    },
                );
            }
            if !stream_groups.is_empty() {
                out.groups.insert(name.clone(), stream_groups);
            }
        }
        out.streams.insert(name, entries);
    }
    true
}

// ---- Primitive readers ------------------------------------------------------

/// Bound a declared element count by the bytes actually left in `data`.
///
/// `min_elem_bytes` is the smallest number of bytes one element can possibly
/// occupy, so `remaining / min_elem_bytes` is a hard upper bound on how many
/// elements this buffer can really contain. Reserving that instead of the
/// declared count cannot over-reserve at all, and the caller's loop still stops
/// (and fails) the moment a read runs off the end — so a corrupt count yields a
/// clean failure rather than a `handle_alloc_error` abort.
fn bounded_by_remaining(data: &[u8], pos: usize, declared: usize, min_elem_bytes: usize) -> usize {
    declared.min(data.len().saturating_sub(pos) / min_elem_bytes)
}

/// Read a length-prefixed run of entry ids.
///
/// The count is off-disk and this log has no checksum, so it goes through
/// `bounded_by_remaining` before `with_capacity` — an id costs 16 bytes
/// (ms + seq), which is an exact bound on how many the buffer can hold.
fn read_ids(data: &[u8], pos: &mut usize) -> Option<Vec<StreamEntryId>> {
    let n = read_u32(data, pos)?;
    let mut ids = Vec::with_capacity(bounded_by_remaining(data, *pos, n as usize, 16));
    for _ in 0..n as usize {
        let ms = read_u64(data, pos)?;
        let seq = read_u64(data, pos)?;
        ids.push(StreamEntryId::new(ms, seq));
    }
    Some(ids)
}

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
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

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── S63: the recovery filter ──────────────────────────────────────────

    /// One buffered log exercising every filter decision at once: legacy and
    /// autocommit records keep, committed ids keep, unknown ids discard —
    /// and a discarded record in the MIDDLE does not stop the records after
    /// it (they are parsed past, not abandoned).
    #[test]
    fn tagged_records_filter_on_the_committed_set() {
        let mut buf = Vec::new();
        // Legacy untagged XADD (pre-S63 log): keep unconditionally.
        buf.push(ENTRY_XADD);
        push_str_field(&mut buf, "legacy");
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        push_str_field(&mut buf, "k");
        push_str_field(&mut buf, "legacy-v");
        // Tagged autocommit (0): keep.
        let xadd_tagged = |buf: &mut Vec<u8>, xact: u64, name: &str, val: &str| {
            buf.push(ENTRY_XADD_XACT);
            buf.extend_from_slice(&xact.to_le_bytes());
            push_str_field(buf, name);
            buf.extend_from_slice(&1u64.to_le_bytes());
            buf.extend_from_slice(&0u64.to_le_bytes());
            buf.extend_from_slice(&1u32.to_le_bytes());
            push_str_field(buf, "k");
            push_str_field(buf, val);
        };
        xadd_tagged(&mut buf, 0, "s", "auto");
        xadd_tagged(&mut buf, 7, "s", "committed");
        xadd_tagged(&mut buf, 8, "s", "never-committed"); // discarded, mid-log
        xadd_tagged(&mut buf, 9, "s", "committed-late");

        let committed: HashSet<u64> = [7u64, 9u64].into_iter().collect();
        let (state, valid_end) = replay(&buf, &committed);
        assert_eq!(valid_end, buf.len(), "every record parses");
        assert_eq!(
            state.max_xact_id, 9,
            "discarded records still feed the floor"
        );
        let entries = &state.streams["s"];
        assert_eq!(entries.len(), 3, "auto + 7 + 9; 8 is discarded");
        let vals: Vec<&str> = entries.iter().map(|(_, f)| f[0].1.as_str()).collect();
        assert_eq!(vals, vec!["auto", "committed", "committed-late"]);
        assert!(state.streams.contains_key("legacy"));
    }

    /// Group records filter the same way: an uncommitted group's create is
    /// discarded, so its later deliveries (if any) find no group to touch.
    #[test]
    fn tagged_group_records_filter_on_the_committed_set() {
        let mut buf = Vec::new();
        // Group created inside txn 5, which never committed.
        buf.push(ENTRY_XGROUP_CREATE_XACT);
        buf.extend_from_slice(&5u64.to_le_bytes());
        push_str_field(&mut buf, "s");
        push_str_field(&mut buf, "g");
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        // Same group, committed txn 6: the group exists.
        buf.push(ENTRY_XGROUP_CREATE_XACT);
        buf.extend_from_slice(&6u64.to_le_bytes());
        push_str_field(&mut buf, "s");
        push_str_field(&mut buf, "g2");
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());

        let committed: HashSet<u64> = [6u64].into_iter().collect();
        let (state, valid_end) = replay(&buf, &committed);
        assert_eq!(valid_end, buf.len());
        assert!(
            !state.groups.get("s").is_some_and(|m| m.contains_key("g")),
            "the uncommitted group must not be resurrected"
        );
        assert!(
            state.groups.get("s").is_some_and(|m| m.contains_key("g2")),
            "the committed group must exist"
        );
    }

    // ── Unbounded-preallocation class (NU-385) ──
    //
    // These counts are `u32`s read straight out of the WAL, and this file
    // carries NO checksum, so nothing rejects a bad length before the decoder
    // sees it. Handing one to `Vec::with_capacity` reserves it, and a Rust
    // allocation failure ABORTS the process (SIGABRT, no unwind, no `Err`, no
    // log) on Linux while silently succeeding on an overcommitting macOS — a
    // boot crash-loop from one corrupt file. An instrumented allocator recorded
    // peak single reservations of 206.2 GB / 171.8 GB / 206.2 GB here for
    // `u32::MAX` counts. Round-tripping honest data does NOT cover this, which
    // is why the class survived: each test below hands the replayer a count the
    // bytes cannot back and requires a refusal.

    fn push_str_field(buf: &mut Vec<u8>, s: &str) {
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    }

    /// The bound itself: a `u32::MAX` count against a near-empty buffer must
    /// collapse to a handful of elements, never to the declared count.
    #[test]
    fn declared_count_is_bounded_by_bytes_present() {
        let data = [0u8; 16];
        assert_eq!(bounded_by_remaining(&data, 0, u32::MAX as usize, 8), 2);
        assert_eq!(bounded_by_remaining(&data, 0, u32::MAX as usize, 20), 0);
        // Past the end must saturate, not underflow.
        assert_eq!(bounded_by_remaining(&data, 999, u32::MAX as usize, 8), 0);
        // An honest count under the bound is passed through untouched.
        assert_eq!(bounded_by_remaining(&data, 0, 1, 8), 1);
    }

    /// XADD arm of `replay`: `n_fields` = `u32::MAX` with no fields behind it.
    #[test]
    fn xadd_absurd_field_count_is_refused_not_reserved() {
        let mut buf = vec![ENTRY_XADD];
        push_str_field(&mut buf, "events");
        buf.extend_from_slice(&1000u64.to_le_bytes()); // ms
        buf.extend_from_slice(&0u64.to_le_bytes()); // seq
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // n_fields, a lie
        let (state, _) = replay(&buf, &HashSet::new());
        assert!(
            state.streams.is_empty(),
            "a field count the bytes cannot back must abandon the entry"
        );
    }

    /// `replay_snapshot`: `n_entries` = `u32::MAX` with no entries behind it.
    #[test]
    fn snapshot_absurd_entry_count_is_refused_not_reserved() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // n_streams
        push_str_field(&mut buf, "events");
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // n_entries, a lie
        let mut pos = 0usize;
        let mut out = StreamsWalState::default();
        assert!(
            !replay_snapshot(&buf, &mut pos, &mut out, false),
            "an entry count the bytes cannot back must fail the snapshot"
        );
        assert!(out.streams.is_empty());
    }

    /// `replay_snapshot`, per-entry: `n_fields` = `u32::MAX` inside an
    /// otherwise well-formed entry.
    #[test]
    fn snapshot_absurd_per_entry_field_count_is_refused_not_reserved() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // n_streams
        push_str_field(&mut buf, "events");
        buf.extend_from_slice(&1u32.to_le_bytes()); // n_entries, honest
        buf.extend_from_slice(&1000u64.to_le_bytes()); // ms
        buf.extend_from_slice(&0u64.to_le_bytes()); // seq
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // n_fields, a lie
        let mut pos = 0usize;
        let mut out = StreamsWalState::default();
        assert!(
            !replay_snapshot(&buf, &mut pos, &mut out, false),
            "a per-entry field count the bytes cannot back must fail the snapshot"
        );
        assert!(out.streams.is_empty());
    }

    /// A whole snapshot entry reached through `replay`, so the corrupt count
    /// must not wipe state already recovered from the entries before it.
    #[test]
    fn corrupt_snapshot_count_does_not_discard_recovered_state() {
        let mut buf = vec![ENTRY_XADD];
        push_str_field(&mut buf, "events");
        buf.extend_from_slice(&1000u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        push_str_field(&mut buf, "k");
        push_str_field(&mut buf, "v");
        // Now a snapshot whose stream count is a lie.
        buf.push(ENTRY_SNAPSHOT);
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        let (state, _) = replay(&buf, &HashSet::new());
        assert_eq!(state.streams.len(), 1);
        assert_eq!(state.streams["events"].len(), 1);
    }

    #[test]
    fn group_sync_marks_clean() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        assert!(!wal.is_dirty(), "a fresh WAL has no un-fsynced appends");
        wal.log_xadd(
            None,
            "s",
            &StreamEntryId::new(1, 0),
            &[("k".into(), "v".into())],
        )
        .unwrap();
        assert!(wal.is_dirty(), "an append is uncovered until fsync");
        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "group_sync fsyncs the tail");
    }

    /// S31-14: a checkpoint whose reopen fails must not leave the writer
    /// appending into the unlinked inode the rename displaced. Those appends
    /// report success while no future recovery can ever read them, so an
    /// acknowledged entry silently vanishes at restart. The discriminator is
    /// durability: the post-failure append must land in the replaced file.
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_xadd(
                None,
                "s",
                &StreamEntryId::new(1, 0),
                &[("k".into(), "before".into())],
            )
            .unwrap();
            let mut stream = Stream::new();
            stream.xadd_with_id(
                StreamEntryId::new(1, 0),
                vec![("k".into(), "before".into())],
            );
            let mut streams = HashMap::new();
            streams.insert("s".to_string(), stream);
            wal.fail_reopen_once
                .store(true, std::sync::atomic::Ordering::SeqCst);
            wal.checkpoint(&streams)
                .expect_err("the injected reopen failure must fail the checkpoint");
            wal.log_xadd(
                None,
                "s",
                &StreamEntryId::new(2, 0),
                &[("k".into(), "after".into())],
            )
            .expect("a later append must reattach, not strand");
        }
        let (_, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let entries = state.streams.get("s").expect("stream survived");
        assert_eq!(
            entries.len(),
            2,
            "the post-checkpoint-failure append went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
        assert_eq!(entries[1].0, StreamEntryId::new(2, 0));
    }

    #[test]
    fn test_xadd_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        assert!(state.streams.is_empty());

        wal.log_xadd(
            None,
            "events",
            &StreamEntryId::new(1000, 0),
            &[
                ("user".into(), "alice".into()),
                ("action".into(), "login".into()),
            ],
        )
        .unwrap();
        wal.log_xadd(
            None,
            "events",
            &StreamEntryId::new(1001, 0),
            &[
                ("user".into(), "bob".into()),
                ("action".into(), "logout".into()),
            ],
        )
        .unwrap();
        wal.log_xadd(
            None,
            "logs",
            &StreamEntryId::new(2000, 0),
            &[("level".into(), "info".into())],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state2) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        assert_eq!(state2.streams.len(), 2);
        assert_eq!(state2.streams["events"].len(), 2);
        assert_eq!(state2.streams["logs"].len(), 1);

        let (id, fields) = &state2.streams["events"][0];
        assert_eq!(id.ms, 1000);
        assert_eq!(id.seq, 0);
        assert_eq!(fields[0], ("user".into(), "alice".into()));
    }

    #[test]
    fn test_rebuild_streams() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();

        wal.log_xadd(
            None,
            "mystream",
            &StreamEntryId::new(100, 0),
            &[("k".into(), "v1".into())],
        )
        .unwrap();
        wal.log_xadd(
            None,
            "mystream",
            &StreamEntryId::new(200, 0),
            &[("k".into(), "v2".into())],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let rebuilt = rebuild_streams(&state);
        assert_eq!(rebuilt.len(), 1);
        let stream = &rebuilt["mystream"];
        assert_eq!(stream.xlen(), 2);
        assert_eq!(stream.entries[0].id, StreamEntryId::new(100, 0));
        assert_eq!(stream.entries[1].id, StreamEntryId::new(200, 0));
    }

    #[test]
    fn test_checkpoint_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();

        // Add entries to two streams
        wal.log_xadd(
            None,
            "s1",
            &StreamEntryId::new(1, 0),
            &[("a".into(), "1".into())],
        )
        .unwrap();
        wal.log_xadd(
            None,
            "s2",
            &StreamEntryId::new(2, 0),
            &[("b".into(), "2".into())],
        )
        .unwrap();

        // Checkpoint with only s1
        let mut checkpoint_streams = HashMap::new();
        let mut s1 = Stream::new();
        s1.xadd_with_id(StreamEntryId::new(1, 0), vec![("a".into(), "1".into())]);
        checkpoint_streams.insert("s1".to_string(), s1);
        wal.checkpoint(&checkpoint_streams).unwrap();

        // Add new entry after checkpoint
        wal.log_xadd(
            None,
            "s1",
            &StreamEntryId::new(3, 0),
            &[("c".into(), "3".into())],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        // s2 was dropped by checkpoint, s1 has 2 entries (snapshot + post-checkpoint)
        assert_eq!(state.streams.len(), 1);
        assert!(state.streams.contains_key("s1"));
        assert!(!state.streams.contains_key("s2"));
        assert_eq!(state.streams["s1"].len(), 2);
    }

    #[test]
    fn test_empty_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        assert!(state.streams.is_empty());
    }

    #[test]
    fn test_corrupt_wal_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("streams.wal");

        {
            let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_xadd(
                None,
                "good_stream",
                &StreamEntryId::new(42, 0),
                &[("k".into(), "v".into())],
            )
            .unwrap();
            drop(wal);
        }

        // Append garbage
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD]).unwrap();
            f.flush().unwrap();
        }

        let (_wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        assert_eq!(state.streams.len(), 1);
        assert!(state.streams.contains_key("good_stream"));
        assert_eq!(state.streams["good_stream"].len(), 1);
    }

    /// S31-03: a torn tail must be truncated on open, so that everything
    /// appended afterwards is replayable. Before the fix `open()` reopened in
    /// append mode without truncating, so every later record sat behind the
    /// garbage and was silently lost by every future replay — while `log_xadd`
    /// returned Ok and `group_sync` reported it durable.
    #[test]
    fn torn_tail_is_truncated_and_later_appends_survive() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("streams.wal");

        {
            let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_xadd(
                None,
                "s",
                &StreamEntryId::new(1, 0),
                &[("k".into(), "a".into())],
            )
            .unwrap();
            wal.group_sync().unwrap();
        }
        let clean_len = std::fs::metadata(&wal_path).unwrap().len();

        // Hand-build a torn tail: an XADD tag plus a truncated stream name.
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            let mut torn = vec![ENTRY_XADD];
            torn.extend_from_slice(&64u32.to_le_bytes()); // name_len, unbacked
            torn.extend_from_slice(b"tor");
            f.write_all(&torn).unwrap();
            f.flush().unwrap();
        }
        assert!(std::fs::metadata(&wal_path).unwrap().len() > clean_len);

        // Reopen: the torn bytes must be gone, and the good prefix intact.
        {
            let (wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            assert_eq!(state.streams["s"].len(), 1);
            assert_eq!(
                std::fs::metadata(&wal_path).unwrap().len(),
                clean_len,
                "the torn tail must be truncated away on open"
            );
            // Append a good record behind where the garbage used to be.
            wal.log_xadd(
                None,
                "s",
                &StreamEntryId::new(2, 0),
                &[("k".into(), "b".into())],
            )
            .unwrap();
            wal.group_sync().unwrap();
        }

        // The record written after the torn tail must survive a reopen.
        let (_wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let entries = &state.streams["s"];
        assert_eq!(
            entries.len(),
            2,
            "a record appended after a torn tail must be recovered"
        );
        assert_eq!(entries[1].0, StreamEntryId::new(2, 0));
        assert_eq!(entries[1].1, vec![("k".to_string(), "b".to_string())]);
    }

    #[test]
    fn test_multiple_streams_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();

        for i in 0..5 {
            let name = format!("stream_{}", i);
            for j in 0..3 {
                wal.log_xadd(
                    None,
                    &name,
                    &StreamEntryId::new(i * 100 + j, 0),
                    &[("idx".into(), format!("{}-{}", i, j))],
                )
                .unwrap();
            }
        }
        drop(wal);

        let (_wal2, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        assert_eq!(state.streams.len(), 5);
        for i in 0..5 {
            let name = format!("stream_{}", i);
            assert_eq!(state.streams[&name].len(), 3);
        }
    }

    // ── Consumer-group durability (S31-05) ──
    //
    // Group state — the cursor, the consumer set and the pending list — was
    // never logged and never checkpointed, so every restart dropped it. Each
    // test here CROSSES A REOPEN: the in-memory path was always correct, which
    // is exactly why the gap survived.

    /// Cursor, consumers and PEL rebuilt from the record tail (no checkpoint).
    #[test]
    fn group_state_survives_reopen_from_the_log_tail() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = StreamEntryId::new(10, 0);
        let id2 = StreamEntryId::new(20, 0);
        {
            let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_xadd(None, "s", &id1, &[("k".into(), "a".into())])
                .unwrap();
            wal.log_xadd(None, "s", &id2, &[("k".into(), "b".into())])
                .unwrap();
            wal.log_xgroup_create(None, "s", "g", &StreamEntryId::new(0, 0))
                .unwrap();
            wal.log_xreadgroup(None, "s", "g", "c1", &id2, &[id1.clone(), id2.clone()])
                .unwrap();
            wal.log_xack(None, "s", "g", std::slice::from_ref(&id1))
                .unwrap();
            wal.group_sync().unwrap();
        }

        let (_wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let rebuilt = rebuild_streams(&state);
        let stream = &rebuilt["s"];
        assert_eq!(stream.xlen(), 2);
        let g = &stream.groups["g"];
        assert_eq!(
            g.last_delivered_id, id2,
            "the cursor must resume where the group left off, not at the start — \
             a lost cursor redelivers the whole backlog"
        );
        assert!(g.consumers.contains("c1"));
        assert_eq!(
            g.pending["c1"],
            vec![id2],
            "the acknowledged id must be gone from the PEL and the unacknowledged one must stay"
        );
    }

    /// S31-15: the ack record names, per consumer, the ids that consumer's
    /// pending list owned at ack time, and replay removes them from exactly
    /// those consumers — a consumer not named keeps its PEL untouched. A v1
    /// record (no owners) beside it still replays with its old
    /// remove-from-everyone meaning: addition-only compatibility.
    #[test]
    fn xack_v2_records_owners_and_replays_per_consumer() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = StreamEntryId::new(10, 0);
        let id2 = StreamEntryId::new(20, 0);
        let id3 = StreamEntryId::new(30, 0);
        {
            let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_xadd(None, "s", &id1, &[("k".into(), "a".into())])
                .unwrap();
            wal.log_xadd(None, "s", &id2, &[("k".into(), "b".into())])
                .unwrap();
            wal.log_xadd(None, "s", &id3, &[("k".into(), "c".into())])
                .unwrap();
            wal.log_xgroup_create(None, "s", "g", &StreamEntryId::new(0, 0))
                .unwrap();
            wal.log_xreadgroup(None, "s", "g", "c1", &id2, &[id1.clone(), id2.clone()])
                .unwrap();
            wal.log_xreadgroup(None, "s", "g", "c2", &id3, std::slice::from_ref(&id3))
                .unwrap();
            // c1 acks id1; the record must carry that c1 (not c2) owned it.
            wal.log_xack_owned(None, "s", "g", &[("c1".into(), vec![id1.clone()])])
                .unwrap();
            wal.group_sync().unwrap();
        }

        let (_wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let g = &rebuild_streams(&state)["s"].groups["g"];
        assert_eq!(
            g.pending["c1"],
            vec![id2.clone()],
            "only the owned id left c1's pending list"
        );
        assert_eq!(
            g.pending["c2"],
            vec![id3.clone()],
            "a consumer the record does not name keeps its PEL untouched"
        );

        // A v1 XACK beside a V2 one still replays against every consumer.
        {
            let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_xack(None, "s", "g", std::slice::from_ref(&id2))
                .unwrap();
            wal.group_sync().unwrap();
        }
        let (_wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let g = &rebuild_streams(&state)["s"].groups["g"];
        assert!(
            g.pending["c1"].is_empty(),
            "the v1 record removed id2 from wherever it was pending"
        );
        assert_eq!(g.pending["c2"], vec![id3]);
    }

    /// NU-385 class on the new opcode: a consumer count the bytes cannot
    /// back must be refused, not reserved.
    #[test]
    fn absurd_xack_v2_consumer_count_is_refused_not_reserved() {
        let mut buf = vec![ENTRY_XACK_V2];
        push_str_field(&mut buf, "s");
        push_str_field(&mut buf, "g");
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // n_consumers, a lie
        let (state, end) = replay(&buf, &HashSet::new());
        assert_eq!(end, 0, "the record must be abandoned at its own start");
        assert!(state.groups.is_empty());

        // Same for the torn body of the per-consumer sections themselves.
        let mut buf = vec![ENTRY_XACK_V2];
        push_str_field(&mut buf, "s");
        push_str_field(&mut buf, "g");
        buf.extend_from_slice(&1u32.to_le_bytes()); // one consumer section
        push_str_field(&mut buf, "c");
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // n_ids, a lie
        let (state, end) = replay(&buf, &HashSet::new());
        assert_eq!(end, 0);
        assert!(state.groups.is_empty());
    }

    /// A checkpoint rewrites the log from live memory, so a snapshot that
    /// dropped group state would silently un-persist every record above.
    #[test]
    fn checkpoint_round_trips_groups_and_max_len() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();

        let mut s = Stream::with_max_len(4);
        let a = s.xadd_with_id(StreamEntryId::new(1, 0), vec![("k".into(), "a".into())]);
        let b = s.xadd_with_id(StreamEntryId::new(2, 0), vec![("k".into(), "b".into())]);
        s.xgroup_create("g", StreamEntryId::new(0, 0)).unwrap();
        let _ = s.xreadgroup("g", "c1", 10);
        s.xack("g", std::slice::from_ref(&a));
        // A group with no deliveries at all must round-trip too.
        s.xgroup_create("idle", StreamEntryId::new(7, 3)).unwrap();

        let mut live = HashMap::new();
        live.insert("s".to_string(), s);
        wal.checkpoint(&live).unwrap();
        drop(wal);

        let (_wal2, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let rebuilt = rebuild_streams(&state);
        let got = &rebuilt["s"];
        assert_eq!(got.xlen(), 2);
        assert_eq!(got.max_len, Some(4), "the cap must survive the snapshot");
        assert_eq!(got.groups["g"].last_delivered_id, b);
        assert_eq!(got.groups["g"].pending["c1"], vec![b]);
        assert!(got.groups["g"].consumers.contains("c1"));
        assert_eq!(
            got.groups["idle"].last_delivered_id,
            StreamEntryId::new(7, 3)
        );
        assert!(got.groups["idle"].pending.is_empty());
    }

    /// Checkpointing unchanged state must produce identical bytes — sorted
    /// iteration, so a hash-order shuffle cannot masquerade as a real change.
    #[test]
    fn checkpoint_is_byte_stable() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();

        let mut live = HashMap::new();
        for name in ["z", "a", "m"] {
            let mut s = Stream::new();
            s.xadd_with_id(StreamEntryId::new(1, 0), vec![("k".into(), name.into())]);
            for g in ["g2", "g1"] {
                s.xgroup_create(g, StreamEntryId::new(0, 0)).unwrap();
                let _ = s.xreadgroup(g, "c2", 10);
                let _ = s.xreadgroup(g, "c1", 10);
            }
            live.insert(name.to_string(), s);
        }

        let (wal_a, _) = StreamsWal::open(dir_a.path(), &HashSet::new()).unwrap();
        wal_a.checkpoint(&live).unwrap();
        let (wal_b, _) = StreamsWal::open(dir_b.path(), &HashSet::new()).unwrap();
        wal_b.checkpoint(&live).unwrap();
        assert_eq!(
            std::fs::read(dir_a.path().join("streams.wal")).unwrap(),
            std::fs::read(dir_b.path().join("streams.wal")).unwrap(),
        );
    }

    /// A group created on a stream with no entries is still a group.
    #[test]
    fn group_on_an_entryless_stream_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_xgroup_create(None, "empty", "g", &StreamEntryId::new(0, 0))
                .unwrap();
        }
        let (_wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let rebuilt = rebuild_streams(&state);
        assert!(
            rebuilt["empty"].groups.contains_key("g"),
            "a group must not need entries to exist"
        );
        assert_eq!(rebuilt["empty"].xlen(), 0);
    }

    /// Replaying a second XGROUP_CREATE record for a live group resets its
    /// cursor and drops its pending list — last-wins, matching
    /// `Stream::xgroup_recreate`. Since S31-11 only an explicit recreate can
    /// write that second record; a plain create fails with BUSYGROUP and logs
    /// nothing.
    #[test]
    fn replayed_group_recreate_resets_cursor_and_pel() {
        let dir = tempfile::tempdir().unwrap();
        let id = StreamEntryId::new(5, 0);
        {
            let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_xadd(None, "s", &id, &[("k".into(), "v".into())])
                .unwrap();
            wal.log_xgroup_create(None, "s", "g", &StreamEntryId::new(0, 0))
                .unwrap();
            wal.log_xreadgroup(None, "s", "g", "c", &id, std::slice::from_ref(&id))
                .unwrap();
            wal.log_xgroup_create(None, "s", "g", &StreamEntryId::new(0, 0))
                .unwrap();
        }
        let (_wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let g = &rebuild_streams(&state)["s"].groups["g"];
        assert_eq!(g.last_delivered_id, StreamEntryId::new(0, 0));
        assert!(g.pending.is_empty());
        assert!(g.consumers.is_empty());
    }

    // ── Backward compatibility with a pre-groups log ──

    /// A log written before opcodes 0x03-0x06 existed must still replay. The
    /// compatibility rule is addition-only: 0x01 and 0x02 keep their exact byte
    /// layouts, and there is no version header to consult (adding one would
    /// break precisely these files).
    #[test]
    fn a_log_written_before_groups_existed_still_replays() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("streams.wal");

        // Hand-build the OLD format by hand rather than by calling the current
        // writer, so this test keeps failing if the old layout is ever changed.
        let mut old = Vec::new();
        // A v1 SNAPSHOT: one stream, one entry, one field.
        old.push(0x02u8);
        old.extend_from_slice(&1u32.to_le_bytes()); // n_streams
        push_str_field(&mut old, "events");
        old.extend_from_slice(&1u32.to_le_bytes()); // n_entries
        old.extend_from_slice(&1000u64.to_le_bytes()); // ms
        old.extend_from_slice(&0u64.to_le_bytes()); // seq
        old.extend_from_slice(&1u32.to_le_bytes()); // n_fields
        push_str_field(&mut old, "user");
        push_str_field(&mut old, "alice");
        // A v1 XADD behind it, the shape a running server leaves.
        old.push(0x01u8);
        push_str_field(&mut old, "events");
        old.extend_from_slice(&1001u64.to_le_bytes());
        old.extend_from_slice(&0u64.to_le_bytes());
        old.extend_from_slice(&1u32.to_le_bytes());
        push_str_field(&mut old, "user");
        push_str_field(&mut old, "bob");
        let old_len = old.len();
        std::fs::write(&path, &old).unwrap();

        let (wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len() as usize,
            old_len,
            "an old log is fully valid and must NOT be truncated as a torn tail"
        );
        assert_eq!(state.streams["events"].len(), 2);
        assert!(
            state.groups.is_empty() && state.max_len.is_empty(),
            "a pre-groups log recorded no groups, which is what it must recover"
        );

        // And the upgraded server can keep writing group records into it.
        wal.log_xgroup_create(None, "events", "g", &StreamEntryId::new(0, 0))
            .unwrap();
        wal.group_sync().unwrap();
        drop(wal);
        let (_wal2, state2) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        assert_eq!(state2.streams["events"].len(), 2);
        assert!(state2.groups["events"].contains_key("g"));
    }

    /// A v1 SNAPSHOT still resets state, including group state recovered from
    /// records before it — a snapshot means "this is everything".
    #[test]
    fn a_v1_snapshot_still_resets_group_state() {
        let mut buf = Vec::new();
        buf.push(ENTRY_XGROUP_CREATE);
        push_str_field(&mut buf, "s");
        push_str_field(&mut buf, "g");
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        // Then an old-format snapshot declaring zero streams.
        buf.push(ENTRY_SNAPSHOT);
        buf.extend_from_slice(&0u32.to_le_bytes());
        let (state, end) = replay(&buf, &HashSet::new());
        assert_eq!(end, buf.len());
        assert!(state.streams.is_empty());
        assert!(state.groups.is_empty(), "a snapshot resets ALL state");
    }

    // ── Torn tails and hostile counts on the new opcodes ──

    /// The new records must obey the same truncation contract as XADD: a torn
    /// one is abandoned at its own start, so `open` truncates to the last valid
    /// boundary and later appends are still replayable.
    #[test]
    fn a_torn_group_record_is_truncated_and_later_appends_survive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("streams.wal");
        {
            let (wal, _) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            wal.log_xgroup_create(None, "s", "g", &StreamEntryId::new(0, 0))
                .unwrap();
            wal.group_sync().unwrap();
        }
        let clean_len = std::fs::metadata(&path).unwrap().len();

        // A half-written XREADGROUP: tag, stream, group, then nothing.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            let mut torn = vec![ENTRY_XREADGROUP];
            push_str_field(&mut torn, "s");
            push_str_field(&mut torn, "g");
            f.write_all(&torn).unwrap();
            f.flush().unwrap();
        }

        {
            let (wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
            assert!(state.groups["s"].contains_key("g"));
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                clean_len,
                "the torn record must be truncated away on open"
            );
            wal.log_xreadgroup(
                None,
                "s",
                "g",
                "c",
                &StreamEntryId::new(9, 0),
                &[StreamEntryId::new(9, 0)],
            )
            .unwrap();
            wal.group_sync().unwrap();
        }

        let (_wal, state) = StreamsWal::open(dir.path(), &HashSet::new()).unwrap();
        let g = &state.groups["s"]["g"];
        assert_eq!(g.last_delivered_id, StreamEntryId::new(9, 0));
        assert_eq!(g.pending["c"], vec![StreamEntryId::new(9, 0)]);
    }

    /// `read_ids`: an id count the bytes cannot back must be refused, not
    /// reserved. 4.29e9 x 16 bytes is 68 GB, which aborts the process on Linux
    /// rather than returning an error (NU-385 class).
    #[test]
    fn absurd_pending_id_count_is_refused_not_reserved() {
        let mut buf = vec![ENTRY_XACK];
        push_str_field(&mut buf, "s");
        push_str_field(&mut buf, "g");
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // n_ids, a lie
        let (state, end) = replay(&buf, &HashSet::new());
        assert_eq!(end, 0, "the record must be abandoned at its own start");
        assert!(state.groups.is_empty());

        // Same inside a SNAPSHOT2 body, where the failure must fail the whole
        // snapshot rather than half-apply it.
        let mut snap = vec![ENTRY_SNAPSHOT_V2];
        snap.extend_from_slice(&1u32.to_le_bytes()); // n_streams
        push_str_field(&mut snap, "s");
        snap.push(0); // has_max_len
        snap.extend_from_slice(&0u64.to_le_bytes());
        snap.extend_from_slice(&0u32.to_le_bytes()); // n_entries
        snap.extend_from_slice(&1u32.to_le_bytes()); // n_groups
        push_str_field(&mut snap, "g");
        snap.extend_from_slice(&0u64.to_le_bytes()); // last_ms
        snap.extend_from_slice(&0u64.to_le_bytes()); // last_seq
        snap.extend_from_slice(&0u32.to_le_bytes()); // n_consumers
        snap.extend_from_slice(&1u32.to_le_bytes()); // n_pending
        push_str_field(&mut snap, "c");
        snap.extend_from_slice(&u32::MAX.to_le_bytes()); // n_ids, a lie
        let (state, end) = replay(&snap, &HashSet::new());
        assert_eq!(end, 0);
        assert!(state.streams.is_empty() && state.groups.is_empty());
    }

    /// A hostile consumer count inside a SNAPSHOT2 group must fail the parse
    /// rather than loop 4.29e9 times building strings.
    #[test]
    fn absurd_consumer_count_fails_the_snapshot() {
        let mut snap = vec![ENTRY_SNAPSHOT_V2];
        snap.extend_from_slice(&1u32.to_le_bytes());
        push_str_field(&mut snap, "s");
        snap.push(0);
        snap.extend_from_slice(&0u64.to_le_bytes());
        snap.extend_from_slice(&0u32.to_le_bytes()); // n_entries
        snap.extend_from_slice(&1u32.to_le_bytes()); // n_groups
        push_str_field(&mut snap, "g");
        snap.extend_from_slice(&0u64.to_le_bytes());
        snap.extend_from_slice(&0u64.to_le_bytes());
        snap.extend_from_slice(&u32::MAX.to_le_bytes()); // n_consumers, a lie
        let (state, end) = replay(&snap, &HashSet::new());
        assert_eq!(end, 0);
        assert!(state.streams.is_empty() && state.groups.is_empty());
    }
}
