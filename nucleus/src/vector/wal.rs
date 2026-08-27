//! Write-ahead log for vector indexes (HNSW / IVFFlat).
//!
//! Uses a **snapshot + delta** strategy because HNSW graph structure depends on
//! insertion order — incremental replay alone would produce a different (and
//! potentially worse) graph. Instead we periodically write a full binary
//! snapshot via [`HnswIndex::serialize()`] and only replay delta operations
//! (inserts / deletes) that came *after* the last snapshot.
//!
//! ## Binary entry format
//! ```text
//! CREATE_INDEX: [0x01] [name_len: u32 LE] [name: bytes] [dims: u32 LE] [metric: u8] [m: u32 LE] [ef: u32 LE]
//! INSERT_VEC:   [0x02] [name_len: u32 LE] [name: bytes] [id: u64 LE] [n_dims: u32 LE] [floats: f32 LE * n_dims] [meta_len: u32 LE] [meta_json: bytes]
//! DELETE_VEC:   [0x03] [name_len: u32 LE] [name: bytes] [id: u64 LE]
//! SNAPSHOT:     [0x04] [n_indexes: u32 LE] [per index: name_len + name + dims(u32) + metric(u8) + m(u32) + ef(u32) + serialized_hnsw_bytes_len(u32) + serialized_hnsw_bytes]
//! SNAPSHOT_V2:  [0x05] [n_indexes: u32 LE] [per index: ... + serialized_hnsw_bytes_len(u32) + crc32c(u32) + serialized_hnsw_bytes]
//! ```
//!
//! `0x04` is read but never written: it carries no checksum, so corruption
//! inside a serialized blob deserializes into a structurally valid index
//! holding data nobody wrote. `0x05` adds a CRC32C per blob, checked before
//! deserialization.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use super::{DistanceMetric, HnswConfig, HnswIndex, RegistrySection, Vector};

// ─── Entry type tags ──────────────────────────────────────────────────────────

const TAG_CREATE_INDEX: u8 = 0x01;
const TAG_INSERT_VEC: u8 = 0x02;
const TAG_DELETE_VEC: u8 = 0x03;
/// Snapshot without per-blob checksums. Still read, never written.
///
/// `HnswIndex::deserialize` rejects bad lengths and counts, which is why a
/// snapshot whose *framing* is damaged is reported. It is not an integrity
/// check and cannot be one: a corrupted float is still a valid float, and a
/// corrupted adjacency entry is still a `u32`. Measured on 24 vectors — XOR 64
/// bytes of payload and roughly one attempt in twenty opened successfully with
/// all 24 vectors present and silently wrong. That is the gap `TAG_SNAPSHOT_V2`
/// closes, and the reason the regression test for a corrupt snapshot was flaky:
/// it depended on where in a randomly-shaped graph the corruption landed.
const TAG_SNAPSHOT: u8 = 0x04;
/// Snapshot with a CRC32C per serialized HNSW blob, written before the blob.
///
/// Forward-compatible in the direction this codebase guarantees elsewhere (see
/// `backup.rs`'s legacy manifests): a new binary reads old snapshots and says
/// out loud that it cannot verify them. The other direction is not supported —
/// an older binary meets an unknown tag and stops replaying — which is the same
/// position every other durable format here takes.
const TAG_SNAPSHOT_V2: u8 = 0x05;
/// INSERT_VEC carrying a coordinating transaction id (S63). Same fields as
/// [`TAG_INSERT_VEC`] plus a trailing `[xact_id u64 LE]`. Written whenever
/// the executor knows the id — including `XACT_AUTOCOMMIT` (0) for
/// autocommit writes; replay keeps those. v1 tags (0x01–0x05) replay as
/// keep: their durability point is their own fsync and they predate
/// enlistment. An older binary meets 0x06/0x07 and stops replaying — the
/// one-way-compat position every durable format here takes (see
/// [`TAG_SNAPSHOT_V2`]'s header for the same statement).
const TAG_INSERT_VEC_XACT: u8 = 0x06;
/// DELETE_VEC carrying a coordinating transaction id (S63). See
/// [`TAG_INSERT_VEC_XACT`].
const TAG_DELETE_VEC_XACT: u8 = 0x07;
/// CREATE_INDEX carrying a coordinating transaction id (S63) — an index
/// created inside a rolled-back transaction must not survive replay as an
/// empty shell. Same fields as [`TAG_CREATE_INDEX`] plus a trailing
/// `[xact_id u64 LE]`.
const TAG_CREATE_INDEX_XACT: u8 = 0x08;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Metadata about a recovered vector index.
#[derive(Debug)]
pub struct RecoveredIndex {
    /// The deserialized HNSW index (from snapshot, with deltas applied).
    pub hnsw: HnswIndex,
    /// Dimensionality recorded at creation time.
    pub dims: u32,
    /// Distance metric encoded as u8 (0=L2, 1=Cosine, 2=InnerProduct).
    pub metric: u8,
    /// HNSW M parameter.
    pub m: u32,
    /// HNSW ef_search parameter.
    pub ef: u32,
    /// The PK registry recovered from the snapshot section plus pk-carrying
    /// delta records, when either existed (`None` for logs that predate
    /// registry persistence — an empty registry, which is faithful).
    pub registry: Option<RegistrySection>,
}

/// State recovered from replaying the WAL.
#[derive(Debug)]
pub struct VectorWalState {
    /// Recovered indexes keyed by index name.
    pub indexes: HashMap<String, RecoveredIndex>,
    /// The highest coordinating transaction id any surviving record carries
    /// (S63). Seeds the executor's XactId floor so a rolled-back or crashed
    /// transaction's id is never re-minted — re-minting is what would let a
    /// stale tagged record match a fresh committed id and be RESURRECTED by
    /// the very filter built to discard it.
    pub max_xact_id: u64,
}

/// Append-only WAL for vector indexes.
pub struct VectorWal {
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
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint`.
    #[cfg(test)]
    fail_reopen_once: std::sync::atomic::AtomicBool,
    /// Test-only append fault switch; see `append`.
    #[cfg(test)]
    fail_appends: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl VectorWal {
    /// Make every subsequent `append` fail with ENOSPC. The `NUCLEUS_IOFAULT`
    /// machinery reads its environment through a `OnceLock` initialised by
    /// whichever call site runs first, so it cannot be armed from inside a
    /// shared test binary — only from a freshly spawned process.
    pub fn set_fail_appends(&self, on: bool) {
        self.fail_appends
            .store(on, std::sync::atomic::Ordering::SeqCst);
    }
}

impl VectorWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state contains no indexes. Corrupt trailing bytes are silently ignored
    /// (best-effort recovery up to the last valid entry).
    /// `committed` is the set of coordinating transaction ids that durably
    /// committed on the SQL side (S63); a tagged record whose id is neither
    /// in it nor `XACT_AUTOCOMMIT` belongs to a transaction that never
    /// committed and is discarded — absence of a commit record means
    /// discard, always. The SQL engine recovers before this executor is
    /// constructed, which is what makes the set available here.
    pub fn open(
        dir: &Path,
        committed: &std::collections::HashSet<u64>,
    ) -> io::Result<(Self, VectorWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("vector.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            let outcome = replay(&data, committed);
            // A rejected snapshot used to come back as an empty index and an
            // `Ok`, so `Executor::open_durable` — which exists to announce
            // exactly this — never fired for the vector store.
            if let Some(reason) = outcome.corruption {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: {reason}", path.display()),
                ));
            }
            outcome.state
        } else {
            VectorWalState {
                indexes: HashMap::new(),
                max_xact_id: 0,
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: crate::storage::wal_util::WalSync::new(),
                stranded: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                fail_reopen_once: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                fail_appends: std::sync::atomic::AtomicBool::new(false),
            },
            state,
        ))
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

    /// Return the directory containing the WAL file.
    pub fn dir(&self) -> &Path {
        self.path.parent().unwrap_or(Path::new("."))
    }

    /// Append one complete record; the single reattach point for a writer
    /// stranded by a failed checkpoint reopen, and the single place the
    /// `vector.wal_append` fault is checked (NU-048: a failed append must
    /// fail the statement, never be printed-and-acknowledged).
    fn append(&self, buf: &[u8]) -> io::Result<()> {
        if let Some(e) = crate::storage::crashpoint::io_fault("vector.wal_append") {
            return Err(e);
        }
        // In-process arming for unit tests; see `set_fail_appends`.
        #[cfg(test)]
        if self.fail_appends.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "injected vector WAL append failure",
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
        if let Some(e) = crate::storage::crashpoint::io_fault("vector.wal_reopen") {
            return Err(e);
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "vector WAL writer is stranded: a checkpoint replaced {} but its \
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

    /// Log a CREATE INDEX operation.
    pub fn log_create_index(
        &self,
        xact: Option<u64>,
        name: &str,
        dims: u32,
        metric: u8,
        m: u32,
        ef: u32,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        let nb = name.as_bytes();
        match xact {
            Some(x) => {
                buf.push(TAG_CREATE_INDEX_XACT);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            None => buf.push(TAG_CREATE_INDEX),
        }
        buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
        buf.extend_from_slice(nb);
        buf.extend_from_slice(&dims.to_le_bytes());
        buf.push(metric);
        buf.extend_from_slice(&m.to_le_bytes());
        buf.extend_from_slice(&ef.to_le_bytes());
        self.append(&buf)
    }

    /// Log a vector insertion.
    ///
    /// `metadata` carries the row's primary key (as a decimal u64 string) for
    /// PK-keyed HNSW indexes, so replay can rebuild the pk -> node registry
    /// from delta records; empty for positional indexes and records written
    /// before the registry was persisted.
    ///
    /// The `vector.wal_append` fault point makes NU-048 testable from outside:
    /// a failed append here must fail the originating DML statement, never be
    /// printed-and-acknowledged.
    pub fn log_insert(
        &self,
        xact: Option<u64>,
        name: &str,
        id: u64,
        vector: &[f32],
        metadata: &str,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        let nb = name.as_bytes();
        match xact {
            Some(x) => {
                buf.push(TAG_INSERT_VEC_XACT);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            None => buf.push(TAG_INSERT_VEC),
        }
        buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
        buf.extend_from_slice(nb);
        buf.extend_from_slice(&id.to_le_bytes());
        buf.extend_from_slice(&(vector.len() as u32).to_le_bytes());
        for &f in vector {
            buf.extend_from_slice(&f.to_le_bytes());
        }
        let mb = metadata.as_bytes();
        buf.extend_from_slice(&(mb.len() as u32).to_le_bytes());
        buf.extend_from_slice(mb);
        self.append(&buf)
    }

    /// Log a vector deletion (soft-delete in HNSW). Same fault point as
    /// `log_insert`: a deleted vector resurrecting across restart is the
    /// other half of NU-048.
    pub fn log_delete(&self, xact: Option<u64>, name: &str, id: u64) -> io::Result<()> {
        let mut buf = Vec::new();
        let nb = name.as_bytes();
        match xact {
            Some(x) => {
                buf.push(TAG_DELETE_VEC_XACT);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            None => buf.push(TAG_DELETE_VEC),
        }
        buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
        buf.extend_from_slice(nb);
        buf.extend_from_slice(&id.to_le_bytes());
        self.append(&buf)
    }

    /// Write the complete current state of all HNSW indexes as a single
    /// SNAPSHOT entry and truncate the log to just that entry.
    ///
    /// Uses [`HnswIndex::serialize()`] for the heavy lifting.
    pub fn checkpoint(&self, indexes: &HashMap<String, IndexSnapshot>) -> io::Result<()> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(indexes.len() as u32).to_le_bytes());
        for (name, snap) in indexes {
            let nb = name.as_bytes();
            payload.extend_from_slice(&(nb.len() as u32).to_le_bytes());
            payload.extend_from_slice(nb);
            payload.extend_from_slice(&snap.dims.to_le_bytes());
            payload.push(snap.metric);
            payload.extend_from_slice(&snap.m.to_le_bytes());
            payload.extend_from_slice(&snap.ef.to_le_bytes());
            let serialized = snap.hnsw.serialize(snap.registry);
            payload.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
            payload.extend_from_slice(&crc32c::crc32c(&serialized).to_le_bytes());
            payload.extend_from_slice(&serialized);
        }

        // Serialize the complete new log body (SNAPSHOT tag + payload).
        let mut contents = Vec::with_capacity(payload.len() + 1);
        contents.push(TAG_SNAPSHOT_V2);
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
            .then(|| io::Error::other("injected vector WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = if let Some(e) = injected {
            Err(e)
        } else if let Some(e) = crate::storage::crashpoint::io_fault("vector.wal_reopen") {
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

/// Data needed to write a snapshot for one index.
pub struct IndexSnapshot<'a> {
    pub hnsw: &'a HnswIndex,
    pub dims: u32,
    pub metric: u8,
    pub m: u32,
    pub ef: u32,
    /// PK registry persisted inside the HNSW blob (CRC-covered with it).
    pub registry: Option<&'a RegistrySection>,
}

// ─── Replay ───────────────────────────────────────────────────────────────────

/// Intermediate state during WAL replay before HNSW indexes are built.
struct ReplayIndex {
    dims: u32,
    metric: u8,
    m: u32,
    ef: u32,
    /// Full HNSW index from the last snapshot (if any).
    hnsw: Option<HnswIndex>,
    /// Delta inserts after the last snapshot: (id, vector, pk).
    delta_inserts: Vec<(u64, Vec<f32>, Option<u64>)>,
    /// Delta deletes after the last snapshot.
    delta_deletes: Vec<u64>,
    /// PK registry recovered so far: the snapshot's section (if any) with
    /// pk-carrying delta records applied on top. Deleted nodes are un-mapped
    /// as their DELETE records replay, so the result matches what the live
    /// registry held at the moment the log stopped.
    registry: RegistrySection,
    /// Reverse of `registry.pk_to_node`, maintained only during replay.
    node_to_pk: HashMap<u64, u64>,
    /// Whether any registry source existed (snapshot section or pk-carrying
    /// delta) — distinguishes a recovered registry from an old log that never
    /// carried one.
    has_registry: bool,
}

/// Replay all entries in `data` to reconstruct vector index state.
///
/// SNAPSHOT entries reset all state. After the last snapshot, incremental
/// delta entries (INSERT_VEC, DELETE_VEC) are collected and applied on top.
/// What replay produced, and whether it is trustworthy. Same shape and same
/// reason as `kv::collections_wal::ReplayOutcome`: replay could detect
/// corruption and had no channel to report it.
struct ReplayOutcome {
    state: VectorWalState,
    /// `Some` when replay stopped on structural corruption rather than a clean
    /// end of data. A torn trailing record is not corruption.
    corruption: Option<String>,
}

fn replay(data: &[u8], committed: &std::collections::HashSet<u64>) -> ReplayOutcome {
    let mut indexes: HashMap<String, ReplayIndex> = HashMap::new();
    let mut corruption: Option<String> = None;
    let mut pos = 0usize;
    let mut max_xact_id: u64 = 0;
    // S63 recovery filter: a tagged record survives replay iff its
    // transaction durably committed. AUTOCOMMIT (0) is its own durability
    // point. The record is PARSED either way — replay must consume it to
    // continue — and dropped only after its bytes are accounted for.
    let keep = |xid: u64, max_xact_id: &mut u64| {
        *max_xact_id = (*max_xact_id).max(xid);
        xid == 0 || committed.contains(&xid)
    };

    while pos < data.len() {
        let Some(&tag) = data.get(pos) else { break };
        pos += 1;

        match tag {
            TAG_CREATE_INDEX | TAG_CREATE_INDEX_XACT => {
                let xid = if tag == TAG_CREATE_INDEX_XACT {
                    let Some(x) = read_u64(data, &mut pos) else {
                        break;
                    };
                    x
                } else {
                    0
                };
                let survived = keep(xid, &mut max_xact_id);
                let Some(name) = read_string(data, &mut pos) else {
                    break;
                };
                let Some(dims) = read_u32(data, &mut pos) else {
                    break;
                };
                let Some(&metric) = data.get(pos) else { break };
                pos += 1;
                let Some(m) = read_u32(data, &mut pos) else {
                    break;
                };
                let Some(ef) = read_u32(data, &mut pos) else {
                    break;
                };
                if survived {
                    indexes.insert(
                        name,
                        ReplayIndex {
                            dims,
                            metric,
                            m,
                            ef,
                            hnsw: None,
                            delta_inserts: Vec::new(),
                            delta_deletes: Vec::new(),
                            registry: RegistrySection::default(),
                            node_to_pk: HashMap::new(),
                            has_registry: false,
                        },
                    );
                }
            }
            TAG_INSERT_VEC | TAG_INSERT_VEC_XACT => {
                let xid = if tag == TAG_INSERT_VEC_XACT {
                    let Some(x) = read_u64(data, &mut pos) else {
                        break;
                    };
                    x
                } else {
                    0
                };
                let Some(name) = read_string(data, &mut pos) else {
                    break;
                };
                let Some(id) = read_u64(data, &mut pos) else {
                    break;
                };
                let Some(n_dims) = read_u32(data, &mut pos) else {
                    break;
                };
                let n_dims = n_dims as usize;
                if pos + n_dims * 4 > data.len() {
                    break;
                }
                let mut floats = Vec::with_capacity(n_dims);
                for _ in 0..n_dims {
                    let b = &data[pos..pos + 4];
                    floats.push(f32::from_le_bytes([b[0], b[1], b[2], b[3]]));
                    pos += 4;
                }
                // Read metadata. For PK-keyed indexes it carries the row's
                // primary key as a decimal u64 string (empty for old records
                // and positional indexes), so replay can rebuild the pk ->
                // node registry from delta records.
                let Some(meta_len) = read_u32(data, &mut pos) else {
                    break;
                };
                let meta_len = meta_len as usize;
                if pos + meta_len > data.len() {
                    break;
                }
                let meta = &data[pos..pos + meta_len];
                pos += meta_len;
                let pk = std::str::from_utf8(meta)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok());

                if keep(xid, &mut max_xact_id)
                    && let Some(idx) = indexes.get_mut(&name)
                {
                    idx.delta_inserts.push((id, floats, pk));
                }
            }
            TAG_DELETE_VEC | TAG_DELETE_VEC_XACT => {
                let xid = if tag == TAG_DELETE_VEC_XACT {
                    let Some(x) = read_u64(data, &mut pos) else {
                        break;
                    };
                    x
                } else {
                    0
                };
                let Some(name) = read_string(data, &mut pos) else {
                    break;
                };
                let Some(id) = read_u64(data, &mut pos) else {
                    break;
                };
                if keep(xid, &mut max_xact_id)
                    && let Some(idx) = indexes.get_mut(&name)
                {
                    idx.delta_deletes.push(id);
                }
            }
            TAG_SNAPSHOT | TAG_SNAPSHOT_V2 => {
                // V2 carries a CRC per blob; 0x04 predates it and cannot be
                // verified. See the constant's doc comment for why that matters.
                let verified = tag == TAG_SNAPSHOT_V2;
                if !verified {
                    tracing::warn!(
                        target: "nucleus::vector",
                        "vector snapshot predates per-blob checksums — payload corruption \
                         in it cannot be detected; it will be re-written checksummed on the \
                         next checkpoint"
                    );
                }
                // A snapshot resets everything.
                indexes.clear();
                let Some(n_indexes) = read_u32(data, &mut pos) else {
                    break;
                };
                let mut ok = true;
                for _ in 0..n_indexes {
                    let Some(name) = read_string(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(dims) = read_u32(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(&metric) = data.get(pos) else {
                        ok = false;
                        break;
                    };
                    pos += 1;
                    let Some(m) = read_u32(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(ef) = read_u32(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(blob_len) = read_u32(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let expected_crc = if verified {
                        let Some(c) = read_u32(data, &mut pos) else {
                            ok = false;
                            break;
                        };
                        Some(c)
                    } else {
                        None
                    };
                    let blob_len = blob_len as usize;
                    if pos + blob_len > data.len() {
                        ok = false;
                        break;
                    }
                    let blob = &data[pos..pos + blob_len];
                    pos += blob_len;

                    // Checked BEFORE `deserialize`, because deserialize is not
                    // an integrity check and was never going to be one: it
                    // rejects bad lengths and counts, and a corrupt float is
                    // still a float. Measured before this existed — corrupting
                    // 64 bytes of payload produced an index that opened
                    // successfully with all 24 vectors present and wrong.
                    if let Some(want) = expected_crc {
                        let got = crc32c::crc32c(blob);
                        if got != want {
                            corruption = Some(format!(
                                "HNSW snapshot for index '{name}' ({blob_len} bytes) failed its \
                                 checksum: expected {want:#010x}, computed {got:#010x}"
                            ));
                            ok = false;
                            break;
                        }
                    }

                    // `.ok()` here was silent, total data loss. `hnsw: None` is
                    // a LEGITIMATE state — `TAG_CREATE_INDEX` produces it for an
                    // index that exists but has never been checkpointed — so a
                    // rejected snapshot blob was indistinguishable from "no
                    // snapshot yet", and the index was rebuilt from deltas
                    // alone. Every vector in the snapshot vanished at restart
                    // with no warning and no error. A snapshot record always
                    // carries a blob (`IndexSnapshot::hnsw` is `&HnswIndex`, not
                    // an Option), so a failure to deserialize one is always
                    // corruption and never an empty index.
                    let (hnsw_index, registry_section) =
                        match HnswIndex::deserialize_with_registry(blob) {
                            Ok(h) => h,
                            Err(e) => {
                                corruption = Some(format!(
                                    "HNSW snapshot for index '{name}' ({blob_len} bytes) did not \
                                     deserialize: {e}"
                                ));
                                ok = false;
                                break;
                            }
                        };
                    let node_to_pk = registry_section
                        .as_ref()
                        .map(|r| r.pk_to_node.iter().map(|(&p, &n)| (n, p)).collect())
                        .unwrap_or_default();
                    let has_registry = registry_section.is_some();

                    indexes.insert(
                        name,
                        ReplayIndex {
                            dims,
                            metric,
                            m,
                            ef,
                            registry: registry_section.unwrap_or_default(),
                            node_to_pk,
                            has_registry,
                            hnsw: Some(hnsw_index),
                            delta_inserts: Vec::new(),
                            delta_deletes: Vec::new(),
                        },
                    );
                }
                if !ok {
                    break;
                }
            }
            _ => {
                // Unknown tag — stop replay (corrupt data).
                break;
            }
        }
    }

    // Build final state: apply deltas on top of snapshots.
    let mut result = HashMap::new();
    for (name, mut ri) in indexes {
        let metric_enum = match ri.metric {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            _ => DistanceMetric::L2,
        };

        let mut hnsw = match ri.hnsw {
            Some(h) => h,
            None => {
                let config = HnswConfig {
                    m: ri.m as usize,
                    m_max0: (ri.m as usize) * 2,
                    ef_construction: 200,
                    ef_search: ri.ef as usize,
                    metric: metric_enum,
                };
                HnswIndex::new(config)
            }
        };

        // Apply delta inserts.
        for (id, floats, pk) in ri.delta_inserts {
            hnsw.insert(id, Vector::new(floats));
            if let Some(pk) = pk {
                // Mirror the live `PkRegistry::upsert`: a pk re-inserted
                // without an intervening DELETE record tombstoned its old
                // node at write time, so count it here too.
                if let Some(old) = ri.registry.pk_to_node.insert(pk, id) {
                    ri.node_to_pk.remove(&old);
                    ri.registry.tombstones += 1;
                }
                ri.node_to_pk.insert(id, pk);
                ri.has_registry = true;
            }
            // Delta records replayed after a snapshot can hold node ids the
            // checkpoint-time registry never allocated; the floor follows
            // every id seen, pk-carrying or not.
            ri.registry.next_node = ri.registry.next_node.max(id + 1);
        }

        // Apply delta deletes.
        for id in ri.delta_deletes {
            hnsw.mark_deleted(id);
            if let Some(pk) = ri.node_to_pk.remove(&id) {
                ri.registry.pk_to_node.remove(&pk);
                ri.registry.tombstones += 1;
            }
        }

        result.insert(
            name,
            RecoveredIndex {
                hnsw,
                dims: ri.dims,
                metric: ri.metric,
                m: ri.m,
                ef: ri.ef,
                registry: ri.has_registry.then_some(ri.registry),
            },
        );
    }

    ReplayOutcome {
        state: VectorWalState {
            indexes: result,
            max_xact_id,
        },
        corruption,
    }
}

// ─── Primitive readers ────────────────────────────────────────────────────────

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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a simple HNSW index with `n` random-ish vectors of dimension `dim`.
    fn make_index(n: usize, dim: usize, metric: DistanceMetric) -> HnswIndex {
        let config = HnswConfig {
            m: 8,
            m_max0: 16,
            ef_construction: 100,
            ef_search: 50,
            metric,
        };
        let mut idx = HnswIndex::new(config);
        for i in 0..n {
            // Deterministic pseudo-random vectors seeded by id.
            let data: Vec<f32> = (0..dim)
                .map(|d| ((i * 73 + d * 37) % 1000) as f32 / 1000.0)
                .collect();
            idx.insert(i as u64, Vector::new(data));
        }
        idx
    }

    /// NU-204: a snapshot blob that does not deserialize used to become
    /// `hnsw: None`, which is the SAME state `TAG_CREATE_INDEX` produces for an
    /// index that has never been checkpointed. So the index was rebuilt from
    /// deltas alone, every vector in the snapshot was lost at restart, and
    /// `open` returned `Ok`.
    #[test]
    fn a_corrupt_hnsw_snapshot_is_reported_not_silently_emptied() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let idx = make_index(24, 8, DistanceMetric::L2);
        let mut snaps = HashMap::new();
        snaps.insert(
            "v".to_string(),
            IndexSnapshot {
                hnsw: &idx,
                dims: 8,
                metric: 0,
                m: 8,
                ef: 50,
                registry: None,
            },
        );
        wal.checkpoint(&snaps).unwrap();
        drop(wal);

        // Clean reopen holds the vectors, so the fixture is real.
        {
            let (_w, st) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            assert!(
                st.indexes.contains_key("v"),
                "clean reopen should recover the index"
            );
        }

        // Corrupt the middle of the serialized HNSW blob, leaving the record
        // framing (tag, name, dims, metric, m, ef, blob_len) intact — so this
        // is a bad payload, not a torn tail.
        let path = dir.path().join("vector.wal");
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        for b in bytes[mid..].iter_mut().take(64) {
            *b ^= 0xFF;
        }
        std::fs::write(&path, &bytes).unwrap();

        match VectorWal::open(dir.path(), &Default::default()) {
            Ok((_w, st)) => panic!(
                "a corrupt HNSW snapshot opened successfully with {} index(es); \
                 an empty index and a lost one must not look the same",
                st.indexes.len()
            ),
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                assert!(
                    e.to_string().contains("HNSW snapshot"),
                    "the error should name what failed, got: {e}"
                );
            }
        }
    }

    /// An index created but never checkpointed legitimately has no snapshot,
    /// and must still recover cleanly — a fix that treated every missing HNSW
    /// as corruption would pass the test above and break normal operation.
    #[test]
    fn an_index_with_no_snapshot_yet_still_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        wal.log_create_index(None, "fresh", 4, 0, 8, 50).unwrap();
        wal.log_insert(None, "fresh", 1, &[1.0, 2.0, 3.0, 4.0], "")
            .unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        let (_w, st) = VectorWal::open(dir.path(), &Default::default())
            .expect("an index with deltas and no checkpoint is not corrupt");
        assert!(st.indexes.contains_key("fresh"));
    }

    #[test]
    fn group_sync_marks_clean() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        assert!(!wal.is_dirty(), "a fresh WAL has no un-fsynced appends");
        wal.log_insert(None, "idx", 1, &[1.0, 2.0, 3.0], "")
            .unwrap();
        assert!(wal.is_dirty(), "an append is uncovered until fsync");
        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "group_sync fsyncs the tail");
    }

    /// S31-14: same class as the streams test — a checkpoint reopen failure
    /// must not leave later inserts acknowledged into an unlinked inode. The
    /// discriminator is durability: the post-failure insert must land in the
    /// replaced file (snapshot 3 + one delta = 4 on recovery).
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "idx", 4, 0, 8, 50).unwrap();
            let idx = make_index(3, 4, DistanceMetric::L2);
            let mut snaps = HashMap::new();
            snaps.insert(
                "idx".to_string(),
                IndexSnapshot {
                    hnsw: &idx,
                    dims: 4,
                    metric: 0,
                    m: 8,
                    ef: 50,
                    registry: None,
                },
            );
            wal.fail_reopen_once
                .store(true, std::sync::atomic::Ordering::SeqCst);
            wal.checkpoint(&snaps)
                .expect_err("the injected reopen failure must fail the checkpoint");
            wal.log_insert(None, "idx", 99, &[9.0, 9.0, 9.0, 9.0], "")
                .expect("a later append must reattach, not strand");
        }
        let (_w, st) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        assert_eq!(
            st.indexes["idx"].hnsw.len(),
            4,
            "the post-checkpoint-failure insert went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
    }

    /// F1b: the pk -> node registry survives a reopen built purely from delta
    /// records — no checkpoint ever ran, so the snapshot section cannot be the
    /// carrier; the pk rides in the INSERT records' metadata field. Without
    /// it, the first post-reopen delete of a pre-restart row cannot resolve
    /// its node and silently declines.
    #[test]
    fn pk_registry_rebuilds_from_pk_carrying_delta_records() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "idx", 4, 0, 8, 50).unwrap();
            wal.log_insert(None, "idx", 0, &[1.0, 0.0, 0.0, 0.0], "7")
                .unwrap();
            wal.log_insert(None, "idx", 1, &[0.0, 1.0, 0.0, 0.0], "8")
                .unwrap();
            wal.log_delete(None, "idx", 0).unwrap();
        }
        let (_w, st) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let registry = st.indexes["idx"]
            .registry
            .as_ref()
            .expect("pk-carrying delta records must recover a registry");
        assert_eq!(registry.pk_to_node.get(&8), Some(&1));
        assert!(
            !registry.pk_to_node.contains_key(&7),
            "the deleted node's mapping must not survive"
        );
        assert_eq!(registry.tombstones, 1);
        assert!(
            registry.next_node >= 2,
            "the allocator floor follows ids seen"
        );
    }

    /// F1b: a registry checkpointed into a snapshot merges with the pk-carrying
    /// deltas logged after it, and the recovered `next_node` can never sit
    /// below an id those deltas hold.
    #[test]
    fn a_snapshotted_registry_merges_with_later_pk_deltas() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "idx", 4, 0, 8, 50).unwrap();
            let idx = make_index(2, 4, DistanceMetric::L2);
            let mut registry = RegistrySection::default();
            registry.pk_to_node.insert(1, 0);
            registry.pk_to_node.insert(2, 1);
            registry.next_node = 2;
            let mut snaps = HashMap::new();
            snaps.insert(
                "idx".to_string(),
                IndexSnapshot {
                    hnsw: &idx,
                    dims: 4,
                    metric: 0,
                    m: 8,
                    ef: 50,
                    registry: Some(&registry),
                },
            );
            wal.checkpoint(&snaps).unwrap();
            // Delta after the snapshot: a fresh node the checkpoint-time
            // registry never allocated, logged with its pk.
            wal.log_insert(None, "idx", 9, &[9.0, 9.0, 9.0, 9.0], "3")
                .unwrap();
            wal.log_delete(None, "idx", 0).unwrap();
        }
        let (_w, st) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let registry = st.indexes["idx"]
            .registry
            .as_ref()
            .expect("the snapshot carried a registry section");
        assert_eq!(registry.pk_to_node.get(&2), Some(&1), "snapshot half");
        assert_eq!(registry.pk_to_node.get(&3), Some(&9), "delta half");
        assert!(
            !registry.pk_to_node.contains_key(&1),
            "the delta delete of node 0 must un-map pk 1"
        );
        assert!(
            registry.next_node >= 10,
            "next_node must cover delta ids beyond the snapshot's registry"
        );
    }

    // ── Test 1: Insert 50 vectors, reopen, search returns same results ──────
    #[test]
    fn test_insert_reopen_search() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 8;
        let n = 50;

        // Phase 1: create WAL, log CREATE + 50 INSERTs, drop.
        {
            let (wal, state) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            assert!(state.indexes.is_empty());

            wal.log_create_index(None, "idx1", dim as u32, 0, 8, 50)
                .unwrap();
            for i in 0..n {
                let v: Vec<f32> = (0..dim)
                    .map(|d| ((i * 73 + d * 37) % 1000) as f32 / 1000.0)
                    .collect();
                wal.log_insert(None, "idx1", i as u64, &v, "").unwrap();
            }
            drop(wal);
        }

        // Phase 2: reopen and verify.
        let (_wal2, state2) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let recovered = state2.indexes.get("idx1").unwrap();
        assert_eq!(recovered.hnsw.len(), n);

        // Search should find vector 0 nearest to itself.
        let q: Vec<f32> = (0..dim)
            .map(|d| ((d * 37) % 1000) as f32 / 1000.0)
            .collect();
        let results = recovered.hnsw.search(&Vector::new(q), 5);
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 0); // exact match
    }

    // ── Test 2: Delete vector, reopen, verify excluded ──────────────────────
    #[test]
    fn test_delete_reopen_excluded() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "idx1", 4, 0, 8, 50).unwrap();
            for i in 0..10u64 {
                let v = vec![i as f32; 4];
                wal.log_insert(None, "idx1", i, &v, "").unwrap();
            }
            wal.log_delete(None, "idx1", 5).unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let recovered = &state.indexes["idx1"];
        // HNSW stores nodes even if deleted; len() includes them.
        assert_eq!(recovered.hnsw.len(), 10);
        // But searching near vector 5 should not return id 5.
        let q = Vector::new(vec![5.0; 4]);
        let results = recovered.hnsw.search(&q, 10);
        let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(!ids.contains(&5));
    }

    // ── Test 3: Multiple indexes survive restart ────────────────────────────
    #[test]
    fn test_multiple_indexes_survive() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "a", 4, 0, 8, 50).unwrap();
            wal.log_create_index(None, "b", 4, 1, 16, 100).unwrap();
            wal.log_insert(None, "a", 1, &[1.0, 0.0, 0.0, 0.0], "")
                .unwrap();
            wal.log_insert(None, "a", 2, &[0.0, 1.0, 0.0, 0.0], "")
                .unwrap();
            wal.log_insert(None, "b", 10, &[0.5, 0.5, 0.0, 0.0], "")
                .unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        assert_eq!(state.indexes.len(), 2);
        assert_eq!(state.indexes["a"].hnsw.len(), 2);
        assert_eq!(state.indexes["b"].hnsw.len(), 1);
        assert_eq!(state.indexes["a"].metric, 0); // L2
        assert_eq!(state.indexes["b"].metric, 1); // Cosine
    }

    // ── Test 4: Snapshot + delta replay works ───────────────────────────────
    #[test]
    fn test_snapshot_plus_delta() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "idx1", 4, 0, 8, 50).unwrap();
            for i in 0..20u64 {
                let v = vec![i as f32; 4];
                wal.log_insert(None, "idx1", i, &v, "").unwrap();
            }

            // Build the in-memory index for the snapshot.
            let idx = make_index(20, 4, DistanceMetric::L2);
            let mut snaps = HashMap::new();
            snaps.insert(
                "idx1".to_string(),
                IndexSnapshot {
                    hnsw: &idx,
                    dims: 4,
                    metric: 0,
                    m: 8,
                    ef: 50,
                    registry: None,
                },
            );
            wal.checkpoint(&snaps).unwrap();

            // Insert 5 more after the snapshot.
            for i in 20..25u64 {
                let v = vec![i as f32; 4];
                wal.log_insert(None, "idx1", i, &v, "").unwrap();
            }
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let recovered = &state.indexes["idx1"];
        // Snapshot had 20 + 5 delta inserts = 25 total.
        assert_eq!(recovered.hnsw.len(), 25);
    }

    // ── Test 5: Cosine / L2 / InnerProduct metrics preserved ────────────────
    #[test]
    fn test_metrics_preserved() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "l2", 4, 0, 8, 50).unwrap();
            wal.log_create_index(None, "cos", 4, 1, 8, 50).unwrap();
            wal.log_create_index(None, "ip", 4, 2, 8, 50).unwrap();
            wal.log_insert(None, "l2", 1, &[1.0, 0.0, 0.0, 0.0], "")
                .unwrap();
            wal.log_insert(None, "cos", 1, &[1.0, 0.0, 0.0, 0.0], "")
                .unwrap();
            wal.log_insert(None, "ip", 1, &[1.0, 0.0, 0.0, 0.0], "")
                .unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        assert_eq!(state.indexes["l2"].metric, 0);
        assert_eq!(state.indexes["cos"].metric, 1);
        assert_eq!(state.indexes["ip"].metric, 2);
        // Verify the HNSW config metric matches.
        let l2_q = Vector::new(vec![1.0, 0.0, 0.0, 0.0]);
        let cos_q = Vector::new(vec![0.0, 1.0, 0.0, 0.0]);
        // L2: same vector → distance 0
        let l2_res = state.indexes["l2"].hnsw.search(&l2_q, 1);
        assert!(!l2_res.is_empty());
        assert!(l2_res[0].1 < 1e-5);
        // Cosine: orthogonal → distance 1
        let cos_res = state.indexes["cos"].hnsw.search(&cos_q, 1);
        assert!(!cos_res.is_empty());
        assert!((cos_res[0].1 - 1.0).abs() < 1e-4);
    }

    // ── Test 6: Metadata filtering works after recovery ─────────────────────
    #[test]
    fn test_metadata_preserved_after_recovery() {
        // The WAL stores metadata strings; verify they don't corrupt parsing.
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "idx1", 4, 0, 8, 50).unwrap();
            wal.log_insert(None, "idx1", 1, &[1.0, 0.0, 0.0, 0.0], r#"{"color":"red"}"#)
                .unwrap();
            wal.log_insert(
                None,
                "idx1",
                2,
                &[0.0, 1.0, 0.0, 0.0],
                r#"{"color":"blue"}"#,
            )
            .unwrap();
            wal.log_insert(None, "idx1", 3, &[0.0, 0.0, 1.0, 0.0], "")
                .unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let recovered = &state.indexes["idx1"];
        assert_eq!(recovered.hnsw.len(), 3);
        // Search still works (metadata doesn't corrupt index).
        let q = Vector::new(vec![1.0, 0.0, 0.0, 0.0]);
        let results = recovered.hnsw.search(&q, 3);
        assert_eq!(results[0].0, 1);
    }

    // ── Test 7: Corrupt WAL falls back to last snapshot ─────────────────────
    #[test]
    fn test_corrupt_wal_falls_back_to_snapshot() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "idx1", 4, 0, 8, 50).unwrap();
            for i in 0..10u64 {
                let v = vec![i as f32; 4];
                wal.log_insert(None, "idx1", i, &v, "").unwrap();
            }

            // Checkpoint with a real index.
            let idx = make_index(10, 4, DistanceMetric::L2);
            let mut snaps = HashMap::new();
            snaps.insert(
                "idx1".to_string(),
                IndexSnapshot {
                    hnsw: &idx,
                    dims: 4,
                    metric: 0,
                    m: 8,
                    ef: 50,
                    registry: None,
                },
            );
            wal.checkpoint(&snaps).unwrap();

            // Insert 3 more valid deltas after snapshot.
            for i in 10..13u64 {
                let v = vec![i as f32; 4];
                wal.log_insert(None, "idx1", i, &v, "").unwrap();
            }
            drop(wal);
        }

        // Append garbage bytes to the WAL file.
        {
            let path = dir.path().join("vector.wal");
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD, 0xFC, 0xFB]).unwrap();
            f.flush().unwrap();
        }

        // Reopen — should recover snapshot (10) + 3 valid deltas = 13, ignoring garbage.
        let (_wal2, state) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let recovered = &state.indexes["idx1"];
        assert_eq!(recovered.hnsw.len(), 13);
    }

    // ── Test 8: Large index → checkpoint → reopen ───────────────────────────
    #[test]
    fn test_large_index_checkpoint_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let n = 200;
        let dim = 16;

        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            wal.log_create_index(None, "big", dim as u32, 0, 16, 50)
                .unwrap();

            let idx = make_index(n, dim, DistanceMetric::L2);
            // Log all inserts (for completeness, though snapshot will supersede them).
            for i in 0..n {
                let data: Vec<f32> = (0..dim)
                    .map(|d| ((i * 73 + d * 37) % 1000) as f32 / 1000.0)
                    .collect();
                wal.log_insert(None, "big", i as u64, &data, "").unwrap();
            }

            // Checkpoint.
            let mut snaps = HashMap::new();
            snaps.insert(
                "big".to_string(),
                IndexSnapshot {
                    hnsw: &idx,
                    dims: dim as u32,
                    metric: 0,
                    m: 16,
                    ef: 50,
                    registry: None,
                },
            );
            wal.checkpoint(&snaps).unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let recovered = &state.indexes["big"];
        assert_eq!(recovered.hnsw.len(), n);

        // Search produces valid results.
        let q_data: Vec<f32> = (0..dim)
            .map(|d| ((d * 37) % 1000) as f32 / 1000.0)
            .collect();
        let results = recovered.hnsw.search(&Vector::new(q_data), 5);
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 0); // vector 0 should be nearest to itself
    }

    /// S35 F1a: a checkpoint snapshot must carry the tombstone set.
    ///
    /// `checkpoint` snapshots through `HnswIndex::serialize`, which never
    /// wrote `deleted`. An acknowledged DELETE followed by a checkpoint (the
    /// server's recurring task, or the probe's explicit call) resurrected the
    /// vector on the next reopen — replay had a snapshot in which the delete
    /// never happened. Asserted through `live_ids`, not a search: `len()`
    /// counts tombstoned nodes and cannot see a resurrection.
    #[test]
    fn checkpointed_tombstones_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
            let mut idx = make_index(24, 8, DistanceMetric::L2);
            idx.mark_deleted(5);
            idx.mark_deleted(11);
            let mut snaps = HashMap::new();
            snaps.insert(
                "v".to_string(),
                IndexSnapshot {
                    hnsw: &idx,
                    dims: 8,
                    metric: 0,
                    m: 8,
                    ef: 50,
                    registry: None,
                },
            );
            wal.checkpoint(&snaps).unwrap();
            drop(wal);
        }

        let (_w, st) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let live = st.indexes["v"].hnsw.live_ids();
        assert_eq!(
            live.len(),
            22,
            "checkpoint dropped tombstones: {}/24 live, expected 22 — deleted vectors \
             resurrect across a checkpoint-restart cycle",
            live.len()
        );
        assert!(!live.contains(&5) && !live.contains(&11));
    }

    /// A snapshot written by a build that predates per-blob checksums must
    /// still open.
    ///
    /// The checksum is a new record tag, so the legacy layout has to be
    /// exercised by hand — nothing writes it any more. Without this, "old
    /// databases still open" would be an assumption, and the FTS dual-
    /// persistence finding is the standing reminder of what that assumption
    /// costs when it is wrong.
    #[test]
    fn a_pre_checksum_snapshot_still_opens() {
        let dir = tempfile::tempdir().unwrap();
        let idx = make_index(24, 8, DistanceMetric::L2);
        let blob = idx.serialize(None);

        // TAG_SNAPSHOT (0x04): count, name, dims, metric, m, ef, blob_len, blob
        // — no CRC field between blob_len and blob.
        let mut bytes = vec![TAG_SNAPSHOT];
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(b"v");
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&50u32.to_le_bytes());
        bytes.extend_from_slice(&(blob.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&blob);
        std::fs::write(dir.path().join("vector.wal"), &bytes).unwrap();

        let (_w, st) = VectorWal::open(dir.path(), &Default::default())
            .expect("a legacy snapshot must still open");
        assert!(
            st.indexes.contains_key("v"),
            "the legacy snapshot's index was lost"
        );
        assert_eq!(
            st.indexes["v"].hnsw.len(),
            24,
            "the legacy snapshot must recover every vector"
        );
    }

    /// Payload corruption must be caught, not absorbed.
    ///
    /// This is the case `deserialize` structurally cannot catch: flip bits deep
    /// inside the serialized blob, leaving every length and count intact, and
    /// what comes back parses. Before the checksum it opened with all 24
    /// vectors present and wrong — a database reporting success while serving
    /// data nobody wrote. Corrupting the last quarter keeps it clear of the
    /// record framing regardless of how the graph happened to be shaped.
    #[test]
    fn corrupt_snapshot_payload_is_caught_by_the_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let idx = make_index(24, 8, DistanceMetric::L2);
        let mut snaps = HashMap::new();
        snaps.insert(
            "v".to_string(),
            IndexSnapshot {
                hnsw: &idx,
                dims: 8,
                metric: 0,
                m: 8,
                ef: 50,
                registry: None,
            },
        );
        wal.checkpoint(&snaps).unwrap();
        drop(wal);

        let path = dir.path().join("vector.wal");
        let mut bytes = std::fs::read(&path).unwrap();
        let start = bytes.len() * 3 / 4;
        for b in bytes[start..].iter_mut().take(16) {
            *b ^= 0xFF;
        }
        std::fs::write(&path, &bytes).unwrap();

        match VectorWal::open(dir.path(), &Default::default()) {
            Ok((_w, st)) => panic!(
                "payload corruption was absorbed: opened with {} index(es) holding {} \
                 vector(s) nobody wrote",
                st.indexes.len(),
                st.indexes.get("v").map(|i| i.hnsw.len()).unwrap_or(0)
            ),
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                assert!(
                    e.to_string().contains("checksum"),
                    "the error should name the checksum, got: {e}"
                );
            }
        }
    }

    /// The checksum is what makes the corrupt-snapshot guarantee hold for every
    /// byte rather than for most of them, so a clean round-trip has to keep
    /// working — a checksum that rejected valid data would pass both tests
    /// above and break every restart.
    #[test]
    fn a_checksummed_snapshot_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        let idx = make_index(24, 8, DistanceMetric::L2);
        let mut snaps = HashMap::new();
        snaps.insert(
            "v".to_string(),
            IndexSnapshot {
                hnsw: &idx,
                dims: 8,
                metric: 0,
                m: 8,
                ef: 50,
                registry: None,
            },
        );
        wal.checkpoint(&snaps).unwrap();
        drop(wal);

        let (_w, st) = VectorWal::open(dir.path(), &Default::default())
            .expect("a checksummed snapshot must open");
        assert_eq!(st.indexes["v"].hnsw.len(), 24);
    }
}

#[cfg(test)]
mod s63_tests {
    use super::*;

    fn committed(ids: &[u64]) -> std::collections::HashSet<u64> {
        ids.iter().copied().collect()
    }

    /// S63: tagged records survive replay iff their transaction durably
    /// committed (or they are autocommit); v1 records always keep.
    #[test]
    fn tagged_records_filter_by_committed_set() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        wal.log_create_index(None, "v", 4, 0, 8, 48).unwrap();
        // xact 7 committed; xact 9 never did; autocommit (0) always keeps.
        wal.log_insert(Some(7), "v", 1, &[1.0, 0.0, 0.0, 0.0], "")
            .unwrap();
        wal.log_insert(Some(9), "v", 2, &[0.0, 1.0, 0.0, 0.0], "")
            .unwrap();
        wal.log_insert(Some(0), "v", 3, &[0.0, 0.0, 1.0, 0.0], "")
            .unwrap();
        wal.log_delete(Some(9), "v", 1).unwrap();
        wal.group_sync().unwrap();

        let (_w, state) = VectorWal::open(dir.path(), &committed(&[7])).unwrap();
        let idx = state.indexes.get("v").unwrap();
        // Node 1 was inserted by 7 (kept) then DELETED by 9 (discarded) — so
        // it survives the filter race-free: the delete never happened.
        assert!(idx.hnsw.vector_of(1).is_some(), "kept insert");
        assert!(
            idx.hnsw.vector_of(2).is_none(),
            "uncommitted insert discarded"
        );
        assert!(idx.hnsw.vector_of(3).is_some(), "autocommit insert kept");
        // The floor is pinned by EVERY tagged id, committed or not — a
        // rolled-back id must never be re-minted.
        assert_eq!(state.max_xact_id, 9);
    }

    /// A rolled-back index CREATE leaves no empty shell behind: the tagged
    /// CREATE_INDEX record is discarded with its transaction.
    #[test]
    fn uncommitted_create_index_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = VectorWal::open(dir.path(), &Default::default()).unwrap();
        wal.log_create_index(Some(5), "ghost", 4, 0, 8, 48).unwrap();
        wal.group_sync().unwrap();

        let (_w, state) = VectorWal::open(dir.path(), &committed(&[])).unwrap();
        assert!(
            !state.indexes.contains_key("ghost"),
            "an uncommitted CREATE must not survive replay"
        );
        assert_eq!(state.max_xact_id, 5, "the floor still pins the dead id");
    }
}
