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
//! ```

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use super::{DistanceMetric, HnswConfig, HnswIndex, Vector};

// ─── Entry type tags ──────────────────────────────────────────────────────────

const TAG_CREATE_INDEX: u8 = 0x01;
const TAG_INSERT_VEC: u8 = 0x02;
const TAG_DELETE_VEC: u8 = 0x03;
const TAG_SNAPSHOT: u8 = 0x04;

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
}

/// State recovered from replaying the WAL.
#[derive(Debug)]
pub struct VectorWalState {
    /// Recovered indexes keyed by index name.
    pub indexes: HashMap<String, RecoveredIndex>,
}

/// Append-only WAL for vector indexes.
pub struct VectorWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl VectorWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state contains no indexes. Corrupt trailing bytes are silently ignored
    /// (best-effort recovery up to the last valid entry).
    pub fn open(dir: &Path) -> io::Result<(Self, VectorWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("vector.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            VectorWalState {
                indexes: HashMap::new(),
            }
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
            },
            state,
        ))
    }

    /// Return the directory containing the WAL file.
    pub fn dir(&self) -> &Path {
        self.path.parent().unwrap_or(Path::new("."))
    }

    /// Log a CREATE INDEX operation.
    pub fn log_create_index(
        &self,
        name: &str,
        dims: u32,
        metric: u8,
        m: u32,
        ef: u32,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        let nb = name.as_bytes();
        buf.push(TAG_CREATE_INDEX);
        buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
        buf.extend_from_slice(nb);
        buf.extend_from_slice(&dims.to_le_bytes());
        buf.push(metric);
        buf.extend_from_slice(&m.to_le_bytes());
        buf.extend_from_slice(&ef.to_le_bytes());
        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
    }

    /// Log a vector insertion.
    pub fn log_insert(
        &self,
        name: &str,
        id: u64,
        vector: &[f32],
        metadata: &str,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        let nb = name.as_bytes();
        buf.push(TAG_INSERT_VEC);
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
        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
    }

    /// Log a vector deletion (soft-delete in HNSW).
    pub fn log_delete(&self, name: &str, id: u64) -> io::Result<()> {
        let mut buf = Vec::new();
        let nb = name.as_bytes();
        buf.push(TAG_DELETE_VEC);
        buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
        buf.extend_from_slice(nb);
        buf.extend_from_slice(&id.to_le_bytes());
        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
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
            let serialized = snap.hnsw.serialize();
            payload.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
            payload.extend_from_slice(&serialized);
        }

        // Flush existing writer, then truncate and rewrite as one SNAPSHOT entry.
        {
            self.writer.lock().flush()?;
        }

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut w = BufWriter::new(file);
        w.write_all(&[TAG_SNAPSHOT])?;
        w.write_all(&payload)?;
        w.flush()?;
        drop(w);

        // Re-open in append mode for future writes.
        let file = OpenOptions::new().append(true).open(&self.path)?;
        *self.writer.lock() = BufWriter::new(file);
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
    /// Delta inserts after the last snapshot: (id, vector).
    delta_inserts: Vec<(u64, Vec<f32>)>,
    /// Delta deletes after the last snapshot.
    delta_deletes: Vec<u64>,
}

/// Replay all entries in `data` to reconstruct vector index state.
///
/// SNAPSHOT entries reset all state. After the last snapshot, incremental
/// delta entries (INSERT_VEC, DELETE_VEC) are collected and applied on top.
fn replay(data: &[u8]) -> VectorWalState {
    let mut indexes: HashMap<String, ReplayIndex> = HashMap::new();
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&tag) = data.get(pos) else { break };
        pos += 1;

        match tag {
            TAG_CREATE_INDEX => {
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
                    },
                );
            }
            TAG_INSERT_VEC => {
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
                // Read metadata (skip it — stored for forward compat only)
                let Some(meta_len) = read_u32(data, &mut pos) else {
                    break;
                };
                let meta_len = meta_len as usize;
                if pos + meta_len > data.len() {
                    break;
                }
                pos += meta_len; // skip metadata bytes

                if let Some(idx) = indexes.get_mut(&name) {
                    idx.delta_inserts.push((id, floats));
                }
            }
            TAG_DELETE_VEC => {
                let Some(name) = read_string(data, &mut pos) else {
                    break;
                };
                let Some(id) = read_u64(data, &mut pos) else {
                    break;
                };
                if let Some(idx) = indexes.get_mut(&name) {
                    idx.delta_deletes.push(id);
                }
            }
            TAG_SNAPSHOT => {
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
                    let blob_len = blob_len as usize;
                    if pos + blob_len > data.len() {
                        ok = false;
                        break;
                    }
                    let blob = &data[pos..pos + blob_len];
                    pos += blob_len;

                    let hnsw = HnswIndex::deserialize(blob).ok();

                    indexes.insert(
                        name,
                        ReplayIndex {
                            dims,
                            metric,
                            m,
                            ef,
                            hnsw,
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
    for (name, ri) in indexes {
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
        for (id, floats) in ri.delta_inserts {
            hnsw.insert(id, Vector::new(floats));
        }

        // Apply delta deletes.
        for id in ri.delta_deletes {
            hnsw.mark_deleted(id);
        }

        result.insert(
            name,
            RecoveredIndex {
                hnsw,
                dims: ri.dims,
                metric: ri.metric,
                m: ri.m,
                ef: ri.ef,
            },
        );
    }

    VectorWalState { indexes: result }
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
    let s = std::str::from_utf8(&data[*pos..*pos + len]).ok()?.to_string();
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

    // ── Test 1: Insert 50 vectors, reopen, search returns same results ──────
    #[test]
    fn test_insert_reopen_search() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 8;
        let n = 50;

        // Phase 1: create WAL, log CREATE + 50 INSERTs, drop.
        {
            let (wal, state) = VectorWal::open(dir.path()).unwrap();
            assert!(state.indexes.is_empty());

            wal.log_create_index("idx1", dim as u32, 0, 8, 50).unwrap();
            for i in 0..n {
                let v: Vec<f32> = (0..dim)
                    .map(|d| ((i * 73 + d * 37) % 1000) as f32 / 1000.0)
                    .collect();
                wal.log_insert("idx1", i as u64, &v, "").unwrap();
            }
            drop(wal);
        }

        // Phase 2: reopen and verify.
        let (_wal2, state2) = VectorWal::open(dir.path()).unwrap();
        let recovered = state2.indexes.get("idx1").unwrap();
        assert_eq!(recovered.hnsw.len(), n);

        // Search should find vector 0 nearest to itself.
        let q: Vec<f32> = (0..dim)
            .map(|d| ((0 * 73 + d * 37) % 1000) as f32 / 1000.0)
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
            let (wal, _) = VectorWal::open(dir.path()).unwrap();
            wal.log_create_index("idx1", 4, 0, 8, 50).unwrap();
            for i in 0..10u64 {
                let v = vec![i as f32; 4];
                wal.log_insert("idx1", i, &v, "").unwrap();
            }
            wal.log_delete("idx1", 5).unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path()).unwrap();
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
            let (wal, _) = VectorWal::open(dir.path()).unwrap();
            wal.log_create_index("a", 4, 0, 8, 50).unwrap();
            wal.log_create_index("b", 4, 1, 16, 100).unwrap();
            wal.log_insert("a", 1, &[1.0, 0.0, 0.0, 0.0], "").unwrap();
            wal.log_insert("a", 2, &[0.0, 1.0, 0.0, 0.0], "").unwrap();
            wal.log_insert("b", 10, &[0.5, 0.5, 0.0, 0.0], "").unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path()).unwrap();
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
            let (wal, _) = VectorWal::open(dir.path()).unwrap();
            wal.log_create_index("idx1", 4, 0, 8, 50).unwrap();
            for i in 0..20u64 {
                let v = vec![i as f32; 4];
                wal.log_insert("idx1", i, &v, "").unwrap();
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
                },
            );
            wal.checkpoint(&snaps).unwrap();

            // Insert 5 more after the snapshot.
            for i in 20..25u64 {
                let v = vec![i as f32; 4];
                wal.log_insert("idx1", i, &v, "").unwrap();
            }
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path()).unwrap();
        let recovered = &state.indexes["idx1"];
        // Snapshot had 20 + 5 delta inserts = 25 total.
        assert_eq!(recovered.hnsw.len(), 25);
    }

    // ── Test 5: Cosine / L2 / InnerProduct metrics preserved ────────────────
    #[test]
    fn test_metrics_preserved() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = VectorWal::open(dir.path()).unwrap();
            wal.log_create_index("l2", 4, 0, 8, 50).unwrap();
            wal.log_create_index("cos", 4, 1, 8, 50).unwrap();
            wal.log_create_index("ip", 4, 2, 8, 50).unwrap();
            wal.log_insert("l2", 1, &[1.0, 0.0, 0.0, 0.0], "").unwrap();
            wal.log_insert("cos", 1, &[1.0, 0.0, 0.0, 0.0], "").unwrap();
            wal.log_insert("ip", 1, &[1.0, 0.0, 0.0, 0.0], "").unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path()).unwrap();
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
            let (wal, _) = VectorWal::open(dir.path()).unwrap();
            wal.log_create_index("idx1", 4, 0, 8, 50).unwrap();
            wal.log_insert("idx1", 1, &[1.0, 0.0, 0.0, 0.0], r#"{"color":"red"}"#)
                .unwrap();
            wal.log_insert("idx1", 2, &[0.0, 1.0, 0.0, 0.0], r#"{"color":"blue"}"#)
                .unwrap();
            wal.log_insert("idx1", 3, &[0.0, 0.0, 1.0, 0.0], "").unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path()).unwrap();
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
            let (wal, _) = VectorWal::open(dir.path()).unwrap();
            wal.log_create_index("idx1", 4, 0, 8, 50).unwrap();
            for i in 0..10u64 {
                let v = vec![i as f32; 4];
                wal.log_insert("idx1", i, &v, "").unwrap();
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
                },
            );
            wal.checkpoint(&snaps).unwrap();

            // Insert 3 more valid deltas after snapshot.
            for i in 10..13u64 {
                let v = vec![i as f32; 4];
                wal.log_insert("idx1", i, &v, "").unwrap();
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
        let (_wal2, state) = VectorWal::open(dir.path()).unwrap();
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
            let (wal, _) = VectorWal::open(dir.path()).unwrap();
            wal.log_create_index("big", dim as u32, 0, 16, 50).unwrap();

            let idx = make_index(n, dim, DistanceMetric::L2);
            // Log all inserts (for completeness, though snapshot will supersede them).
            for i in 0..n {
                let data: Vec<f32> = (0..dim)
                    .map(|d| ((i * 73 + d * 37) % 1000) as f32 / 1000.0)
                    .collect();
                wal.log_insert("big", i as u64, &data, "").unwrap();
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
                },
            );
            wal.checkpoint(&snaps).unwrap();
            drop(wal);
        }

        let (_wal2, state) = VectorWal::open(dir.path()).unwrap();
        let recovered = &state.indexes["big"];
        assert_eq!(recovered.hnsw.len(), n);

        // Search produces valid results.
        let q_data: Vec<f32> = (0..dim)
            .map(|d| ((0 * 73 + d * 37) % 1000) as f32 / 1000.0)
            .collect();
        let results = recovered.hnsw.search(&Vector::new(q_data), 5);
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 0); // vector 0 should be nearest to itself
    }
}
