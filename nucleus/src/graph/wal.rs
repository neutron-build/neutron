//! Write-ahead log for the property graph store.
//!
//! Provides crash-recovery by recording all graph mutations to an append-only
//! log file (`graph.wal`). On restart the log is replayed from top to bottom
//! to reconstruct in-memory state.
//!
//! ## Log entry binary format
//! ```text
//! ADD_NODE:   [0x01] [node_id: u64 LE] [n_labels: u32 LE] [per label: len(u32)+bytes]
//!             [n_props: u32 LE] [per prop: key_len(u32)+key + val_tag(u8) + val_payload]
//! ADD_EDGE:   [0x02] [edge_id: u64 LE] [src: u64 LE] [dst: u64 LE]
//!             [type_len: u32 LE] [type: bytes]
//!             [n_props: u32 LE] [per prop: key_len(u32)+key + val_tag(u8) + val_payload]
//! DEL_NODE:   [0x03] [node_id: u64 LE]
//! DEL_EDGE:   [0x04] [edge_id: u64 LE]
//! SET_PROP:   [0x05] [node_or_edge: u8 (0=node,1=edge)] [id: u64 LE]
//!             [key_len: u32 LE] [key: bytes] [val_tag(u8) + val_payload]
//! ADD_INDEX:  [0x06] [label_len(u32)+label] [prop_len(u32)+prop]
//! DEL_INDEX:  [0x07] [label_len(u32)+label] [prop_len(u32)+prop]
//! SNAPSHOT:   [0x10] [full graph binary dump — all nodes + all edges + index defs]
//! ```
//!
//! ## What is and is not persisted for property indexes
//!
//! Only the *definition* `(label, property)` is logged. The index contents are
//! never written: they are rebuilt from the recovered nodes in
//! `GraphStore::open`. A definition without its data would be a silent
//! wrong-answer machine, and rebuilding a BTreeMap over an already in-memory
//! node set is cheap, so the definition is the only durable part.
//!
//! A SNAPSHOT resets all state. After `checkpoint()` the file is truncated to a
//! single SNAPSHOT entry so the log stays small.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use super::{EdgeId, NodeId, PropValue, Properties};

// ─── Entry type tags ────────────────────────────────────────────────────────

const TAG_ADD_NODE: u8 = 0x01;
const TAG_ADD_EDGE: u8 = 0x02;
const TAG_DEL_NODE: u8 = 0x03;
const TAG_DEL_EDGE: u8 = 0x04;
const TAG_SET_PROP: u8 = 0x05;
const TAG_ADD_INDEX: u8 = 0x06;
const TAG_DEL_INDEX: u8 = 0x07;
const TAG_SNAPSHOT: u8 = 0x10;

// PropValue wire tags
const PV_NULL: u8 = 0;
const PV_BOOL: u8 = 1;
const PV_INT: u8 = 2;
const PV_FLOAT: u8 = 3;
const PV_TEXT: u8 = 4;

// ─── Public types ───────────────────────────────────────────────────────────

/// A recovered node from WAL replay.
#[derive(Debug, Clone)]
pub struct WalNode {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: Properties,
}

/// A recovered edge from WAL replay.
#[derive(Debug, Clone)]
pub struct WalEdge {
    pub id: EdgeId,
    pub from: NodeId,
    pub to: NodeId,
    pub edge_type: String,
    pub properties: Properties,
}

/// Complete graph state recovered from a WAL replay.
#[derive(Debug, Clone, Default)]
pub struct GraphWalState {
    pub nodes: HashMap<NodeId, WalNode>,
    pub edges: HashMap<EdgeId, WalEdge>,
    pub next_node_id: NodeId,
    pub next_edge_id: EdgeId,
    /// Node property index definitions `(label, property)`. Contents are not
    /// persisted — the store rebuilds each index from the recovered nodes.
    pub node_indexes: BTreeSet<(String, String)>,
}

/// A snapshot of the current graph suitable for checkpointing.
pub struct GraphSnapshot<'a> {
    pub nodes: Vec<(&'a NodeId, &'a Vec<String>, &'a Properties)>,
    pub edges: Vec<(&'a EdgeId, &'a NodeId, &'a NodeId, &'a str, &'a Properties)>,
    pub next_node_id: NodeId,
    pub next_edge_id: EdgeId,
    /// Node property index definitions `(label, property)`.
    pub node_indexes: Vec<(&'a str, &'a str)>,
}

/// Append-only WAL for the property graph store.
pub struct GraphWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Group-commit fsync coordinator (durability of the un-checkpointed tail).
    syncer: crate::storage::wal_util::WalSync,
    /// The writer holds an inode a checkpoint's rename displaced: it is
    /// unlinked, so appends to it "succeed" into a file no future recovery
    /// reads while `group_sync`/`is_dirty` report healthy. Set when a
    /// checkpoint replaced the log but its reopen failed; cleared by the next
    /// successful reattach (or checkpoint reopen). See `reattach_if_stranded`.
    stranded: std::sync::atomic::AtomicBool,
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint`.
    #[cfg(test)]
    fail_reopen_once: std::sync::atomic::AtomicBool,
}

impl GraphWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored (best-effort
    /// recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, GraphWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("graph.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            GraphWalState::default()
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
            },
            state,
        ))
    }

    /// Log an ADD_NODE operation.
    pub fn log_add_node(
        &self,
        id: NodeId,
        labels: &[String],
        props: &Properties,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(TAG_ADD_NODE);
        buf.extend_from_slice(&id.to_le_bytes());
        encode_labels(labels, &mut buf);
        encode_props(props, &mut buf);
        self.append_raw(&buf)
    }

    /// Log an ADD_EDGE operation.
    pub fn log_add_edge(
        &self,
        id: EdgeId,
        src: NodeId,
        dst: NodeId,
        edge_type: &str,
        props: &Properties,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(TAG_ADD_EDGE);
        buf.extend_from_slice(&id.to_le_bytes());
        buf.extend_from_slice(&src.to_le_bytes());
        buf.extend_from_slice(&dst.to_le_bytes());
        encode_str(edge_type, &mut buf);
        encode_props(props, &mut buf);
        self.append_raw(&buf)
    }

    /// Log a DEL_NODE operation.
    pub fn log_del_node(&self, id: NodeId) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(TAG_DEL_NODE);
        buf.extend_from_slice(&id.to_le_bytes());
        self.append_raw(&buf)
    }

    /// Log a DEL_EDGE operation.
    pub fn log_del_edge(&self, id: EdgeId) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(TAG_DEL_EDGE);
        buf.extend_from_slice(&id.to_le_bytes());
        self.append_raw(&buf)
    }

    /// Log a SET_PROP operation.
    ///
    /// `target` is `0` for node, `1` for edge.
    pub fn log_set_prop(
        &self,
        target: u8,
        id: u64,
        key: &str,
        value: &PropValue,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(TAG_SET_PROP);
        buf.push(target);
        buf.extend_from_slice(&id.to_le_bytes());
        encode_str(key, &mut buf);
        encode_prop_value(value, &mut buf);
        self.append_raw(&buf)
    }

    /// Log the creation of a node property index definition.
    pub fn log_add_node_index(&self, label: &str, property: &str) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(TAG_ADD_INDEX);
        encode_str(label, &mut buf);
        encode_str(property, &mut buf);
        self.append_raw(&buf)
    }

    /// Log the removal of a node property index definition.
    pub fn log_drop_node_index(&self, label: &str, property: &str) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(TAG_DEL_INDEX);
        encode_str(label, &mut buf);
        encode_str(property, &mut buf);
        self.append_raw(&buf)
    }

    /// Write the complete current state of the graph as a single SNAPSHOT
    /// entry and truncate the log to just that entry.
    pub fn checkpoint(&self, snap: &GraphSnapshot<'_>) -> io::Result<()> {
        let mut payload = Vec::new();

        // next_node_id, next_edge_id
        payload.extend_from_slice(&snap.next_node_id.to_le_bytes());
        payload.extend_from_slice(&snap.next_edge_id.to_le_bytes());

        // nodes
        payload.extend_from_slice(&(snap.nodes.len() as u32).to_le_bytes());
        for &(id, labels, props) in &snap.nodes {
            payload.extend_from_slice(&id.to_le_bytes());
            encode_labels(labels, &mut payload);
            encode_props(props, &mut payload);
        }

        // edges
        payload.extend_from_slice(&(snap.edges.len() as u32).to_le_bytes());
        for &(id, src, dst, etype, props) in &snap.edges {
            payload.extend_from_slice(&id.to_le_bytes());
            payload.extend_from_slice(&src.to_le_bytes());
            payload.extend_from_slice(&dst.to_le_bytes());
            encode_str(etype, &mut payload);
            encode_props(props, &mut payload);
        }

        // Node property index definitions. Appended after the edges so a
        // snapshot written by an older build (which ends here) still decodes:
        // `decode_snapshot` treats "no bytes left" as "no indexes".
        payload.extend_from_slice(&(snap.node_indexes.len() as u32).to_le_bytes());
        for &(label, property) in &snap.node_indexes {
            encode_str(label, &mut payload);
            encode_str(property, &mut payload);
        }

        // Serialize the complete new log body (a single snapshot entry).
        let mut contents = Vec::with_capacity(payload.len() + 5);
        contents.push(TAG_SNAPSHOT);
        contents.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        contents.extend_from_slice(&payload);

        // Hold the writer lock across the whole checkpoint so no append can interleave
        // between the flush and the reopen (it would land in the about-to-be-replaced
        // file and be lost). Replace atomically — temp file + fsync + rename — so a crash
        // mid-checkpoint leaves the old log or the new snapshot, never an empty file.
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
            .then(|| io::Error::other("injected graph WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = if let Some(e) = injected {
            Err(e)
        } else if let Some(e) = crate::storage::crashpoint::io_fault("graph.wal_reopen") {
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

    // ─── Internal helpers ─────────────────────────────────────────────────

    fn append_raw(&self, data: &[u8]) -> io::Result<()> {
        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        w.write_all(data)?;
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
        if let Some(e) = crate::storage::crashpoint::io_fault("graph.wal_reopen") {
            return Err(e);
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "graph WAL writer is stranded: a checkpoint replaced {} but its \
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
}

// ─── Binary encoding helpers ────────────────────────────────────────────────

fn encode_str(s: &str, buf: &mut Vec<u8>) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

fn encode_labels(labels: &[String], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(labels.len() as u32).to_le_bytes());
    for l in labels {
        encode_str(l, buf);
    }
}

fn encode_prop_value(val: &PropValue, buf: &mut Vec<u8>) {
    match val {
        PropValue::Null => buf.push(PV_NULL),
        PropValue::Bool(b) => {
            buf.push(PV_BOOL);
            buf.push(*b as u8);
        }
        PropValue::Int(n) => {
            buf.push(PV_INT);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        PropValue::Float(f) => {
            buf.push(PV_FLOAT);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        PropValue::Text(s) => {
            buf.push(PV_TEXT);
            encode_str(s, buf);
        }
    }
}

fn encode_props(props: &Properties, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(props.len() as u32).to_le_bytes());
    for (k, v) in props {
        encode_str(k, buf);
        encode_prop_value(v, buf);
    }
}

// ─── Replay ─────────────────────────────────────────────────────────────────

/// Replay all entries in `data` to reconstruct graph state.
///
/// SNAPSHOT entries reset all state to their embedded snapshot, so only the
/// *last* SNAPSHOT (and subsequent incremental entries) matter in practice.
fn replay(data: &[u8]) -> GraphWalState {
    let mut state = GraphWalState::default();
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&tag) = data.get(pos) else { break };
        pos += 1;

        match tag {
            TAG_ADD_NODE => {
                let Some(id) = read_u64(data, &mut pos) else {
                    break;
                };
                let Some(labels) = decode_labels(data, &mut pos) else {
                    break;
                };
                let Some(props) = decode_props(data, &mut pos) else {
                    break;
                };
                if id >= state.next_node_id {
                    state.next_node_id = id + 1;
                }
                state.nodes.insert(
                    id,
                    WalNode {
                        id,
                        labels,
                        properties: props,
                    },
                );
            }
            TAG_ADD_EDGE => {
                let Some(id) = read_u64(data, &mut pos) else {
                    break;
                };
                let Some(src) = read_u64(data, &mut pos) else {
                    break;
                };
                let Some(dst) = read_u64(data, &mut pos) else {
                    break;
                };
                let Some(etype) = decode_string(data, &mut pos) else {
                    break;
                };
                let Some(props) = decode_props(data, &mut pos) else {
                    break;
                };
                if id >= state.next_edge_id {
                    state.next_edge_id = id + 1;
                }
                state.edges.insert(
                    id,
                    WalEdge {
                        id,
                        from: src,
                        to: dst,
                        edge_type: etype,
                        properties: props,
                    },
                );
            }
            TAG_DEL_NODE => {
                let Some(id) = read_u64(data, &mut pos) else {
                    break;
                };
                state.nodes.remove(&id);
                // Cascade: remove edges referencing this node.
                state.edges.retain(|_, e| e.from != id && e.to != id);
            }
            TAG_DEL_EDGE => {
                let Some(id) = read_u64(data, &mut pos) else {
                    break;
                };
                state.edges.remove(&id);
            }
            TAG_SET_PROP => {
                let Some(&target_byte) = data.get(pos) else {
                    break;
                };
                pos += 1;
                let Some(id) = read_u64(data, &mut pos) else {
                    break;
                };
                let Some(key) = decode_string(data, &mut pos) else {
                    break;
                };
                let Some(val) = decode_prop_value(data, &mut pos) else {
                    break;
                };
                if target_byte == 0 {
                    if let Some(node) = state.nodes.get_mut(&id) {
                        node.properties.insert(key, val);
                    }
                } else if let Some(edge) = state.edges.get_mut(&id) {
                    edge.properties.insert(key, val);
                }
            }
            TAG_ADD_INDEX => {
                let Some(label) = decode_string(data, &mut pos) else {
                    break;
                };
                let Some(property) = decode_string(data, &mut pos) else {
                    break;
                };
                state.node_indexes.insert((label, property));
            }
            TAG_DEL_INDEX => {
                let Some(label) = decode_string(data, &mut pos) else {
                    break;
                };
                let Some(property) = decode_string(data, &mut pos) else {
                    break;
                };
                state.node_indexes.remove(&(label, property));
            }
            TAG_SNAPSHOT => {
                let Some(payload_len) = read_u32(data, &mut pos) else {
                    break;
                };
                let payload_len = payload_len as usize;
                if pos + payload_len > data.len() {
                    break;
                }
                let payload = &data[pos..pos + payload_len];
                pos += payload_len;
                match decode_snapshot(payload) {
                    Some(s) => state = s,
                    None => {
                        tracing::error!(
                            target: "nucleus::graph::wal",
                            "graph WAL: corrupt SNAPSHOT at offset {pos}; stopping replay (later entries dropped)"
                        );
                        break;
                    }
                }
            }
            tag => {
                // Unknown tag — corrupt data. Stop replay, but make the data loss
                // visible rather than silently truncating the recovered state.
                tracing::error!(
                    target: "nucleus::graph::wal",
                    "graph WAL: unknown record tag {tag:#x} at offset {pos}; stopping replay (later entries dropped)"
                );
                break;
            }
        }
    }

    state
}

fn decode_snapshot(data: &[u8]) -> Option<GraphWalState> {
    let mut pos = 0;
    let next_node_id = read_u64(data, &mut pos)?;
    let next_edge_id = read_u64(data, &mut pos)?;

    // nodes
    let n_nodes = read_u32(data, &mut pos)? as usize;
    let mut nodes = HashMap::with_capacity(crate::storage::wal_util::bounded_capacity(n_nodes));
    for _ in 0..n_nodes {
        let id = read_u64(data, &mut pos)?;
        let labels = decode_labels(data, &mut pos)?;
        let props = decode_props(data, &mut pos)?;
        nodes.insert(
            id,
            WalNode {
                id,
                labels,
                properties: props,
            },
        );
    }

    // edges
    let n_edges = read_u32(data, &mut pos)? as usize;
    let mut edges = HashMap::with_capacity(crate::storage::wal_util::bounded_capacity(n_edges));
    for _ in 0..n_edges {
        let id = read_u64(data, &mut pos)?;
        let src = read_u64(data, &mut pos)?;
        let dst = read_u64(data, &mut pos)?;
        let etype = decode_string(data, &mut pos)?;
        let props = decode_props(data, &mut pos)?;
        edges.insert(
            id,
            WalEdge {
                id,
                from: src,
                to: dst,
                edge_type: etype,
                properties: props,
            },
        );
    }

    // Index definitions. Snapshots written before property indexes existed end
    // exactly here, so an exhausted buffer means "no indexes", not corruption.
    let mut node_indexes = BTreeSet::new();
    if pos < data.len() {
        let n_idx = read_u32(data, &mut pos)? as usize;
        for _ in 0..n_idx {
            let label = decode_string(data, &mut pos)?;
            let property = decode_string(data, &mut pos)?;
            node_indexes.insert((label, property));
        }
    }

    Some(GraphWalState {
        nodes,
        edges,
        next_node_id,
        next_edge_id,
        node_indexes,
    })
}

// ─── Decode helpers ─────────────────────────────────────────────────────────

fn decode_string(data: &[u8], pos: &mut usize) -> Option<String> {
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

fn decode_labels(data: &[u8], pos: &mut usize) -> Option<Vec<String>> {
    let n = read_u32(data, pos)? as usize;
    let mut labels = Vec::with_capacity(crate::storage::wal_util::bounded_capacity(n));
    for _ in 0..n {
        labels.push(decode_string(data, pos)?);
    }
    Some(labels)
}

fn decode_prop_value(data: &[u8], pos: &mut usize) -> Option<PropValue> {
    let tag = *data.get(*pos)?;
    *pos += 1;
    match tag {
        PV_NULL => Some(PropValue::Null),
        PV_BOOL => {
            let b = *data.get(*pos)?;
            *pos += 1;
            Some(PropValue::Bool(b != 0))
        }
        PV_INT => {
            let v = read_i64(data, pos)?;
            Some(PropValue::Int(v))
        }
        PV_FLOAT => {
            let v = read_f64(data, pos)?;
            Some(PropValue::Float(v))
        }
        PV_TEXT => {
            let s = decode_string(data, pos)?;
            Some(PropValue::Text(s))
        }
        _ => None,
    }
}

fn decode_props(data: &[u8], pos: &mut usize) -> Option<Properties> {
    let n = read_u32(data, pos)? as usize;
    let mut props = BTreeMap::new();
    for _ in 0..n {
        let key = decode_string(data, pos)?;
        let val = decode_prop_value(data, pos)?;
        props.insert(key, val);
    }
    Some(props)
}

// ─── Primitive readers ──────────────────────────────────────────────────────

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

fn read_i64(data: &[u8], pos: &mut usize) -> Option<i64> {
    read_u64(data, pos).map(|u| u as i64)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    read_u64(data, pos).map(f64::from_bits)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    // 3.14/3.14159 here are arbitrary test fixtures, not PI approximations.
    #![allow(clippy::approx_constant)]
    use super::*;
    use std::collections::HashSet;

    fn make_props(pairs: &[(&str, PropValue)]) -> Properties {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn group_sync_marks_clean() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GraphWal::open(dir.path()).unwrap();
        assert!(!wal.is_dirty(), "a fresh WAL has no un-fsynced appends");
        let props = make_props(&[("name", PropValue::Text("Alice".into()))]);
        wal.log_add_node(1, &["Person".into()], &props).unwrap();
        assert!(wal.is_dirty(), "an append is uncovered until fsync");
        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "group_sync fsyncs the tail");
    }

    #[test]
    fn test_add_nodes_edges_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = GraphWal::open(dir.path()).unwrap();
        assert!(state.nodes.is_empty());
        assert!(state.edges.is_empty());

        // Add two nodes and an edge.
        let props1 = make_props(&[
            ("name", PropValue::Text("Alice".into())),
            ("age", PropValue::Int(30)),
        ]);
        wal.log_add_node(1, &["Person".into()], &props1).unwrap();
        let props2 = make_props(&[("name", PropValue::Text("Bob".into()))]);
        wal.log_add_node(2, &["Person".into()], &props2).unwrap();
        let eprops = make_props(&[("since", PropValue::Int(2020))]);
        wal.log_add_edge(1, 1, 2, "KNOWS", &eprops).unwrap();
        drop(wal);

        // Reopen and verify.
        let (_wal2, state2) = GraphWal::open(dir.path()).unwrap();
        assert_eq!(state2.nodes.len(), 2);
        assert_eq!(state2.edges.len(), 1);
        let n1 = &state2.nodes[&1];
        assert_eq!(n1.labels, vec!["Person".to_string()]);
        assert_eq!(
            n1.properties.get("name"),
            Some(&PropValue::Text("Alice".into()))
        );
        assert_eq!(n1.properties.get("age"), Some(&PropValue::Int(30)));
        let e1 = &state2.edges[&1];
        assert_eq!(e1.from, 1);
        assert_eq!(e1.to, 2);
        assert_eq!(e1.edge_type, "KNOWS");
        assert_eq!(e1.properties.get("since"), Some(&PropValue::Int(2020)));
    }

    #[test]
    fn test_delete_node_cascade_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GraphWal::open(dir.path()).unwrap();
        wal.log_add_node(1, &["A".into()], &Properties::new())
            .unwrap();
        wal.log_add_node(2, &["B".into()], &Properties::new())
            .unwrap();
        wal.log_add_edge(1, 1, 2, "LINK", &Properties::new())
            .unwrap();
        wal.log_add_edge(2, 2, 1, "LINK", &Properties::new())
            .unwrap();
        // Delete node 1 — should cascade edges 1 and 2.
        wal.log_del_node(1).unwrap();
        drop(wal);

        let (_wal2, state) = GraphWal::open(dir.path()).unwrap();
        assert_eq!(state.nodes.len(), 1);
        assert!(state.nodes.contains_key(&2));
        assert!(state.edges.is_empty());
    }

    #[test]
    fn test_properties_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GraphWal::open(dir.path()).unwrap();
        let p = make_props(&[
            ("s", PropValue::Text("hello".into())),
            ("i", PropValue::Int(42)),
            ("f", PropValue::Float(3.14)),
            ("b", PropValue::Bool(true)),
            ("n", PropValue::Null),
        ]);
        wal.log_add_node(1, &[], &p).unwrap();
        drop(wal);

        let (_, st) = GraphWal::open(dir.path()).unwrap();
        let n = &st.nodes[&1];
        assert_eq!(
            n.properties.get("s"),
            Some(&PropValue::Text("hello".into()))
        );
        assert_eq!(n.properties.get("i"), Some(&PropValue::Int(42)));
        assert_eq!(n.properties.get("f"), Some(&PropValue::Float(3.14)));
        assert_eq!(n.properties.get("b"), Some(&PropValue::Bool(true)));
        assert_eq!(n.properties.get("n"), Some(&PropValue::Null));
    }

    #[test]
    fn test_label_index_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GraphWal::open(dir.path()).unwrap();
        wal.log_add_node(1, &["Person".into(), "Employee".into()], &Properties::new())
            .unwrap();
        wal.log_add_node(2, &["Person".into()], &Properties::new())
            .unwrap();
        wal.log_add_node(3, &["Company".into()], &Properties::new())
            .unwrap();
        drop(wal);

        let (_, st) = GraphWal::open(dir.path()).unwrap();
        // Build label index from state.
        let mut label_index: HashMap<String, HashSet<NodeId>> = HashMap::new();
        for (id, node) in &st.nodes {
            for label in &node.labels {
                label_index.entry(label.clone()).or_default().insert(*id);
            }
        }
        assert_eq!(label_index["Person"].len(), 2);
        assert!(label_index["Person"].contains(&1));
        assert!(label_index["Person"].contains(&2));
        assert_eq!(label_index["Employee"].len(), 1);
        assert_eq!(label_index["Company"].len(), 1);
    }

    #[test]
    fn test_adjacency_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GraphWal::open(dir.path()).unwrap();
        wal.log_add_node(1, &[], &Properties::new()).unwrap();
        wal.log_add_node(2, &[], &Properties::new()).unwrap();
        wal.log_add_node(3, &[], &Properties::new()).unwrap();
        wal.log_add_edge(1, 1, 2, "A", &Properties::new()).unwrap();
        wal.log_add_edge(2, 1, 3, "B", &Properties::new()).unwrap();
        wal.log_add_edge(3, 2, 3, "C", &Properties::new()).unwrap();
        drop(wal);

        let (_, st) = GraphWal::open(dir.path()).unwrap();
        // Build adjacency from state.
        let mut outgoing: HashMap<NodeId, Vec<EdgeId>> = HashMap::new();
        let mut incoming: HashMap<NodeId, Vec<EdgeId>> = HashMap::new();
        for (eid, edge) in &st.edges {
            outgoing.entry(edge.from).or_default().push(*eid);
            incoming.entry(edge.to).or_default().push(*eid);
        }
        assert_eq!(outgoing[&1].len(), 2); // edges 1, 2
        assert_eq!(outgoing[&2].len(), 1); // edge 3
        assert!(!outgoing.contains_key(&3) || outgoing[&3].is_empty());
        assert_eq!(incoming[&2].len(), 1);
        assert_eq!(incoming[&3].len(), 2);
    }

    #[test]
    fn test_corrupt_wal_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.wal");

        // Write two valid entries then corrupt trailing bytes.
        {
            let (wal, _) = GraphWal::open(dir.path()).unwrap();
            wal.log_add_node(1, &["X".into()], &Properties::new())
                .unwrap();
            wal.log_add_node(2, &["Y".into()], &Properties::new())
                .unwrap();
            drop(wal);
        }

        // Append garbage.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD, 0xFC]).unwrap();
        }

        // Should recover the two valid nodes.
        let (_, st) = GraphWal::open(dir.path()).unwrap();
        assert_eq!(st.nodes.len(), 2);
    }

    #[test]
    fn test_empty_graph_clean_open() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = GraphWal::open(dir.path()).unwrap();
        assert!(state.nodes.is_empty());
        assert!(state.edges.is_empty());
        assert_eq!(state.next_node_id, 0);
        assert_eq!(state.next_edge_id, 0);
        drop(wal);

        // Reopen empty.
        let (_, state2) = GraphWal::open(dir.path()).unwrap();
        assert!(state2.nodes.is_empty());
    }

    #[test]
    fn test_large_graph_checkpoint_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GraphWal::open(dir.path()).unwrap();

        // Create 150 nodes and 149 chain edges.
        for i in 1..=150u64 {
            wal.log_add_node(
                i,
                &["N".into()],
                &make_props(&[("idx", PropValue::Int(i as i64))]),
            )
            .unwrap();
        }
        for i in 1..150u64 {
            wal.log_add_edge(i, i, i + 1, "NEXT", &Properties::new())
                .unwrap();
        }

        // Checkpoint.
        let snap_nodes: Vec<_> = (1..=150u64)
            .map(|i| {
                let labels = vec!["N".to_string()];
                let props: Properties = make_props(&[("idx", PropValue::Int(i as i64))]);
                (i, labels, props)
            })
            .collect();
        let snap_edges: Vec<_> = (1..150u64)
            .map(|i| {
                let props = Properties::new();
                (i, i, i + 1, "NEXT".to_string(), props)
            })
            .collect();
        let snap_nodes_refs: Vec<_> = snap_nodes.iter().map(|(id, l, p)| (id, l, p)).collect();
        let snap_edges_refs: Vec<_> = snap_edges
            .iter()
            .map(|(id, s, d, t, p)| (id, s, d, t.as_str(), p))
            .collect();

        wal.checkpoint(&GraphSnapshot {
            nodes: snap_nodes_refs,
            edges: snap_edges_refs,
            next_node_id: 151,
            next_edge_id: 150,
            node_indexes: vec![],
        })
        .unwrap();
        drop(wal);

        // Reopen.
        let (_, st) = GraphWal::open(dir.path()).unwrap();
        assert_eq!(st.nodes.len(), 150);
        assert_eq!(st.edges.len(), 149);
        assert_eq!(st.next_node_id, 151);
        assert_eq!(st.next_edge_id, 150);
        // Verify a property.
        assert_eq!(
            st.nodes[&42].properties.get("idx"),
            Some(&PropValue::Int(42))
        );
    }

    /// S31-14: a checkpoint whose reopen fails must not leave the writer
    /// appending into the unlinked inode the rename displaced. Those appends
    /// report success while no future recovery can ever read them, so an
    /// acknowledged node silently vanishes at restart. The discriminator is
    /// durability: the post-failure append must land in the replaced file.
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = GraphWal::open(dir.path()).unwrap();
            wal.log_add_node(1, &["N".into()], &Properties::new())
                .unwrap();
            let nodes = [(1u64, vec!["N".to_string()], Properties::new())];
            let node_refs: Vec<_> = nodes.iter().map(|(id, l, p)| (id, l, p)).collect();
            wal.fail_reopen_once
                .store(true, std::sync::atomic::Ordering::SeqCst);
            wal.checkpoint(&GraphSnapshot {
                nodes: node_refs,
                edges: vec![],
                next_node_id: 2,
                next_edge_id: 1,
                node_indexes: vec![],
            })
            .expect_err("the injected reopen failure must fail the checkpoint");
            wal.log_add_node(2, &["N".into()], &Properties::new())
                .expect("a later append must reattach, not strand");
        }
        let (_, st) = GraphWal::open(dir.path()).unwrap();
        assert_eq!(
            st.nodes.len(),
            2,
            "the post-checkpoint-failure append went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
        assert_eq!(st.next_node_id, 3);
    }

    #[test]
    fn test_property_updates_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GraphWal::open(dir.path()).unwrap();
        wal.log_add_node(1, &[], &make_props(&[("x", PropValue::Int(1))]))
            .unwrap();
        wal.log_set_prop(0, 1, "x", &PropValue::Int(99)).unwrap();
        wal.log_set_prop(0, 1, "new_key", &PropValue::Text("hello".into()))
            .unwrap();
        drop(wal);

        let (_, st) = GraphWal::open(dir.path()).unwrap();
        let n = &st.nodes[&1];
        assert_eq!(n.properties.get("x"), Some(&PropValue::Int(99)));
        assert_eq!(
            n.properties.get("new_key"),
            Some(&PropValue::Text("hello".into()))
        );
    }

    #[test]
    fn test_delete_edge_only() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GraphWal::open(dir.path()).unwrap();
        wal.log_add_node(1, &[], &Properties::new()).unwrap();
        wal.log_add_node(2, &[], &Properties::new()).unwrap();
        wal.log_add_edge(1, 1, 2, "X", &Properties::new()).unwrap();
        wal.log_add_edge(2, 2, 1, "Y", &Properties::new()).unwrap();
        wal.log_del_edge(1).unwrap();
        drop(wal);

        let (_, st) = GraphWal::open(dir.path()).unwrap();
        assert_eq!(st.nodes.len(), 2);
        assert_eq!(st.edges.len(), 1);
        assert!(st.edges.contains_key(&2));
    }

    #[test]
    fn test_checkpoint_then_incremental() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GraphWal::open(dir.path()).unwrap();

        wal.log_add_node(1, &["A".into()], &Properties::new())
            .unwrap();
        wal.log_add_node(2, &["B".into()], &Properties::new())
            .unwrap();

        // Checkpoint with 2 nodes.
        let labels1 = vec!["A".to_string()];
        let labels2 = vec!["B".to_string()];
        let p = Properties::new();
        let id1 = 1u64;
        let id2 = 2u64;
        wal.checkpoint(&GraphSnapshot {
            nodes: vec![(&id1, &labels1, &p), (&id2, &labels2, &p)],
            edges: vec![],
            next_node_id: 3,
            next_edge_id: 1,
            node_indexes: vec![],
        })
        .unwrap();

        // Add more after checkpoint.
        wal.log_add_node(3, &["C".into()], &Properties::new())
            .unwrap();
        wal.log_add_edge(1, 1, 3, "LINK", &Properties::new())
            .unwrap();
        drop(wal);

        let (_, st) = GraphWal::open(dir.path()).unwrap();
        assert_eq!(st.nodes.len(), 3);
        assert_eq!(st.edges.len(), 1);
        assert_eq!(st.next_node_id, 4); // 3+1 from incremental entry
    }
}
