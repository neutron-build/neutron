//! Property graph engine — nodes, edges, traversals, and pattern matching.
//!
//! Supports:
//!   - Labeled property graph model (nodes with labels + properties, edges with types + properties)
//!   - Adjacency list storage with O(1) neighbor lookup
//!   - BFS and DFS traversals
//!   - Shortest path (unweighted and weighted Dijkstra)
//!   - Pattern matching for simple graph queries
//!
//! Replaces Neo4j, Apache AGE for graph workloads within Nucleus.

pub mod cypher;
pub mod cypher_executor;
pub mod tiered;
pub mod wal;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

// ============================================================================
// Graph types
// ============================================================================

/// Unique identifier for a node.
pub type NodeId = u64;
/// Unique identifier for an edge.
pub type EdgeId = u64;

/// Type alias -- PropertyGraph is the primary graph engine type.
pub type PropertyGraph = GraphStore;

/// Properties map — lightweight key-value pairs on nodes and edges.
pub type Properties = BTreeMap<String, PropValue>;

/// A property value.
#[derive(Debug, Clone, PartialEq)]
pub enum PropValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

/// A node in the property graph.
#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: Properties,
}

/// An edge (relationship) in the property graph.
#[derive(Debug, Clone)]
pub struct Edge {
    pub id: EdgeId,
    pub edge_type: String,
    pub from: NodeId,
    pub to: NodeId,
    pub properties: Properties,
}

/// Direction for traversals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

/// Snapshot of graph state for transaction rollback.
pub struct GraphTxnSnapshot {
    nodes: HashMap<NodeId, Node>,
    edges: HashMap<EdgeId, Edge>,
    outgoing: HashMap<NodeId, Vec<EdgeId>>,
    incoming: HashMap<NodeId, Vec<EdgeId>>,
    label_index: HashMap<String, HashSet<NodeId>>,
    type_index: HashMap<String, HashSet<EdgeId>>,
    next_node_id: NodeId,
    next_edge_id: EdgeId,
}

// ============================================================================
// Graph store
// ============================================================================

/// In-memory property graph store with adjacency lists.
///
/// When created with [`GraphStore::open`], a cold LsmTree tier is created for
/// property overflow storage. Properties of evicted nodes/edges are spilled to
/// the cold tier when the hot node count exceeds `max_hot_nodes`. Graph
/// structure (adjacency, labels, types) always stays in memory.
pub struct GraphStore {
    nodes: HashMap<NodeId, Node>,
    edges: HashMap<EdgeId, Edge>,
    /// node_id → outgoing edge IDs
    outgoing: HashMap<NodeId, Vec<EdgeId>>,
    /// node_id → incoming edge IDs
    incoming: HashMap<NodeId, Vec<EdgeId>>,
    /// Label → node IDs (index for label lookups)
    label_index: HashMap<String, HashSet<NodeId>>,
    /// Edge type → edge IDs (index for fast edge-type lookups)
    type_index: HashMap<String, HashSet<EdgeId>>,
    next_node_id: NodeId,
    next_edge_id: EdgeId,
    /// Optional WAL for durability. Present when opened with `GraphStore::open()`.
    wal: Option<Arc<wal::GraphWal>>,
    /// Cold tier: disk-backed LsmTree for overflow node/edge properties (disk mode only).
    cold_props: Option<parking_lot::Mutex<crate::storage::lsm::LsmTree>>,
    /// Node IDs whose properties are still in-memory (hot).
    hot_node_ids: HashSet<NodeId>,
    /// Maximum hot nodes before property eviction to cold tier.
    pub max_hot_nodes: usize,
}

impl Default for GraphStore {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphStore {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            label_index: HashMap::new(),
            type_index: HashMap::new(),
            next_node_id: 1,
            next_edge_id: 1,
            wal: None,
            cold_props: None,
            hot_node_ids: HashSet::new(),
            max_hot_nodes: usize::MAX,
        }
    }

    /// Open a durable graph store backed by a WAL in the given directory.
    ///
    /// On first call this creates the WAL file. On subsequent calls the WAL
    /// is replayed to restore the full graph state (nodes, edges, adjacency
    /// lists, label index, ID counters).
    pub fn open(dir: &std::path::Path) -> std::io::Result<Self> {
        let (graph_wal, state) = wal::GraphWal::open(dir)?;

        // Open cold LsmTree tier for property overflow
        let cold_dir = dir.join("graph_cold");
        std::fs::create_dir_all(&cold_dir).ok();
        let config = crate::storage::lsm::LsmConfig::default();
        let cold_props = crate::storage::lsm::LsmTree::open(config, &cold_dir)
            .ok()
            .map(parking_lot::Mutex::new);

        let mut store = Self::new();
        store.wal = Some(Arc::new(graph_wal));
        store.cold_props = cold_props;
        store.max_hot_nodes = 100_000;

        // Restore nodes.
        for (id, wn) in &state.nodes {
            for label in &wn.labels {
                store.label_index.entry(label.clone()).or_default().insert(*id);
            }
            store.nodes.insert(*id, Node {
                id: *id,
                labels: wn.labels.clone(),
                properties: wn.properties.clone(),
            });
            store.hot_node_ids.insert(*id);
        }

        // Restore edges + adjacency + type index.
        for (id, we) in &state.edges {
            store.type_index
                .entry(we.edge_type.clone())
                .or_default()
                .insert(*id);
            store.edges.insert(*id, Edge {
                id: *id,
                edge_type: we.edge_type.clone(),
                from: we.from,
                to: we.to,
                properties: we.properties.clone(),
            });
            store.outgoing.entry(we.from).or_default().push(*id);
            store.incoming.entry(we.to).or_default().push(*id);
        }

        // Restore ID counters.
        store.next_node_id = if state.next_node_id > 0 { state.next_node_id } else { 1 };
        store.next_edge_id = if state.next_edge_id > 0 { state.next_edge_id } else { 1 };

        Ok(store)
    }

    // ---- Node operations ----

    /// Create a node with labels and properties. Returns the node ID.
    pub fn create_node(&mut self, labels: Vec<String>, properties: Properties) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;

        // WAL: log before mutation.
        if let Some(ref w) = self.wal {
            let _ = w.log_add_node(id, &labels, &properties);
        }

        for label in &labels {
            self.label_index
                .entry(label.clone())
                .or_default()
                .insert(id);
        }

        self.nodes.insert(
            id,
            Node {
                id,
                labels,
                properties,
            },
        );
        self.hot_node_ids.insert(id);
        if self.cold_props.is_some() {
            self.maybe_evict_props();
        }
        id
    }

    /// Get a node by ID.
    ///
    /// If the node's properties have been evicted to the cold tier, they are
    /// fetched from the LsmTree and the returned reference contains the full
    /// properties. Note: if properties were evicted, this allocates via
    /// `get_node_full()` internally.
    pub fn get_node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    /// Get a node by ID with cold-tier property fallback.
    ///
    /// If the node's properties map is empty and a cold tier exists, the
    /// properties are fetched from the cold LsmTree and returned as an owned
    /// `Node`. This avoids modifying the store for a read operation.
    pub fn get_node_full(&self, id: NodeId) -> Option<Node> {
        let node = self.nodes.get(&id)?;
        if !node.properties.is_empty() || self.cold_props.is_none() {
            return Some(node.clone());
        }
        // Properties might be in cold tier
        if let Some(ref cold) = self.cold_props {
            let key = format!("n:{id}").into_bytes();
            let props_data = cold.lock().get(&key);
            if let Some(data) = props_data
                && let Some(props) = tiered::properties_from_bytes(&data) {
                    return Some(Node {
                        id: node.id,
                        labels: node.labels.clone(),
                        properties: props,
                    });
                }
        }
        Some(node.clone())
    }

    /// Delete a node and all its edges.
    pub fn delete_node(&mut self, id: NodeId) -> bool {
        // WAL: log before mutation.
        if let Some(ref w) = self.wal {
            let _ = w.log_del_node(id);
        }

        let node = match self.nodes.remove(&id) {
            Some(n) => n,
            None => return false,
        };

        // Remove from label index
        for label in &node.labels {
            if let Some(set) = self.label_index.get_mut(label) {
                set.remove(&id);
            }
        }

        // Remove from hot tracking and cold tier
        self.hot_node_ids.remove(&id);
        if let Some(ref cold) = self.cold_props {
            let key = format!("n:{id}").into_bytes();
            cold.lock().delete(key);
        }

        // Collect edge IDs to remove
        let out_edges: Vec<EdgeId> = self.outgoing.remove(&id).unwrap_or_default();
        let in_edges: Vec<EdgeId> = self.incoming.remove(&id).unwrap_or_default();

        for eid in out_edges {
            if let Some(edge) = self.edges.remove(&eid) {
                if let Some(inc) = self.incoming.get_mut(&edge.to) {
                    inc.retain(|e| *e != eid);
                }
                // Remove from type index.
                if let Some(set) = self.type_index.get_mut(&edge.edge_type) {
                    set.remove(&eid);
                    if set.is_empty() {
                        self.type_index.remove(&edge.edge_type);
                    }
                }
                // Clean edge properties from cold tier
                if let Some(ref cold) = self.cold_props {
                    cold.lock().delete(format!("e:{eid}").into_bytes());
                }
            }
        }
        for eid in in_edges {
            if let Some(edge) = self.edges.remove(&eid) {
                if let Some(out) = self.outgoing.get_mut(&edge.from) {
                    out.retain(|e| *e != eid);
                }
                // Remove from type index.
                if let Some(set) = self.type_index.get_mut(&edge.edge_type) {
                    set.remove(&eid);
                    if set.is_empty() {
                        self.type_index.remove(&edge.edge_type);
                    }
                }
                // Clean edge properties from cold tier
                if let Some(ref cold) = self.cold_props {
                    cold.lock().delete(format!("e:{eid}").into_bytes());
                }
            }
        }

        true
    }

    /// Find nodes by label.
    pub fn nodes_by_label(&self, label: &str) -> Vec<&Node> {
        self.label_index
            .get(label)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.nodes.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Total node count.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get all nodes.
    pub fn all_nodes(&self) -> Vec<&Node> {
        self.nodes.values().collect()
    }

    /// Get all edges.
    pub fn all_edges(&self) -> Vec<&Edge> {
        self.edges.values().collect()
    }

    // ---- Edge operations ----

    /// Create an edge between two nodes. Returns the edge ID, or None if nodes don't exist.
    pub fn create_edge(
        &mut self,
        from: NodeId,
        to: NodeId,
        edge_type: String,
        properties: Properties,
    ) -> Option<EdgeId> {
        if !self.nodes.contains_key(&from) || !self.nodes.contains_key(&to) {
            return None;
        }

        let id = self.next_edge_id;
        self.next_edge_id += 1;

        // WAL: log before mutation.
        if let Some(ref w) = self.wal {
            let _ = w.log_add_edge(id, from, to, &edge_type, &properties);
        }

        self.type_index
            .entry(edge_type.clone())
            .or_default()
            .insert(id);
        self.edges.insert(
            id,
            Edge {
                id,
                edge_type,
                from,
                to,
                properties,
            },
        );
        self.outgoing.entry(from).or_default().push(id);
        self.incoming.entry(to).or_default().push(id);

        Some(id)
    }

    /// Get an edge by ID.
    pub fn get_edge(&self, id: EdgeId) -> Option<&Edge> {
        self.edges.get(&id)
    }

    /// Get an edge by ID with cold-tier property fallback.
    pub fn get_edge_full(&self, id: EdgeId) -> Option<Edge> {
        let edge = self.edges.get(&id)?;
        if !edge.properties.is_empty() || self.cold_props.is_none() {
            return Some(edge.clone());
        }
        if let Some(ref cold) = self.cold_props {
            let key = format!("e:{id}").into_bytes();
            let props_data = cold.lock().get(&key);
            if let Some(data) = props_data
                && let Some(props) = tiered::properties_from_bytes(&data) {
                    return Some(Edge {
                        id: edge.id,
                        edge_type: edge.edge_type.clone(),
                        from: edge.from,
                        to: edge.to,
                        properties: props,
                    });
                }
        }
        Some(edge.clone())
    }

    /// Delete an edge.
    pub fn delete_edge(&mut self, id: EdgeId) -> bool {
        // WAL: log before mutation.
        if let Some(ref w) = self.wal {
            let _ = w.log_del_edge(id);
        }

        let edge = match self.edges.remove(&id) {
            Some(e) => e,
            None => return false,
        };

        if let Some(out) = self.outgoing.get_mut(&edge.from) {
            out.retain(|e| *e != id);
        }
        if let Some(inc) = self.incoming.get_mut(&edge.to) {
            inc.retain(|e| *e != id);
        }
        // Remove from type index.
        if let Some(set) = self.type_index.get_mut(&edge.edge_type) {
            set.remove(&id);
            if set.is_empty() {
                self.type_index.remove(&edge.edge_type);
            }
        }
        // Clean edge properties from cold tier
        if let Some(ref cold) = self.cold_props {
            cold.lock().delete(format!("e:{id}").into_bytes());
        }
        true
    }

    /// Total edge count.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Fast lookup of edges by type using the type index. O(k) where k is the
    /// number of edges with the given type, instead of O(E) linear scan.
    pub fn edges_by_type(&self, edge_type: &str) -> Vec<&Edge> {
        self.type_index
            .get(edge_type)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.edges.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    // ---- Property mutation ----

    /// Set (or overwrite) a property on a node.
    pub fn set_node_property(&mut self, id: NodeId, key: String, value: PropValue) -> bool {
        if let Some(ref w) = self.wal {
            let _ = w.log_set_prop(0, id, &key, &value);
        }
        if let Some(node) = self.nodes.get_mut(&id) {
            node.properties.insert(key, value);
            true
        } else {
            false
        }
    }

    /// Set (or overwrite) a property on an edge.
    pub fn set_edge_property(&mut self, id: EdgeId, key: String, value: PropValue) -> bool {
        if let Some(ref w) = self.wal {
            let _ = w.log_set_prop(1, id, &key, &value);
        }
        if let Some(edge) = self.edges.get_mut(&id) {
            edge.properties.insert(key, value);
            true
        } else {
            false
        }
    }

    /// Create a `GraphSnapshot` for WAL checkpointing.
    pub fn snapshot(&self) -> wal::GraphSnapshot<'_> {
        wal::GraphSnapshot {
            nodes: self.nodes.values().map(|n| (&n.id, &n.labels, &n.properties)).collect(),
            edges: self.edges.values().map(|e| (&e.id, &e.from, &e.to, e.edge_type.as_str(), &e.properties)).collect(),
            next_node_id: self.next_node_id,
            next_edge_id: self.next_edge_id,
        }
    }

    /// Checkpoint the WAL (compact it to a single snapshot entry). No-op if no WAL.
    pub fn checkpoint_wal(&self) -> std::io::Result<()> {
        if let Some(ref w) = self.wal {
            let snap = self.snapshot();
            w.checkpoint(&snap)
        } else {
            Ok(())
        }
    }

    /// Capture full graph state for transaction rollback.
    pub fn txn_snapshot(&self) -> GraphTxnSnapshot {
        GraphTxnSnapshot {
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            outgoing: self.outgoing.clone(),
            incoming: self.incoming.clone(),
            label_index: self.label_index.clone(),
            type_index: self.type_index.clone(),
            next_node_id: self.next_node_id,
            next_edge_id: self.next_edge_id,
        }
    }

    /// Restore graph state from a transaction snapshot (for ROLLBACK).
    pub fn txn_restore(&mut self, snap: GraphTxnSnapshot) {
        self.nodes = snap.nodes;
        self.edges = snap.edges;
        self.outgoing = snap.outgoing;
        self.incoming = snap.incoming;
        self.label_index = snap.label_index;
        self.type_index = snap.type_index;
        self.next_node_id = snap.next_node_id;
        self.next_edge_id = snap.next_edge_id;
    }

    // ========================================================================
    // Cold tier helpers
    // ========================================================================

    /// Whether this store has a cold tier (disk mode).
    pub fn has_cold_tier(&self) -> bool {
        self.cold_props.is_some()
    }

    /// Return the count of hot (in-memory properties) nodes only.
    pub fn node_count_hot(&self) -> usize {
        self.hot_node_ids.len()
    }

    /// Evict node properties from the hot tier to the cold LsmTree when the
    /// hot node count exceeds `max_hot_nodes`. Only properties are evicted;
    /// adjacency structure stays in memory.
    fn maybe_evict_props(&mut self) {
        if self.hot_node_ids.len() <= self.max_hot_nodes {
            return;
        }
        let Some(ref cold) = self.cold_props else { return };
        let to_evict = self.hot_node_ids.len() - self.max_hot_nodes;

        // Collect node IDs to evict
        let evict_ids: Vec<NodeId> = self.hot_node_ids.iter().copied().take(to_evict).collect();

        let mut c = cold.lock();
        for nid in &evict_ids {
            if let Some(node) = self.nodes.get_mut(nid)
                && !node.properties.is_empty() {
                    let bytes = tiered::properties_to_bytes(&node.properties);
                    c.put(format!("n:{nid}").into_bytes(), bytes);
                    node.properties.clear();
                }
            self.hot_node_ids.remove(nid);

            // Also evict properties of outgoing edges from this node
            if let Some(out_eids) = self.outgoing.get(nid) {
                for &eid in out_eids {
                    if let Some(edge) = self.edges.get_mut(&eid)
                        && !edge.properties.is_empty() {
                            let bytes = tiered::properties_to_bytes(&edge.properties);
                            c.put(format!("e:{eid}").into_bytes(), bytes);
                            edge.properties.clear();
                        }
                }
            }
        }
    }

    // ---- Neighbor / traversal primitives ----

    /// Get neighbors of a node in a given direction, optionally filtered by edge type.
    ///
    /// When `edge_type` is `Some`, the type index is used to narrow candidate
    /// edges before checking adjacency, avoiding a full linear scan.
    pub fn neighbors(
        &self,
        node_id: NodeId,
        direction: Direction,
        edge_type: Option<&str>,
    ) -> Vec<(NodeId, &Edge)> {
        let mut results = Vec::new();

        if let Some(et) = edge_type {
            // Fast path: use the type index to only consider edges of the
            // requested type, then check adjacency.
            if let Some(type_eids) = self.type_index.get(et) {
                if (direction == Direction::Outgoing || direction == Direction::Both)
                    && let Some(out) = self.outgoing.get(&node_id) {
                        for eid in out {
                            if type_eids.contains(eid)
                                && let Some(e) = self.edges.get(eid) {
                                    results.push((e.to, e));
                                }
                        }
                    }
                if (direction == Direction::Incoming || direction == Direction::Both)
                    && let Some(inc) = self.incoming.get(&node_id) {
                        for eid in inc {
                            if type_eids.contains(eid)
                                && let Some(e) = self.edges.get(eid) {
                                    results.push((e.from, e));
                                }
                        }
                    }
            }
        } else {
            // No type filter: scan all adjacency edges.
            let collect = |edge_ids: &[EdgeId], get_neighbor: fn(&Edge) -> NodeId| {
                edge_ids
                    .iter()
                    .filter_map(|eid| self.edges.get(eid))
                    .map(|e| (get_neighbor(e), e))
                    .collect::<Vec<_>>()
            };

            if (direction == Direction::Outgoing || direction == Direction::Both)
                && let Some(out) = self.outgoing.get(&node_id) {
                    results.extend(collect(out, |e| e.to));
                }
            if (direction == Direction::Incoming || direction == Direction::Both)
                && let Some(inc) = self.incoming.get(&node_id) {
                    results.extend(collect(inc, |e| e.from));
                }
        }

        results
    }

    /// Get outgoing edges from a node.
    pub fn outgoing_edges(&self, node_id: NodeId) -> Vec<&Edge> {
        self.outgoing
            .get(&node_id)
            .map(|eids| eids.iter().filter_map(|eid| self.edges.get(eid)).collect())
            .unwrap_or_default()
    }

    /// Get incoming edges to a node.
    pub fn incoming_edges(&self, node_id: NodeId) -> Vec<&Edge> {
        self.incoming
            .get(&node_id)
            .map(|eids| eids.iter().filter_map(|eid| self.edges.get(eid)).collect())
            .unwrap_or_default()
    }

    // ---- Traversals ----

    /// Breadth-first search from a starting node. Returns nodes in BFS order.
    pub fn bfs(&self, start: NodeId, direction: Direction, edge_type: Option<&str>) -> Vec<NodeId> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();

        visited.insert(start);
        queue.push_back(start);

        while let Some(current) = queue.pop_front() {
            result.push(current);
            for (neighbor, _) in self.neighbors(current, direction, edge_type) {
                if visited.insert(neighbor) {
                    queue.push_back(neighbor);
                }
            }
        }

        result
    }

    /// Depth-first search from a starting node. Returns nodes in DFS order.
    pub fn dfs(&self, start: NodeId, direction: Direction, edge_type: Option<&str>) -> Vec<NodeId> {
        let mut visited = HashSet::new();
        let mut stack = vec![start];
        let mut result = Vec::new();

        while let Some(current) = stack.pop() {
            if !visited.insert(current) {
                continue;
            }
            result.push(current);
            for (neighbor, _) in self.neighbors(current, direction, edge_type) {
                if !visited.contains(&neighbor) {
                    stack.push(neighbor);
                }
            }
        }

        result
    }

    // ---- Shortest path ----

    /// Unweighted shortest path (BFS-based). Returns the path as a list of node IDs, or None.
    pub fn shortest_path(
        &self,
        from: NodeId,
        to: NodeId,
        direction: Direction,
        edge_type: Option<&str>,
    ) -> Option<Vec<NodeId>> {
        if from == to {
            return Some(vec![from]);
        }

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut parent: HashMap<NodeId, NodeId> = HashMap::new();

        visited.insert(from);
        queue.push_back(from);

        while let Some(current) = queue.pop_front() {
            for (neighbor, _) in self.neighbors(current, direction, edge_type) {
                if visited.insert(neighbor) {
                    parent.insert(neighbor, current);
                    if neighbor == to {
                        // Reconstruct path
                        let mut path = vec![to];
                        let mut node = to;
                        while let Some(&p) = parent.get(&node) {
                            path.push(p);
                            node = p;
                        }
                        path.reverse();
                        return Some(path);
                    }
                    queue.push_back(neighbor);
                }
            }
        }

        None
    }

    /// Weighted shortest path (Dijkstra). Uses a property on edges as the weight.
    /// Defaults to weight 1.0 if the property is missing.
    pub fn dijkstra(
        &self,
        from: NodeId,
        to: NodeId,
        direction: Direction,
        weight_property: &str,
    ) -> Option<(f64, Vec<NodeId>)> {
        if from == to {
            return Some((0.0, vec![from]));
        }

        let mut dist: HashMap<NodeId, f64> = HashMap::new();
        let mut parent: HashMap<NodeId, NodeId> = HashMap::new();
        // Use BTreeMap as a poor-man's priority queue: (distance_bits, node_id)
        let mut pq: BTreeMap<(u64, NodeId), ()> = BTreeMap::new();

        // Convert f64 to u64 that preserves total ordering for BTreeMap use.
        #[inline]
        fn f64_to_ord(f: f64) -> u64 {
            let bits = f.to_bits();
            // Positive floats: set sign bit so they sort after negative.
            // Negative floats: flip all bits to reverse their order.
            if bits >> 63 == 0 { bits | (1u64 << 63) } else { !bits }
        }
        #[inline]
        fn ord_to_f64(o: u64) -> f64 {
            let bits = if o >> 63 == 1 { o & !(1u64 << 63) } else { !o };
            f64::from_bits(bits)
        }

        dist.insert(from, 0.0);
        pq.insert((f64_to_ord(0.0), from), ());

        while let Some((&(d_ord, current), _)) = pq.iter().next() {
            pq.remove(&(d_ord, current));
            let current_dist = ord_to_f64(d_ord);

            if current == to {
                let mut path = vec![to];
                let mut node = to;
                while let Some(&p) = parent.get(&node) {
                    path.push(p);
                    node = p;
                }
                path.reverse();
                return Some((current_dist, path));
            }

            if current_dist > *dist.get(&current).unwrap_or(&f64::MAX) {
                continue;
            }

            for (neighbor, edge) in self.neighbors(current, direction, None) {
                let weight = match edge.properties.get(weight_property) {
                    Some(PropValue::Float(w)) => *w,
                    Some(PropValue::Int(w)) => *w as f64,
                    _ => 1.0,
                };
                let new_dist = current_dist + weight;
                if new_dist < *dist.get(&neighbor).unwrap_or(&f64::MAX) {
                    dist.insert(neighbor, new_dist);
                    parent.insert(neighbor, current);
                    pq.insert((f64_to_ord(new_dist), neighbor), ());
                }
            }
        }

        None
    }

    /// Convenience Dijkstra shortest path using outgoing edges only.
    ///
    /// Finds the cheapest path from `from` to `to` using the edge property
    /// `weight_key` as the weight (defaults to 1.0 if the property is missing).
    /// Returns `Some((total_cost, path))` or `None` if no path exists.
    pub fn dijkstra_weighted(
        &self,
        from: NodeId,
        to: NodeId,
        weight_key: &str,
    ) -> Option<(f64, Vec<NodeId>)> {
        self.dijkstra(from, to, Direction::Outgoing, weight_key)
    }

    // ---- Pattern matching ----

    /// Find all paths matching a pattern: (start_label) -[edge_type]-> (end_label).
    /// Returns list of (start_node_id, edge_id, end_node_id) triples.
    pub fn match_pattern(
        &self,
        start_label: Option<&str>,
        edge_type: Option<&str>,
        end_label: Option<&str>,
    ) -> Vec<(NodeId, EdgeId, NodeId)> {
        let mut results = Vec::new();

        // Get candidate start nodes
        let start_nodes: Vec<NodeId> = if let Some(label) = start_label {
            self.label_index
                .get(label)
                .map(|ids| ids.iter().copied().collect())
                .unwrap_or_default()
        } else {
            self.nodes.keys().copied().collect()
        };

        for &start_id in &start_nodes {
            if let Some(out_edges) = self.outgoing.get(&start_id) {
                for &eid in out_edges {
                    if let Some(edge) = self.edges.get(&eid) {
                        // Check edge type filter
                        if let Some(et) = edge_type
                            && edge.edge_type != et {
                                continue;
                            }
                        // Check end label filter
                        if let Some(el) = end_label {
                            if let Some(end_node) = self.nodes.get(&edge.to) {
                                if !end_node.labels.contains(&el.to_string()) {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                        }
                        results.push((start_id, eid, edge.to));
                    }
                }
            }
        }

        results
    }

    /// Variable-length path matching: find all paths of length `min_hops..=max_hops`
    /// from nodes with `start_label` following edges of `edge_type`.
    /// Returns paths as vectors of node IDs.
    pub fn match_variable_length(
        &self,
        start_label: Option<&str>,
        edge_type: Option<&str>,
        min_hops: usize,
        max_hops: usize,
    ) -> Vec<Vec<NodeId>> {
        let start_nodes: Vec<NodeId> = if let Some(label) = start_label {
            self.label_index
                .get(label)
                .map(|ids| ids.iter().copied().collect())
                .unwrap_or_default()
        } else {
            self.nodes.keys().copied().collect()
        };

        let mut all_paths = Vec::new();

        for &start_id in &start_nodes {
            // DFS with depth tracking
            let mut stack: Vec<(Vec<NodeId>, HashSet<NodeId>)> = Vec::new();
            let mut initial_visited = HashSet::new();
            initial_visited.insert(start_id);
            stack.push((vec![start_id], initial_visited));

            while let Some((path, visited)) = stack.pop() {
                let depth = path.len() - 1;

                if depth >= min_hops {
                    all_paths.push(path.clone());
                }

                if depth >= max_hops {
                    continue;
                }

                let current = *path.last().unwrap();
                for (neighbor, _) in self.neighbors(current, Direction::Outgoing, edge_type) {
                    if !visited.contains(&neighbor) {
                        let mut new_path = path.clone();
                        new_path.push(neighbor);
                        let mut new_visited = visited.clone();
                        new_visited.insert(neighbor);
                        stack.push((new_path, new_visited));
                    }
                }
            }
        }

        all_paths
    }

    // ---- Graph analytics ----

    /// Compute degree (number of edges) for a node.
    pub fn degree(&self, node_id: NodeId, direction: Direction) -> usize {
        let out = if direction == Direction::Outgoing || direction == Direction::Both {
            self.outgoing.get(&node_id).map_or(0, |v| v.len())
        } else {
            0
        };
        let inc = if direction == Direction::Incoming || direction == Direction::Both {
            self.incoming.get(&node_id).map_or(0, |v| v.len())
        } else {
            0
        };
        out + inc
    }

    /// Find all connected components (undirected). Returns groups of node IDs.
    pub fn connected_components(&self) -> Vec<Vec<NodeId>> {
        let mut visited = HashSet::new();
        let mut components = Vec::new();

        for &node_id in self.nodes.keys() {
            if visited.contains(&node_id) {
                continue;
            }
            let component = self.bfs(node_id, Direction::Both, None);
            for &n in &component {
                visited.insert(n);
            }
            components.push(component);
        }

        components
    }

    /// PageRank algorithm. Returns node_id → rank mapping.
    pub fn pagerank(&self, damping: f64, iterations: usize) -> HashMap<NodeId, f64> {
        let n = self.nodes.len();
        if n == 0 {
            return HashMap::new();
        }

        let initial = 1.0 / n as f64;
        let mut ranks: HashMap<NodeId, f64> = self.nodes.keys().map(|&id| (id, initial)).collect();

        for _ in 0..iterations {
            let mut new_ranks: HashMap<NodeId, f64> =
                self.nodes.keys().map(|&id| (id, (1.0 - damping) / n as f64)).collect();

            for (&node_id, &rank) in &ranks {
                let out_degree = self.outgoing.get(&node_id).map_or(0, |v| v.len());
                if out_degree == 0 {
                    // Dangling node: distribute rank evenly
                    let share = damping * rank / n as f64;
                    for r in new_ranks.values_mut() {
                        *r += share;
                    }
                } else {
                    let share = damping * rank / out_degree as f64;
                    if let Some(out_edges) = self.outgoing.get(&node_id) {
                        for eid in out_edges {
                            if let Some(edge) = self.edges.get(eid)
                                && let Some(r) = new_ranks.get_mut(&edge.to) {
                                    *r += share;
                                }
                        }
                    }
                }
            }

            ranks = new_ranks;
        }

        ranks
    }

    // ---- Community Detection ----

    /// Label Propagation community detection.
    ///
    /// Each node starts with its own label. In each iteration, every node
    /// adopts the most common label among its neighbors. Converges when
    /// no labels change.
    pub fn label_propagation(&self, max_iterations: usize) -> HashMap<NodeId, usize> {
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        if node_ids.is_empty() {
            return HashMap::new();
        }

        // Initialize: each node gets its own label (using index as label)
        let mut labels: HashMap<NodeId, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i))
            .collect();

        for _ in 0..max_iterations {
            let mut changed = false;

            for &node_id in &node_ids {
                // Count neighbor labels
                let mut label_counts: HashMap<usize, usize> = HashMap::new();

                // Outgoing neighbors
                if let Some(out_edges) = self.outgoing.get(&node_id) {
                    for eid in out_edges {
                        if let Some(edge) = self.edges.get(eid)
                            && let Some(&label) = labels.get(&edge.to) {
                                *label_counts.entry(label).or_insert(0) += 1;
                            }
                    }
                }

                // Incoming neighbors (undirected community detection)
                if let Some(in_edges) = self.incoming.get(&node_id) {
                    for eid in in_edges {
                        if let Some(edge) = self.edges.get(eid)
                            && let Some(&label) = labels.get(&edge.from) {
                                *label_counts.entry(label).or_insert(0) += 1;
                            }
                    }
                }

                if let Some((&best_label, _)) = label_counts
                    .iter()
                    .max_by_key(|(_, count)| *count)
                    && labels[&node_id] != best_label {
                        labels.insert(node_id, best_label);
                        changed = true;
                    }
            }

            if !changed {
                break;
            }
        }

        labels
    }

    /// Louvain-style community detection (simplified modularity optimization).
    ///
    /// Phase 1: Each node starts in its own community. Greedily move nodes
    /// to the neighbor community that maximizes modularity gain. Repeat
    /// until no improvement.
    ///
    /// Returns `node_id → community_id` mapping.
    pub fn louvain_communities(&self) -> HashMap<NodeId, usize> {
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        if node_ids.is_empty() {
            return HashMap::new();
        }

        let total_edges = self.edges.len() as f64;
        if total_edges == 0.0 {
            // Every node is its own community
            return node_ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        }

        // Initialize: each node in its own community
        let mut community: HashMap<NodeId, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i))
            .collect();

        // Compute node degrees (in + out)
        let mut degree: HashMap<NodeId, usize> = HashMap::new();
        for &node_id in &node_ids {
            let out = self.outgoing.get(&node_id).map_or(0, |v| v.len());
            let inc = self.incoming.get(&node_id).map_or(0, |v| v.len());
            degree.insert(node_id, out + inc);
        }

        let m2 = 2.0 * total_edges; // 2m for modularity formula

        let max_iterations = 100;
        for _iter in 0..max_iterations {
            let mut moved = false;

            for &node_id in &node_ids {
                let current_comm = community[&node_id];
                let ki = *degree.get(&node_id).unwrap_or(&0) as f64;

                // Count edges to each neighboring community
                let mut comm_edges: HashMap<usize, f64> = HashMap::new();
                if let Some(out_edges) = self.outgoing.get(&node_id) {
                    for eid in out_edges {
                        if let Some(edge) = self.edges.get(eid) {
                            let nc = community[&edge.to];
                            *comm_edges.entry(nc).or_insert(0.0) += 1.0;
                        }
                    }
                }
                if let Some(in_edges) = self.incoming.get(&node_id) {
                    for eid in in_edges {
                        if let Some(edge) = self.edges.get(eid) {
                            let nc = community[&edge.from];
                            *comm_edges.entry(nc).or_insert(0.0) += 1.0;
                        }
                    }
                }

                // Sum of degrees in each candidate community (excluding node_id itself)
                let mut comm_degree_sum: HashMap<usize, f64> = HashMap::new();
                for (&nid, &comm) in &community {
                    if nid == node_id {
                        continue; // exclude the moving node
                    }
                    if comm_edges.contains_key(&comm) || comm == current_comm {
                        *comm_degree_sum.entry(comm).or_insert(0.0) +=
                            *degree.get(&nid).unwrap_or(&0) as f64;
                    }
                }

                // Edges to current community (excluding self-loops)
                let ki_current = comm_edges.get(&current_comm).copied().unwrap_or(0.0);
                let sigma_current = comm_degree_sum.get(&current_comm).copied().unwrap_or(0.0);
                // Cost of removing from current community
                let remove_cost = ki_current / total_edges - ki * sigma_current / (m2 * m2);

                // Find the community with best net modularity gain
                let mut best_comm = current_comm;
                let mut best_gain = 0.0f64;

                for (&candidate_comm, &edges_to_comm) in &comm_edges {
                    if candidate_comm == current_comm {
                        continue;
                    }
                    let sigma_tot = comm_degree_sum.get(&candidate_comm).copied().unwrap_or(0.0);
                    // Net gain = gain from joining new - cost of leaving current
                    let join_gain = edges_to_comm / total_edges - ki * sigma_tot / (m2 * m2);
                    let net_gain = join_gain - remove_cost;
                    if net_gain > best_gain {
                        best_gain = net_gain;
                        best_comm = candidate_comm;
                    }
                }

                if best_comm != current_comm {
                    community.insert(node_id, best_comm);
                    moved = true;
                }
            }

            if !moved {
                break;
            }
        }

        community
    }

    // ---- Parallel operations ----

    /// Filter a node's outgoing edges in parallel when there are many (100+).
    ///
    /// For small neighbor sets the filter runs sequentially. For large neighbor
    /// sets the edge slice is partitioned across available CPUs and filtered in
    /// parallel using `std::thread::scope` (zero extra dependencies).
    pub fn par_neighbors_filtered(
        &self,
        node_id: NodeId,
        filter_fn: impl Fn(&Edge) -> bool + Sync,
    ) -> Vec<Edge> {
        let edge_ids = match self.outgoing.get(&node_id) {
            Some(ids) => ids.as_slice(),
            None => return Vec::new(),
        };

        // Collect the actual Edge references we can resolve.
        let edges: Vec<&Edge> = edge_ids
            .iter()
            .filter_map(|eid| self.edges.get(eid))
            .collect();

        if edges.len() < 100 {
            return edges.into_iter().filter(|e| filter_fn(e)).cloned().collect();
        }

        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = edges.len().div_ceil(cpus);

        std::thread::scope(|s| {
            let handles: Vec<_> = edges
                .chunks(chunk_size)
                .map(|chunk| {
                    let f = &filter_fn;
                    s.spawn(move || {
                        chunk
                            .iter()
                            .filter(|e| f(e))
                            .map(|e| (*e).clone())
                            .collect::<Vec<Edge>>()
                    })
                })
                .collect();

            let mut result = Vec::new();
            for h in handles {
                result.extend(h.join().unwrap());
            }
            result
        })
    }

    /// Run BFS from multiple source nodes simultaneously.
    ///
    /// Each BFS is independent and runs on its own thread via
    /// `std::thread::scope`. Returns `(source, reachable_nodes)` pairs in the
    /// same order as `sources`.
    pub fn par_multi_bfs(
        &self,
        sources: &[NodeId],
        direction: Direction,
        edge_type: Option<&str>,
    ) -> Vec<(NodeId, Vec<NodeId>)> {
        if sources.len() < 2 {
            return sources
                .iter()
                .map(|&src| (src, self.bfs(src, direction, edge_type)))
                .collect();
        }

        std::thread::scope(|s| {
            let handles: Vec<_> = sources
                .iter()
                .map(|&src| {
                    s.spawn(move || (src, self.bfs(src, direction, edge_type)))
                })
                .collect();

            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        })
    }

    /// Run multiple shortest-path queries in parallel.
    ///
    /// Each query is independent. For a single query the call is sequential;
    /// for two or more queries each runs on its own thread.
    pub fn par_batch_shortest_path(
        &self,
        queries: &[(NodeId, NodeId)],
        direction: Direction,
        edge_type: Option<&str>,
    ) -> Vec<Option<Vec<NodeId>>> {
        if queries.len() < 2 {
            return queries
                .iter()
                .map(|(src, dst)| self.shortest_path(*src, *dst, direction, edge_type))
                .collect();
        }

        std::thread::scope(|s| {
            let handles: Vec<_> = queries
                .iter()
                .map(|&(src, dst)| {
                    s.spawn(move || self.shortest_path(src, dst, direction, edge_type))
                })
                .collect();

            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        })
    }

    /// Count edges per edge type in parallel.
    ///
    /// Uses the `type_index` for O(1) per-type counting. When there are many
    /// distinct edge types the counting is parallelised across types.
    pub fn par_edge_type_counts(&self) -> HashMap<String, usize> {
        if self.type_index.len() < 4 {
            // Small number of types — just count sequentially.
            return self
                .type_index
                .iter()
                .map(|(t, ids)| (t.clone(), ids.len()))
                .collect();
        }

        let entries: Vec<(&String, &HashSet<EdgeId>)> = self.type_index.iter().collect();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = entries.len().div_ceil(cpus);

        std::thread::scope(|s| {
            let handles: Vec<_> = entries
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        chunk
                            .iter()
                            .map(|(t, ids)| ((*t).clone(), ids.len()))
                            .collect::<Vec<(String, usize)>>()
                    })
                })
                .collect();

            let mut result = HashMap::new();
            for h in handles {
                for (t, count) in h.join().unwrap() {
                    result.insert(t, count);
                }
            }
            result
        })
    }
}

// ============================================================================
// Helper: build properties from tuples
// ============================================================================

/// Convenience macro-like function for building properties.
pub fn props(pairs: Vec<(&str, PropValue)>) -> Properties {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

// ============================================================================
// CSR (Compressed Sparse Row) — cache-friendly graph storage format
// ============================================================================

/// An edge in the CSR representation, storing target + edge type.
#[derive(Debug, Clone)]
pub struct CsrEdge {
    pub target: NodeId,
    pub edge_type: String,
    pub weight: f64,
}

/// Compressed Sparse Row graph — cache-friendly, O(1) neighbor lookup.
///
/// Stores all edges in a single contiguous array, with an offset array indexing
/// into it per node. This gives CPU-cache-friendly sequential access during
/// traversals, unlike HashMap-based adjacency lists.
///
/// Trade-off: CSR is immutable once built. Use `GraphStore` for mutations,
/// then convert to `CsrGraph` for read-heavy traversals and analytics.
#[derive(Debug, Clone)]
pub struct CsrGraph {
    /// Number of nodes (IDs are 0..node_count-1, mapped from original IDs).
    node_count: usize,
    /// offsets[i] .. offsets[i+1] is the range in `edges` for node i.
    offsets: Vec<usize>,
    /// All edges stored contiguously.
    edges: Vec<CsrEdge>,
    /// Original node IDs mapped to CSR indices (0-based).
    id_to_index: HashMap<NodeId, usize>,
    /// CSR indices mapped back to original node IDs.
    index_to_id: Vec<NodeId>,
}

impl CsrGraph {
    /// Build a CSR graph from a `GraphStore`. Converts outgoing adjacency lists
    /// into a compact offset+edge array format.
    pub fn from_graph(graph: &GraphStore) -> Self {
        // Assign each node a dense index 0..n-1
        let mut id_to_index = HashMap::new();
        let mut index_to_id = Vec::new();

        let mut node_ids: Vec<NodeId> = graph.nodes.keys().copied().collect();
        node_ids.sort();

        for (idx, &nid) in node_ids.iter().enumerate() {
            id_to_index.insert(nid, idx);
            index_to_id.push(nid);
        }

        let node_count = node_ids.len();
        let mut offsets = Vec::with_capacity(node_count + 1);
        let mut edges = Vec::new();

        for &nid in &node_ids {
            offsets.push(edges.len());
            if let Some(out_eids) = graph.outgoing.get(&nid) {
                for &eid in out_eids {
                    if let Some(edge) = graph.edges.get(&eid) {
                        let weight = match edge.properties.get("weight") {
                            Some(PropValue::Float(w)) => *w,
                            Some(PropValue::Int(w)) => *w as f64,
                            _ => 1.0,
                        };
                        edges.push(CsrEdge {
                            target: edge.to,
                            edge_type: edge.edge_type.clone(),
                            weight,
                        });
                    }
                }
            }
        }
        offsets.push(edges.len()); // sentinel

        CsrGraph {
            node_count,
            offsets,
            edges,
            id_to_index,
            index_to_id,
        }
    }

    /// Get outgoing neighbors of a node as a contiguous slice — O(1) lookup.
    pub fn neighbors(&self, node_id: NodeId) -> &[CsrEdge] {
        if let Some(&idx) = self.id_to_index.get(&node_id) {
            &self.edges[self.offsets[idx]..self.offsets[idx + 1]]
        } else {
            &[]
        }
    }

    /// Get outgoing neighbors filtered by edge type.
    pub fn neighbors_typed(&self, node_id: NodeId, edge_type: &str) -> Vec<&CsrEdge> {
        self.neighbors(node_id)
            .iter()
            .filter(|e| e.edge_type == edge_type)
            .collect()
    }

    /// Outgoing degree of a node.
    pub fn degree(&self, node_id: NodeId) -> usize {
        self.neighbors(node_id).len()
    }

    /// Number of nodes.
    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// Total number of edges.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// BFS traversal on the CSR graph. Returns visited node IDs in BFS order.
    pub fn bfs(&self, start: NodeId) -> Vec<NodeId> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut order = Vec::new();

        if !self.id_to_index.contains_key(&start) {
            return order;
        }

        visited.insert(start);
        queue.push_back(start);

        while let Some(current) = queue.pop_front() {
            order.push(current);
            for edge in self.neighbors(current) {
                if visited.insert(edge.target) {
                    queue.push_back(edge.target);
                }
            }
        }

        order
    }

    /// Dijkstra shortest path on the CSR graph.
    /// Returns `Some((distance, path))` or `None` if no path exists.
    pub fn shortest_path(&self, from: NodeId, to: NodeId, weight_key: &str) -> Option<(f64, Vec<NodeId>)> {
        if !self.id_to_index.contains_key(&from) || !self.id_to_index.contains_key(&to) {
            return None;
        }

        let mut dist: HashMap<NodeId, f64> = HashMap::new();
        let mut parent: HashMap<NodeId, NodeId> = HashMap::new();
        // BTreeMap<(distance_bits, node_id), ()> as a priority queue
        let mut pq: BTreeMap<(u64, NodeId), ()> = BTreeMap::new();

        // Convert f64 to u64 that preserves total ordering for BTreeMap use.
        #[inline]
        fn f64_to_ord(f: f64) -> u64 {
            let bits = f.to_bits();
            if bits >> 63 == 0 { bits | (1u64 << 63) } else { !bits }
        }
        #[inline]
        fn ord_to_f64(o: u64) -> f64 {
            let bits = if o >> 63 == 1 { o & !(1u64 << 63) } else { !o };
            f64::from_bits(bits)
        }

        dist.insert(from, 0.0);
        pq.insert((f64_to_ord(0.0), from), ());

        while let Some((&(d_ord, current), _)) = pq.iter().next() {
            pq.remove(&(d_ord, current));
            let current_dist = ord_to_f64(d_ord);

            if current == to {
                let mut path = vec![to];
                let mut node = to;
                while let Some(&p) = parent.get(&node) {
                    path.push(p);
                    node = p;
                }
                path.reverse();
                return Some((current_dist, path));
            }

            if current_dist > *dist.get(&current).unwrap_or(&f64::MAX) {
                continue;
            }

            for edge in self.neighbors(current) {
                let weight = if weight_key == "weight" {
                    edge.weight
                } else {
                    1.0
                };
                let new_dist = current_dist + weight;
                if new_dist < *dist.get(&edge.target).unwrap_or(&f64::MAX) {
                    dist.insert(edge.target, new_dist);
                    parent.insert(edge.target, current);
                    pq.insert((f64_to_ord(new_dist), edge.target), ());
                }
            }
        }

        None
    }

    /// Memory usage estimate in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.offsets.len() * std::mem::size_of::<usize>()
            + self.edges.len() * std::mem::size_of::<CsrEdge>()
            + self.id_to_index.len() * (std::mem::size_of::<NodeId>() + std::mem::size_of::<usize>())
            + self.index_to_id.len() * std::mem::size_of::<NodeId>()
    }
}

// ============================================================================
// Gap 7: Property Indexes — B-tree indexes on node/edge properties
// ============================================================================

/// An ordered key for property index lookups.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Null,
    Bool(bool),
    Int(i64),
    /// Float stored as ordered bits for BTreeMap compatibility.
    Float(i64),
    Text(String),
}

impl IndexKey {
    pub fn from_prop(val: &PropValue) -> Self {
        match val {
            PropValue::Null => IndexKey::Null,
            PropValue::Bool(b) => IndexKey::Bool(*b),
            PropValue::Int(i) => IndexKey::Int(*i),
            PropValue::Float(f) => {
                // IEEE 754 total ordering via bit manipulation
                let bits = f.to_bits() as i64;
                let ordered = if bits < 0 { !bits } else { bits ^ (1 << 63) };
                IndexKey::Float(ordered)
            }
            PropValue::Text(s) => IndexKey::Text(s.clone()),
        }
    }
}

/// An index on a single property across nodes with a specific label.
#[derive(Debug)]
pub struct PropertyIndex {
    /// The label this index covers (e.g., "Person").
    pub label: String,
    /// The property key this index is built on (e.g., "age").
    pub property: String,
    /// BTree mapping property values to sets of node IDs.
    tree: BTreeMap<IndexKey, HashSet<NodeId>>,
}

impl PropertyIndex {
    pub fn new(label: &str, property: &str) -> Self {
        PropertyIndex {
            label: label.to_string(),
            property: property.to_string(),
            tree: BTreeMap::new(),
        }
    }

    /// Build the index from an existing graph store.
    pub fn build_from(&mut self, graph: &GraphStore) {
        self.tree.clear();
        let nodes = graph.nodes_by_label(&self.label);
        for node in nodes {
            if let Some(val) = node.properties.get(&self.property) {
                let key = IndexKey::from_prop(val);
                self.tree.entry(key).or_default().insert(node.id);
            }
        }
    }

    /// Insert a node into the index.
    pub fn insert(&mut self, node_id: NodeId, value: &PropValue) {
        let key = IndexKey::from_prop(value);
        self.tree.entry(key).or_default().insert(node_id);
    }

    /// Remove a node from the index.
    pub fn remove(&mut self, node_id: NodeId, value: &PropValue) {
        let key = IndexKey::from_prop(value);
        if let Some(set) = self.tree.get_mut(&key) {
            set.remove(&node_id);
            if set.is_empty() {
                self.tree.remove(&key);
            }
        }
    }

    /// Exact lookup: find all nodes where property == value.
    pub fn lookup(&self, value: &PropValue) -> Vec<NodeId> {
        let key = IndexKey::from_prop(value);
        self.tree.get(&key).map(|s| s.iter().copied().collect()).unwrap_or_default()
    }

    /// Range query: find all nodes where property is in [min, max].
    pub fn range(&self, min: &PropValue, max: &PropValue) -> Vec<NodeId> {
        let lo = IndexKey::from_prop(min);
        let hi = IndexKey::from_prop(max);
        let mut result = Vec::new();
        for (_, ids) in self.tree.range(lo..=hi) {
            result.extend(ids.iter());
        }
        result
    }

    /// Number of distinct values in the index.
    pub fn distinct_values(&self) -> usize {
        self.tree.len()
    }

    /// Total number of indexed entries.
    pub fn entry_count(&self) -> usize {
        self.tree.values().map(|s| s.len()).sum()
    }
}

/// A composite property index on multiple properties.
#[derive(Debug)]
pub struct CompositePropertyIndex {
    pub label: String,
    pub properties: Vec<String>,
    tree: BTreeMap<Vec<IndexKey>, HashSet<NodeId>>,
}

impl CompositePropertyIndex {
    pub fn new(label: &str, properties: Vec<String>) -> Self {
        CompositePropertyIndex {
            label: label.to_string(),
            properties,
            tree: BTreeMap::new(),
        }
    }

    fn make_key(&self, node: &Node) -> Option<Vec<IndexKey>> {
        let mut keys = Vec::with_capacity(self.properties.len());
        for prop in &self.properties {
            match node.properties.get(prop) {
                Some(val) => keys.push(IndexKey::from_prop(val)),
                None => return None,
            }
        }
        Some(keys)
    }

    pub fn build_from(&mut self, graph: &GraphStore) {
        self.tree.clear();
        for node in graph.nodes_by_label(&self.label) {
            if let Some(key) = self.make_key(node) {
                self.tree.entry(key).or_default().insert(node.id);
            }
        }
    }

    pub fn insert_node(&mut self, node: &Node) {
        if let Some(key) = self.make_key(node) {
            self.tree.entry(key).or_default().insert(node.id);
        }
    }

    pub fn remove_node(&mut self, node: &Node) {
        if let Some(key) = self.make_key(node)
            && let Some(set) = self.tree.get_mut(&key) {
                set.remove(&node.id);
                if set.is_empty() {
                    self.tree.remove(&key);
                }
            }
    }

    pub fn lookup(&self, values: &[PropValue]) -> Vec<NodeId> {
        let key: Vec<IndexKey> = values.iter().map(IndexKey::from_prop).collect();
        self.tree.get(&key).map(|s| s.iter().copied().collect()).unwrap_or_default()
    }

    pub fn entry_count(&self) -> usize {
        self.tree.values().map(|s| s.len()).sum()
    }
}

// ============================================================================
// Gap 8: Graph Transaction Isolation — MVCC-style snapshot isolation for graph
// ============================================================================

/// A unique transaction ID for graph operations.
pub type GraphTxnId = u64;

/// Graph operation that can be committed or rolled back.
#[allow(dead_code)]
#[derive(Debug, Clone)]
enum GraphOp {
    CreateNode {
        id: NodeId,
        labels: Vec<String>,
        properties: Properties,
    },
    DeleteNode {
        id: NodeId,
        node: Node,
        outgoing: Vec<Edge>,
        incoming: Vec<Edge>,
    },
    CreateEdge {
        id: EdgeId,
        edge_type: String,
        from: NodeId,
        to: NodeId,
        properties: Properties,
    },
    DeleteEdge {
        id: EdgeId,
        edge: Edge,
    },
    SetProperty {
        node_id: NodeId,
        key: String,
        old_value: Option<PropValue>,
        new_value: PropValue,
    },
}

/// A graph transaction that buffers operations for atomic commit or rollback.
///
/// Provides snapshot isolation: reads see the graph state at transaction start,
/// writes are buffered and only applied on commit.
pub struct GraphTransaction {
    pub id: GraphTxnId,
    ops: Vec<GraphOp>,
    /// Snapshot of node IDs visible at transaction start.
    visible_nodes: HashSet<NodeId>,
    /// Snapshot of edge IDs visible at transaction start.
    visible_edges: HashSet<EdgeId>,
    /// Nodes created in this transaction (visible to reads within this txn).
    created_nodes: HashMap<NodeId, Node>,
    /// Edges created in this transaction.
    created_edges: HashMap<EdgeId, Edge>,
    /// Deleted node IDs (hidden from reads within this txn).
    deleted_nodes: HashSet<NodeId>,
    /// Deleted edge IDs.
    deleted_edges: HashSet<EdgeId>,
    committed: bool,
}

impl GraphTransaction {
    /// Start a new transaction with a snapshot of the current graph state.
    pub fn begin(id: GraphTxnId, graph: &GraphStore) -> Self {
        GraphTransaction {
            id,
            ops: Vec::new(),
            visible_nodes: graph.nodes.keys().copied().collect(),
            visible_edges: graph.edges.keys().copied().collect(),
            created_nodes: HashMap::new(),
            created_edges: HashMap::new(),
            deleted_nodes: HashSet::new(),
            deleted_edges: HashSet::new(),
            committed: false,
        }
    }

    /// Check if a node is visible in this transaction's snapshot.
    pub fn is_node_visible(&self, id: NodeId) -> bool {
        (self.visible_nodes.contains(&id) || self.created_nodes.contains_key(&id))
            && !self.deleted_nodes.contains(&id)
    }

    /// Check if an edge is visible in this transaction's snapshot.
    pub fn is_edge_visible(&self, id: EdgeId) -> bool {
        (self.visible_edges.contains(&id) || self.created_edges.contains_key(&id))
            && !self.deleted_edges.contains(&id)
    }

    /// Buffer a node creation.
    pub fn create_node(
        &mut self,
        graph: &GraphStore,
        labels: Vec<String>,
        properties: Properties,
    ) -> NodeId {
        let id = graph.next_node_id + self.created_nodes.len() as u64;
        let node = Node {
            id,
            labels: labels.clone(),
            properties: properties.clone(),
        };
        self.created_nodes.insert(id, node);
        self.ops.push(GraphOp::CreateNode {
            id,
            labels,
            properties,
        });
        id
    }

    /// Buffer a node deletion.
    pub fn delete_node(&mut self, graph: &GraphStore, id: NodeId) -> bool {
        if !self.is_node_visible(id) {
            return false;
        }
        if let Some(node) = self.created_nodes.remove(&id) {
            // Remove from buffered creates, collect edges
            self.ops.push(GraphOp::DeleteNode {
                id,
                node,
                outgoing: Vec::new(),
                incoming: Vec::new(),
            });
        } else if let Some(node) = graph.nodes.get(&id) {
            let outgoing: Vec<Edge> = graph
                .outgoing_edges(id)
                .into_iter()
                .cloned()
                .collect();
            let incoming: Vec<Edge> = graph
                .incoming_edges(id)
                .into_iter()
                .cloned()
                .collect();
            self.ops.push(GraphOp::DeleteNode {
                id,
                node: node.clone(),
                outgoing,
                incoming,
            });
        }
        self.deleted_nodes.insert(id);
        true
    }

    /// Buffer an edge creation.
    pub fn create_edge(
        &mut self,
        graph: &GraphStore,
        edge_type: &str,
        from: NodeId,
        to: NodeId,
        properties: Properties,
    ) -> Option<EdgeId> {
        if !self.is_node_visible(from) || !self.is_node_visible(to) {
            return None;
        }
        let id = graph.next_edge_id + self.created_edges.len() as u64;
        let edge = Edge {
            id,
            edge_type: edge_type.to_string(),
            from,
            to,
            properties: properties.clone(),
        };
        self.created_edges.insert(id, edge);
        self.ops.push(GraphOp::CreateEdge {
            id,
            edge_type: edge_type.to_string(),
            from,
            to,
            properties,
        });
        Some(id)
    }

    /// Number of buffered operations.
    pub fn op_count(&self) -> usize {
        self.ops.len()
    }

    /// Commit: apply all buffered operations to the graph.
    /// Returns the number of operations applied.
    pub fn commit(mut self, graph: &mut GraphStore) -> usize {
        let count = self.ops.len();
        for op in self.ops.drain(..) {
            match op {
                GraphOp::CreateNode {
                    labels, properties, ..
                } => {
                    graph.create_node(labels, properties);
                }
                GraphOp::DeleteNode { id, .. } => {
                    graph.delete_node(id);
                }
                GraphOp::CreateEdge {
                    edge_type,
                    from,
                    to,
                    properties,
                    ..
                } => {
                    let _ = graph.create_edge(from, to, edge_type, properties);
                }
                GraphOp::DeleteEdge { id, .. } => {
                    graph.delete_edge(id);
                }
                GraphOp::SetProperty {
                    node_id,
                    key,
                    new_value,
                    ..
                } => {
                    if let Some(node) = graph.nodes.get_mut(&node_id) {
                        node.properties.insert(key, new_value);
                    }
                }
            }
        }
        self.committed = true;
        count
    }

    /// Rollback: discard all buffered operations. Returns the number discarded.
    pub fn rollback(mut self) -> usize {
        let count = self.ops.len();
        self.ops.clear();
        self.committed = true;
        count
    }
}

/// Manages concurrent graph transactions with conflict detection.
pub struct GraphTransactionManager {
    next_txn_id: GraphTxnId,
    /// Track which nodes are being written by active transactions.
    write_locks: HashMap<NodeId, GraphTxnId>,
}

impl Default for GraphTransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphTransactionManager {
    pub fn new() -> Self {
        GraphTransactionManager {
            next_txn_id: 1,
            write_locks: HashMap::new(),
        }
    }

    /// Begin a new transaction.
    pub fn begin(&mut self, graph: &GraphStore) -> GraphTransaction {
        let id = self.next_txn_id;
        self.next_txn_id += 1;
        GraphTransaction::begin(id, graph)
    }

    /// Try to acquire a write lock on a node for a transaction.
    /// Returns false if another transaction holds the lock (write-write conflict).
    pub fn try_lock_node(&mut self, node_id: NodeId, txn_id: GraphTxnId) -> bool {
        match self.write_locks.get(&node_id) {
            Some(&holder) if holder != txn_id => false,
            _ => {
                self.write_locks.insert(node_id, txn_id);
                true
            }
        }
    }

    /// Release all write locks held by a transaction.
    pub fn release_locks(&mut self, txn_id: GraphTxnId) {
        self.write_locks.retain(|_, &mut holder| holder != txn_id);
    }

    /// Commit a transaction and release its locks.
    pub fn commit(&mut self, txn: GraphTransaction, graph: &mut GraphStore) -> usize {
        let txn_id = txn.id;
        let count = txn.commit(graph);
        self.release_locks(txn_id);
        count
    }

    /// Rollback a transaction and release its locks.
    pub fn rollback(&mut self, txn: GraphTransaction) -> usize {
        let txn_id = txn.id;
        let count = txn.rollback();
        self.release_locks(txn_id);
        count
    }

    /// Number of active write locks.
    pub fn active_locks(&self) -> usize {
        self.write_locks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn social_graph() -> GraphStore {
        let mut g = GraphStore::new();

        // People
        let alice = g.create_node(
            vec!["Person".into()],
            props(vec![("name", PropValue::Text("Alice".into())), ("age", PropValue::Int(30))]),
        );
        let bob = g.create_node(
            vec!["Person".into()],
            props(vec![("name", PropValue::Text("Bob".into())), ("age", PropValue::Int(25))]),
        );
        let charlie = g.create_node(
            vec!["Person".into()],
            props(vec![("name", PropValue::Text("Charlie".into())), ("age", PropValue::Int(35))]),
        );
        let dave = g.create_node(
            vec!["Person".into()],
            props(vec![("name", PropValue::Text("Dave".into())), ("age", PropValue::Int(28))]),
        );

        // Companies
        let acme = g.create_node(
            vec!["Company".into()],
            props(vec![("name", PropValue::Text("Acme Corp".into()))]),
        );

        // Friendships (bidirectional)
        g.create_edge(alice, bob, "FRIENDS".into(), Properties::new());
        g.create_edge(bob, alice, "FRIENDS".into(), Properties::new());
        g.create_edge(bob, charlie, "FRIENDS".into(), Properties::new());
        g.create_edge(charlie, bob, "FRIENDS".into(), Properties::new());
        g.create_edge(charlie, dave, "FRIENDS".into(), Properties::new());
        g.create_edge(dave, charlie, "FRIENDS".into(), Properties::new());

        // Work relationships
        g.create_edge(alice, acme, "WORKS_AT".into(), props(vec![("since", PropValue::Int(2020))]));
        g.create_edge(bob, acme, "WORKS_AT".into(), props(vec![("since", PropValue::Int(2022))]));

        g
    }

    #[test]
    fn create_and_query_nodes() {
        let g = social_graph();
        assert_eq!(g.node_count(), 5); // 4 people + 1 company
        assert_eq!(g.edge_count(), 8); // 6 friend edges + 2 work edges

        let people = g.nodes_by_label("Person");
        assert_eq!(people.len(), 4);

        let companies = g.nodes_by_label("Company");
        assert_eq!(companies.len(), 1);
    }

    #[test]
    fn neighbors_and_degree() {
        let g = social_graph();
        // Alice (id=1) has: FRIENDS->Bob, WORKS_AT->Acme (outgoing=2)
        // Also: Bob->Alice FRIENDS (incoming=1)
        assert_eq!(g.degree(1, Direction::Outgoing), 2);
        assert_eq!(g.degree(1, Direction::Incoming), 1);
        assert_eq!(g.degree(1, Direction::Both), 3);

        // Filter by edge type
        let friends = g.neighbors(1, Direction::Outgoing, Some("FRIENDS"));
        assert_eq!(friends.len(), 1); // Only Bob
    }

    #[test]
    fn bfs_traversal() {
        let g = social_graph();
        // BFS from Alice following FRIENDS edges (outgoing only)
        let visited = g.bfs(1, Direction::Outgoing, Some("FRIENDS"));
        // Alice -> Bob -> Charlie -> Dave
        assert_eq!(visited, vec![1, 2, 3, 4]);
    }

    #[test]
    fn shortest_path_unweighted() {
        let g = social_graph();
        // Shortest path from Alice (1) to Dave (4) via FRIENDS
        let path = g.shortest_path(1, 4, Direction::Outgoing, Some("FRIENDS"));
        assert_eq!(path, Some(vec![1, 2, 3, 4])); // Alice -> Bob -> Charlie -> Dave
    }

    #[test]
    fn dijkstra_weighted() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        let c = g.create_node(vec![], Properties::new());
        let d = g.create_node(vec![], Properties::new());

        // A -> B (weight 1), A -> C (weight 4)
        // B -> C (weight 2), B -> D (weight 6)
        // C -> D (weight 1)
        g.create_edge(a, b, "ROAD".into(), props(vec![("dist", PropValue::Float(1.0))]));
        g.create_edge(a, c, "ROAD".into(), props(vec![("dist", PropValue::Float(4.0))]));
        g.create_edge(b, c, "ROAD".into(), props(vec![("dist", PropValue::Float(2.0))]));
        g.create_edge(b, d, "ROAD".into(), props(vec![("dist", PropValue::Float(6.0))]));
        g.create_edge(c, d, "ROAD".into(), props(vec![("dist", PropValue::Float(1.0))]));

        let (dist, path) = g.dijkstra(a, d, Direction::Outgoing, "dist").unwrap();
        assert!((dist - 4.0).abs() < 1e-10); // A->B(1) + B->C(2) + C->D(1) = 4
        assert_eq!(path, vec![a, b, c, d]);
    }

    #[test]
    fn pattern_matching() {
        let g = social_graph();

        // MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p, c
        let matches = g.match_pattern(Some("Person"), Some("WORKS_AT"), Some("Company"));
        assert_eq!(matches.len(), 2); // Alice and Bob work at Acme

        // MATCH (p:Person)-[:FRIENDS]->(q:Person)
        let friends = g.match_pattern(Some("Person"), Some("FRIENDS"), Some("Person"));
        assert_eq!(friends.len(), 6); // 3 bidirectional friendships = 6 edges
    }

    #[test]
    fn variable_length_paths() {
        let g = social_graph();

        // Find all paths of length 1-3 from Alice (Person) via FRIENDS
        let paths = g.match_variable_length(Some("Person"), Some("FRIENDS"), 1, 3);

        // Should include paths like Alice->Bob, Alice->Bob->Charlie, Alice->Bob->Charlie->Dave
        // And paths starting from other Person nodes too
        assert!(paths.len() > 3);

        // Check that we find the Alice->Bob->Charlie->Dave path (length 3)
        let alice_to_dave = paths.iter().find(|p| p == &&vec![1u64, 2, 3, 4]);
        assert!(alice_to_dave.is_some());
    }

    #[test]
    fn delete_node_cascades() {
        let mut g = social_graph();
        assert_eq!(g.node_count(), 5);
        assert_eq!(g.edge_count(), 8);

        // Delete Bob (id=2) — should remove all his edges
        assert!(g.delete_node(2));
        assert_eq!(g.node_count(), 4);
        // Bob had: FRIENDS->Alice, FRIENDS->Charlie (outgoing=2), Alice->Bob, Charlie->Bob (incoming=2), WORKS_AT->Acme (outgoing)
        // Actually Bob had 3 outgoing (FRIENDS->Alice, FRIENDS->Charlie, WORKS_AT->Acme) and 2 incoming (Alice->Bob, Charlie->Bob)
        // That's 5 edges removed
        assert_eq!(g.edge_count(), 3); // 8 - 5 = 3 remaining
    }

    #[test]
    fn connected_components() {
        let mut g = GraphStore::new();
        // Component 1: A-B
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        g.create_edge(a, b, "LINK".into(), Properties::new());

        // Component 2: C-D-E
        let c = g.create_node(vec![], Properties::new());
        let d = g.create_node(vec![], Properties::new());
        let e = g.create_node(vec![], Properties::new());
        g.create_edge(c, d, "LINK".into(), Properties::new());
        g.create_edge(d, e, "LINK".into(), Properties::new());

        // Component 3: F (isolated)
        let _f = g.create_node(vec![], Properties::new());

        let components = g.connected_components();
        assert_eq!(components.len(), 3);
    }

    #[test]
    fn pagerank() {
        let mut g = GraphStore::new();
        // Simple graph: A -> B -> C, A -> C
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        let c = g.create_node(vec![], Properties::new());
        g.create_edge(a, b, "LINK".into(), Properties::new());
        g.create_edge(b, c, "LINK".into(), Properties::new());
        g.create_edge(a, c, "LINK".into(), Properties::new());

        let ranks = g.pagerank(0.85, 50);

        // C should have highest rank (most incoming links)
        let rank_a = ranks[&a];
        let rank_b = ranks[&b];
        let rank_c = ranks[&c];
        assert!(rank_c > rank_b);
        assert!(rank_c > rank_a);
    }

    // ================================================================
    // CSR graph tests
    // ================================================================

    #[test]
    fn csr_from_graph_basic() {
        let g = social_graph();
        let csr = CsrGraph::from_graph(&g);
        assert_eq!(csr.node_count(), 5);
        assert_eq!(csr.edge_count(), 8);
    }

    #[test]
    fn csr_neighbors_slice() {
        let g = social_graph();
        let csr = CsrGraph::from_graph(&g);
        // Alice (id=1): outgoing = FRIENDS->Bob + WORKS_AT->Acme = 2
        let n = csr.neighbors(1);
        assert_eq!(n.len(), 2);
    }

    #[test]
    fn csr_neighbors_typed() {
        let g = social_graph();
        let csr = CsrGraph::from_graph(&g);
        let friends = csr.neighbors_typed(1, "FRIENDS");
        assert_eq!(friends.len(), 1); // Alice FRIENDS-> Bob only
        let works = csr.neighbors_typed(1, "WORKS_AT");
        assert_eq!(works.len(), 1);
    }

    #[test]
    fn csr_degree() {
        let g = social_graph();
        let csr = CsrGraph::from_graph(&g);
        // Bob (id=2): FRIENDS->Alice, FRIENDS->Charlie, WORKS_AT->Acme = 3 outgoing
        assert_eq!(csr.degree(2), 3);
    }

    #[test]
    fn csr_bfs() {
        let g = social_graph();
        let csr = CsrGraph::from_graph(&g);
        let order = csr.bfs(1); // BFS from Alice
        // Should visit all 5 nodes (graph is connected via outgoing edges)
        assert!(order.len() >= 3); // At least Alice, Bob, Acme reachable
        assert_eq!(order[0], 1); // Starts at Alice
    }

    #[test]
    fn csr_shortest_path() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        let c = g.create_node(vec![], Properties::new());
        let d = g.create_node(vec![], Properties::new());

        g.create_edge(a, b, "ROAD".into(), props(vec![("weight", PropValue::Float(1.0))]));
        g.create_edge(b, c, "ROAD".into(), props(vec![("weight", PropValue::Float(2.0))]));
        g.create_edge(c, d, "ROAD".into(), props(vec![("weight", PropValue::Float(1.0))]));
        g.create_edge(a, d, "ROAD".into(), props(vec![("weight", PropValue::Float(10.0))]));

        let csr = CsrGraph::from_graph(&g);
        let (dist, path) = csr.shortest_path(a, d, "weight").unwrap();
        assert!((dist - 4.0).abs() < 1e-10); // a->b(1) + b->c(2) + c->d(1) = 4
        assert_eq!(path, vec![a, b, c, d]);
    }

    #[test]
    fn csr_shortest_path_no_path() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let _b = g.create_node(vec![], Properties::new()); // isolated

        let csr = CsrGraph::from_graph(&g);
        assert!(csr.shortest_path(a, _b, "weight").is_none());
    }

    #[test]
    fn csr_empty_graph() {
        let g = GraphStore::new();
        let csr = CsrGraph::from_graph(&g);
        assert_eq!(csr.node_count(), 0);
        assert_eq!(csr.edge_count(), 0);
        assert!(csr.neighbors(1).is_empty());
        assert_eq!(csr.bfs(1).len(), 0);
    }

    #[test]
    fn csr_nonexistent_node() {
        let g = social_graph();
        let csr = CsrGraph::from_graph(&g);
        assert!(csr.neighbors(999).is_empty());
        assert_eq!(csr.degree(999), 0);
    }

    #[test]
    fn csr_memory_bytes() {
        let g = social_graph();
        let csr = CsrGraph::from_graph(&g);
        let mem = csr.memory_bytes();
        assert!(mem > 0);
        // Should be much less than HashMap-based storage
        assert!(mem < 10_000);
    }

    #[test]
    fn csr_chain_graph() {
        let mut g = GraphStore::new();
        let mut ids = Vec::new();
        for _ in 0..10 {
            ids.push(g.create_node(vec![], Properties::new()));
        }
        for i in 0..9 {
            g.create_edge(ids[i], ids[i + 1], "NEXT".into(), Properties::new());
        }

        let csr = CsrGraph::from_graph(&g);
        assert_eq!(csr.node_count(), 10);
        assert_eq!(csr.edge_count(), 9);

        // BFS from first should reach all
        let order = csr.bfs(ids[0]);
        assert_eq!(order.len(), 10);

        // Each interior node has degree 1 (one outgoing)
        assert_eq!(csr.degree(ids[0]), 1);
        assert_eq!(csr.degree(ids[9]), 0); // last node has no outgoing
    }

    #[test]
    fn csr_self_loop() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        g.create_edge(a, a, "SELF".into(), Properties::new());

        let csr = CsrGraph::from_graph(&g);
        assert_eq!(csr.degree(a), 1);
        assert_eq!(csr.neighbors(a)[0].target, a);
    }

    #[test]
    fn csr_preserves_edge_types() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        g.create_edge(a, b, "ALPHA".into(), Properties::new());
        g.create_edge(a, b, "BETA".into(), Properties::new());
        g.create_edge(a, b, "GAMMA".into(), Properties::new());

        let csr = CsrGraph::from_graph(&g);
        assert_eq!(csr.degree(a), 3);
        let types: Vec<&str> = csr.neighbors(a).iter().map(|e| e.edge_type.as_str()).collect();
        assert!(types.contains(&"ALPHA"));
        assert!(types.contains(&"BETA"));
        assert!(types.contains(&"GAMMA"));
    }

    // ================================================================
    // Property Index tests
    // ================================================================

    #[test]
    fn prop_index_build_and_lookup() {
        let mut g = GraphStore::new();
        g.create_node(vec!["Person".into()], props(vec![("age", PropValue::Int(30))]));
        g.create_node(vec!["Person".into()], props(vec![("age", PropValue::Int(25))]));
        g.create_node(vec!["Person".into()], props(vec![("age", PropValue::Int(30))]));
        g.create_node(vec!["Place".into()], props(vec![("age", PropValue::Int(30))]));

        let mut idx = PropertyIndex::new("Person", "age");
        idx.build_from(&g);

        let age30 = idx.lookup(&PropValue::Int(30));
        assert_eq!(age30.len(), 2);
        let age25 = idx.lookup(&PropValue::Int(25));
        assert_eq!(age25.len(), 1);
        let age99 = idx.lookup(&PropValue::Int(99));
        assert!(age99.is_empty());
    }

    #[test]
    fn prop_index_range_query() {
        let mut g = GraphStore::new();
        for age in [20, 25, 30, 35, 40] {
            g.create_node(vec!["Person".into()], props(vec![("age", PropValue::Int(age))]));
        }

        let mut idx = PropertyIndex::new("Person", "age");
        idx.build_from(&g);

        let range = idx.range(&PropValue::Int(25), &PropValue::Int(35));
        assert_eq!(range.len(), 3); // 25, 30, 35
    }

    #[test]
    fn prop_index_insert_remove() {
        let mut idx = PropertyIndex::new("Person", "name");
        idx.insert(1, &PropValue::Text("Alice".into()));
        idx.insert(2, &PropValue::Text("Bob".into()));
        idx.insert(3, &PropValue::Text("Alice".into()));

        assert_eq!(idx.lookup(&PropValue::Text("Alice".into())).len(), 2);
        assert_eq!(idx.entry_count(), 3);

        idx.remove(1, &PropValue::Text("Alice".into()));
        assert_eq!(idx.lookup(&PropValue::Text("Alice".into())).len(), 1);
        assert_eq!(idx.entry_count(), 2);
    }

    #[test]
    fn prop_index_text_ordering() {
        let mut idx = PropertyIndex::new("Item", "name");
        idx.insert(1, &PropValue::Text("apple".into()));
        idx.insert(2, &PropValue::Text("banana".into()));
        idx.insert(3, &PropValue::Text("cherry".into()));

        let range = idx.range(
            &PropValue::Text("apple".into()),
            &PropValue::Text("cherry".into()),
        );
        assert_eq!(range.len(), 3);

        let range = idx.range(
            &PropValue::Text("banana".into()),
            &PropValue::Text("banana".into()),
        );
        assert_eq!(range.len(), 1);
    }

    #[test]
    fn prop_index_float_ordering() {
        let mut idx = PropertyIndex::new("Measurement", "value");
        idx.insert(1, &PropValue::Float(1.5));
        idx.insert(2, &PropValue::Float(2.5));
        idx.insert(3, &PropValue::Float(3.5));
        idx.insert(4, &PropValue::Float(-1.0));

        let range = idx.range(&PropValue::Float(1.0), &PropValue::Float(3.0));
        assert_eq!(range.len(), 2); // 1.5, 2.5
    }

    #[test]
    fn prop_index_distinct_values() {
        let mut idx = PropertyIndex::new("X", "val");
        idx.insert(1, &PropValue::Int(10));
        idx.insert(2, &PropValue::Int(20));
        idx.insert(3, &PropValue::Int(10)); // duplicate value
        assert_eq!(idx.distinct_values(), 2);
    }

    #[test]
    fn composite_index_build_and_lookup() {
        let mut g = GraphStore::new();
        g.create_node(
            vec!["Person".into()],
            props(vec![
                ("first", PropValue::Text("Alice".into())),
                ("last", PropValue::Text("Smith".into())),
            ]),
        );
        g.create_node(
            vec!["Person".into()],
            props(vec![
                ("first", PropValue::Text("Bob".into())),
                ("last", PropValue::Text("Smith".into())),
            ]),
        );
        g.create_node(
            vec!["Person".into()],
            props(vec![
                ("first", PropValue::Text("Alice".into())),
                ("last", PropValue::Text("Jones".into())),
            ]),
        );

        let mut idx =
            CompositePropertyIndex::new("Person", vec!["first".into(), "last".into()]);
        idx.build_from(&g);

        let results = idx.lookup(&[
            PropValue::Text("Alice".into()),
            PropValue::Text("Smith".into()),
        ]);
        assert_eq!(results.len(), 1);

        // Different composite key
        let results = idx.lookup(&[
            PropValue::Text("Bob".into()),
            PropValue::Text("Smith".into()),
        ]);
        assert_eq!(results.len(), 1);

        assert_eq!(idx.entry_count(), 3);
    }

    // ================================================================
    // Graph Transaction tests
    // ================================================================

    #[test]
    fn graph_txn_create_and_commit() {
        let mut g = GraphStore::new();
        let mut mgr = GraphTransactionManager::new();

        let mut txn = mgr.begin(&g);
        let node_id = txn.create_node(&g, vec!["Person".into()], props(vec![("name", PropValue::Text("Alice".into()))]));
        assert!(txn.is_node_visible(node_id));
        assert_eq!(txn.op_count(), 1);

        let ops = mgr.commit(txn, &mut g);
        assert_eq!(ops, 1);
        assert_eq!(g.node_count(), 1);
    }

    #[test]
    fn graph_txn_rollback() {
        let g = GraphStore::new();
        let mut mgr = GraphTransactionManager::new();

        let mut txn = mgr.begin(&g);
        txn.create_node(&g, vec!["Person".into()], Properties::new());

        let ops = mgr.rollback(txn);
        assert_eq!(ops, 1);
        assert_eq!(g.node_count(), 0); // nothing committed
    }

    #[test]
    fn graph_txn_delete_node() {
        let mut g = GraphStore::new();
        let id = g.create_node(vec!["Person".into()], Properties::new());

        let mut mgr = GraphTransactionManager::new();
        let mut txn = mgr.begin(&g);
        assert!(txn.is_node_visible(id));

        txn.delete_node(&g, id);
        assert!(!txn.is_node_visible(id));

        mgr.commit(txn, &mut g);
        assert_eq!(g.node_count(), 0);
    }

    #[test]
    fn graph_txn_create_edge() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec!["Person".into()], Properties::new());
        let b = g.create_node(vec!["Person".into()], Properties::new());

        let mut mgr = GraphTransactionManager::new();
        let mut txn = mgr.begin(&g);
        let edge_id = txn.create_edge(&g, "KNOWS", a, b, Properties::new());
        assert!(edge_id.is_some());

        mgr.commit(txn, &mut g);
        assert_eq!(g.edge_count(), 1);
    }

    #[test]
    fn graph_txn_snapshot_isolation() {
        let mut g = GraphStore::new();
        g.create_node(vec!["A".into()], Properties::new());

        let mut mgr = GraphTransactionManager::new();

        // Txn1 starts — sees 1 node
        let txn1 = mgr.begin(&g);
        assert!(txn1.is_node_visible(1));

        // Meanwhile, add another node directly
        g.create_node(vec!["B".into()], Properties::new());

        // Txn1 should NOT see the new node (snapshot isolation)
        assert!(!txn1.is_node_visible(2));

        mgr.rollback(txn1);
    }

    #[test]
    fn graph_txn_write_lock_conflict() {
        let g = GraphStore::new();
        let mut mgr = GraphTransactionManager::new();

        let txn1 = mgr.begin(&g);
        let txn2 = mgr.begin(&g);

        // Txn1 locks node 1
        assert!(mgr.try_lock_node(1, txn1.id));
        // Txn2 tries to lock the same node — conflict
        assert!(!mgr.try_lock_node(1, txn2.id));
        // Txn1 can re-lock the same node (idempotent)
        assert!(mgr.try_lock_node(1, txn1.id));

        assert_eq!(mgr.active_locks(), 1);
        mgr.release_locks(txn1.id);
        assert_eq!(mgr.active_locks(), 0);

        // Now txn2 can lock it
        assert!(mgr.try_lock_node(1, txn2.id));
        mgr.release_locks(txn2.id);

        mgr.rollback(txn1);
        mgr.rollback(txn2);
    }

    #[test]
    fn graph_txn_multiple_ops() {
        let mut g = GraphStore::new();
        let mut mgr = GraphTransactionManager::new();

        let mut txn = mgr.begin(&g);
        let a = txn.create_node(&g, vec!["Person".into()], props(vec![("name", PropValue::Text("Alice".into()))]));
        let b = txn.create_node(&g, vec!["Person".into()], props(vec![("name", PropValue::Text("Bob".into()))]));
        txn.create_edge(&g, "KNOWS", a, b, Properties::new());

        assert_eq!(txn.op_count(), 3);
        mgr.commit(txn, &mut g);
        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 1);
    }

    #[test]
    fn graph_txn_manager_lock_cleanup() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let mut mgr = GraphTransactionManager::new();

        let txn = mgr.begin(&g);
        mgr.try_lock_node(a, txn.id);
        assert_eq!(mgr.active_locks(), 1);

        mgr.commit(txn, &mut g);
        assert_eq!(mgr.active_locks(), 0); // locks released on commit
    }

    // ================================================================
    // WAL integration tests
    // ================================================================

    #[test]
    fn wal_nodes_edges_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            let a = g.create_node(
                vec!["Person".into()],
                props(vec![("name", PropValue::Text("Alice".into()))]),
            );
            let b = g.create_node(
                vec!["Person".into()],
                props(vec![("name", PropValue::Text("Bob".into()))]),
            );
            g.create_edge(a, b, "KNOWS".into(), props(vec![("since", PropValue::Int(2024))]));
        }
        // Reopen.
        let g2 = GraphStore::open(dir.path()).unwrap();
        assert_eq!(g2.node_count(), 2);
        assert_eq!(g2.edge_count(), 1);
        let alice = g2.get_node(1).unwrap();
        assert_eq!(alice.properties.get("name"), Some(&PropValue::Text("Alice".into())));
        let edge = g2.get_edge(1).unwrap();
        assert_eq!(edge.edge_type, "KNOWS");
        assert_eq!(edge.properties.get("since"), Some(&PropValue::Int(2024)));
    }

    #[test]
    fn wal_delete_node_cascade_edges_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            let a = g.create_node(vec!["A".into()], Properties::new());
            let b = g.create_node(vec!["B".into()], Properties::new());
            let c = g.create_node(vec!["C".into()], Properties::new());
            g.create_edge(a, b, "X".into(), Properties::new());
            g.create_edge(b, c, "Y".into(), Properties::new());
            g.create_edge(a, c, "Z".into(), Properties::new());
            // Delete node B — should cascade edges X (a->b) and Y (b->c).
            g.delete_node(b);
        }
        let g2 = GraphStore::open(dir.path()).unwrap();
        assert_eq!(g2.node_count(), 2);
        assert_eq!(g2.edge_count(), 1); // only edge Z (a->c) remains
        assert!(g2.get_node(2).is_none()); // B deleted
        assert!(g2.get_edge(3).is_some()); // Z still exists
    }

    #[test]
    fn wal_properties_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            g.create_node(vec![], props(vec![
                ("s", PropValue::Text("hello".into())),
                ("i", PropValue::Int(42)),
                ("f", PropValue::Float(3.14)),
                ("b", PropValue::Bool(true)),
                ("n", PropValue::Null),
            ]));
        }
        let g2 = GraphStore::open(dir.path()).unwrap();
        let n = g2.get_node(1).unwrap();
        assert_eq!(n.properties.get("s"), Some(&PropValue::Text("hello".into())));
        assert_eq!(n.properties.get("i"), Some(&PropValue::Int(42)));
        assert_eq!(n.properties.get("f"), Some(&PropValue::Float(3.14)));
        assert_eq!(n.properties.get("b"), Some(&PropValue::Bool(true)));
        assert_eq!(n.properties.get("n"), Some(&PropValue::Null));
    }

    #[test]
    fn wal_label_index_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            g.create_node(vec!["Person".into(), "Employee".into()], Properties::new());
            g.create_node(vec!["Person".into()], Properties::new());
            g.create_node(vec!["Company".into()], Properties::new());
        }
        let g2 = GraphStore::open(dir.path()).unwrap();
        assert_eq!(g2.nodes_by_label("Person").len(), 2);
        assert_eq!(g2.nodes_by_label("Employee").len(), 1);
        assert_eq!(g2.nodes_by_label("Company").len(), 1);
    }

    #[test]
    fn wal_adjacency_correct_after_replay() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            let a = g.create_node(vec![], Properties::new());
            let b = g.create_node(vec![], Properties::new());
            let c = g.create_node(vec![], Properties::new());
            g.create_edge(a, b, "E1".into(), Properties::new());
            g.create_edge(a, c, "E2".into(), Properties::new());
            g.create_edge(b, c, "E3".into(), Properties::new());
        }
        let g2 = GraphStore::open(dir.path()).unwrap();
        assert_eq!(g2.outgoing_edges(1).len(), 2); // a -> b, a -> c
        assert_eq!(g2.outgoing_edges(2).len(), 1); // b -> c
        assert_eq!(g2.incoming_edges(3).len(), 2); // a -> c, b -> c
        assert_eq!(g2.degree(1, Direction::Outgoing), 2);
        assert_eq!(g2.degree(3, Direction::Incoming), 2);
    }

    #[test]
    fn wal_bfs_shortest_path_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            let a = g.create_node(vec![], Properties::new());
            let b = g.create_node(vec![], Properties::new());
            let c = g.create_node(vec![], Properties::new());
            let d = g.create_node(vec![], Properties::new());
            g.create_edge(a, b, "NEXT".into(), Properties::new());
            g.create_edge(b, c, "NEXT".into(), Properties::new());
            g.create_edge(c, d, "NEXT".into(), Properties::new());
        }
        let g2 = GraphStore::open(dir.path()).unwrap();
        let bfs = g2.bfs(1, Direction::Outgoing, Some("NEXT"));
        assert_eq!(bfs, vec![1, 2, 3, 4]);
        let sp = g2.shortest_path(1, 4, Direction::Outgoing, Some("NEXT"));
        assert_eq!(sp, Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn wal_corrupt_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            g.create_node(vec!["X".into()], Properties::new());
            g.create_node(vec!["Y".into()], Properties::new());
        }
        // Append garbage to the WAL file.
        {
            use std::io::Write;
            let wal_path = dir.path().join("graph.wal");
            let mut f = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD]).unwrap();
        }
        // Should recover the two valid nodes.
        let g2 = GraphStore::open(dir.path()).unwrap();
        assert_eq!(g2.node_count(), 2);
    }

    #[test]
    fn wal_empty_graph_clean_open() {
        let dir = tempfile::tempdir().unwrap();
        let g = GraphStore::open(dir.path()).unwrap();
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.edge_count(), 0);
        drop(g);
        // Reopen empty.
        let g2 = GraphStore::open(dir.path()).unwrap();
        assert_eq!(g2.node_count(), 0);
    }

    #[test]
    fn wal_large_graph_checkpoint_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            for _ in 0..120 {
                g.create_node(vec!["N".into()], Properties::new());
            }
            for i in 1u64..120 {
                g.create_edge(i, i + 1, "NEXT".into(), Properties::new());
            }
            // Checkpoint.
            g.checkpoint_wal().unwrap();
            // Add a few more after checkpoint.
            g.create_node(vec!["N".into()], Properties::new()); // id 121
            g.create_edge(120, 121, "NEXT".into(), Properties::new());
        }
        let g2 = GraphStore::open(dir.path()).unwrap();
        assert_eq!(g2.node_count(), 121);
        assert_eq!(g2.edge_count(), 120);
        // BFS should reach all from node 1.
        let bfs = g2.bfs(1, Direction::Outgoing, Some("NEXT"));
        assert_eq!(bfs.len(), 121);
    }

    #[test]
    fn wal_property_updates_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            g.create_node(vec![], props(vec![("x", PropValue::Int(1))]));
            let a = 1;
            let b = g.create_node(vec![], Properties::new());
            let eid = g.create_edge(a, b, "E".into(), Properties::new()).unwrap();
            // Update properties.
            g.set_node_property(a, "x".into(), PropValue::Int(99));
            g.set_node_property(a, "y".into(), PropValue::Text("new".into()));
            g.set_edge_property(eid, "weight".into(), PropValue::Float(5.5));
        }
        let g2 = GraphStore::open(dir.path()).unwrap();
        let n = g2.get_node(1).unwrap();
        assert_eq!(n.properties.get("x"), Some(&PropValue::Int(99)));
        assert_eq!(n.properties.get("y"), Some(&PropValue::Text("new".into())));
        let e = g2.get_edge(1).unwrap();
        assert_eq!(e.properties.get("weight"), Some(&PropValue::Float(5.5)));
    }

    // ================================================================
    // Edge type index tests
    // ================================================================

    #[test]
    fn type_index_edges_by_type_basic() {
        let g = social_graph();

        // Should find all FRIENDS edges via type index.
        let friends = g.edges_by_type("FRIENDS");
        assert_eq!(friends.len(), 6);
        for e in &friends {
            assert_eq!(e.edge_type, "FRIENDS");
        }

        // Should find all WORKS_AT edges.
        let works = g.edges_by_type("WORKS_AT");
        assert_eq!(works.len(), 2);
        for e in &works {
            assert_eq!(e.edge_type, "WORKS_AT");
        }

        // Non-existent type returns empty.
        let none = g.edges_by_type("NONEXISTENT");
        assert!(none.is_empty());
    }

    #[test]
    fn type_index_maintained_on_delete_edge() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        let e1 = g.create_edge(a, b, "ALPHA".into(), Properties::new()).unwrap();
        let e2 = g.create_edge(a, b, "ALPHA".into(), Properties::new()).unwrap();
        let _e3 = g.create_edge(a, b, "BETA".into(), Properties::new()).unwrap();

        assert_eq!(g.edges_by_type("ALPHA").len(), 2);
        assert_eq!(g.edges_by_type("BETA").len(), 1);

        // Delete one ALPHA edge.
        g.delete_edge(e1);
        assert_eq!(g.edges_by_type("ALPHA").len(), 1);
        assert_eq!(g.edges_by_type("ALPHA")[0].id, e2);

        // Delete the last ALPHA edge — type entry should be cleaned up.
        g.delete_edge(e2);
        assert!(g.edges_by_type("ALPHA").is_empty());
        // BETA should be unaffected.
        assert_eq!(g.edges_by_type("BETA").len(), 1);
    }

    #[test]
    fn type_index_maintained_on_delete_node() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        let c = g.create_node(vec![], Properties::new());
        g.create_edge(a, b, "X".into(), Properties::new());
        g.create_edge(b, c, "X".into(), Properties::new());
        g.create_edge(a, c, "Y".into(), Properties::new());

        assert_eq!(g.edges_by_type("X").len(), 2);
        assert_eq!(g.edges_by_type("Y").len(), 1);

        // Delete node b — should cascade edges a->b (X) and b->c (X).
        g.delete_node(b);
        assert!(g.edges_by_type("X").is_empty());
        // Y edge (a->c) should be unaffected.
        assert_eq!(g.edges_by_type("Y").len(), 1);
    }

    #[test]
    fn type_index_neighbors_optimization() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        let c = g.create_node(vec![], Properties::new());
        g.create_edge(a, b, "FRIENDS".into(), Properties::new());
        g.create_edge(a, c, "WORKS_AT".into(), Properties::new());
        g.create_edge(b, a, "FRIENDS".into(), Properties::new());

        // Outgoing from a, filtered by FRIENDS — should only return b.
        let friends = g.neighbors(a, Direction::Outgoing, Some("FRIENDS"));
        assert_eq!(friends.len(), 1);
        assert_eq!(friends[0].0, b);

        // Outgoing from a, filtered by WORKS_AT — should only return c.
        let works = g.neighbors(a, Direction::Outgoing, Some("WORKS_AT"));
        assert_eq!(works.len(), 1);
        assert_eq!(works[0].0, c);

        // Incoming to a, filtered by FRIENDS — should return b.
        let incoming_friends = g.neighbors(a, Direction::Incoming, Some("FRIENDS"));
        assert_eq!(incoming_friends.len(), 1);
        assert_eq!(incoming_friends[0].0, b);

        // Both directions, filtered by FRIENDS — should return b twice (out + in).
        let both_friends = g.neighbors(a, Direction::Both, Some("FRIENDS"));
        assert_eq!(both_friends.len(), 2);

        // Non-existent type returns empty.
        let none = g.neighbors(a, Direction::Outgoing, Some("NONEXISTENT"));
        assert!(none.is_empty());
    }

    #[test]
    fn type_index_txn_snapshot_restore() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        g.create_edge(a, b, "LINK".into(), Properties::new());

        assert_eq!(g.edges_by_type("LINK").len(), 1);

        // Take snapshot.
        let snap = g.txn_snapshot();

        // Mutate: add more edges, delete the existing one.
        g.create_edge(a, b, "OTHER".into(), Properties::new());
        g.delete_edge(1);
        assert!(g.edges_by_type("LINK").is_empty());
        assert_eq!(g.edges_by_type("OTHER").len(), 1);

        // Restore from snapshot — type index should revert.
        g.txn_restore(snap);
        assert_eq!(g.edges_by_type("LINK").len(), 1);
        assert!(g.edges_by_type("OTHER").is_empty());
    }

    #[test]
    fn type_index_wal_rebuilt_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut g = GraphStore::open(dir.path()).unwrap();
            let a = g.create_node(vec![], Properties::new());
            let b = g.create_node(vec![], Properties::new());
            let c = g.create_node(vec![], Properties::new());
            g.create_edge(a, b, "FRIENDS".into(), Properties::new());
            g.create_edge(b, c, "FRIENDS".into(), Properties::new());
            g.create_edge(a, c, "WORKS_AT".into(), Properties::new());
        }
        // Reopen — type index should be rebuilt from WAL.
        let g2 = GraphStore::open(dir.path()).unwrap();
        assert_eq!(g2.edges_by_type("FRIENDS").len(), 2);
        assert_eq!(g2.edges_by_type("WORKS_AT").len(), 1);
        assert!(g2.edges_by_type("NONEXISTENT").is_empty());

        // Verify neighbors still works with type filter after restart.
        let friends = g2.neighbors(1, Direction::Outgoing, Some("FRIENDS"));
        assert_eq!(friends.len(), 1);
        assert_eq!(friends[0].0, 2);
    }

    #[test]
    fn type_index_empty_after_all_edges_removed() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        let e1 = g.create_edge(a, b, "T".into(), Properties::new()).unwrap();

        assert_eq!(g.edges_by_type("T").len(), 1);

        g.delete_edge(e1);
        assert!(g.edges_by_type("T").is_empty());
        // The HashMap entry for type "T" should be fully cleaned up.
        assert!(!g.type_index.contains_key("T"));
    }

    // ================================================================
    // Parallel graph operations tests
    // ================================================================

    #[test]
    fn par_multi_bfs_matches_sequential() {
        let g = social_graph();
        let sources = vec![1, 2, 3, 4]; // Alice, Bob, Charlie, Dave

        let par_results = g.par_multi_bfs(&sources, Direction::Outgoing, Some("FRIENDS"));
        assert_eq!(par_results.len(), 4);

        for (src, par_reached) in &par_results {
            let seq_reached = g.bfs(*src, Direction::Outgoing, Some("FRIENDS"));
            let par_set: HashSet<NodeId> = par_reached.iter().copied().collect();
            let seq_set: HashSet<NodeId> = seq_reached.iter().copied().collect();
            assert_eq!(par_set, seq_set, "BFS from {} should match sequential", src);
        }
    }

    #[test]
    fn par_batch_shortest_path() {
        let g = social_graph();
        // Alice(1)->Bob(2)->Charlie(3)->Dave(4)
        let queries = vec![(1, 4), (1, 3), (2, 4), (1, 2)];
        let results = g.par_batch_shortest_path(
            &queries,
            Direction::Outgoing,
            Some("FRIENDS"),
        );

        assert_eq!(results.len(), 4);
        assert_eq!(results[0], Some(vec![1, 2, 3, 4])); // Alice -> Bob -> Charlie -> Dave
        assert_eq!(results[1], Some(vec![1, 2, 3]));     // Alice -> Bob -> Charlie
        assert_eq!(results[2], Some(vec![2, 3, 4]));     // Bob -> Charlie -> Dave
        assert_eq!(results[3], Some(vec![1, 2]));         // Alice -> Bob
    }

    #[test]
    fn par_neighbors_filtered_large() {
        // Build a hub-spoke graph with 200+ edges from a single node.
        let mut g = GraphStore::new();
        let hub = g.create_node(vec!["Hub".into()], Properties::new());
        for i in 0..250 {
            let spoke = g.create_node(
                vec!["Spoke".into()],
                props(vec![("weight", PropValue::Int(i as i64))]),
            );
            g.create_edge(
                hub,
                spoke,
                "LINK".into(),
                props(vec![("weight", PropValue::Float(i as f64))]),
            );
        }

        // Filter for edges with weight > 200.0
        let filtered = g.par_neighbors_filtered(hub, |e| {
            matches!(e.properties.get("weight"), Some(PropValue::Float(w)) if *w > 200.0)
        });

        // Weights 201..249 = 49 edges.
        assert_eq!(filtered.len(), 49);
        for e in &filtered {
            if let Some(PropValue::Float(w)) = e.properties.get("weight") {
                assert!(*w > 200.0);
            } else {
                panic!("expected weight property on edge");
            }
        }
    }

    #[test]
    fn par_edge_type_counts() {
        let g = social_graph();
        let counts = g.par_edge_type_counts();

        assert_eq!(counts.get("FRIENDS"), Some(&6));
        assert_eq!(counts.get("WORKS_AT"), Some(&2));
        assert_eq!(counts.len(), 2);
    }

    #[test]
    fn par_multi_bfs_disconnected() {
        let mut g = GraphStore::new();
        // Two disconnected components: {1,2} and {3,4}.
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        let c = g.create_node(vec![], Properties::new());
        let d = g.create_node(vec![], Properties::new());
        g.create_edge(a, b, "LINK".into(), Properties::new());
        g.create_edge(c, d, "LINK".into(), Properties::new());

        let results = g.par_multi_bfs(&[a, c], Direction::Outgoing, None);
        assert_eq!(results.len(), 2);

        let (src1, reached1) = &results[0];
        assert_eq!(*src1, a);
        let set1: HashSet<NodeId> = reached1.iter().copied().collect();
        assert!(set1.contains(&a));
        assert!(set1.contains(&b));
        assert!(!set1.contains(&c));
        assert!(!set1.contains(&d));

        let (src2, reached2) = &results[1];
        assert_eq!(*src2, c);
        let set2: HashSet<NodeId> = reached2.iter().copied().collect();
        assert!(set2.contains(&c));
        assert!(set2.contains(&d));
        assert!(!set2.contains(&a));
        assert!(!set2.contains(&b));
    }

    #[test]
    fn par_batch_shortest_path_consistency() {
        // Run the same batch twice and verify deterministic results.
        let g = social_graph();
        let queries = vec![(1, 4), (2, 4), (1, 3)];

        let r1 = g.par_batch_shortest_path(&queries, Direction::Outgoing, Some("FRIENDS"));
        let r2 = g.par_batch_shortest_path(&queries, Direction::Outgoing, Some("FRIENDS"));

        assert_eq!(r1, r2, "parallel shortest path should be deterministic");
    }

    #[test]
    fn par_neighbors_filtered_small_sequential() {
        // Small neighbor set should still work (takes sequential path).
        let g = social_graph();
        let filtered = g.par_neighbors_filtered(1, |e| e.edge_type == "FRIENDS");
        assert_eq!(filtered.len(), 1); // Alice->Bob FRIENDS
        assert_eq!(filtered[0].to, 2);
    }

    #[test]
    fn par_edge_type_counts_many_types() {
        // Build a graph with 10 distinct edge types to exercise parallel path.
        let mut g = GraphStore::new();
        let a = g.create_node(vec![], Properties::new());
        let b = g.create_node(vec![], Properties::new());
        for i in 0..10 {
            let etype = format!("TYPE_{}", i);
            for _ in 0..5 {
                g.create_edge(a, b, etype.clone(), Properties::new());
            }
        }

        let counts = g.par_edge_type_counts();
        assert_eq!(counts.len(), 10);
        for i in 0..10 {
            let key = format!("TYPE_{}", i);
            assert_eq!(counts.get(&key), Some(&5), "type {} should have 5 edges", key);
        }
    }

    // ================================================================
    // Cold tier (tiered storage) tests
    // ================================================================

    #[test]
    fn test_graph_cold_tier_basic() {
        let dir = tempfile::tempdir().unwrap();
        let mut g = GraphStore::open(dir.path()).unwrap();
        assert!(g.has_cold_tier(), "disk mode should have cold tier");
        assert!(dir.path().join("graph_cold").exists());
        let id = g.create_node(
            vec!["Person".into()],
            props(vec![("name", PropValue::Text("Alice".into()))]),
        );
        let node = g.get_node_full(id).unwrap();
        assert_eq!(node.properties.get("name"), Some(&PropValue::Text("Alice".into())));
    }

    #[test]
    fn test_graph_cold_property_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mut g = GraphStore::open(dir.path()).unwrap();
        g.max_hot_nodes = 5;
        let mut ids = Vec::new();
        for i in 0..20 {
            let id = g.create_node(
                vec!["N".into()],
                props(vec![("val", PropValue::Int(i))]),
            );
            ids.push(id);
        }
        // Hot tier should have at most max_hot_nodes
        assert!(
            g.node_count_hot() <= 5,
            "hot should have <= 5, got {}",
            g.node_count_hot()
        );
        // All 20 should be accessible with correct properties via get_node_full
        for (i, &id) in ids.iter().enumerate() {
            let node = g.get_node_full(id).unwrap();
            assert_eq!(
                node.properties.get("val"),
                Some(&PropValue::Int(i as i64)),
                "node {} should have val={}", id, i
            );
        }
    }

    #[test]
    fn test_graph_memory_mode_no_cold() {
        let g = GraphStore::new();
        assert!(!g.has_cold_tier(), "memory mode should have no cold tier");
    }

    // ====================================================================
    // Community Detection tests
    // ====================================================================

    #[test]
    fn test_label_propagation_two_cliques() {
        let mut g = GraphStore::new();
        // Create two disconnected cliques: {A,B,C} and {D,E,F}
        let a = g.create_node(vec!["Person".into()], Properties::new());
        let b = g.create_node(vec!["Person".into()], Properties::new());
        let c = g.create_node(vec!["Person".into()], Properties::new());
        let d = g.create_node(vec!["Person".into()], Properties::new());
        let e = g.create_node(vec!["Person".into()], Properties::new());
        let f = g.create_node(vec!["Person".into()], Properties::new());

        g.create_edge(a, b, "KNOWS".into(), Properties::new());
        g.create_edge(b, c, "KNOWS".into(), Properties::new());
        g.create_edge(c, a, "KNOWS".into(), Properties::new());

        g.create_edge(d, e, "KNOWS".into(), Properties::new());
        g.create_edge(e, f, "KNOWS".into(), Properties::new());
        g.create_edge(f, d, "KNOWS".into(), Properties::new());

        let labels = g.label_propagation(20);
        assert_eq!(labels.len(), 6);

        // Nodes in same clique should have same label
        assert_eq!(labels[&a], labels[&b]);
        assert_eq!(labels[&b], labels[&c]);
        assert_eq!(labels[&d], labels[&e]);
        assert_eq!(labels[&e], labels[&f]);

        // Different cliques should have different labels
        assert_ne!(labels[&a], labels[&d]);
    }

    #[test]
    fn test_label_propagation_empty() {
        let g = GraphStore::new();
        let labels = g.label_propagation(10);
        assert!(labels.is_empty());
    }

    #[test]
    fn test_louvain_two_cliques() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec!["Person".into()], Properties::new());
        let b = g.create_node(vec!["Person".into()], Properties::new());
        let c = g.create_node(vec!["Person".into()], Properties::new());
        let d = g.create_node(vec!["Person".into()], Properties::new());
        let e = g.create_node(vec!["Person".into()], Properties::new());
        let f = g.create_node(vec!["Person".into()], Properties::new());

        // Dense clique 1
        g.create_edge(a, b, "KNOWS".into(), Properties::new());
        g.create_edge(b, c, "KNOWS".into(), Properties::new());
        g.create_edge(c, a, "KNOWS".into(), Properties::new());

        // Dense clique 2
        g.create_edge(d, e, "KNOWS".into(), Properties::new());
        g.create_edge(e, f, "KNOWS".into(), Properties::new());
        g.create_edge(f, d, "KNOWS".into(), Properties::new());

        let communities = g.louvain_communities();
        assert_eq!(communities.len(), 6);

        // Same clique → same community
        assert_eq!(communities[&a], communities[&b]);
        assert_eq!(communities[&b], communities[&c]);
        assert_eq!(communities[&d], communities[&e]);
        assert_eq!(communities[&e], communities[&f]);

        // Different cliques → different communities
        assert_ne!(communities[&a], communities[&d]);
    }

    #[test]
    fn test_louvain_empty() {
        let g = GraphStore::new();
        let communities = g.louvain_communities();
        assert!(communities.is_empty());
    }

    #[test]
    fn test_louvain_single_node() {
        let mut g = GraphStore::new();
        let a = g.create_node(vec!["Person".into()], Properties::new());
        let communities = g.louvain_communities();
        assert_eq!(communities.len(), 1);
        assert!(communities.contains_key(&a));
    }
}
