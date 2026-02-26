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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

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

// ============================================================================
// Graph store
// ============================================================================

/// In-memory property graph store with adjacency lists.
pub struct GraphStore {
    nodes: HashMap<NodeId, Node>,
    edges: HashMap<EdgeId, Edge>,
    /// node_id → outgoing edge IDs
    outgoing: HashMap<NodeId, Vec<EdgeId>>,
    /// node_id → incoming edge IDs
    incoming: HashMap<NodeId, Vec<EdgeId>>,
    /// Label → node IDs (index for label lookups)
    label_index: HashMap<String, HashSet<NodeId>>,
    next_node_id: NodeId,
    next_edge_id: EdgeId,
}

impl GraphStore {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            label_index: HashMap::new(),
            next_node_id: 1,
            next_edge_id: 1,
        }
    }

    // ---- Node operations ----

    /// Create a node with labels and properties. Returns the node ID.
    pub fn create_node(&mut self, labels: Vec<String>, properties: Properties) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;

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
        id
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    /// Delete a node and all its edges.
    pub fn delete_node(&mut self, id: NodeId) -> bool {
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

        // Collect edge IDs to remove
        let out_edges: Vec<EdgeId> = self.outgoing.remove(&id).unwrap_or_default();
        let in_edges: Vec<EdgeId> = self.incoming.remove(&id).unwrap_or_default();

        for eid in out_edges {
            if let Some(edge) = self.edges.remove(&eid) {
                if let Some(inc) = self.incoming.get_mut(&edge.to) {
                    inc.retain(|e| *e != eid);
                }
            }
        }
        for eid in in_edges {
            if let Some(edge) = self.edges.remove(&eid) {
                if let Some(out) = self.outgoing.get_mut(&edge.from) {
                    out.retain(|e| *e != eid);
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

    /// Delete an edge.
    pub fn delete_edge(&mut self, id: EdgeId) -> bool {
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
        true
    }

    /// Total edge count.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    // ---- Neighbor / traversal primitives ----

    /// Get neighbors of a node in a given direction, optionally filtered by edge type.
    pub fn neighbors(
        &self,
        node_id: NodeId,
        direction: Direction,
        edge_type: Option<&str>,
    ) -> Vec<(NodeId, &Edge)> {
        let mut results = Vec::new();

        let collect = |edge_ids: &[EdgeId], get_neighbor: fn(&Edge) -> NodeId| {
            edge_ids
                .iter()
                .filter_map(|eid| self.edges.get(eid))
                .filter(|e| edge_type.map_or(true, |t| e.edge_type == t))
                .map(|e| (get_neighbor(e), e))
                .collect::<Vec<_>>()
        };

        if direction == Direction::Outgoing || direction == Direction::Both {
            if let Some(out) = self.outgoing.get(&node_id) {
                results.extend(collect(out, |e| e.to));
            }
        }
        if direction == Direction::Incoming || direction == Direction::Both {
            if let Some(inc) = self.incoming.get(&node_id) {
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

        dist.insert(from, 0.0);
        pq.insert((0u64, from), ());

        while let Some((&(d_bits, current), _)) = pq.iter().next() {
            pq.remove(&(d_bits, current));
            let current_dist = f64::from_bits(d_bits);

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
                    pq.insert((new_dist.to_bits(), neighbor), ());
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
                        if let Some(et) = edge_type {
                            if edge.edge_type != et {
                                continue;
                            }
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
                            if let Some(edge) = self.edges.get(eid) {
                                if let Some(r) = new_ranks.get_mut(&edge.to) {
                                    *r += share;
                                }
                            }
                        }
                    }
                }
            }

            ranks = new_ranks;
        }

        ranks
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

        dist.insert(from, 0.0);
        pq.insert((0u64, from), ());

        while let Some((&(d_bits, current), _)) = pq.iter().next() {
            pq.remove(&(d_bits, current));
            let current_dist = f64::from_bits(d_bits);

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
                    pq.insert((new_dist.to_bits(), edge.target), ());
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
        if let Some(key) = self.make_key(node) {
            if let Some(set) = self.tree.get_mut(&key) {
                set.remove(&node.id);
                if set.is_empty() {
                    self.tree.remove(&key);
                }
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
}
