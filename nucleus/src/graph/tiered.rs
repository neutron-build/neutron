//! Tiered graph store — adjacency always in memory, node/edge properties
//! spill to a cold LSM-tree when memory pressure exceeds a threshold.
//!
//! Design:
//!   - Adjacency lists (outgoing, incoming) ALWAYS stay in memory — just Vec<u64> IDs.
//!   - Label/type indexes ALWAYS stay in memory — just HashSet<u64> IDs.
//!   - Node/edge structure (id, labels, edge_type, from, to) ALWAYS in memory.
//!   - **Properties** spill to cold when the number of hot nodes exceeds `max_hot_nodes`.
//!
//! Key format for cold properties:
//!   - Node properties: `"n:{node_id}"` -> JSON bytes of BTreeMap<String, PropValue>
//!   - Edge properties: `"e:{edge_id}"` -> JSON bytes of BTreeMap<String, PropValue>

use std::collections::{BTreeMap, HashMap, HashSet};

use parking_lot::Mutex;

use super::{Direction, Edge, EdgeId, Node, NodeId, PropValue, Properties};
use crate::storage::lsm::{LsmConfig, LsmTree};

// ============================================================================
// PropValue <-> JSON serialization
// ============================================================================

fn prop_value_to_json(v: &PropValue) -> String {
    match v {
        PropValue::Null => "null".to_string(),
        PropValue::Bool(b) => b.to_string(),
        PropValue::Int(i) => format!("{{\"__int\":{i}}}"),
        PropValue::Float(f) => {
            if f.is_finite() {
                format!("{{\"__float\":{f}}}")
            } else {
                "null".to_string()
            }
        }
        PropValue::Text(s) => format!("\"{}\"", escape_json(s)),
    }
}

fn escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

pub fn properties_to_bytes(props: &Properties) -> Vec<u8> {
    let mut parts = Vec::new();
    for (k, v) in props {
        parts.push(format!("\"{}\":{}", escape_json(k), prop_value_to_json(v)));
    }
    format!("{{{}}}", parts.join(",")).into_bytes()
}

pub fn properties_from_bytes(bytes: &[u8]) -> Option<Properties> {
    let s = std::str::from_utf8(bytes).ok()?;
    parse_properties(s)
}

// Minimal JSON parser for Properties map.
fn parse_properties(s: &str) -> Option<Properties> {
    let s = s.trim();
    let s = s.strip_prefix('{')?.trim();
    let mut map = BTreeMap::new();
    if s.starts_with('}') {
        return Some(map);
    }
    let mut s = s;
    loop {
        let (key, rest) = parse_json_string(s.trim())?;
        let rest = rest.trim().strip_prefix(':')?;
        let (val, rest) = parse_prop_value(rest.trim())?;
        map.insert(key, val);
        s = rest.trim();
        if s.starts_with('}') {
            return Some(map);
        }
        s = s.strip_prefix(',')?.trim();
    }
}

fn parse_prop_value(s: &str) -> Option<(PropValue, &str)> {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix("null") {
        return Some((PropValue::Null, rest));
    }
    if let Some(rest) = s.strip_prefix("true") {
        return Some((PropValue::Bool(true), rest));
    }
    if let Some(rest) = s.strip_prefix("false") {
        return Some((PropValue::Bool(false), rest));
    }
    if s.starts_with('"') {
        let (st, rest) = parse_json_string(s)?;
        return Some((PropValue::Text(st), rest));
    }
    if s.starts_with('{') {
        // Could be __int or __float tagged object.
        return parse_tagged_object(s);
    }
    // Plain number — try int first, then float.
    parse_number_prop(s)
}

fn parse_tagged_object(s: &str) -> Option<(PropValue, &str)> {
    let inner = s.strip_prefix('{')?;
    let inner = inner.trim();
    let (key, rest) = parse_json_string(inner)?;
    let rest = rest.trim().strip_prefix(':')?;
    let rest = rest.trim();

    match key.as_str() {
        "__int" => {
            let (num_str, rest) = take_number(rest)?;
            let rest = rest.trim().strip_prefix('}')?;
            let n: i64 = num_str.parse().ok()?;
            Some((PropValue::Int(n), rest))
        }
        "__float" => {
            let (num_str, rest) = take_number(rest)?;
            let rest = rest.trim().strip_prefix('}')?;
            let f: f64 = num_str.parse().ok()?;
            Some((PropValue::Float(f), rest))
        }
        _ => None,
    }
}

fn take_number(s: &str) -> Option<(String, &str)> {
    let s = s.trim();
    let mut end = 0;
    let bytes = s.as_bytes();
    if end < bytes.len() && bytes[end] == b'-' {
        end += 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end < bytes.len() && bytes[end] == b'.' {
        end += 1;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
    }
    if end < bytes.len() && (bytes[end] == b'e' || bytes[end] == b'E') {
        end += 1;
        if end < bytes.len() && (bytes[end] == b'+' || bytes[end] == b'-') {
            end += 1;
        }
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
    }
    if end == 0 {
        return None;
    }
    Some((s[..end].to_string(), &s[end..]))
}

fn parse_number_prop(s: &str) -> Option<(PropValue, &str)> {
    let (num_str, rest) = take_number(s)?;
    // If it has a dot, it's a float.
    if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
        let f: f64 = num_str.parse().ok()?;
        Some((PropValue::Float(f), rest))
    } else {
        let n: i64 = num_str.parse().ok()?;
        Some((PropValue::Int(n), rest))
    }
}

fn parse_json_string(s: &str) -> Option<(String, &str)> {
    if !s.starts_with('"') {
        return None;
    }
    let s = &s[1..];
    let mut out = String::new();
    let mut chars = s.chars();
    loop {
        match chars.next()? {
            '"' => return Some((out, chars.as_str())),
            '\\' => match chars.next()? {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                '/' => out.push('/'),
                'u' => {
                    let mut hex = String::with_capacity(4);
                    for _ in 0..4 {
                        hex.push(chars.next()?);
                    }
                    let cp = u32::from_str_radix(&hex, 16).ok()?;
                    if let Some(c) = char::from_u32(cp) {
                        out.push(c);
                    }
                }
                _ => return None,
            },
            c => out.push(c),
        }
    }
}

// ============================================================================
// Key helpers
// ============================================================================

fn node_key(id: NodeId) -> Vec<u8> {
    format!("n:{id}").into_bytes()
}

fn edge_key(id: EdgeId) -> Vec<u8> {
    format!("e:{id}").into_bytes()
}

// ============================================================================
// TieredGraphStore
// ============================================================================

/// A lightweight node without properties — used when properties are in cold tier.
struct NodeShell {
    id: NodeId,
    labels: Vec<String>,
    /// `None` means properties are in the cold tier.
    properties: Option<Properties>,
}

/// A lightweight edge without properties — used when properties are in cold tier.
struct EdgeShell {
    id: EdgeId,
    edge_type: String,
    from: NodeId,
    to: NodeId,
    /// `None` means properties are in the cold tier.
    properties: Option<Properties>,
}

/// Graph store with in-memory adjacency and cold-tier property spilling.
///
/// Graph structure (adjacency lists, labels, types) is always in memory for
/// O(1) traversal. Only node/edge properties are evicted to the cold LsmTree
/// when the hot node count exceeds `max_hot_nodes`.
pub struct TieredGraphStore {
    /// Node shells: id -> NodeShell (properties may be None if cold).
    nodes: HashMap<NodeId, NodeShell>,
    /// Edge shells: id -> EdgeShell (properties may be None if cold).
    edges: HashMap<EdgeId, EdgeShell>,
    /// Adjacency: node_id -> outgoing edge IDs.
    outgoing: HashMap<NodeId, Vec<EdgeId>>,
    /// Adjacency: node_id -> incoming edge IDs.
    incoming: HashMap<NodeId, Vec<EdgeId>>,
    /// Label index: label -> set of node IDs.
    label_index: HashMap<String, HashSet<NodeId>>,
    /// Type index: edge_type -> set of edge IDs.
    type_index: HashMap<String, HashSet<EdgeId>>,
    /// Cold tier for property storage.
    property_cold: Mutex<LsmTree>,
    /// Nodes with hot (in-memory) properties.
    hot_node_ids: HashSet<NodeId>,
    /// Edges with hot (in-memory) properties.
    hot_edge_ids: HashSet<EdgeId>,
    max_hot_nodes: usize,
    next_node_id: NodeId,
    next_edge_id: EdgeId,
}

impl TieredGraphStore {
    /// Create a new tiered graph store with an in-memory cold tier (for tests).
    pub fn new(max_hot_nodes: usize) -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            label_index: HashMap::new(),
            type_index: HashMap::new(),
            property_cold: Mutex::new(LsmTree::new(LsmConfig::default())),
            hot_node_ids: HashSet::new(),
            hot_edge_ids: HashSet::new(),
            max_hot_nodes,
            next_node_id: 1,
            next_edge_id: 1,
        }
    }

    /// Open a tiered graph store with a disk-backed cold tier.
    pub fn open(dir: &str, max_hot_nodes: usize) -> Self {
        let cold = LsmTree::open(LsmConfig::default(), std::path::Path::new(dir))
            .unwrap_or_else(|_| LsmTree::new(LsmConfig::default()));
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            label_index: HashMap::new(),
            type_index: HashMap::new(),
            property_cold: Mutex::new(cold),
            hot_node_ids: HashSet::new(),
            hot_edge_ids: HashSet::new(),
            max_hot_nodes,
            next_node_id: 1,
            next_edge_id: 1,
        }
    }

    /// Create a node with labels and properties. Returns the node ID.
    pub fn create_node(&mut self, labels: Vec<String>, properties: Properties) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;

        for label in &labels {
            self.label_index.entry(label.clone()).or_default().insert(id);
        }

        self.nodes.insert(id, NodeShell {
            id,
            labels,
            properties: Some(properties),
        });
        self.hot_node_ids.insert(id);

        self.maybe_evict_properties();
        id
    }

    /// Create an edge between two nodes. Returns the edge ID, or None if nodes don't exist.
    pub fn create_edge(
        &mut self,
        from: NodeId,
        to: NodeId,
        edge_type: &str,
        properties: Properties,
    ) -> Option<EdgeId> {
        if !self.nodes.contains_key(&from) || !self.nodes.contains_key(&to) {
            return None;
        }

        let id = self.next_edge_id;
        self.next_edge_id += 1;

        self.type_index
            .entry(edge_type.to_string())
            .or_default()
            .insert(id);

        self.edges.insert(id, EdgeShell {
            id,
            edge_type: edge_type.to_string(),
            from,
            to,
            properties: Some(properties),
        });
        self.hot_edge_ids.insert(id);
        self.outgoing.entry(from).or_default().push(id);
        self.incoming.entry(to).or_default().push(id);

        Some(id)
    }

    /// Get a full Node by ID. Fetches properties from cold tier if needed.
    pub fn get_node(&self, id: NodeId) -> Option<Node> {
        let shell = self.nodes.get(&id)?;
        let properties = if let Some(ref props) = shell.properties {
            props.clone()
        } else {
            // Fetch from cold tier.
            let cold = self.property_cold.lock();
            cold.get(&node_key(id))
                .and_then(|bytes| properties_from_bytes(&bytes))
                .unwrap_or_default()
        };
        Some(Node {
            id: shell.id,
            labels: shell.labels.clone(),
            properties,
        })
    }

    /// Get a full Edge by ID. Fetches properties from cold tier if needed.
    pub fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        let shell = self.edges.get(&id)?;
        let properties = if let Some(ref props) = shell.properties {
            props.clone()
        } else {
            let cold = self.property_cold.lock();
            cold.get(&edge_key(id))
                .and_then(|bytes| properties_from_bytes(&bytes))
                .unwrap_or_default()
        };
        Some(Edge {
            id: shell.id,
            edge_type: shell.edge_type.clone(),
            from: shell.from,
            to: shell.to,
            properties,
        })
    }

    /// Get neighbors of a node. Adjacency is always in memory; edge properties
    /// are fetched from cold tier if needed.
    pub fn neighbors(
        &self,
        node_id: NodeId,
        direction: Direction,
        edge_type: Option<&str>,
    ) -> Vec<(NodeId, Edge)> {
        let mut results = Vec::new();

        let collect = |edge_ids: &[EdgeId], get_neighbor: fn(&EdgeShell) -> NodeId| -> Vec<(NodeId, Edge)> {
            edge_ids
                .iter()
                .filter_map(|eid| {
                    let shell = self.edges.get(eid)?;
                    if let Some(et) = edge_type
                        && shell.edge_type != et {
                            return None;
                        }
                    let edge = self.get_edge(*eid)?;
                    Some((get_neighbor(shell), edge))
                })
                .collect()
        };

        if (direction == Direction::Outgoing || direction == Direction::Both)
            && let Some(out) = self.outgoing.get(&node_id) {
                results.extend(collect(out, |e| e.to));
            }
        if (direction == Direction::Incoming || direction == Direction::Both)
            && let Some(inc) = self.incoming.get(&node_id) {
                results.extend(collect(inc, |e| e.from));
            }

        results
    }

    /// Delete a node and all its edges. Removes from both tiers.
    pub fn delete_node(&mut self, id: NodeId) -> bool {
        let shell = match self.nodes.remove(&id) {
            Some(s) => s,
            None => return false,
        };

        // Remove from label index.
        for label in &shell.labels {
            if let Some(set) = self.label_index.get_mut(label) {
                set.remove(&id);
            }
        }

        // Remove properties from hot or cold.
        self.hot_node_ids.remove(&id);
        {
            let mut cold = self.property_cold.lock();
            cold.delete(node_key(id));
        }

        // Remove connected edges.
        let out_edges: Vec<EdgeId> = self.outgoing.remove(&id).unwrap_or_default();
        let in_edges: Vec<EdgeId> = self.incoming.remove(&id).unwrap_or_default();

        for eid in out_edges {
            self.remove_edge_internal(eid);
        }
        for eid in in_edges {
            self.remove_edge_internal(eid);
        }

        true
    }

    /// Delete an edge. Removes from both tiers.
    pub fn delete_edge(&mut self, id: EdgeId) -> bool {
        let shell = match self.edges.get(&id) {
            Some(s) => s,
            None => return false,
        };
        let from = shell.from;
        let to = shell.to;

        if let Some(out) = self.outgoing.get_mut(&from) {
            out.retain(|e| *e != id);
        }
        if let Some(inc) = self.incoming.get_mut(&to) {
            inc.retain(|e| *e != id);
        }

        self.remove_edge_internal(id);
        true
    }

    /// Total node count.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Total edge count.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Find nodes by label.
    pub fn nodes_by_label(&self, label: &str) -> Vec<Node> {
        self.label_index
            .get(label)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.get_node(*id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Flush all hot-tier node/edge properties to the cold tier (for persistence).
    /// After calling this, the cold tier's memtable should be flushed to disk.
    pub fn flush_to_cold(&mut self) {
        let mut cold = self.property_cold.lock();
        for (nid, shell) in &mut self.nodes {
            if let Some(props) = shell.properties.take() {
                let bytes = properties_to_bytes(&props);
                cold.put(node_key(*nid), bytes);
            }
        }
        for (eid, shell) in &mut self.edges {
            if let Some(props) = shell.properties.take() {
                let bytes = properties_to_bytes(&props);
                cold.put(edge_key(*eid), bytes);
            }
        }
        self.hot_node_ids.clear();
        self.hot_edge_ids.clear();
        cold.force_flush();
    }

    // ---- Internal helpers ----

    fn remove_edge_internal(&mut self, id: EdgeId) {
        if let Some(shell) = self.edges.remove(&id) {
            if let Some(set) = self.type_index.get_mut(&shell.edge_type) {
                set.remove(&id);
                if set.is_empty() {
                    self.type_index.remove(&shell.edge_type);
                }
            }
            self.hot_edge_ids.remove(&id);
            let mut cold = self.property_cold.lock();
            cold.delete(edge_key(id));
        }
    }

    /// Evict node/edge properties from hot to cold if over capacity.
    fn maybe_evict_properties(&mut self) {
        if self.hot_node_ids.len() <= self.max_hot_nodes {
            return;
        }

        let to_evict = self.hot_node_ids.len() - self.max_hot_nodes;
        let evict_ids: Vec<NodeId> = self.hot_node_ids.iter().copied().take(to_evict).collect();

        let mut cold = self.property_cold.lock();
        for nid in &evict_ids {
            if let Some(shell) = self.nodes.get_mut(nid)
                && let Some(props) = shell.properties.take() {
                    let bytes = properties_to_bytes(&props);
                    cold.put(node_key(*nid), bytes);
                }
            self.hot_node_ids.remove(nid);
        }

        // Also evict some edge properties for nodes being evicted.
        for nid in &evict_ids {
            if let Some(out_eids) = self.outgoing.get(nid) {
                for eid in out_eids.clone() {
                    if self.hot_edge_ids.contains(&eid) {
                        if let Some(eshell) = self.edges.get_mut(&eid)
                            && let Some(props) = eshell.properties.take() {
                                let bytes = properties_to_bytes(&props);
                                cold.put(edge_key(eid), bytes);
                            }
                        self.hot_edge_ids.remove(&eid);
                    }
                }
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, PropValue)]) -> Properties {
        let mut map = BTreeMap::new();
        for (k, v) in pairs {
            map.insert(k.to_string(), v.clone());
        }
        map
    }

    #[test]
    fn test_tiered_graph_create_node() {
        let mut store = TieredGraphStore::new(100);
        let id = store.create_node(
            vec!["Person".to_string()],
            props(&[("name", PropValue::Text("Alice".to_string()))]),
        );
        let node = store.get_node(id).unwrap();
        assert_eq!(node.id, id);
        assert_eq!(node.labels, vec!["Person".to_string()]);
        assert_eq!(
            node.properties.get("name"),
            Some(&PropValue::Text("Alice".to_string()))
        );
    }

    #[test]
    fn test_tiered_graph_create_edge() {
        let mut store = TieredGraphStore::new(100);
        let n1 = store.create_node(vec!["Person".to_string()], props(&[]));
        let n2 = store.create_node(vec!["Person".to_string()], props(&[]));
        let eid = store
            .create_edge(n1, n2, "KNOWS", props(&[("since", PropValue::Int(2020))]))
            .unwrap();
        let edge = store.get_edge(eid).unwrap();
        assert_eq!(edge.from, n1);
        assert_eq!(edge.to, n2);
        assert_eq!(edge.edge_type, "KNOWS");
        assert_eq!(edge.properties.get("since"), Some(&PropValue::Int(2020)));
    }

    #[test]
    fn test_tiered_graph_neighbors() {
        let mut store = TieredGraphStore::new(100);
        let a = store.create_node(vec!["A".to_string()], props(&[]));
        let b = store.create_node(vec!["B".to_string()], props(&[]));
        let c = store.create_node(vec!["C".to_string()], props(&[]));
        store.create_edge(a, b, "LINKS", props(&[]));
        store.create_edge(a, c, "LINKS", props(&[]));

        let neighbors = store.neighbors(a, Direction::Outgoing, None);
        assert_eq!(neighbors.len(), 2);

        let filtered = store.neighbors(a, Direction::Outgoing, Some("LINKS"));
        assert_eq!(filtered.len(), 2);

        let none = store.neighbors(a, Direction::Outgoing, Some("OTHER"));
        assert_eq!(none.len(), 0);
    }

    #[test]
    fn test_tiered_graph_property_eviction() {
        let mut store = TieredGraphStore::new(3);
        // Create 5 nodes — properties of first 2 should be evicted to cold.
        let mut ids = Vec::new();
        for i in 0..5 {
            let id = store.create_node(
                vec!["Node".to_string()],
                props(&[("val", PropValue::Int(i))]),
            );
            ids.push(id);
        }
        assert!(store.hot_node_ids.len() <= 3);

        // All nodes should still be accessible.
        for (i, &id) in ids.iter().enumerate() {
            let node = store.get_node(id).unwrap();
            assert_eq!(node.properties.get("val"), Some(&PropValue::Int(i as i64)));
        }
    }

    #[test]
    fn test_tiered_graph_get_node_cold_properties() {
        let mut store = TieredGraphStore::new(2);
        let n1 = store.create_node(
            vec!["X".to_string()],
            props(&[("key", PropValue::Text("value1".to_string()))]),
        );
        let n2 = store.create_node(
            vec!["X".to_string()],
            props(&[("key", PropValue::Text("value2".to_string()))]),
        );
        let n3 = store.create_node(
            vec!["X".to_string()],
            props(&[("key", PropValue::Text("value3".to_string()))]),
        );

        // At least one node should have been evicted to cold.
        assert!(store.hot_node_ids.len() <= 2);

        // All nodes should still be retrievable with correct properties.
        let node1 = store.get_node(n1).unwrap();
        assert_eq!(
            node1.properties.get("key"),
            Some(&PropValue::Text("value1".to_string()))
        );
        let node2 = store.get_node(n2).unwrap();
        assert_eq!(
            node2.properties.get("key"),
            Some(&PropValue::Text("value2".to_string()))
        );
        let node3 = store.get_node(n3).unwrap();
        assert_eq!(
            node3.properties.get("key"),
            Some(&PropValue::Text("value3".to_string()))
        );
    }

    #[test]
    fn test_tiered_graph_delete_node() {
        let mut store = TieredGraphStore::new(10);
        let n1 = store.create_node(vec!["A".to_string()], props(&[]));
        let n2 = store.create_node(vec!["B".to_string()], props(&[]));
        store.create_edge(n1, n2, "REL", props(&[]));

        assert!(store.delete_node(n1));
        assert!(store.get_node(n1).is_none());
        assert_eq!(store.node_count(), 1);
        assert_eq!(store.edge_count(), 0); // Edge should be removed too.
    }

    #[test]
    fn test_tiered_graph_delete_edge() {
        let mut store = TieredGraphStore::new(10);
        let n1 = store.create_node(vec![], props(&[]));
        let n2 = store.create_node(vec![], props(&[]));
        let eid = store.create_edge(n1, n2, "REL", props(&[])).unwrap();

        assert!(store.delete_edge(eid));
        assert!(store.get_edge(eid).is_none());
        assert_eq!(store.edge_count(), 0);
        // Nodes should still exist.
        assert_eq!(store.node_count(), 2);
    }

    #[test]
    fn test_tiered_graph_traversal_always_works() {
        let mut store = TieredGraphStore::new(2);
        // Create a chain: a -> b -> c -> d -> e.
        let a = store.create_node(vec!["Start".to_string()], props(&[("n", PropValue::Int(0))]));
        let b = store.create_node(vec![], props(&[("n", PropValue::Int(1))]));
        let c = store.create_node(vec![], props(&[("n", PropValue::Int(2))]));
        let d = store.create_node(vec![], props(&[("n", PropValue::Int(3))]));
        let e = store.create_node(vec!["End".to_string()], props(&[("n", PropValue::Int(4))]));

        store.create_edge(a, b, "NEXT", props(&[]));
        store.create_edge(b, c, "NEXT", props(&[]));
        store.create_edge(c, d, "NEXT", props(&[]));
        store.create_edge(d, e, "NEXT", props(&[]));

        // Properties of early nodes are cold, but traversal should still work.
        assert!(store.hot_node_ids.len() <= 2);

        // Traverse the whole chain.
        let mut current = a;
        let mut visited = Vec::new();
        visited.push(current);
        loop {
            let nbrs = store.neighbors(current, Direction::Outgoing, Some("NEXT"));
            if nbrs.is_empty() {
                break;
            }
            current = nbrs[0].0;
            visited.push(current);
        }
        assert_eq!(visited, vec![a, b, c, d, e]);
    }

    #[test]
    fn test_tiered_graph_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        // Create nodes and evict properties to cold (disk).
        let mut node_ids;
        {
            let mut store = TieredGraphStore::open(dir_path, 2);
            node_ids = Vec::new();
            for i in 0..5 {
                let id = store.create_node(
                    vec!["Persist".to_string()],
                    props(&[("idx", PropValue::Int(i))]),
                );
                node_ids.push(id);
            }
            // Flush all hot properties to cold, then cold memtable to disk.
            store.flush_to_cold();
        }

        // Reopen and verify cold properties survive.
        {
            let store = TieredGraphStore::open(dir_path, 2);
            // We can't recover node structure (that's in memory only), but
            // cold property storage should have the data.
            let cold = store.property_cold.lock();
            for &id in &node_ids {
                let bytes = cold.get(&node_key(id));
                assert!(bytes.is_some(), "node {id} properties should persist");
                let parsed = properties_from_bytes(&bytes.unwrap());
                assert!(parsed.is_some());
            }
        }
    }

    #[test]
    fn test_tiered_graph_large_dataset() {
        let mut store = TieredGraphStore::new(500);
        let mut ids = Vec::new();
        for i in 0..5000 {
            let id = store.create_node(
                vec!["N".to_string()],
                props(&[("i", PropValue::Int(i))]),
            );
            ids.push(id);
        }
        assert_eq!(store.node_count(), 5000);
        assert!(store.hot_node_ids.len() <= 500);

        // Create edges between consecutive nodes.
        for w in ids.windows(2) {
            store.create_edge(w[0], w[1], "SEQ", props(&[]));
        }
        assert_eq!(store.edge_count(), 4999);

        // Spot-check cold properties.
        for &i in &[0, 100, 999, 2500, 4999] {
            let node = store.get_node(ids[i as usize]).unwrap();
            assert_eq!(
                node.properties.get("i"),
                Some(&PropValue::Int(i as i64))
            );
        }
    }

    #[test]
    fn test_tiered_graph_edge_not_created_for_missing_nodes() {
        let mut store = TieredGraphStore::new(10);
        let n1 = store.create_node(vec![], props(&[]));
        // Non-existent node 999.
        let result = store.create_edge(n1, 999, "BAD", props(&[]));
        assert!(result.is_none());
    }

    #[test]
    fn test_tiered_graph_nodes_by_label() {
        let mut store = TieredGraphStore::new(100);
        store.create_node(vec!["Cat".to_string()], props(&[]));
        store.create_node(vec!["Dog".to_string()], props(&[]));
        store.create_node(vec!["Cat".to_string(), "Pet".to_string()], props(&[]));

        let cats = store.nodes_by_label("Cat");
        assert_eq!(cats.len(), 2);

        let dogs = store.nodes_by_label("Dog");
        assert_eq!(dogs.len(), 1);
    }

    #[test]
    fn test_tiered_graph_prop_serialization_roundtrip() {
        // Test all PropValue variants survive serialization.
        let original = props(&[
            ("null_val", PropValue::Null),
            ("bool_val", PropValue::Bool(true)),
            ("int_val", PropValue::Int(-42)),
            ("float_val", PropValue::Float(3.14)),
            ("text_val", PropValue::Text("hello \"world\"".to_string())),
        ]);
        let bytes = properties_to_bytes(&original);
        let parsed = properties_from_bytes(&bytes).unwrap();
        assert_eq!(parsed, original);
    }
}
