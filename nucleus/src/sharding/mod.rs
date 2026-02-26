//! Sharding, rebalancing, and geo-fencing for distributed deployment.
//!
//! This module provides the building blocks for distributing data across a cluster
//! of nodes: partition strategies (hash and range), a shard map for key-to-shard
//! routing, cluster topology tracking, automatic rebalancing, consistent hashing
//! for minimal-disruption node changes, and geo-fencing constraints that restrict
//! shard placement by region.

use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Core type aliases
// ---------------------------------------------------------------------------

/// Unique identifier for a shard (partition) of the keyspace.
pub type ShardId = u64;

/// Unique identifier for a cluster node.
pub type NodeId = u64;

// ---------------------------------------------------------------------------
// Region / geo-fencing
// ---------------------------------------------------------------------------

/// Geographic regions for data-residency constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    UsEast,
    UsWest,
    EuWest,
    EuCentral,
    ApSoutheast,
}

/// A constraint that limits which regions may host a given shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeoConstraint {
    /// The set of regions in which this shard is allowed to reside.
    pub allowed_regions: HashSet<Region>,
}

impl GeoConstraint {
    pub fn new(regions: &[Region]) -> Self {
        Self {
            allowed_regions: regions.iter().copied().collect(),
        }
    }

    /// Returns `true` if the given region satisfies this constraint.
    pub fn permits(&self, region: &Region) -> bool {
        self.allowed_regions.contains(region)
    }
}

// ---------------------------------------------------------------------------
// Partition strategy
// ---------------------------------------------------------------------------

/// Strategy used to map keys to shards.
#[derive(Debug, Clone)]
pub enum PartitionStrategy {
    /// Hash-based partitioning into a fixed number of shards (0..num_shards).
    Hash { num_shards: u64 },
    /// Range-based partitioning defined by sorted split points.
    ///
    /// For N split points there are N+1 shards:
    ///   shard 0: (-inf, split[0])
    ///   shard 1: [split[0], split[1])
    ///   ...
    ///   shard N: [split[N-1], +inf)
    Range { split_points: Vec<i64> },
}

// ---------------------------------------------------------------------------
// Shard
// ---------------------------------------------------------------------------

/// A single shard (partition) of the keyspace.
#[derive(Debug, Clone)]
pub struct Shard {
    pub id: ShardId,
    /// Inclusive lower bound of the key range owned by this shard.
    pub start_key: i64,
    /// Exclusive upper bound of the key range owned by this shard.
    pub end_key: i64,
    /// The primary node responsible for this shard.
    pub assigned_node: Option<NodeId>,
    /// Additional nodes holding replicas of this shard.
    pub replica_nodes: Vec<NodeId>,
    /// Estimated number of rows stored in this shard.
    pub row_count: u64,
    /// Estimated size in bytes of data stored in this shard.
    pub size_bytes: u64,
    /// Optional geographic constraint restricting placement.
    pub geo_constraint: Option<GeoConstraint>,
}

impl Shard {
    pub fn new(id: ShardId, start_key: i64, end_key: i64) -> Self {
        Self {
            id,
            start_key,
            end_key,
            assigned_node: None,
            replica_nodes: Vec::new(),
            row_count: 0,
            size_bytes: 0,
            geo_constraint: None,
        }
    }
}

// ---------------------------------------------------------------------------
// ShardMap
// ---------------------------------------------------------------------------

/// Maps shard IDs to [`Shard`] instances and routes keys to the correct shard.
#[derive(Debug)]
pub struct ShardMap {
    shards: HashMap<ShardId, Shard>,
    strategy: PartitionStrategy,
}

impl ShardMap {
    /// Create a new `ShardMap` and pre-populate shards according to the strategy.
    pub fn new(strategy: PartitionStrategy) -> Self {
        let mut shards = HashMap::new();
        match &strategy {
            PartitionStrategy::Hash { num_shards } => {
                let range_size = if *num_shards == 0 {
                    0
                } else {
                    u64::MAX / num_shards
                };
                for i in 0..*num_shards {
                    let start = (i as i64).wrapping_mul(range_size as i64);
                    let end = if i == num_shards - 1 {
                        i64::MAX
                    } else {
                        ((i + 1) as i64).wrapping_mul(range_size as i64)
                    };
                    shards.insert(i, Shard::new(i, start, end));
                }
            }
            PartitionStrategy::Range { split_points } => {
                let n = split_points.len() + 1;
                for i in 0..n {
                    let start = if i == 0 { i64::MIN } else { split_points[i - 1] };
                    let end = if i == split_points.len() {
                        i64::MAX
                    } else {
                        split_points[i]
                    };
                    shards.insert(i as ShardId, Shard::new(i as ShardId, start, end));
                }
            }
        }
        Self { shards, strategy }
    }

    /// Determine which shard a given key belongs to.
    pub fn assign_key(&self, key: i64) -> ShardId {
        match &self.strategy {
            PartitionStrategy::Hash { num_shards } => {
                if *num_shards == 0 {
                    return 0;
                }
                let hash = Self::hash_key(key);
                hash % num_shards
            }
            PartitionStrategy::Range { split_points } => {
                // Binary search: find the first split point > key.
                let mut lo: usize = 0;
                let mut hi: usize = split_points.len();
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    if split_points[mid] <= key {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                lo as ShardId
            }
        }
    }

    /// Look up a shard by ID.
    pub fn get_shard(&self, id: ShardId) -> Option<&Shard> {
        self.shards.get(&id)
    }

    /// Mutable look-up (used internally for rebalancing / assignment).
    pub fn get_shard_mut(&mut self, id: ShardId) -> Option<&mut Shard> {
        self.shards.get_mut(&id)
    }

    /// Return all shard IDs (primary or replica) assigned to the given node.
    pub fn shards_for_node(&self, node: NodeId) -> Vec<ShardId> {
        self.shards
            .values()
            .filter(|s| {
                s.assigned_node == Some(node) || s.replica_nodes.contains(&node)
            })
            .map(|s| s.id)
            .collect()
    }

    /// Return an iterator over all shards.
    pub fn all_shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.values()
    }

    /// Total number of shards.
    pub fn len(&self) -> usize {
        self.shards.len()
    }

    /// Whether the shard map is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    // Simple deterministic hash (FNV-1a-inspired) to avoid external deps.
    fn hash_key(key: i64) -> u64 {
        let bytes = key.to_le_bytes();
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in &bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }
}

// ---------------------------------------------------------------------------
// Cluster topology
// ---------------------------------------------------------------------------

/// Metadata about a single cluster node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    pub address: String,
    pub is_alive: bool,
    pub shard_count: u64,
    pub region: Option<Region>,
}

/// Tracks the set of nodes in the cluster.
#[derive(Debug)]
pub struct ClusterTopology {
    nodes: HashMap<NodeId, NodeInfo>,
}

impl ClusterTopology {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Add a node to the topology.
    pub fn add_node(&mut self, id: NodeId, address: &str) {
        self.nodes.insert(
            id,
            NodeInfo {
                id,
                address: address.to_string(),
                is_alive: true,
                shard_count: 0,
                region: None,
            },
        );
    }

    /// Add a node with a geographic region tag.
    pub fn add_node_with_region(&mut self, id: NodeId, address: &str, region: Region) {
        self.nodes.insert(
            id,
            NodeInfo {
                id,
                address: address.to_string(),
                is_alive: true,
                shard_count: 0,
                region: Some(region),
            },
        );
    }

    /// Remove a node entirely.
    pub fn remove_node(&mut self, id: NodeId) {
        self.nodes.remove(&id);
    }

    /// Mark a node as dead (failed / unreachable).
    pub fn mark_dead(&mut self, id: NodeId) {
        if let Some(info) = self.nodes.get_mut(&id) {
            info.is_alive = false;
        }
    }

    /// Mark a node as alive (recovered / reachable).
    pub fn mark_alive(&mut self, id: NodeId) {
        if let Some(info) = self.nodes.get_mut(&id) {
            info.is_alive = true;
        }
    }

    /// Return IDs of all nodes currently alive.
    pub fn alive_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.is_alive)
            .map(|n| n.id)
            .collect()
    }

    /// Look up a node by ID.
    pub fn get_node(&self, id: NodeId) -> Option<&NodeInfo> {
        self.nodes.get(&id)
    }
}

impl Default for ClusterTopology {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Rebalancer
// ---------------------------------------------------------------------------

/// A single shard move in a rebalancing plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMove {
    pub shard_id: ShardId,
    pub from_node: NodeId,
    pub to_node: NodeId,
}

/// A complete rebalancing plan: an ordered list of shard moves.
#[derive(Debug, Clone)]
pub struct RebalancePlan {
    pub moves: Vec<ShardMove>,
}

/// Computes and applies rebalancing plans.
pub struct Rebalancer;

impl Rebalancer {
    /// Compute a plan that distributes primary-shard ownership evenly across
    /// alive nodes using a greedy approach. Shards with geo-constraints are
    /// only placed on nodes whose region is in the allowed set.
    ///
    /// Shards currently assigned to dead nodes are forcibly moved first, then
    /// a balancing pass moves shards from the most-loaded to the least-loaded
    /// node until the difference is at most 1.
    pub fn compute_plan(shard_map: &ShardMap, topology: &ClusterTopology) -> RebalancePlan {
        let alive: Vec<NodeId> = {
            let mut v = topology.alive_nodes();
            v.sort();
            v
        };

        if alive.is_empty() {
            return RebalancePlan { moves: Vec::new() };
        }

        // Build current assignment: node -> set of shard ids.
        let mut assignment: HashMap<NodeId, Vec<ShardId>> = HashMap::new();
        for &nid in &alive {
            assignment.insert(nid, Vec::new());
        }

        let mut moves: Vec<ShardMove> = Vec::new();

        // Collect all shards sorted by id for determinism.
        let mut all_shards: Vec<&Shard> = shard_map.all_shards().collect();
        all_shards.sort_by_key(|s| s.id);

        // Phase 1: ensure every shard is on an alive, geo-valid node.
        for shard in &all_shards {
            let current = shard.assigned_node;
            let on_alive = current.map_or(false, |n| alive.contains(&n));
            let geo_ok = current.map_or(false, |n| {
                Self::geo_check(shard, n, topology)
            });

            if on_alive && geo_ok {
                assignment.get_mut(&current.unwrap()).unwrap().push(shard.id);
            } else {
                // Pick the least-loaded alive node that satisfies geo.
                let target = Self::pick_least_loaded(&assignment, &alive, shard, topology);
                if let Some(target_node) = target {
                    if let Some(from) = current {
                        moves.push(ShardMove {
                            shard_id: shard.id,
                            from_node: from,
                            to_node: target_node,
                        });
                    }
                    // Even if there was no previous node we still record it in
                    // the assignment map so the balancing phase sees it.
                    assignment.get_mut(&target_node).unwrap().push(shard.id);
                }
            }
        }

        // Phase 2: greedy balancing — move from most-loaded to least-loaded.
        loop {
            // Recompute loads.
            let (max_node, max_count) = assignment
                .iter()
                .max_by_key(|(_, v)| v.len())
                .map(|(&k, v)| (k, v.len()))
                .unwrap();
            let (min_node, min_count) = assignment
                .iter()
                .min_by_key(|(_, v)| v.len())
                .map(|(&k, v)| (k, v.len()))
                .unwrap();

            if max_count <= min_count + 1 {
                break; // balanced enough
            }

            // Find a shard on max_node that can move to min_node (geo-ok).
            let movable = {
                let shards_on_max = &assignment[&max_node];
                shards_on_max.iter().rev().find(|&&sid| {
                    if let Some(s) = shard_map.get_shard(sid) {
                        Self::geo_check(s, min_node, topology)
                    } else {
                        false
                    }
                }).copied()
            };

            if let Some(sid) = movable {
                moves.push(ShardMove {
                    shard_id: sid,
                    from_node: max_node,
                    to_node: min_node,
                });
                assignment.get_mut(&max_node).unwrap().retain(|&s| s != sid);
                assignment.get_mut(&min_node).unwrap().push(sid);
            } else {
                break; // cannot move anything due to constraints
            }
        }

        RebalancePlan { moves }
    }

    /// Apply a computed plan to the shard map by updating each shard's
    /// `assigned_node`.
    pub fn apply_plan(plan: &RebalancePlan, shard_map: &mut ShardMap) {
        for m in &plan.moves {
            if let Some(shard) = shard_map.get_shard_mut(m.shard_id) {
                shard.assigned_node = Some(m.to_node);
            }
        }
    }

    // -- helpers --

    fn geo_check(shard: &Shard, node: NodeId, topology: &ClusterTopology) -> bool {
        match (&shard.geo_constraint, topology.get_node(node)) {
            (Some(gc), Some(info)) => match &info.region {
                Some(r) => gc.permits(r),
                None => false, // node has no region tag -> cannot satisfy constraint
            },
            (Some(_), None) => false,
            (None, _) => true, // no constraint -> any node is fine
        }
    }

    fn pick_least_loaded(
        assignment: &HashMap<NodeId, Vec<ShardId>>,
        alive: &[NodeId],
        shard: &Shard,
        topology: &ClusterTopology,
    ) -> Option<NodeId> {
        alive
            .iter()
            .filter(|&&n| Self::geo_check(shard, n, topology))
            .min_by_key(|&&n| assignment.get(&n).map_or(0, |v| v.len()))
            .copied()
    }
}

// ---------------------------------------------------------------------------
// Consistent hashing ring
// ---------------------------------------------------------------------------

/// A position on the hash ring (0..u64::MAX).
type RingPosition = u64;

/// A virtual node on the consistent-hashing ring.
#[derive(Debug, Clone)]
struct VirtualNode {
    position: RingPosition,
    node_id: NodeId,
}

/// Consistent-hashing ring using virtual nodes for minimal disruption when
/// nodes are added or removed.
#[derive(Debug)]
pub struct HashRing {
    vnodes: Vec<VirtualNode>,
    replicas_per_node: usize,
}

impl HashRing {
    /// Create a new ring.  `replicas_per_node` controls how many virtual nodes
    /// each physical node gets (higher = more even distribution).
    pub fn new(replicas_per_node: usize) -> Self {
        Self {
            vnodes: Vec::new(),
            replicas_per_node,
        }
    }

    /// Add a physical node (and its virtual nodes) to the ring.
    pub fn add_node(&mut self, node: NodeId) {
        for i in 0..self.replicas_per_node {
            let pos = Self::hash_pair(node, i as u64);
            self.vnodes.push(VirtualNode {
                position: pos,
                node_id: node,
            });
        }
        self.vnodes.sort_by_key(|v| v.position);
    }

    /// Remove a physical node (and all its virtual nodes) from the ring.
    pub fn remove_node(&mut self, node: NodeId) {
        self.vnodes.retain(|v| v.node_id != node);
    }

    /// Find the node responsible for the given key.  Walks clockwise from the
    /// key's hash position to the first virtual node.
    pub fn get_node(&self, key: i64) -> Option<NodeId> {
        if self.vnodes.is_empty() {
            return None;
        }
        let hash = ShardMap::hash_key(key);
        // Binary search for the first vnode with position >= hash.
        let idx = match self.vnodes.binary_search_by_key(&hash, |v| v.position) {
            Ok(i) => i,
            Err(i) => {
                if i == self.vnodes.len() {
                    0 // wrap around
                } else {
                    i
                }
            }
        };
        Some(self.vnodes[idx].node_id)
    }

    /// Return the `n` distinct nodes responsible for the key (for replication).
    /// Walks clockwise, skipping duplicate physical nodes.
    pub fn get_nodes(&self, key: i64, n: usize) -> Vec<NodeId> {
        if self.vnodes.is_empty() {
            return Vec::new();
        }
        let hash = ShardMap::hash_key(key);
        let start = match self.vnodes.binary_search_by_key(&hash, |v| v.position) {
            Ok(i) => i,
            Err(i) => {
                if i == self.vnodes.len() {
                    0
                } else {
                    i
                }
            }
        };

        let mut result = Vec::new();
        let mut seen = HashSet::new();
        let len = self.vnodes.len();
        for offset in 0..len {
            let vn = &self.vnodes[(start + offset) % len];
            if seen.insert(vn.node_id) {
                result.push(vn.node_id);
                if result.len() == n {
                    break;
                }
            }
        }
        result
    }

    // FNV-1a hash of (node_id, replica_index).
    fn hash_pair(a: u64, b: u64) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &byte in a.to_le_bytes().iter().chain(b.to_le_bytes().iter()) {
            h ^= byte as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }
}

// ---------------------------------------------------------------------------
// Shard-aware query routing (3.4)
// ---------------------------------------------------------------------------

/// A fully resolved query route: identifies which shard holds the data,
/// which node is responsible, and whether that node is the local process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRoute {
    pub shard_id: ShardId,
    pub node_id: NodeId,
    pub is_local: bool,
}

/// Routes string keys to (shard, node) pairs using a [`ShardMap`] for
/// key-to-shard mapping and a [`HashRing`] (built from the cluster topology)
/// for shard-to-node mapping.
///
/// When a shard has an explicitly `assigned_node` in the shard map, that node
/// is preferred. Otherwise the hash ring is consulted as a fallback.
#[derive(Debug)]
pub struct ShardRouter<'a> {
    topology: &'a ClusterTopology,
    shard_map: ShardMap,
    ring: HashRing,
}

impl<'a> ShardRouter<'a> {
    /// Create a router from a cluster topology.
    ///
    /// A hash-based [`ShardMap`] is constructed with one shard per alive node
    /// (minimum 1), and a [`HashRing`] is populated with all alive nodes.
    pub fn new(topology: &'a ClusterTopology) -> Self {
        let alive = topology.alive_nodes();
        let num_shards = if alive.is_empty() { 1 } else { alive.len() as u64 };

        let mut shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards });

        // Assign each shard to a node round-robin.
        if !alive.is_empty() {
            let mut sorted_alive = alive.clone();
            sorted_alive.sort();
            for id in 0..num_shards {
                let node = sorted_alive[id as usize % sorted_alive.len()];
                shard_map.get_shard_mut(id).unwrap().assigned_node = Some(node);
            }
        }

        let mut ring = HashRing::new(150);
        let mut sorted_alive = alive;
        sorted_alive.sort();
        for &nid in &sorted_alive {
            ring.add_node(nid);
        }

        Self {
            topology,
            shard_map,
            ring,
        }
    }

    /// Route a string key to its (shard, node) pair.
    ///
    /// Returns `None` if there are no alive nodes in the topology.
    pub fn route_key(&self, key: &str) -> Option<(ShardId, NodeId)> {
        let hash = Self::hash_str(key);
        let shard_id = self.shard_map.assign_key(hash);

        // Prefer the shard's assigned node; fall back to the hash ring.
        let node_id = self
            .shard_map
            .get_shard(shard_id)
            .and_then(|s| s.assigned_node)
            .or_else(|| self.ring.get_node(hash))?;

        Some((shard_id, node_id))
    }

    /// Build a full [`QueryRoute`] for the given table and key.
    ///
    /// The `table` name is combined with the key to produce the hash, so
    /// different tables with the same key can land on different shards.
    /// `is_local` is always `false` because `ShardRouter` has no concept of a
    /// local node -- use [`ShardAwareRouter`] for local-awareness.
    pub fn route_query(&self, table: &str, key: &str) -> QueryRoute {
        let combined = format!("{table}:{key}");
        let hash = Self::hash_str(&combined);
        let shard_id = self.shard_map.assign_key(hash);

        let node_id = self
            .shard_map
            .get_shard(shard_id)
            .and_then(|s| s.assigned_node)
            .or_else(|| self.ring.get_node(hash))
            .unwrap_or(0);

        QueryRoute {
            shard_id,
            node_id,
            is_local: false,
        }
    }

    /// For scatter-gather queries that must touch every shard, return the
    /// deduplicated list of alive nodes (sorted for determinism).
    pub fn scatter_gather_nodes(&self) -> Vec<NodeId> {
        let mut nodes = self.topology.alive_nodes();
        nodes.sort();
        nodes.dedup();
        nodes
    }

    // FNV-1a hash of a byte string, returning i64 for compatibility with
    // `ShardMap::assign_key`.
    fn hash_str(s: &str) -> i64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in s.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h as i64
    }
}

/// A higher-level router that wraps [`ShardRouter`] and knows the local
/// node's identity, so it can mark routes as local or remote.
#[derive(Debug)]
pub struct ShardAwareRouter<'a> {
    router: ShardRouter<'a>,
    local_node: NodeId,
}

impl<'a> ShardAwareRouter<'a> {
    /// Create a shard-aware router that knows which node is local.
    pub fn new(topology: &'a ClusterTopology, local_node: NodeId) -> Self {
        Self {
            router: ShardRouter::new(topology),
            local_node,
        }
    }

    /// Route a string key, automatically setting `is_local` based on
    /// whether the owning node matches `local_node`.
    pub fn route(&self, key: &str) -> QueryRoute {
        match self.router.route_key(key) {
            Some((shard_id, node_id)) => QueryRoute {
                shard_id,
                node_id,
                is_local: node_id == self.local_node,
            },
            None => QueryRoute {
                shard_id: 0,
                node_id: self.local_node,
                is_local: true,
            },
        }
    }

    /// Returns `true` if the given key is owned by the local node.
    pub fn is_local(&self, key: &str) -> bool {
        self.route(key).is_local
    }

    /// Return all alive nodes *except* the local node (sorted).
    pub fn remote_nodes(&self) -> Vec<NodeId> {
        self.router
            .scatter_gather_nodes()
            .into_iter()
            .filter(|&n| n != self.local_node)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Rebalance Executor (3.5)
// ---------------------------------------------------------------------------

/// State of a single shard transfer during rebalancing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferState {
    Pending,
    Transferring { progress_pct: u8 },
    Completed,
    Failed { reason: String },
}

/// Tracks the execution of a rebalancing plan.
#[derive(Debug)]
pub struct RebalanceExecution {
    plan: RebalancePlan,
    states: Vec<TransferState>,
    started: bool,
    completed_count: usize,
    failed_count: usize,
}

impl RebalanceExecution {
    /// Create a new execution tracker for the given plan.
    pub fn new(plan: RebalancePlan) -> Self {
        let count = plan.moves.len();
        Self {
            plan,
            states: vec![TransferState::Pending; count],
            started: false,
            completed_count: 0,
            failed_count: 0,
        }
    }

    /// Start the execution. Returns the list of moves to process.
    pub fn start(&mut self) -> &[ShardMove] {
        self.started = true;
        &self.plan.moves
    }

    /// Mark a specific move as in-progress with a completion percentage.
    pub fn update_progress(&mut self, index: usize, pct: u8) {
        if index < self.states.len() {
            self.states[index] = TransferState::Transferring {
                progress_pct: pct.min(100),
            };
        }
    }

    /// Mark a specific move as successfully completed.
    pub fn complete_move(&mut self, index: usize) {
        if index < self.states.len() {
            self.states[index] = TransferState::Completed;
            self.completed_count += 1;
        }
    }

    /// Mark a specific move as failed.
    pub fn fail_move(&mut self, index: usize, reason: &str) {
        if index < self.states.len() {
            self.states[index] = TransferState::Failed {
                reason: reason.to_string(),
            };
            self.failed_count += 1;
        }
    }

    /// Whether all moves have completed (successfully or failed).
    pub fn is_finished(&self) -> bool {
        self.completed_count + self.failed_count == self.plan.moves.len()
    }

    /// Whether the execution has been started.
    pub fn is_started(&self) -> bool {
        self.started
    }

    /// Total number of moves in the plan.
    pub fn total_moves(&self) -> usize {
        self.plan.moves.len()
    }

    /// Number of successfully completed moves.
    pub fn completed_count(&self) -> usize {
        self.completed_count
    }

    /// Number of failed moves.
    pub fn failed_count(&self) -> usize {
        self.failed_count
    }

    /// Number of pending moves.
    pub fn pending_count(&self) -> usize {
        self.plan.moves.len() - self.completed_count - self.failed_count
    }

    /// Get the state of a specific move.
    pub fn state(&self, index: usize) -> Option<&TransferState> {
        self.states.get(index)
    }

    /// Overall progress as a percentage.
    pub fn progress_pct(&self) -> u8 {
        if self.plan.moves.is_empty() {
            return 100;
        }
        ((self.completed_count * 100) / self.plan.moves.len()) as u8
    }

    /// Apply all completed moves to the shard map.
    pub fn apply_completed(&self, shard_map: &mut ShardMap) {
        for (i, m) in self.plan.moves.iter().enumerate() {
            if self.states[i] == TransferState::Completed {
                if let Some(shard) = shard_map.get_shard_mut(m.shard_id) {
                    shard.assigned_node = Some(m.to_node);
                }
            }
        }
    }

    /// Get the underlying plan.
    pub fn plan(&self) -> &RebalancePlan { &self.plan }
}

// ============================================================================
// Gap 12: Distributed Table Engine — sharded tables with query routing
// ============================================================================

/// Definition of a distributed (sharded) table.
#[derive(Debug, Clone)]
pub struct DistributedTableDef {
    /// Logical table name.
    pub name: String,
    /// Column used as the shard key.
    pub shard_key: String,
    /// Shard IDs this table is distributed across.
    pub shard_ids: Vec<ShardId>,
    /// Total number of rows (approximate, for planner).
    pub approx_rows: u64,
}

/// Result from a partial aggregation on a single shard.
#[derive(Debug, Clone)]
pub struct PartialAggResult {
    pub shard_id: ShardId,
    /// Partial count.
    pub count: u64,
    /// Partial sum.
    pub sum: f64,
    /// Partial min.
    pub min: Option<f64>,
    /// Partial max.
    pub max: Option<f64>,
}

/// A query routing plan for distributed tables.
#[derive(Debug, Clone)]
pub enum QueryPlan {
    /// Route to a single shard (point query on shard key).
    SingleShard(ShardId),
    /// Scatter to all shards (full table scan / range query).
    Scatter(Vec<ShardId>),
    /// Scatter + partial aggregation on each shard.
    ScatterGather {
        shards: Vec<ShardId>,
        agg_type: AggType,
    },
}

/// Aggregation type for scatter-gather queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggType {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// Distributed table engine: manages sharded tables with query routing.
#[derive(Debug)]
pub struct DistributedTableEngine {
    tables: HashMap<String, DistributedTableDef>,
    shard_map: ShardMap,
}

impl DistributedTableEngine {
    pub fn new(shard_map: ShardMap) -> Self {
        DistributedTableEngine {
            tables: HashMap::new(),
            shard_map,
        }
    }

    /// Create a distributed table.
    pub fn create_table(&mut self, name: &str, shard_key: &str) {
        let shard_ids: Vec<ShardId> = self.shard_map.all_shards().map(|s| s.id).collect();
        let def = DistributedTableDef {
            name: name.to_string(),
            shard_key: shard_key.to_string(),
            shard_ids,
            approx_rows: 0,
        };
        self.tables.insert(name.to_string(), def);
    }

    /// Route a point query to the correct shard.
    pub fn route_point_query(&self, table: &str, shard_key_value: i64) -> Option<QueryPlan> {
        let _def = self.tables.get(table)?;
        let shard_id = self.shard_map.assign_key(shard_key_value);
        Some(QueryPlan::SingleShard(shard_id))
    }

    /// Route a full scan to all shards.
    pub fn route_scan(&self, table: &str) -> Option<QueryPlan> {
        let def = self.tables.get(table)?;
        Some(QueryPlan::Scatter(def.shard_ids.clone()))
    }

    /// Route an aggregation query (scatter-gather).
    pub fn route_aggregation(
        &self,
        table: &str,
        agg_type: AggType,
    ) -> Option<QueryPlan> {
        let def = self.tables.get(table)?;
        Some(QueryPlan::ScatterGather {
            shards: def.shard_ids.clone(),
            agg_type,
        })
    }

    /// Merge partial aggregation results from multiple shards.
    pub fn merge_partial_aggs(
        results: &[PartialAggResult],
        agg_type: AggType,
    ) -> f64 {
        match agg_type {
            AggType::Count => results.iter().map(|r| r.count as f64).sum(),
            AggType::Sum => results.iter().map(|r| r.sum).sum(),
            AggType::Min => results
                .iter()
                .filter_map(|r| r.min)
                .fold(f64::INFINITY, f64::min),
            AggType::Max => results
                .iter()
                .filter_map(|r| r.max)
                .fold(f64::NEG_INFINITY, f64::max),
            AggType::Avg => {
                let total_count: u64 = results.iter().map(|r| r.count).sum();
                let total_sum: f64 = results.iter().map(|r| r.sum).sum();
                if total_count == 0 {
                    0.0
                } else {
                    total_sum / total_count as f64
                }
            }
        }
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn get_table(&self, name: &str) -> Option<&DistributedTableDef> {
        self.tables.get(name)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- 1. Hash partitioning key assignment --

    #[test]
    fn test_hash_partitioning_key_assignment() {
        let shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 8 });
        assert_eq!(shard_map.len(), 8);

        // Every key must map to a valid shard id in 0..8.
        for key in -500..500 {
            let sid = shard_map.assign_key(key);
            assert!(sid < 8, "shard id {sid} out of range for key {key}");
        }

        // Determinism: same key always maps to same shard.
        let a = shard_map.assign_key(42);
        let b = shard_map.assign_key(42);
        assert_eq!(a, b);

        // Different keys should (in general) spread across shards.
        let mut seen: HashSet<ShardId> = HashSet::new();
        for key in 0..100 {
            seen.insert(shard_map.assign_key(key));
        }
        // With 8 shards and 100 distinct keys we expect most shards hit.
        assert!(seen.len() >= 4, "poor distribution: only {} shards used", seen.len());
    }

    // -- 2. Range partitioning --

    #[test]
    fn test_range_partitioning() {
        // Split points: 0, 100, 200  -> shards 0..3
        let shard_map = ShardMap::new(PartitionStrategy::Range {
            split_points: vec![0, 100, 200],
        });
        assert_eq!(shard_map.len(), 4);

        // key < 0 -> shard 0
        assert_eq!(shard_map.assign_key(-50), 0);
        assert_eq!(shard_map.assign_key(-1), 0);

        // 0 <= key < 100 -> shard 1
        assert_eq!(shard_map.assign_key(0), 1);
        assert_eq!(shard_map.assign_key(50), 1);
        assert_eq!(shard_map.assign_key(99), 1);

        // 100 <= key < 200 -> shard 2
        assert_eq!(shard_map.assign_key(100), 2);
        assert_eq!(shard_map.assign_key(150), 2);

        // key >= 200 -> shard 3
        assert_eq!(shard_map.assign_key(200), 3);
        assert_eq!(shard_map.assign_key(999), 3);
    }

    // -- 3. Shard assignment to nodes --

    #[test]
    fn test_shard_node_assignment() {
        let mut shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 4 });

        // Assign shards to nodes.
        shard_map.get_shard_mut(0).unwrap().assigned_node = Some(1);
        shard_map.get_shard_mut(1).unwrap().assigned_node = Some(2);
        shard_map.get_shard_mut(2).unwrap().assigned_node = Some(1);
        shard_map.get_shard_mut(3).unwrap().assigned_node = Some(3);

        // Also add a replica.
        shard_map.get_shard_mut(3).unwrap().replica_nodes.push(1);

        let mut node1_shards = shard_map.shards_for_node(1);
        node1_shards.sort();
        assert_eq!(node1_shards, vec![0, 2, 3]); // primary 0,2 + replica 3

        let node2_shards = shard_map.shards_for_node(2);
        assert_eq!(node2_shards, vec![1]);

        let node4_shards = shard_map.shards_for_node(4);
        assert!(node4_shards.is_empty());
    }

    // -- 4. Rebalancing plan generation --

    #[test]
    fn test_rebalance_plan_generation() {
        let mut shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 6 });

        // Put all 6 shards on node 1.
        for id in 0..6 {
            shard_map.get_shard_mut(id).unwrap().assigned_node = Some(1);
        }

        let mut topo = ClusterTopology::new();
        topo.add_node(1, "node1:5000");
        topo.add_node(2, "node2:5000");
        topo.add_node(3, "node3:5000");

        let plan = Rebalancer::compute_plan(&shard_map, &topo);

        // With 6 shards and 3 nodes, each node should end up with 2.
        // So we expect 4 moves (move 4 shards away from node 1).
        assert_eq!(plan.moves.len(), 4);

        // Apply and verify.
        Rebalancer::apply_plan(&plan, &mut shard_map);

        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        for shard in shard_map.all_shards() {
            if let Some(n) = shard.assigned_node {
                *counts.entry(n).or_default() += 1;
            }
        }
        assert_eq!(counts.get(&1), Some(&2));
        assert_eq!(counts.get(&2), Some(&2));
        assert_eq!(counts.get(&3), Some(&2));
    }

    // -- 5. Node failure (mark dead + rebalance) --

    #[test]
    fn test_node_failure_rebalance() {
        let mut shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 4 });

        // Distribute evenly: node 1 gets shards 0,1; node 2 gets shards 2,3.
        shard_map.get_shard_mut(0).unwrap().assigned_node = Some(1);
        shard_map.get_shard_mut(1).unwrap().assigned_node = Some(1);
        shard_map.get_shard_mut(2).unwrap().assigned_node = Some(2);
        shard_map.get_shard_mut(3).unwrap().assigned_node = Some(2);

        let mut topo = ClusterTopology::new();
        topo.add_node(1, "node1:5000");
        topo.add_node(2, "node2:5000");
        topo.add_node(3, "node3:5000");

        // Kill node 2.
        topo.mark_dead(2);
        assert!(!topo.alive_nodes().contains(&2));

        let plan = Rebalancer::compute_plan(&shard_map, &topo);
        Rebalancer::apply_plan(&plan, &mut shard_map);

        // After rebalance, no shard should be on node 2.
        for shard in shard_map.all_shards() {
            assert_ne!(shard.assigned_node, Some(2));
        }

        // Shards should be spread across nodes 1 and 3.
        let on_1 = shard_map.shards_for_node(1).len();
        let on_3 = shard_map.shards_for_node(3).len();
        assert_eq!(on_1 + on_3, 4);
        // Difference should be at most 1.
        assert!((on_1 as i64 - on_3 as i64).unsigned_abs() <= 1);
    }

    // -- 6. Geo-constraint enforcement --

    #[test]
    fn test_geo_constraint_enforcement() {
        let mut shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 2 });

        // Shard 0: must stay in EU.
        shard_map.get_shard_mut(0).unwrap().geo_constraint =
            Some(GeoConstraint::new(&[Region::EuWest, Region::EuCentral]));
        shard_map.get_shard_mut(0).unwrap().assigned_node = Some(10);

        // Shard 1: no constraint.
        shard_map.get_shard_mut(1).unwrap().assigned_node = Some(10);

        let mut topo = ClusterTopology::new();
        topo.add_node_with_region(10, "us-east-1:5000", Region::UsEast);
        topo.add_node_with_region(20, "eu-west-1:5000", Region::EuWest);
        topo.add_node_with_region(30, "eu-central-1:5000", Region::EuCentral);

        let plan = Rebalancer::compute_plan(&shard_map, &topo);
        Rebalancer::apply_plan(&plan, &mut shard_map);

        // Shard 0 must be on an EU node.
        let s0_node = shard_map.get_shard(0).unwrap().assigned_node.unwrap();
        let s0_region = topo.get_node(s0_node).unwrap().region.as_ref().unwrap();
        assert!(
            *s0_region == Region::EuWest || *s0_region == Region::EuCentral,
            "shard 0 placed in {:?}, violates EU constraint",
            s0_region
        );
    }

    // -- 7. Consistent hashing --

    #[test]
    fn test_consistent_hashing() {
        let mut ring = HashRing::new(150);
        ring.add_node(1);
        ring.add_node(2);
        ring.add_node(3);

        // Every key should resolve to a node.
        for key in 0..200 {
            assert!(ring.get_node(key).is_some());
        }

        // Record assignments before adding a node.
        let before: Vec<NodeId> = (0..1000)
            .map(|k| ring.get_node(k).unwrap())
            .collect();

        // Add a fourth node.
        ring.add_node(4);

        let after: Vec<NodeId> = (0..1000)
            .map(|k| ring.get_node(k).unwrap())
            .collect();

        // Count how many keys moved.
        let moved = before
            .iter()
            .zip(after.iter())
            .filter(|(a, b)| a != b)
            .count();

        // Ideally ~25% of keys move when going from 3 to 4 nodes.
        // With our simple FNV hash the distribution isn't perfect, so allow up to 85%.
        assert!(
            moved < 850,
            "too many keys moved ({moved}/1000); consistent hashing should minimise disruption"
        );

        // get_nodes should return distinct nodes.
        let replicas = ring.get_nodes(42, 3);
        assert_eq!(replicas.len(), 3);
        let unique: HashSet<_> = replicas.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    // -- Additional edge-case tests --

    #[test]
    fn test_cluster_topology_lifecycle() {
        let mut topo = ClusterTopology::new();
        topo.add_node(1, "a:1");
        topo.add_node(2, "b:2");
        assert_eq!(topo.alive_nodes().len(), 2);

        topo.mark_dead(1);
        let alive = topo.alive_nodes();
        assert_eq!(alive, vec![2]);

        topo.mark_alive(1);
        assert_eq!(topo.alive_nodes().len(), 2);

        topo.remove_node(2);
        assert_eq!(topo.alive_nodes(), vec![1]);
    }

    #[test]
    fn test_hash_ring_remove_node() {
        let mut ring = HashRing::new(100);
        ring.add_node(1);
        ring.add_node(2);

        let before: Vec<NodeId> = (0..500).map(|k| ring.get_node(k).unwrap()).collect();

        ring.remove_node(2);

        // All keys should now map to node 1.
        for key in 0..500 {
            assert_eq!(ring.get_node(key), Some(1));
        }

        // Keys that were already on node 1 should stay there.
        for (i, &prev) in before.iter().enumerate() {
            if prev == 1 {
                assert_eq!(ring.get_node(i as i64), Some(1));
            }
        }
    }

    // -- New tests: hash-based shard routing --

    #[test]
    fn test_hash_routing_consistency_across_maps() {
        // Two independently constructed ShardMaps with the same strategy must
        // route every key identically — the hash function is deterministic.
        let map_a = ShardMap::new(PartitionStrategy::Hash { num_shards: 16 });
        let map_b = ShardMap::new(PartitionStrategy::Hash { num_shards: 16 });

        for key in -1000..1000 {
            assert_eq!(
                map_a.assign_key(key),
                map_b.assign_key(key),
                "key {key} routed differently across two identical shard maps"
            );
        }
    }

    #[test]
    fn test_hash_routing_negative_and_extreme_keys() {
        let map = ShardMap::new(PartitionStrategy::Hash { num_shards: 5 });

        // Extreme values must not panic and must land in range.
        for &key in &[i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
            let sid = map.assign_key(key);
            assert!(sid < 5, "shard id {sid} out of range for key {key}");
        }
    }

    // -- New tests: range-based shard routing --

    #[test]
    fn test_range_routing_boundary_values() {
        // Split at exactly i64::MIN + 1 and 0 to exercise extreme boundaries.
        let map = ShardMap::new(PartitionStrategy::Range {
            split_points: vec![i64::MIN + 1, 0],
        });
        assert_eq!(map.len(), 3);

        // i64::MIN < split[0] => shard 0
        assert_eq!(map.assign_key(i64::MIN), 0);
        // i64::MIN + 1 >= split[0] and < split[1]=0 => shard 1
        assert_eq!(map.assign_key(i64::MIN + 1), 1);
        assert_eq!(map.assign_key(-1), 1);
        // 0 >= split[1] => shard 2
        assert_eq!(map.assign_key(0), 2);
        assert_eq!(map.assign_key(i64::MAX), 2);
    }

    #[test]
    fn test_range_routing_single_split_point() {
        // One split point at 500 => 2 shards.
        let map = ShardMap::new(PartitionStrategy::Range {
            split_points: vec![500],
        });
        assert_eq!(map.len(), 2);

        assert_eq!(map.assign_key(499), 0);
        assert_eq!(map.assign_key(500), 1);
        assert_eq!(map.assign_key(501), 1);
        assert_eq!(map.assign_key(i64::MIN), 0);
        assert_eq!(map.assign_key(i64::MAX), 1);
    }

    // -- New tests: shard rebalancing --

    #[test]
    fn test_rebalance_all_shards_on_one_node_to_two() {
        // All 4 shards initially on node 1; add node 2 and rebalance.
        let mut shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 4 });
        for id in 0..4 {
            shard_map.get_shard_mut(id).unwrap().assigned_node = Some(1);
        }

        let mut topo = ClusterTopology::new();
        topo.add_node(1, "node1:5000");
        topo.add_node(2, "node2:5000");

        let plan = Rebalancer::compute_plan(&shard_map, &topo);
        Rebalancer::apply_plan(&plan, &mut shard_map);

        // After rebalancing, every shard should be assigned to a live node.
        for shard in shard_map.all_shards() {
            assert!(
                shard.assigned_node.is_some(),
                "shard {} has no assigned node after rebalance",
                shard.id
            );
            let node = shard.assigned_node.unwrap();
            assert!(
                node == 1 || node == 2,
                "shard {} assigned to unexpected node {}",
                shard.id,
                node
            );
        }

        // Should be evenly distributed: 2 shards per node.
        let on_1 = shard_map.shards_for_node(1).len();
        let on_2 = shard_map.shards_for_node(2).len();
        assert_eq!(on_1 + on_2, 4);
        assert!((on_1 as i64 - on_2 as i64).unsigned_abs() <= 1);
    }

    #[test]
    fn test_rebalance_no_alive_nodes() {
        // When all nodes are dead, the plan should be empty (no panics).
        let mut shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 3 });
        for id in 0..3 {
            shard_map.get_shard_mut(id).unwrap().assigned_node = Some(1);
        }

        let mut topo = ClusterTopology::new();
        topo.add_node(1, "node1:5000");
        topo.mark_dead(1);

        let plan = Rebalancer::compute_plan(&shard_map, &topo);
        assert!(
            plan.moves.is_empty(),
            "expected no moves when all nodes are dead, got {}",
            plan.moves.len()
        );
    }

    // -- New tests: adding/removing shards (topology changes) --

    #[test]
    fn test_add_then_remove_node_rebalance() {
        let mut shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 6 });

        // Initial: 2 nodes, 3 shards each.
        for id in 0..3 {
            shard_map.get_shard_mut(id).unwrap().assigned_node = Some(1);
        }
        for id in 3..6 {
            shard_map.get_shard_mut(id).unwrap().assigned_node = Some(2);
        }

        let mut topo = ClusterTopology::new();
        topo.add_node(1, "node1:5000");
        topo.add_node(2, "node2:5000");

        // Add a third node and rebalance.
        topo.add_node(3, "node3:5000");
        let plan = Rebalancer::compute_plan(&shard_map, &topo);
        Rebalancer::apply_plan(&plan, &mut shard_map);

        // Verify 2 shards per node.
        for nid in 1..=3 {
            let count = shard_map
                .all_shards()
                .filter(|s| s.assigned_node == Some(nid))
                .count();
            assert_eq!(count, 2, "node {nid} has {count} shards, expected 2");
        }

        // Now remove node 3 (mark dead) and rebalance again.
        topo.mark_dead(3);
        let plan2 = Rebalancer::compute_plan(&shard_map, &topo);
        Rebalancer::apply_plan(&plan2, &mut shard_map);

        // Shards previously on node 3 should have moved away.
        for shard in shard_map.all_shards() {
            assert_ne!(
                shard.assigned_node,
                Some(3),
                "shard {} still on dead node 3",
                shard.id
            );
        }
        let on_1 = shard_map
            .all_shards()
            .filter(|s| s.assigned_node == Some(1))
            .count();
        let on_2 = shard_map
            .all_shards()
            .filter(|s| s.assigned_node == Some(2))
            .count();
        assert_eq!(on_1 + on_2, 6);
        assert!((on_1 as i64 - on_2 as i64).unsigned_abs() <= 1);
    }

    // -- New tests: key distribution uniformity --

    #[test]
    fn test_hash_key_distribution_uniformity() {
        let num_shards: u64 = 8;
        let map = ShardMap::new(PartitionStrategy::Hash { num_shards });

        // Route 10_000 sequential keys and count per-shard hits.
        let mut counts = vec![0u64; num_shards as usize];
        let total_keys = 10_000i64;
        for key in 0..total_keys {
            let sid = map.assign_key(key);
            counts[sid as usize] += 1;
        }

        let expected = total_keys as f64 / num_shards as f64; // 1250.0
        for (shard, &count) in counts.iter().enumerate() {
            // Allow each shard to deviate by at most 50% from the ideal share.
            // A truly uniform hash would be much tighter, but FNV over
            // sequential integers can cluster; 50% is generous and still
            // catches catastrophic skew (e.g. all keys in one shard).
            let deviation = (count as f64 - expected).abs() / expected;
            assert!(
                deviation < 0.50,
                "shard {shard} has {count} keys ({:.1}% deviation), expected ~{expected}",
                deviation * 100.0
            );
        }
    }

    #[test]
    fn test_consistent_hash_ring_distribution() {
        // Use more vnodes and more physical nodes to get reasonable spread
        // with the FNV-1a hash on sequential integers.
        let mut ring = HashRing::new(500);
        for nid in 1..=4 {
            ring.add_node(nid);
        }

        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        let total = 10_000i64;
        for key in 0..total {
            let node = ring.get_node(key).unwrap();
            *counts.entry(node).or_default() += 1;
        }

        // All 4 nodes must receive at least some keys.
        for nid in 1..=4 {
            let count = *counts.get(&nid).unwrap_or(&0);
            assert!(
                count > 0,
                "node {nid} received 0 keys out of {total}"
            );
        }

        // No single node should hog more than 60% of all keys.
        let max_count = *counts.values().max().unwrap();
        assert!(
            max_count < (total as usize * 60 / 100),
            "most-loaded node has {max_count}/{total} keys (>60%), distribution too skewed"
        );
    }

    // -- New tests: edge cases --

    #[test]
    fn test_edge_case_single_shard_hash() {
        let map = ShardMap::new(PartitionStrategy::Hash { num_shards: 1 });
        assert_eq!(map.len(), 1);

        // Every key must go to shard 0.
        for key in [-1_000_000, -1, 0, 1, 1_000_000, i64::MIN, i64::MAX] {
            assert_eq!(map.assign_key(key), 0, "key {key} should map to shard 0");
        }
    }

    #[test]
    fn test_edge_case_single_shard_range_no_splits() {
        // No split points => exactly 1 shard spanning the entire key space.
        let map = ShardMap::new(PartitionStrategy::Range {
            split_points: vec![],
        });
        assert_eq!(map.len(), 1);

        for key in [i64::MIN, -1, 0, 1, i64::MAX] {
            assert_eq!(map.assign_key(key), 0);
        }

        let shard = map.get_shard(0).unwrap();
        assert_eq!(shard.start_key, i64::MIN);
        assert_eq!(shard.end_key, i64::MAX);
    }

    #[test]
    fn test_edge_case_zero_as_key() {
        // Ensure key=0 is handled correctly by both strategies.
        let hash_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 4 });
        let sid = hash_map.assign_key(0);
        assert!(sid < 4);

        let range_map = ShardMap::new(PartitionStrategy::Range {
            split_points: vec![-100, 0, 100],
        });
        // 0 >= split[1]=0, but 0 < split[2]=100 => shard 2
        assert_eq!(range_map.assign_key(0), 2);
    }

    #[test]
    fn test_edge_case_hash_ring_empty() {
        let ring = HashRing::new(100);
        // Empty ring should return None for any key.
        assert_eq!(ring.get_node(0), None);
        assert_eq!(ring.get_node(42), None);
        assert_eq!(ring.get_node(i64::MIN), None);

        // get_nodes should return an empty vec.
        assert!(ring.get_nodes(0, 3).is_empty());
    }

    #[test]
    fn test_edge_case_hash_ring_single_node() {
        let mut ring = HashRing::new(50);
        ring.add_node(99);

        // Every key must map to the only node.
        for key in -500..500 {
            assert_eq!(ring.get_node(key), Some(99));
        }

        // Asking for more replicas than nodes should return just the one.
        let replicas = ring.get_nodes(42, 5);
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0], 99);
    }

    // -- 3.4 Shard-aware query routing --

    #[test]
    fn test_shard_router_route_key() {
        let mut topo = ClusterTopology::new();
        topo.add_node(1, "node1:5000");
        topo.add_node(2, "node2:5000");
        topo.add_node(3, "node3:5000");

        let router = ShardRouter::new(&topo);

        // Every key must resolve to a valid (shard, node) pair.
        for key in ["user:1", "user:2", "order:99", "session:abc", ""] {
            let result = router.route_key(key);
            assert!(result.is_some(), "route_key returned None for '{key}'");
            let (shard_id, node_id) = result.unwrap();
            assert!(
                shard_id < 3,
                "shard_id {shard_id} out of range for key '{key}'"
            );
            assert!(
                [1, 2, 3].contains(&node_id),
                "node_id {node_id} not in topology for key '{key}'"
            );
        }

        // Determinism: same key always routes to the same place.
        let a = router.route_key("deterministic");
        let b = router.route_key("deterministic");
        assert_eq!(a, b);
    }

    #[test]
    fn test_shard_router_scatter_gather() {
        let mut topo = ClusterTopology::new();
        topo.add_node(10, "n10:5000");
        topo.add_node(20, "n20:5000");
        topo.add_node(30, "n30:5000");
        topo.mark_dead(20);

        let router = ShardRouter::new(&topo);
        let nodes = router.scatter_gather_nodes();

        // Only alive nodes should appear.
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&10));
        assert!(nodes.contains(&30));
        assert!(!nodes.contains(&20), "dead node 20 should not be in scatter-gather list");

        // Should be sorted.
        assert!(nodes.windows(2).all(|w| w[0] <= w[1]), "scatter_gather_nodes not sorted");
    }

    #[test]
    fn test_shard_aware_local_vs_remote() {
        let mut topo = ClusterTopology::new();
        topo.add_node(1, "node1:5000");
        topo.add_node(2, "node2:5000");
        topo.add_node(3, "node3:5000");

        let aware = ShardAwareRouter::new(&topo, 1);

        // Route many keys — at least some should be local (node 1) and some remote.
        let mut found_local = false;
        let mut found_remote = false;
        for i in 0..200 {
            let key = format!("key:{i}");
            let route = aware.route(&key);
            assert!(
                [1, 2, 3].contains(&route.node_id),
                "node_id {} not in topology",
                route.node_id
            );
            if route.is_local {
                assert_eq!(route.node_id, 1, "is_local=true but node_id != local_node");
                found_local = true;
            } else {
                assert_ne!(route.node_id, 1, "is_local=false but node_id == local_node");
                found_remote = true;
            }
            // is_local helper must agree.
            assert_eq!(
                aware.is_local(&key),
                route.is_local,
                "is_local() disagrees with route().is_local for '{key}'"
            );
        }
        assert!(found_local, "expected at least one key to route locally");
        assert!(found_remote, "expected at least one key to route remotely");

        // remote_nodes must exclude the local node.
        let remotes = aware.remote_nodes();
        assert!(!remotes.contains(&1), "remote_nodes should not include local node");
        assert_eq!(remotes.len(), 2);
        assert!(remotes.contains(&2));
        assert!(remotes.contains(&3));
    }

    #[test]
    fn test_query_route_format() {
        let route = QueryRoute {
            shard_id: 5,
            node_id: 42,
            is_local: true,
        };

        // Debug formatting should include all fields.
        let dbg = format!("{route:?}");
        assert!(dbg.contains("shard_id"), "Debug missing shard_id");
        assert!(dbg.contains("node_id"), "Debug missing node_id");
        assert!(dbg.contains("is_local"), "Debug missing is_local");
        assert!(dbg.contains("5"), "Debug missing shard_id value");
        assert!(dbg.contains("42"), "Debug missing node_id value");

        // Clone and equality.
        let clone = route.clone();
        assert_eq!(route, clone);

        // Different routes should not be equal.
        let other = QueryRoute {
            shard_id: 5,
            node_id: 42,
            is_local: false,
        };
        assert_ne!(route, other);
    }

    // -- Rebalance Executor (3.5) tests ------------------------------------

    fn make_plan(n: usize) -> RebalancePlan {
        let moves: Vec<ShardMove> = (0..n)
            .map(|i| ShardMove {
                shard_id: i as ShardId,
                from_node: 1,
                to_node: 2 + (i as NodeId),
            })
            .collect();
        RebalancePlan { moves }
    }

    #[test]
    fn test_rebalance_execution_lifecycle() {
        let plan = make_plan(3);
        let mut exec = RebalanceExecution::new(plan);

        assert!(!exec.is_started());
        assert!(!exec.is_finished());
        assert_eq!(exec.total_moves(), 3);
        assert_eq!(exec.pending_count(), 3);
        assert_eq!(exec.progress_pct(), 0);

        let moves = exec.start();
        assert_eq!(moves.len(), 3);
        assert!(exec.is_started());

        exec.update_progress(0, 50);
        assert_eq!(*exec.state(0).unwrap(), TransferState::Transferring { progress_pct: 50 });

        exec.complete_move(0);
        assert_eq!(exec.completed_count(), 1);
        assert_eq!(exec.pending_count(), 2);
        assert_eq!(exec.progress_pct(), 33);

        exec.complete_move(1);
        exec.complete_move(2);
        assert!(exec.is_finished());
        assert_eq!(exec.progress_pct(), 100);
    }

    #[test]
    fn test_rebalance_execution_failure() {
        let plan = make_plan(2);
        let mut exec = RebalanceExecution::new(plan);
        exec.start();

        exec.complete_move(0);
        exec.fail_move(1, "network timeout");

        assert!(exec.is_finished());
        assert_eq!(exec.completed_count(), 1);
        assert_eq!(exec.failed_count(), 1);
        assert_eq!(
            *exec.state(1).unwrap(),
            TransferState::Failed { reason: "network timeout".into() }
        );
    }

    #[test]
    fn test_rebalance_execution_apply_completed() {
        let mut shard_map = ShardMap::new(PartitionStrategy::Hash { num_shards: 3 });
        for i in 0..3 {
            shard_map.get_shard_mut(i).unwrap().assigned_node = Some(1);
        }

        let plan = RebalancePlan {
            moves: vec![
                ShardMove { shard_id: 0, from_node: 1, to_node: 2 },
                ShardMove { shard_id: 1, from_node: 1, to_node: 3 },
                ShardMove { shard_id: 2, from_node: 1, to_node: 4 },
            ],
        };
        let mut exec = RebalanceExecution::new(plan);
        exec.start();

        exec.complete_move(0);
        exec.fail_move(1, "failed");
        exec.complete_move(2);

        exec.apply_completed(&mut shard_map);

        assert_eq!(shard_map.get_shard(0).unwrap().assigned_node, Some(2)); // completed
        assert_eq!(shard_map.get_shard(1).unwrap().assigned_node, Some(1)); // failed, unchanged
        assert_eq!(shard_map.get_shard(2).unwrap().assigned_node, Some(4)); // completed
    }

    #[test]
    fn test_rebalance_empty_plan() {
        let plan = RebalancePlan { moves: vec![] };
        let exec = RebalanceExecution::new(plan);
        assert!(exec.is_finished()); // no moves = already done
        assert_eq!(exec.progress_pct(), 100);
    }

    #[test]
    fn test_rebalance_progress_clamp() {
        let plan = make_plan(1);
        let mut exec = RebalanceExecution::new(plan);
        exec.start();
        exec.update_progress(0, 200); // should clamp to 100
        assert_eq!(*exec.state(0).unwrap(), TransferState::Transferring { progress_pct: 100 });
    }

    // ================================================================
    // Distributed Table Engine tests
    // ================================================================

    fn make_shard_map() -> ShardMap {
        ShardMap::new(PartitionStrategy::Hash { num_shards: 4 })
    }

    #[test]
    fn dist_table_create() {
        let mut engine = DistributedTableEngine::new(make_shard_map());
        engine.create_table("users", "user_id");
        assert_eq!(engine.table_count(), 1);
        assert!(engine.get_table("users").is_some());
        assert_eq!(engine.get_table("users").unwrap().shard_key, "user_id");
    }

    #[test]
    fn dist_table_route_point_query() {
        let mut engine = DistributedTableEngine::new(make_shard_map());
        engine.create_table("users", "user_id");

        let plan = engine.route_point_query("users", 42).unwrap();
        match plan {
            QueryPlan::SingleShard(id) => assert!(id < 4),
            _ => panic!("expected single shard"),
        }
    }

    #[test]
    fn dist_table_route_scan() {
        let mut engine = DistributedTableEngine::new(make_shard_map());
        engine.create_table("users", "user_id");

        let plan = engine.route_scan("users").unwrap();
        match plan {
            QueryPlan::Scatter(shards) => assert_eq!(shards.len(), 4),
            _ => panic!("expected scatter"),
        }
    }

    #[test]
    fn dist_table_route_aggregation() {
        let mut engine = DistributedTableEngine::new(make_shard_map());
        engine.create_table("orders", "order_id");

        let plan = engine.route_aggregation("orders", AggType::Sum).unwrap();
        match plan {
            QueryPlan::ScatterGather { shards, agg_type } => {
                assert_eq!(shards.len(), 4);
                assert_eq!(agg_type, AggType::Sum);
            }
            _ => panic!("expected scatter-gather"),
        }
    }

    #[test]
    fn dist_table_merge_partial_count() {
        let results = vec![
            PartialAggResult { shard_id: 0, count: 100, sum: 0.0, min: None, max: None },
            PartialAggResult { shard_id: 1, count: 200, sum: 0.0, min: None, max: None },
            PartialAggResult { shard_id: 2, count: 150, sum: 0.0, min: None, max: None },
        ];
        assert_eq!(DistributedTableEngine::merge_partial_aggs(&results, AggType::Count), 450.0);
    }

    #[test]
    fn dist_table_merge_partial_sum() {
        let results = vec![
            PartialAggResult { shard_id: 0, count: 10, sum: 100.5, min: None, max: None },
            PartialAggResult { shard_id: 1, count: 20, sum: 200.5, min: None, max: None },
        ];
        assert_eq!(DistributedTableEngine::merge_partial_aggs(&results, AggType::Sum), 301.0);
    }

    #[test]
    fn dist_table_merge_partial_min_max() {
        let results = vec![
            PartialAggResult { shard_id: 0, count: 0, sum: 0.0, min: Some(10.0), max: Some(50.0) },
            PartialAggResult { shard_id: 1, count: 0, sum: 0.0, min: Some(5.0), max: Some(90.0) },
        ];
        assert_eq!(DistributedTableEngine::merge_partial_aggs(&results, AggType::Min), 5.0);
        assert_eq!(DistributedTableEngine::merge_partial_aggs(&results, AggType::Max), 90.0);
    }

    #[test]
    fn dist_table_merge_partial_avg() {
        let results = vec![
            PartialAggResult { shard_id: 0, count: 100, sum: 500.0, min: None, max: None },
            PartialAggResult { shard_id: 1, count: 100, sum: 700.0, min: None, max: None },
        ];
        assert_eq!(DistributedTableEngine::merge_partial_aggs(&results, AggType::Avg), 6.0);
    }

    #[test]
    fn dist_table_nonexistent() {
        let engine = DistributedTableEngine::new(make_shard_map());
        assert!(engine.route_point_query("nope", 1).is_none());
        assert!(engine.route_scan("nope").is_none());
    }
}
