//! Dense vector engine — HNSW and IVFFlat indexes for approximate nearest neighbor search.
//!
//! Supports:
//!   - Vector type with arbitrary dimensionality
//!   - Distance metrics: cosine, L2 (Euclidean), inner product
//!   - HNSW (Hierarchical Navigable Small World) index for ANN search
//!   - IVFFlat (Inverted File with Flat) index for ANN search via k-means clustering
//!   - Exact (brute-force) search for small datasets
//!
//! Replaces pgvector, Pinecone, Weaviate, Milvus.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::cmp::Ordering;

// ============================================================================
// Vector type
// ============================================================================

/// A dense vector of f32 values.
#[derive(Debug, Clone, PartialEq)]
pub struct Vector {
    pub data: Vec<f32>,
}

impl Vector {
    pub fn new(data: Vec<f32>) -> Self {
        Self { data }
    }

    pub fn dim(&self) -> usize {
        self.data.len()
    }

    /// L2 (Euclidean) norm.
    pub fn norm(&self) -> f32 {
        self.data.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    /// Normalize to unit vector.
    pub fn normalize(&self) -> Vector {
        let n = self.norm();
        if n == 0.0 {
            return self.clone();
        }
        Vector {
            data: self.data.iter().map(|x| x / n).collect(),
        }
    }
}

// ============================================================================
// Distance metrics
// ============================================================================

/// Distance metric for vector similarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// L2 (Euclidean) distance. Lower = more similar.
    L2,
    /// Cosine distance (1 - cosine_similarity). Lower = more similar.
    Cosine,
    /// Negative inner product. Lower = more similar (higher IP = more similar).
    InnerProduct,
}

/// Compute distance between two vectors.
pub fn distance(a: &Vector, b: &Vector, metric: DistanceMetric) -> f32 {
    debug_assert_eq!(a.dim(), b.dim(), "vector dimensions must match");
    match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::InnerProduct => neg_inner_product(a, b),
    }
}

fn l2_distance(a: &Vector, b: &Vector) -> f32 {
    a.data
        .iter()
        .zip(b.data.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

fn cosine_distance(a: &Vector, b: &Vector) -> f32 {
    let dot: f32 = a.data.iter().zip(b.data.iter()).map(|(x, y)| x * y).sum();
    let na = a.norm();
    let nb = b.norm();
    if na == 0.0 || nb == 0.0 {
        return 1.0;
    }
    1.0 - dot / (na * nb)
}

fn neg_inner_product(a: &Vector, b: &Vector) -> f32 {
    let dot: f32 = a.data.iter().zip(b.data.iter()).map(|(x, y)| x * y).sum();
    -dot // Negate so lower = more similar
}

// ============================================================================
// HNSW Index
// ============================================================================

/// Configuration for HNSW index construction.
#[derive(Debug, Clone)]
pub struct HnswConfig {
    /// Max number of connections per node per layer (M in the paper).
    pub m: usize,
    /// Max connections for layer 0 (typically 2*M).
    pub m_max0: usize,
    /// Size of the dynamic candidate list during construction (ef_construction).
    pub ef_construction: usize,
    /// Size of the dynamic candidate list during search (ef_search).
    pub ef_search: usize,
    /// Distance metric.
    pub metric: DistanceMetric,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ef_search: 50,
            metric: DistanceMetric::Cosine,
        }
    }
}

/// A node in the HNSW graph.
#[derive(Debug)]
struct HnswNode {
    _id: u64,
    vector: Vector,
    /// Neighbors at each layer. neighbors[layer] = list of neighbor IDs.
    neighbors: Vec<Vec<u64>>,
}

/// Scored candidate for priority queues.
#[derive(Debug, Clone)]
struct Candidate {
    id: u64,
    dist: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse ordering so smallest distance comes first
        other
            .dist
            .partial_cmp(&self.dist)
            .unwrap_or(Ordering::Equal)
    }
}

/// Max-heap candidate (for tracking the worst element in top-k).
#[derive(Debug, Clone)]
struct MaxCandidate {
    id: u64,
    dist: f32,
}

impl PartialEq for MaxCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}

impl Eq for MaxCandidate {}

impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(Ordering::Equal)
    }
}

/// HNSW (Hierarchical Navigable Small World) index.
pub struct HnswIndex {
    config: HnswConfig,
    nodes: HashMap<u64, HnswNode>,
    entry_point: Option<u64>,
    max_layer: usize,
    /// Inverse of ln(M) for layer assignment.
    ml: f64,
    /// IDs marked as deleted — skipped during search results.
    deleted: HashSet<u64>,
}

impl HnswIndex {
    pub fn new(config: HnswConfig) -> Self {
        let ml = 1.0 / (config.m as f64).ln();
        Self {
            config,
            nodes: HashMap::new(),
            entry_point: None,
            max_layer: 0,
            ml,
            deleted: HashSet::new(),
        }
    }

    /// Assign a random layer for a new node.
    fn random_layer(&self) -> usize {
        let r: f64 = rand::random();
        (-r.ln() * self.ml).floor() as usize
    }

    /// Insert a vector into the index.
    pub fn insert(&mut self, id: u64, vector: Vector) {
        let node_layer = self.random_layer();

        // First, add the node to the map (with empty neighbors)
        let node = HnswNode {
            _id: id,
            vector: vector.clone(),
            neighbors: vec![Vec::new(); node_layer + 1],
        };
        self.nodes.insert(id, node);

        if self.entry_point.is_none() {
            self.entry_point = Some(id);
            self.max_layer = node_layer;
            return;
        }

        let entry_id = match self.entry_point {
            Some(id) => id,
            None => return, // guarded above, but be safe
        };

        // Phase 1: Traverse from top layer down to node_layer + 1, greedily
        let mut ep = entry_id;
        for layer in (node_layer.saturating_add(1)..=self.max_layer).rev() {
            ep = self.greedy_search(ep, &vector, layer);
        }

        // Phase 2: From min(node_layer, max_layer) down to 0, do ef_construction search
        let top = node_layer.min(self.max_layer);
        for layer in (0..=top).rev() {
            let candidates =
                self.search_layer(ep, &vector, self.config.ef_construction, layer);

            // Update ep to the closest result for the next layer down
            if let Some(first) = candidates.first() {
                ep = first.id;
            }

            // Select M best neighbors
            let m = if layer == 0 {
                self.config.m_max0
            } else {
                self.config.m
            };
            let selected: Vec<u64> = candidates
                .into_iter()
                .filter(|c| c.id != id) // Don't connect to self
                .take(m)
                .map(|c| c.id)
                .collect();

            // Add bidirectional connections
            for &neighbor_id in &selected {
                if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                    while neighbor.neighbors.len() <= layer {
                        neighbor.neighbors.push(Vec::new());
                    }
                    if !neighbor.neighbors[layer].contains(&id) {
                        neighbor.neighbors[layer].push(id);
                        // Prune if too many
                        if neighbor.neighbors[layer].len() > m {
                            self.prune_connections(neighbor_id, layer, m);
                        }
                    }
                }
            }

            // Store neighbors in new node
            if let Some(new_node) = self.nodes.get_mut(&id) {
                while new_node.neighbors.len() <= layer {
                    new_node.neighbors.push(Vec::new());
                }
                new_node.neighbors[layer] = selected;
            }
        }

        // Update entry point if new node has higher layer
        if node_layer > self.max_layer {
            self.entry_point = Some(id);
            self.max_layer = node_layer;
        }
    }

    /// Greedy search at a single layer — find the closest node to query.
    fn greedy_search(&self, start: u64, query: &Vector, layer: usize) -> u64 {
        let mut current = start;
        let mut current_dist = self.dist(current, query);

        loop {
            let mut improved = false;
            if let Some(node) = self.nodes.get(&current) {
                if layer < node.neighbors.len() {
                    for &neighbor_id in &node.neighbors[layer] {
                        let d = self.dist(neighbor_id, query);
                        if d < current_dist {
                            current = neighbor_id;
                            current_dist = d;
                            improved = true;
                        }
                    }
                }
            }
            if !improved {
                break;
            }
        }

        current
    }

    /// ef-bounded search at a single layer. Returns candidates sorted by distance.
    fn search_layer(
        &self,
        start: u64,
        query: &Vector,
        ef: usize,
        layer: usize,
    ) -> Vec<Candidate> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new(); // min-heap
        let mut results = BinaryHeap::new(); // max-heap (worst at top)

        let start_dist = self.dist(start, query);
        visited.insert(start);
        candidates.push(Candidate {
            id: start,
            dist: start_dist,
        });
        results.push(MaxCandidate {
            id: start,
            dist: start_dist,
        });

        while let Some(closest) = candidates.pop() {
            let worst_dist = results.peek().map(|r| r.dist).unwrap_or(f32::MAX);
            if closest.dist > worst_dist {
                break;
            }

            if let Some(node) = self.nodes.get(&closest.id) {
                if layer < node.neighbors.len() {
                    for &neighbor_id in &node.neighbors[layer] {
                        if visited.insert(neighbor_id) {
                            let d = self.dist(neighbor_id, query);
                            let worst = results.peek().map(|r| r.dist).unwrap_or(f32::MAX);

                            if d < worst || results.len() < ef {
                                candidates.push(Candidate {
                                    id: neighbor_id,
                                    dist: d,
                                });
                                results.push(MaxCandidate {
                                    id: neighbor_id,
                                    dist: d,
                                });
                                if results.len() > ef {
                                    results.pop();
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut result: Vec<Candidate> = results
            .into_iter()
            .map(|mc| Candidate {
                id: mc.id,
                dist: mc.dist,
            })
            .collect();
        result.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
        result
    }

    /// Prune connections for a node at a given layer to at most max_connections.
    fn prune_connections(&mut self, node_id: u64, layer: usize, max_conn: usize) {
        let vector = if let Some(node) = self.nodes.get(&node_id) {
            node.vector.clone()
        } else {
            return;
        };

        let neighbors: Vec<u64> = if let Some(node) = self.nodes.get(&node_id) {
            if layer < node.neighbors.len() {
                node.neighbors[layer].clone()
            } else {
                return;
            }
        } else {
            return;
        };

        // Score all neighbors
        let mut scored: Vec<(u64, f32)> = neighbors
            .into_iter()
            .map(|nid| (nid, self.dist(nid, &vector)))
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        scored.truncate(max_conn);

        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.neighbors[layer] = scored.into_iter().map(|(id, _)| id).collect();
        }
    }

    /// Compute distance between a stored node and a query vector.
    fn dist(&self, node_id: u64, query: &Vector) -> f32 {
        if let Some(node) = self.nodes.get(&node_id) {
            distance(&node.vector, query, self.config.metric)
        } else {
            f32::MAX
        }
    }

    /// Search for the k nearest neighbors of a query vector.
    /// Returns (id, distance) pairs sorted by distance ascending.
    pub fn search(&self, query: &Vector, k: usize) -> Vec<(u64, f32)> {
        if self.nodes.is_empty() || self.entry_point.is_none() {
            return vec![];
        }

        let entry = match self.entry_point {
            Some(id) => id,
            None => return vec![], // guarded above, but be safe
        };

        // Phase 1: Greedy search from top to layer 1
        let mut current = entry;
        for layer in (1..=self.max_layer).rev() {
            current = self.greedy_search(current, query, layer);
        }

        // Phase 2: ef-bounded search at layer 0
        let candidates =
            self.search_layer(current, query, self.config.ef_search.max(k), 0);

        candidates
            .into_iter()
            .filter(|c| !self.deleted.contains(&c.id))
            .take(k)
            .map(|c| (c.id, c.dist))
            .collect()
    }

    /// Mark a vector ID as deleted. It will be excluded from search results.
    pub fn mark_deleted(&mut self, id: u64) {
        self.deleted.insert(id);
    }

    /// Number of indexed vectors (including deleted).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

impl std::fmt::Debug for HnswIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswIndex")
            .field("nodes", &self.nodes.len())
            .field("max_layer", &self.max_layer)
            .field("metric", &self.config.metric)
            .finish()
    }
}

// ============================================================================
// Vector index persistence (serialize / deserialize)
// ============================================================================

/// Serialized form of an HNSW index.
/// Format: [metric u8][m u32][ef_search u32][num_nodes u32][...nodes...][max_layer u32][entry u64]
/// Each node: [id u64][dim u32][f32 * dim][num_layers u32][ for each layer: [num_neighbors u32][u64 * num_neighbors] ]
impl HnswIndex {
    /// Serialize the HNSW index to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Header
        buf.push(match self.config.metric {
            DistanceMetric::L2 => 0u8,
            DistanceMetric::Cosine => 1,
            DistanceMetric::InnerProduct => 2,
        });
        buf.extend_from_slice(&(self.config.m as u32).to_le_bytes());
        buf.extend_from_slice(&(self.config.ef_search as u32).to_le_bytes());
        buf.extend_from_slice(&(self.nodes.len() as u32).to_le_bytes());

        // Nodes
        for (&id, node) in &self.nodes {
            buf.extend_from_slice(&id.to_le_bytes());
            buf.extend_from_slice(&(node.vector.dim() as u32).to_le_bytes());
            for &val in &node.vector.data {
                buf.extend_from_slice(&val.to_le_bytes());
            }
            buf.extend_from_slice(&(node.neighbors.len() as u32).to_le_bytes());
            for layer in &node.neighbors {
                buf.extend_from_slice(&(layer.len() as u32).to_le_bytes());
                for &nbr in layer {
                    buf.extend_from_slice(&nbr.to_le_bytes());
                }
            }
        }

        // Footer
        buf.extend_from_slice(&(self.max_layer as u32).to_le_bytes());
        buf.extend_from_slice(&self.entry_point.unwrap_or(u64::MAX).to_le_bytes());
        buf
    }

    /// Deserialize an HNSW index from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        let mut pos = 0;
        if data.len() < 13 {
            return Err("data too short for HNSW header".into());
        }

        let metric = match data[pos] {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            _ => return Err("unknown metric".into()),
        };
        pos += 1;

        let m = u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| "truncated m field")?);
        let m = m as usize;
        pos += 4;
        let ef_search = u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| "truncated ef_search")?);
        let ef_search = ef_search as usize;
        pos += 4;
        let num_nodes = u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| "truncated num_nodes")?);
        let num_nodes = num_nodes as usize;
        pos += 4;

        let config = HnswConfig {
            m,
            m_max0: m * 2,
            ef_construction: 200,
            ef_search,
            metric,
        };

        let mut nodes = HashMap::new();
        for _ in 0..num_nodes {
            if pos + 12 > data.len() {
                return Err("unexpected end of data reading node".into());
            }
            let id = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| "truncated node id")?);
            pos += 8;
            let dim = u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| "truncated dim")?);
            let dim = dim as usize;
            pos += 4;

            if pos + dim * 4 > data.len() {
                return Err("unexpected end of data reading vector".into());
            }
            let mut vec_data = Vec::with_capacity(dim);
            for _ in 0..dim {
                vec_data.push(f32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| "truncated vector element")?));
                pos += 4;
            }

            if pos + 4 > data.len() {
                return Err("unexpected end of data reading num_layers".into());
            }
            let num_layers = u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| "truncated num_layers")?);
            let num_layers = num_layers as usize;
            pos += 4;

            let mut neighbors = Vec::with_capacity(num_layers);
            for _ in 0..num_layers {
                if pos + 4 > data.len() {
                    return Err("unexpected end of data reading neighbor count".into());
                }
                let nn = u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| "truncated neighbor count")?);
                let nn = nn as usize;
                pos += 4;
                if pos + nn * 8 > data.len() {
                    return Err("unexpected end of data reading neighbor ids".into());
                }
                let mut layer = Vec::with_capacity(nn);
                for _ in 0..nn {
                    layer.push(u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| "truncated neighbor id")?));
                    pos += 8;
                }
                neighbors.push(layer);
            }

            nodes.insert(id, HnswNode {
                _id: id,
                vector: Vector::new(vec_data),
                neighbors,
            });
        }

        if pos + 12 > data.len() {
            return Err("unexpected end of data reading footer".into());
        }
        let max_layer = u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| "truncated max_layer")?);
        let max_layer = max_layer as usize;
        pos += 4;
        let entry_raw = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| "truncated entry_point")?);
        let entry_point = if entry_raw == u64::MAX { None } else { Some(entry_raw) };

        let ml = 1.0 / (config.m as f64).ln();
        Ok(Self {
            config,
            nodes,
            max_layer,
            entry_point,
            ml,
            deleted: HashSet::new(),
        })
    }
}

// ============================================================================
// IVFFlat Index
// ============================================================================

/// IVFFlat (Inverted File with Flat) index for approximate nearest neighbor search.
///
/// Works in two phases:
/// 1. **Training**: k-means clustering on training vectors to find `nlist` centroids.
/// 2. **Querying**: Find the `nprobe` nearest centroids, then brute-force search
///    within those clusters.
pub struct IvfFlatIndex {
    /// Centroid vectors, one per cluster (length = nlist after training).
    centroids: Vec<Vec<f32>>,
    /// Inverted lists: for each cluster, a list of (id, vector) pairs.
    inverted_lists: Vec<Vec<(usize, Vec<f32>)>>,
    /// IDs marked as deleted — skipped during search results.
    deleted: HashSet<usize>,
    /// Vector dimensionality.
    dimension: usize,
    /// Number of clusters (Voronoi cells).
    nlist: usize,
    /// Number of clusters to probe during search.
    nprobe: usize,
    /// Distance metric.
    metric: DistanceMetric,
}

impl IvfFlatIndex {
    /// Create a new, untrained IVFFlat index.
    ///
    /// - `dimension`: length of each vector
    /// - `nlist`: number of clusters / inverted lists
    /// - `nprobe`: number of clusters to search at query time
    /// - `metric`: distance metric
    pub fn new(dimension: usize, nlist: usize, nprobe: usize, metric: DistanceMetric) -> Self {
        Self {
            centroids: Vec::new(),
            inverted_lists: Vec::new(),
            deleted: HashSet::new(),
            dimension,
            nlist,
            nprobe: nprobe.min(nlist),
            metric,
        }
    }

    /// Train the index using Lloyd's k-means algorithm.
    ///
    /// Computes `nlist` centroids from the provided training vectors. After
    /// training, vectors can be added with [`add`].
    ///
    /// Panics if `vectors` is empty or contains vectors with wrong dimension.
    pub fn train(&mut self, vectors: &[Vec<f32>]) {
        assert!(!vectors.is_empty(), "training set must not be empty");
        for v in vectors {
            assert_eq!(
                v.len(),
                self.dimension,
                "training vector dimension mismatch"
            );
        }

        let k = self.nlist.min(vectors.len());
        let max_iterations = 20;

        // --- Initialize centroids by picking the first k vectors ---
        // (deterministic; avoids extra rand dependency beyond what's already used)
        let mut centroids: Vec<Vec<f32>> = vectors.iter().take(k).cloned().collect();

        let mut assignments = vec![0usize; vectors.len()];

        for _iter in 0..max_iterations {
            // --- Assignment step: assign each vector to nearest centroid ---
            let mut changed = false;
            for (i, v) in vectors.iter().enumerate() {
                let nearest = self.nearest_centroid(v, &centroids);
                if nearest != assignments[i] {
                    assignments[i] = nearest;
                    changed = true;
                }
            }
            if !changed {
                break; // converged
            }

            // --- Update step: recompute centroids ---
            let mut sums = vec![vec![0.0f32; self.dimension]; k];
            let mut counts = vec![0usize; k];

            for (i, v) in vectors.iter().enumerate() {
                let c = assignments[i];
                counts[c] += 1;
                for (j, &val) in v.iter().enumerate() {
                    sums[c][j] += val;
                }
            }

            for c in 0..k {
                if counts[c] > 0 {
                    for j in 0..self.dimension {
                        centroids[c][j] = sums[c][j] / counts[c] as f32;
                    }
                }
                // If a centroid has no assignments, leave it unchanged.
            }
        }

        self.centroids = centroids;
        self.inverted_lists = vec![Vec::new(); k];
    }

    /// Add a vector to the index. The index must be trained first.
    ///
    /// The vector is assigned to the nearest centroid's inverted list.
    pub fn add(&mut self, id: usize, vector: Vec<f32>) {
        assert_eq!(
            vector.len(),
            self.dimension,
            "vector dimension mismatch"
        );
        assert!(
            !self.centroids.is_empty(),
            "index must be trained before adding vectors"
        );

        let cluster = self.nearest_centroid(&vector, &self.centroids);
        self.inverted_lists[cluster].push((id, vector));
    }

    /// Search for the `k` nearest neighbors of `query`.
    ///
    /// Returns `(id, distance)` pairs sorted by ascending distance.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(usize, f32)> {
        assert_eq!(query.len(), self.dimension, "query dimension mismatch");
        if self.centroids.is_empty() {
            return Vec::new();
        }

        // Find the nprobe nearest centroids
        let mut centroid_dists: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let d = self.compute_distance(query, c);
                (i, d)
            })
            .collect();
        centroid_dists
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        let nprobe = self.nprobe.min(centroid_dists.len());

        // Brute-force search within the selected clusters
        let mut candidates: Vec<(usize, f32)> = Vec::new();
        for &(cluster_idx, _) in centroid_dists.iter().take(nprobe) {
            for (id, vec) in &self.inverted_lists[cluster_idx] {
                if self.deleted.contains(id) {
                    continue;
                }
                let d = self.compute_distance(query, vec);
                candidates.push((*id, d));
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        candidates.truncate(k);
        candidates
    }

    /// Find the index of the nearest centroid to a given vector.
    fn nearest_centroid(&self, vector: &[f32], centroids: &[Vec<f32>]) -> usize {
        let mut best_idx = 0;
        let mut best_dist = f32::MAX;
        for (i, c) in centroids.iter().enumerate() {
            let d = self.compute_distance(vector, c);
            if d < best_dist {
                best_dist = d;
                best_idx = i;
            }
        }
        best_idx
    }

    /// Compute distance between two raw f32 slices using the index's metric.
    fn compute_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        let va = Vector::new(a.to_vec());
        let vb = Vector::new(b.to_vec());
        distance(&va, &vb, self.metric)
    }

    /// Number of vectors stored in the index.
    pub fn len(&self) -> usize {
        self.inverted_lists.iter().map(|l| l.len()).sum()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the index has been trained.
    pub fn is_trained(&self) -> bool {
        !self.centroids.is_empty()
    }

    /// Mark a vector ID as deleted. It will be excluded from search results.
    pub fn mark_deleted(&mut self, id: usize) {
        self.deleted.insert(id);
    }
}

impl std::fmt::Debug for IvfFlatIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IvfFlatIndex")
            .field("dimension", &self.dimension)
            .field("nlist", &self.nlist)
            .field("nprobe", &self.nprobe)
            .field("metric", &self.metric)
            .field("trained", &self.is_trained())
            .field("vectors", &self.len())
            .finish()
    }
}

/// Brute-force exact nearest neighbor search (for small datasets or verification).
pub fn exact_search(
    vectors: &[(u64, Vector)],
    query: &Vector,
    k: usize,
    metric: DistanceMetric,
) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = vectors
        .iter()
        .map(|(id, v)| (*id, distance(v, query, metric)))
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    scored.truncate(k);
    scored
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rand_vec(dim: usize) -> Vector {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        Vector::new((0..dim).map(|_| rng.r#gen::<f32>()).collect())
    }

    #[test]
    fn l2_distance_test() {
        let a = Vector::new(vec![1.0, 0.0, 0.0]);
        let b = Vector::new(vec![0.0, 1.0, 0.0]);
        let d = l2_distance(&a, &b);
        assert!((d - std::f32::consts::SQRT_2).abs() < 1e-5);
    }

    #[test]
    fn cosine_distance_test() {
        let a = Vector::new(vec![1.0, 0.0]);
        let b = Vector::new(vec![0.0, 1.0]);
        let d = cosine_distance(&a, &b);
        assert!((d - 1.0).abs() < 1e-5); // Orthogonal → distance = 1

        let c = Vector::new(vec![1.0, 0.0]);
        let d2 = cosine_distance(&a, &c);
        assert!(d2.abs() < 1e-5); // Same direction → distance = 0
    }

    #[test]
    fn inner_product_test() {
        let a = Vector::new(vec![1.0, 2.0, 3.0]);
        let b = Vector::new(vec![4.0, 5.0, 6.0]);
        let d = neg_inner_product(&a, &b);
        assert!((d - (-32.0)).abs() < 1e-5); // 1*4 + 2*5 + 3*6 = 32, negated
    }

    #[test]
    fn exact_search_test() {
        let vectors = vec![
            (1, Vector::new(vec![1.0, 0.0, 0.0])),
            (2, Vector::new(vec![0.0, 1.0, 0.0])),
            (3, Vector::new(vec![0.9, 0.1, 0.0])),
            (4, Vector::new(vec![0.0, 0.0, 1.0])),
        ];
        let query = Vector::new(vec![1.0, 0.0, 0.0]);

        let results = exact_search(&vectors, &query, 2, DistanceMetric::L2);
        assert_eq!(results[0].0, 1); // Exact match
        assert_eq!(results[1].0, 3); // Closest
    }

    #[test]
    fn hnsw_basic() {
        let config = HnswConfig {
            m: 8,
            m_max0: 16,
            ef_construction: 100,
            ef_search: 50,
            metric: DistanceMetric::L2,
        };
        let mut index = HnswIndex::new(config);

        // Insert known vectors
        index.insert(1, Vector::new(vec![1.0, 0.0, 0.0]));
        index.insert(2, Vector::new(vec![0.0, 1.0, 0.0]));
        index.insert(3, Vector::new(vec![0.9, 0.1, 0.0]));
        index.insert(4, Vector::new(vec![0.0, 0.0, 1.0]));

        assert_eq!(index.len(), 4);

        let query = Vector::new(vec![1.0, 0.0, 0.0]);
        let results = index.search(&query, 2);
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 1); // Exact match should be first
    }

    #[test]
    fn hnsw_recall() {
        // Test that HNSW achieves reasonable recall on random data
        let dim = 32;
        let n = 500;

        let mut vectors: Vec<(u64, Vector)> = Vec::new();
        let config = HnswConfig {
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ef_search: 100,
            metric: DistanceMetric::L2,
        };
        let mut index = HnswIndex::new(config);

        for i in 0..n {
            let v = rand_vec(dim);
            vectors.push((i, v.clone()));
            index.insert(i, v);
        }

        // Run search and compare with exact
        let query = rand_vec(dim);
        let k = 10;
        let hnsw_results = index.search(&query, k);
        let exact_results = exact_search(&vectors, &query, k, DistanceMetric::L2);

        let hnsw_ids: HashSet<u64> = hnsw_results.iter().map(|(id, _)| *id).collect();
        let exact_ids: HashSet<u64> = exact_results.iter().map(|(id, _)| *id).collect();

        let recall = hnsw_ids.intersection(&exact_ids).count() as f64 / k as f64;
        assert!(
            recall >= 0.5,
            "recall too low: {recall:.2} (expected >= 0.5)"
        );
    }

    #[test]
    fn vector_normalize() {
        let v = Vector::new(vec![3.0, 4.0]);
        let n = v.normalize();
        assert!((n.norm() - 1.0).abs() < 1e-5);
    }

    // ========================================================================
    // IVFFlat tests
    // ========================================================================

    #[test]
    fn ivfflat_basic() {
        let dim = 16;
        let n = 200;
        let nlist = 8;
        let nprobe = 4;

        // Generate random vectors
        let mut training: Vec<Vec<f32>> = Vec::with_capacity(n);
        for _ in 0..n {
            training.push(rand_vec(dim).data);
        }

        let mut index = IvfFlatIndex::new(dim, nlist, nprobe, DistanceMetric::L2);
        index.train(&training);

        // Add all vectors
        for (i, v) in training.iter().enumerate() {
            index.add(i, v.clone());
        }
        assert_eq!(index.len(), n);

        // Search for the first vector — it should find itself as the nearest
        let query = &training[0];
        let results = index.search(query, 5);

        assert!(!results.is_empty(), "search returned no results");
        // The top result should be the query vector itself (distance ~0)
        assert_eq!(
            results[0].0, 0,
            "expected id 0 as top result, got {}",
            results[0].0
        );
        assert!(
            results[0].1 < 1e-5,
            "expected near-zero distance for self-match, got {}",
            results[0].1
        );
    }

    #[test]
    fn ivfflat_recall() {
        let dim = 16;
        let n = 200;
        let k = 10;
        let nlist = 8;
        let nprobe = 4;

        let mut training: Vec<Vec<f32>> = Vec::with_capacity(n);
        for _ in 0..n {
            training.push(rand_vec(dim).data);
        }

        let mut index = IvfFlatIndex::new(dim, nlist, nprobe, DistanceMetric::L2);
        index.train(&training);

        for (i, v) in training.iter().enumerate() {
            index.add(i, v.clone());
        }

        // Build the same data for exact_search (which expects (u64, Vector) tuples)
        let exact_data: Vec<(u64, Vector)> = training
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, Vector::new(v.clone())))
            .collect();

        let query_vec = rand_vec(dim);
        let query = &query_vec.data;

        let ivf_results = index.search(query, k);
        let exact_results = exact_search(&exact_data, &query_vec, k, DistanceMetric::L2);

        let ivf_ids: HashSet<usize> = ivf_results.iter().map(|(id, _)| *id).collect();
        let exact_ids: HashSet<usize> = exact_results
            .iter()
            .map(|(id, _)| *id as usize)
            .collect();

        let overlap = ivf_ids.intersection(&exact_ids).count();
        let recall = overlap as f64 / k as f64;
        assert!(
            recall >= 0.5,
            "IVFFlat recall too low: {recall:.2} (expected >= 0.5)"
        );
    }

    // ========================================================================
    // HNSW persistence tests
    // ========================================================================

    #[test]
    fn hnsw_serialize_deserialize_roundtrip() {
        let config = HnswConfig {
            m: 8,
            m_max0: 16,
            ef_construction: 100,
            ef_search: 50,
            metric: DistanceMetric::L2,
        };
        let mut index = HnswIndex::new(config);
        index.insert(1, Vector::new(vec![1.0, 0.0, 0.0]));
        index.insert(2, Vector::new(vec![0.0, 1.0, 0.0]));
        index.insert(3, Vector::new(vec![0.5, 0.5, 0.0]));

        let data = index.serialize();
        let restored = HnswIndex::deserialize(&data).unwrap();

        assert_eq!(restored.len(), 3);
        assert_eq!(restored.config.metric, DistanceMetric::L2);
        assert_eq!(restored.config.m, 8);

        // Search should produce same results
        let query = Vector::new(vec![1.0, 0.0, 0.0]);
        let original_results = index.search(&query, 3);
        let restored_results = restored.search(&query, 3);
        assert_eq!(original_results.len(), restored_results.len());
        assert_eq!(original_results[0].0, restored_results[0].0);
    }

    #[test]
    fn hnsw_serialize_empty_index() {
        let config = HnswConfig::default();
        let index = HnswIndex::new(config);
        let data = index.serialize();
        let restored = HnswIndex::deserialize(&data).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn hnsw_deserialize_invalid_data() {
        assert!(HnswIndex::deserialize(&[]).is_err());
        assert!(HnswIndex::deserialize(&[0xFF; 5]).is_err());
    }

    #[test]
    fn hnsw_serialize_cosine_metric() {
        let config = HnswConfig {
            metric: DistanceMetric::Cosine,
            ..Default::default()
        };
        let mut index = HnswIndex::new(config);
        index.insert(1, Vector::new(vec![1.0, 0.0]).normalize());
        index.insert(2, Vector::new(vec![0.0, 1.0]).normalize());

        let data = index.serialize();
        let restored = HnswIndex::deserialize(&data).unwrap();
        assert_eq!(restored.config.metric, DistanceMetric::Cosine);
        assert_eq!(restored.len(), 2);
    }

    #[test]
    fn ivfflat_cosine() {
        let dim = 16;
        let n = 200;
        let nlist = 8;
        let nprobe = 4;

        let mut training: Vec<Vec<f32>> = Vec::with_capacity(n);
        for _ in 0..n {
            // Generate and normalize so cosine distance is meaningful
            let v = rand_vec(dim).normalize();
            training.push(v.data);
        }

        let mut index = IvfFlatIndex::new(dim, nlist, nprobe, DistanceMetric::Cosine);
        index.train(&training);

        for (i, v) in training.iter().enumerate() {
            index.add(i, v.clone());
        }

        // Query with one of the training vectors
        let query = &training[42];
        let results = index.search(query, 5);

        assert!(!results.is_empty(), "cosine search returned no results");
        // Should find itself
        assert_eq!(
            results[0].0, 42,
            "expected id 42 as top result with cosine metric, got {}",
            results[0].0
        );
        // Cosine distance of a vector with itself should be ~0
        assert!(
            results[0].1 < 1e-5,
            "expected near-zero cosine distance for self-match, got {}",
            results[0].1
        );
    }
}
