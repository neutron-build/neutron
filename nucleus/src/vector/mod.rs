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

pub mod tiered;
pub mod wal;

pub use wal::VectorWal;

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

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
        DistanceMetric::L2 => simd_l2_distance(&a.data, &b.data),
        DistanceMetric::Cosine => simd_cosine_distance(&a.data, &b.data),
        DistanceMetric::InnerProduct => {
            -simd_dot_product(&a.data, &b.data) // Negate so lower = more similar
        }
    }
}

// ============================================================================
// SIMD-accelerated distance calculations (unrolled 8-wide f32 lanes)
// ============================================================================

/// Dot product of two f32 slices, unrolled in chunks of 8 for ILP.
///
/// Processes 8 elements per loop iteration to exploit instruction-level
/// parallelism — the compiler maps these to SIMD (SSE/AVX) on x86 and
/// NEON on ARM when optimisation is enabled.
#[inline]
pub fn simd_dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "slice lengths must match");
    // min(): every raw-pointer access below uses indices < n, so clamping n to
    // the shorter slice makes the unsafe blocks sound for ANY input (not just the
    // debug_assert'd equal-length case) — no out-of-bounds read is possible.
    let n = a.len().min(b.len());
    // Use 4 accumulators to break dependency chains and maximise ILP.
    let mut sum0: f32 = 0.0;
    let mut sum1: f32 = 0.0;
    let mut sum2: f32 = 0.0;
    let mut sum3: f32 = 0.0;

    let chunks = n / 8;
    let remainder = n % 8;

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    for i in 0..chunks {
        let base = i * 8;
        // SAFETY: base+7 < chunks*8 <= n, and both slices have length n.
        unsafe {
            let a0 = *pa.add(base);
            let a1 = *pa.add(base + 1);
            let a2 = *pa.add(base + 2);
            let a3 = *pa.add(base + 3);
            let a4 = *pa.add(base + 4);
            let a5 = *pa.add(base + 5);
            let a6 = *pa.add(base + 6);
            let a7 = *pa.add(base + 7);

            let b0 = *pb.add(base);
            let b1 = *pb.add(base + 1);
            let b2 = *pb.add(base + 2);
            let b3 = *pb.add(base + 3);
            let b4 = *pb.add(base + 4);
            let b5 = *pb.add(base + 5);
            let b6 = *pb.add(base + 6);
            let b7 = *pb.add(base + 7);

            sum0 += a0 * b0 + a4 * b4;
            sum1 += a1 * b1 + a5 * b5;
            sum2 += a2 * b2 + a6 * b6;
            sum3 += a3 * b3 + a7 * b7;
        }
    }

    // Handle remaining elements
    let tail_start = chunks * 8;
    for i in 0..remainder {
        // SAFETY: tail_start + i < n = min(a.len(), b.len()), so both reads are in bounds.
        unsafe {
            sum0 += *pa.add(tail_start + i) * *pb.add(tail_start + i);
        }
    }

    sum0 + sum1 + sum2 + sum3
}

/// L2 (Euclidean) distance between two f32 slices, unrolled in chunks of 8.
///
/// Computes `sqrt(sum((a[i] - b[i])^2))` using the same 4-accumulator
/// technique as [`simd_dot_product`].
#[inline]
pub fn simd_l2_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "slice lengths must match");
    // min(): every raw-pointer access below uses indices < n, so clamping n to
    // the shorter slice makes the unsafe blocks sound for ANY input (not just the
    // debug_assert'd equal-length case) — no out-of-bounds read is possible.
    let n = a.len().min(b.len());
    let mut sum0: f32 = 0.0;
    let mut sum1: f32 = 0.0;
    let mut sum2: f32 = 0.0;
    let mut sum3: f32 = 0.0;

    let chunks = n / 8;
    let remainder = n % 8;

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    for i in 0..chunks {
        let base = i * 8;
        // SAFETY: base+7 < chunks*8 <= n = min(a.len(), b.len()); reads in bounds.
        unsafe {
            let d0 = *pa.add(base) - *pb.add(base);
            let d1 = *pa.add(base + 1) - *pb.add(base + 1);
            let d2 = *pa.add(base + 2) - *pb.add(base + 2);
            let d3 = *pa.add(base + 3) - *pb.add(base + 3);
            let d4 = *pa.add(base + 4) - *pb.add(base + 4);
            let d5 = *pa.add(base + 5) - *pb.add(base + 5);
            let d6 = *pa.add(base + 6) - *pb.add(base + 6);
            let d7 = *pa.add(base + 7) - *pb.add(base + 7);

            sum0 += d0 * d0 + d4 * d4;
            sum1 += d1 * d1 + d5 * d5;
            sum2 += d2 * d2 + d6 * d6;
            sum3 += d3 * d3 + d7 * d7;
        }
    }

    let tail_start = chunks * 8;
    for i in 0..remainder {
        // SAFETY: tail_start + i < n = min(a.len(), b.len()); reads in bounds.
        unsafe {
            let d = *pa.add(tail_start + i) - *pb.add(tail_start + i);
            sum0 += d * d;
        }
    }

    (sum0 + sum1 + sum2 + sum3).sqrt()
}

/// Cosine distance between two f32 slices: `1 - (a·b)/(|a||b|)`.
///
/// Computes dot product, norm-a-squared, and norm-b-squared in a single
/// fused pass over the data (one pass instead of three), unrolled 8-wide.
#[inline]
pub fn simd_cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "slice lengths must match");
    // min(): every raw-pointer access below uses indices < n, so clamping n to
    // the shorter slice makes the unsafe blocks sound for ANY input (not just the
    // debug_assert'd equal-length case) — no out-of-bounds read is possible.
    let n = a.len().min(b.len());
    // Three quantities computed in parallel: dot, norm_a^2, norm_b^2.
    // Each uses 2 accumulators (8 lanes / 4 groups, doubled up).
    let mut dot0: f32 = 0.0;
    let mut dot1: f32 = 0.0;
    let mut na0: f32 = 0.0;
    let mut na1: f32 = 0.0;
    let mut nb0: f32 = 0.0;
    let mut nb1: f32 = 0.0;

    let chunks = n / 8;
    let remainder = n % 8;

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    for i in 0..chunks {
        let base = i * 8;
        // SAFETY: n = a.len().min(b.len()); base = i*8 with i < n/8, so each pa/pb.add(base+j) (j<8) stays < n <= both slice lengths.
        unsafe {
            let a0 = *pa.add(base);
            let a1 = *pa.add(base + 1);
            let a2 = *pa.add(base + 2);
            let a3 = *pa.add(base + 3);
            let a4 = *pa.add(base + 4);
            let a5 = *pa.add(base + 5);
            let a6 = *pa.add(base + 6);
            let a7 = *pa.add(base + 7);

            let b0 = *pb.add(base);
            let b1 = *pb.add(base + 1);
            let b2 = *pb.add(base + 2);
            let b3 = *pb.add(base + 3);
            let b4 = *pb.add(base + 4);
            let b5 = *pb.add(base + 5);
            let b6 = *pb.add(base + 6);
            let b7 = *pb.add(base + 7);

            dot0 += a0 * b0 + a1 * b1 + a2 * b2 + a3 * b3;
            dot1 += a4 * b4 + a5 * b5 + a6 * b6 + a7 * b7;

            na0 += a0 * a0 + a1 * a1 + a2 * a2 + a3 * a3;
            na1 += a4 * a4 + a5 * a5 + a6 * a6 + a7 * a7;

            nb0 += b0 * b0 + b1 * b1 + b2 * b2 + b3 * b3;
            nb1 += b4 * b4 + b5 * b5 + b6 * b6 + b7 * b7;
        }
    }

    let tail_start = chunks * 8;
    for i in 0..remainder {
        // SAFETY: n = a.len().min(b.len()); base = i*8 with i < n/8, so each pa/pb.add(base+j) (j<8) stays < n <= both slice lengths.
        unsafe {
            let ai = *pa.add(tail_start + i);
            let bi = *pb.add(tail_start + i);
            dot0 += ai * bi;
            na0 += ai * ai;
            nb0 += bi * bi;
        }
    }

    let dot = dot0 + dot1;
    let norm_a = (na0 + na1).sqrt();
    let norm_b = (nb0 + nb1).sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }
    1.0 - dot / (norm_a * norm_b)
}

/// Compute distance between two raw f32 slices using the given metric.
///
/// This avoids constructing [`Vector`] wrappers and is used on the
/// hot path inside [`IvfFlatIndex`].
#[inline]
pub fn distance_raw(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::L2 => simd_l2_distance(a, b),
        DistanceMetric::Cosine => simd_cosine_distance(a, b),
        DistanceMetric::InnerProduct => -simd_dot_product(a, b),
    }
}

/// Issue a software prefetch hint for read access to a memory address.
///
/// This is a no-op on architectures that don't support prefetch, and a
/// hint only — the CPU is free to ignore it.
#[inline(always)]
fn prefetch_read_data<T>(ptr: *const T) {
    // SAFETY: _mm_prefetch is a pure CPU hint — it never dereferences `ptr` and
    // cannot fault for any address; the intrinsic is unconditionally available
    // on the gated target_arch.
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "x86")]
    unsafe {
        std::arch::x86::_mm_prefetch(ptr as *const i8, std::arch::x86::_MM_HINT_T0);
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
    {
        let _ = ptr; // suppress unused warning on other architectures
    }
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
#[derive(Debug, Clone)]
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

/// Tag introducing the optional tombstone section that follows the HNSW
/// footer. The on-disk format is positional and carries no version byte, so
/// the section is appended behind this tag instead: a blob that ends at the
/// footer predates the section, and anything else trailing is corruption.
const TOMBSTONE_SECTION_TAG: u32 = 0x5453_4E48; // "HNST"

/// Tag introducing the optional PK-registry section that follows the
/// tombstone section. Same versioning rationale as the tombstone tag: a blob
/// that ends at the tombstones predates the registry and reads as
/// registry-absent, and only this one further tag is accepted after it.
const REGISTRY_SECTION_TAG: u32 = 0x5253_4E48; // "HNSR"

/// The minimal persisted form of the executor's PK registry, carried inside
/// the serialized HNSW blob so it is covered by the snapshot CRC. Only the
/// ground truth is stored — `node_to_pk` is derivable by inverting
/// `pk_to_node`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegistrySection {
    /// pk (bit-cast to u64) -> current live node id.
    pub pk_to_node: std::collections::HashMap<u64, u64>,
    /// Next node id to allocate — monotonic, never reused.
    pub next_node: u64,
    /// Nodes tombstoned since the last rebuild, for the compaction trigger.
    pub tombstones: u64,
}

/// HNSW (Hierarchical Navigable Small World) index.
#[derive(Clone)]
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

    /// Assign a layer for a new node — DETERMINISTICALLY, derived from the
    /// node id rather than a global RNG.
    ///
    /// HNSW layer assignment is the graph's skeleton: which nodes sit on the
    /// express levels. Drawing it from `rand::random()` made every build of
    /// the same corpus a different lottery — measured (BENCH_VS_QDRANT,
    /// 2026-08-20, n=50k clustered, m=16): the `ef` at which recall first hit
    /// 1.000 ranged 96→never-to-192→96 across four runs of identical work,
    /// with one query returning none of its true top-10 even at ef=256.
    /// Recall is not supposed to be a per-boot property of the database.
    ///
    /// Hashing the id (splitmix64 — the standard sequential-id finalizer)
    /// gives the same statistical layer distribution (uniform u, then the
    /// paper's floor(-ln(u) * 1/ln(M))) while making the graph a pure
    /// function of (ids, vectors, insertion order). A rebuilt index is then
    /// bit-identical, and a recall regression is reproducible from its seed
    /// instead of re-rolling on every run.
    ///
    /// Id-keying (not a counter) is deliberate: incremental maintenance
    /// re-inserts an updated row under the same PK-derived node id, and the
    /// re-inserted node keeps its layer — updates cannot reshuffle the
    /// hierarchy out from under live searches.
    fn random_layer(&self, id: u64) -> usize {
        // splitmix64 over the id — cheap, well-distributed for sequential ids.
        let mut z = id.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // Map to (0, 1]: log of 0 is inf, so never return exactly 0.
        let u = ((z >> 11) as f64 / (1u64 << 53) as f64).max(f64::MIN_POSITIVE);
        (-u.ln() * self.ml).floor() as usize
    }

    /// Insert a vector into the index.
    pub fn insert(&mut self, id: u64, vector: Vector) {
        // Re-inserting an id revives it: incremental UPDATE maintenance marks the
        // old posting deleted then re-inserts under the same (PK-derived) id, so
        // the id must not stay tombstoned or the updated row would vanish from
        // search results.
        self.deleted.remove(&id);
        let node_layer = self.random_layer(id);

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
        // (ef=1 per layer, per the paper's Algorithm 1).
        let mut ep = entry_id;
        for layer in (node_layer.saturating_add(1)..=self.max_layer).rev() {
            ep = self.greedy_search(ep, &vector, layer);
        }

        // Phase 2: From min(node_layer, max_layer) down to 0, do ef_construction
        // search. The paper's Algorithm 1 (line "ep ← W") carries the FULL
        // result set of each layer into the next layer's entry points, not
        // just the closest — a single closest entry reproduces the same
        // greedy-descent trap at construction time that query-time ef=1
        // descent has at search time: once the descent parks in one basin,
        // every lower layer is explored only from inside it, and the graph
        // inherits the blind spot.
        let mut eps = vec![ep];
        let top = node_layer.min(self.max_layer);
        for layer in (0..=top).rev() {
            let candidates =
                self.search_layer_multi(&eps, &vector, self.config.ef_construction, layer);

            // Update entries to the full result set for the next layer down.
            eps = candidates.iter().map(|c| c.id).collect();

            // Select M best neighbors
            let m = if layer == 0 {
                self.config.m_max0
            } else {
                self.config.m
            };
            // Don't connect to self, then pick M diverse neighbours (Alg. 4)
            // rather than the M closest — bridge edges keep the graph navigable.
            let pool: Vec<Candidate> = candidates.into_iter().filter(|c| c.id != id).collect();
            let selected: Vec<u64> = self.select_neighbors_heuristic(&pool, m);

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

    /// Diversifying neighbour selection — HNSW paper Algorithm 4
    /// (SELECT-NEIGHBORS-HEURISTIC). Given candidates sorted ascending by their
    /// distance to `base` (each `Candidate.dist` is dist(candidate, base)),
    /// keep a candidate only if it is closer to `base` than to every neighbour
    /// already kept. This drops redundant same-direction links and preserves
    /// long-range "bridge" edges, which is what keeps the graph navigable —
    /// naive take-M-closest starves inter-cluster bridges and collapses recall
    /// on structured (embedding-like) data.
    ///
    /// If the heuristic under-fills `m` slots (it can be aggressive), the
    /// remaining slots are back-filled with the nearest not-yet-kept candidates
    /// (the paper's `keepPrunedConnections`) so nodes stay well-connected.
    /// `candidates[i].dist` must already be dist(candidate_i, base); the base
    /// vector itself is not needed here because those distances are precomputed.
    fn select_neighbors_heuristic(&self, candidates: &[Candidate], m: usize) -> Vec<u64> {
        if m == 0 {
            return Vec::new();
        }
        let mut kept: Vec<(u64, &Vector)> = Vec::with_capacity(m);
        for c in candidates {
            if kept.len() >= m {
                break;
            }
            let cand_vec = match self.nodes.get(&c.id) {
                Some(node) => &node.vector,
                None => continue,
            };
            // Keep iff candidate is nearer to `base` than to any kept neighbour.
            let diverse = kept
                .iter()
                .all(|(_, kv)| c.dist < distance(cand_vec, kv, self.config.metric));
            if diverse {
                kept.push((c.id, cand_vec));
            }
        }
        // Back-fill toward `m` with the nearest candidates we skipped.
        if kept.len() < m {
            for c in candidates {
                if kept.len() >= m {
                    break;
                }
                if kept.iter().any(|(id, _)| *id == c.id) {
                    continue;
                }
                if let Some(node) = self.nodes.get(&c.id) {
                    kept.push((c.id, &node.vector));
                }
            }
        }
        kept.into_iter().map(|(id, _)| id).collect()
    }

    /// Greedy search at a single layer — find the closest node to query.
    fn greedy_search(&self, start: u64, query: &Vector, layer: usize) -> u64 {
        let mut current = start;
        let mut current_dist = self.dist(current, query);

        loop {
            let mut improved = false;
            if let Some(node) = self.nodes.get(&current)
                && layer < node.neighbors.len()
            {
                let neighbors = &node.neighbors[layer];
                for (idx, &neighbor_id) in neighbors.iter().enumerate() {
                    // Prefetch the next neighbor's vector data
                    if idx + 1 < neighbors.len()
                        && let Some(next_node) = self.nodes.get(&neighbors[idx + 1])
                        && !next_node.vector.data.is_empty()
                    {
                        prefetch_read_data(next_node.vector.data.as_ptr());
                    }
                    let d = self.dist(neighbor_id, query);
                    if d < current_dist {
                        current = neighbor_id;
                        current_dist = d;
                        improved = true;
                    }
                }
            }
            if !improved {
                break;
            }
        }

        current
    }

    /// Multi-start variant of layer search — the paper's
    /// SEARCH-LAYER takes its entry points as a set (`C ← ep`), and both
    /// directions need it:
    ///
    /// - Construction carries each layer's result set down as the next
    ///   layer's entries (Algorithm 1, `ep ← W`).
    /// - Query time seeds layer 0 with a beam of upper-layer results instead
    ///   of the single node a greedy ef=1 descent ends on. A greedy descent
    ///   on the sparse upper layers (≈ n/16 nodes at layer 1, fewer above)
    ///   parks in a local minimum and hands layer 0 one entry point that can
    ///   sit in the wrong cluster entirely — measured on the clustered
    ///   recall bench (BENCH_VS_QDRANT shape): descent stuck 8.6 away from
    ///   the query while every true top-10 sat at ≤1.04, an inter-cluster
    ///   valley of 8.8 that the layer-0 admission filter (`add only if
    ///   closer than the current worst result`) then refuses to cross at any
    ///   practical `ef` — one query returned none of its true top-10 even at
    ///   ef=256. Seeding layer 0 across the whole upper-layer beam makes the
    ///   search multi-basin from its first step, which is what bounds the
    ///   valley width by the beam, not by luck.
    fn search_layer_multi(
        &self,
        starts: &[u64],
        query: &Vector,
        ef: usize,
        layer: usize,
    ) -> Vec<Candidate> {
        if starts.is_empty() {
            return Vec::new();
        }
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new(); // min-heap
        let mut results = BinaryHeap::new(); // max-heap (worst at top)

        for &start in starts {
            if !visited.insert(start) {
                continue;
            }
            let start_dist = self.dist(start, query);
            candidates.push(Candidate {
                id: start,
                dist: start_dist,
            });
            results.push(MaxCandidate {
                id: start,
                dist: start_dist,
            });
            if results.len() > ef {
                results.pop();
            }
        }

        while let Some(closest) = candidates.pop() {
            let worst_dist = results.peek().map(|r| r.dist).unwrap_or(f32::MAX);
            if closest.dist > worst_dist {
                break;
            }

            if let Some(node) = self.nodes.get(&closest.id)
                && layer < node.neighbors.len()
            {
                let neighbors = &node.neighbors[layer];
                for (idx, &neighbor_id) in neighbors.iter().enumerate() {
                    if visited.insert(neighbor_id) {
                        // Prefetch the *next* unvisited neighbor's vector
                        // data into L1 cache so it's warm when we reach it.
                        if idx + 1 < neighbors.len() {
                            let next_id = neighbors[idx + 1];
                            if let Some(next_node) = self.nodes.get(&next_id)
                                && !next_node.vector.data.is_empty()
                            {
                                prefetch_read_data(next_node.vector.data.as_ptr());
                            }
                        }

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

        // Score all neighbours by distance to this node, sort ascending, then
        // prune with the diversifying heuristic (not plain take-closest) so the
        // surviving edges keep their spread of directions — otherwise repeated
        // pruning strips every bridge and the node ends up locked to one cluster.
        let mut scored: Vec<Candidate> = neighbors
            .into_iter()
            .map(|nid| Candidate {
                id: nid,
                dist: self.dist(nid, &vector),
            })
            .collect();
        scored.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
        let pruned = self.select_neighbors_heuristic(&scored, max_conn);

        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.neighbors[layer] = pruned;
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
    ///
    /// The DEFAULT beam width scales with index size: a fixed ef that is
    /// ample at 10k–100k vectors starts fully trapping occasional queries in
    /// the wrong cluster at a few hundred thousand (measured: clustered data,
    /// ef=64 → min-recall 0.0 at 300k/1M while ef=128/256 recovers to ≥0.9
    /// with recall ≥0.996). Postgres-style "document a fixed default" would
    /// leave out-of-the-box zero-recall queries at scale — instead the
    /// default follows n (n/2048, clamped to [configured ef, 512]) so recall
    /// floors hold at every size, and an explicit `SET hnsw.ef_search` (the
    /// `search_ef` path) still wins in BOTH directions for callers choosing
    /// their own point on the recall/latency frontier.
    pub fn search(&self, query: &Vector, k: usize) -> Vec<(u64, f32)> {
        let auto = self
            .config
            .ef_search
            .max((self.nodes.len() / 2048).min(512));
        self.search_ef(query, k, auto)
    }

    /// Upper-layer beam cap for query-time descent. The upper layers are
    /// sparse (≈ n/16 nodes on layer 1, geometrically fewer above), so a
    /// modest beam already spans many clusters; carrying a full `ef`-wide
    /// beam through them would multiply distance evaluations per query for
    /// little recall gain. 32 spans ~10 clusters at the bench shape
    /// (n=50k, 256 clusters) and bounds the descent at ~32×M evaluations.
    const UPPER_LAYER_BEAM: usize = 32;

    /// Descend the upper layers with a beam instead of a single greedy
    /// point, returning the layer-0 entry seed set. Beam width here is
    /// `max(min(ef, UPPER_LAYER_BEAM), k)`; see
    /// [`Self::search_layer_multi`] for why layer 0 must be seeded wide.
    fn descend_entries(&self, query: &Vector, ef: usize, k: usize) -> Vec<u64> {
        let entry = match self.entry_point {
            Some(id) => id,
            None => return Vec::new(),
        };
        let beam = ef.min(Self::UPPER_LAYER_BEAM).max(k);
        let mut entries = vec![entry];
        for layer in (1..=self.max_layer).rev() {
            let cands = self.search_layer_multi(&entries, query, beam, layer);
            if cands.is_empty() {
                break;
            }
            entries = cands.iter().map(|c| c.id).collect();
        }
        entries
    }

    /// Like [`search`] but with an explicit layer-0 beam width `ef` for this one
    /// query, overriding the configured default. `ef` is the recall/latency
    /// dial: a larger beam explores more of the graph before committing to the
    /// top-k, so recall rises (toward exact) at the cost of more distance
    /// evaluations. The effective beam is always at least `k`.
    pub fn search_ef(&self, query: &Vector, k: usize, ef: usize) -> Vec<(u64, f32)> {
        if self.nodes.is_empty() || self.entry_point.is_none() {
            return vec![];
        }

        // Phase 1: beam descent from top to layer 1 (see `descend_entries`)
        let entries = self.descend_entries(query, ef, k);

        // Phase 2: ef-bounded search at layer 0, seeded by the full beam
        let candidates = self.search_layer_multi(&entries, query, ef.max(k), 0);

        candidates
            .into_iter()
            .filter(|c| !self.deleted.contains(&c.id))
            .take(k)
            .map(|c| (c.id, c.dist))
            .collect()
    }

    /// Search for the k nearest neighbors that pass a filter predicate.
    ///
    /// Uses an oversampling strategy: search with a larger ef to find more
    /// candidates, then apply the filter and return the top-k passing results.
    /// If the first pass doesn't yield k results, the search retries with
    /// progressively larger ef values (up to 4x) to maintain recall.
    ///
    /// The `filter` closure receives a vector ID and returns `true` if the
    /// vector should be included in results. This allows the caller to check
    /// arbitrary predicates (MVCC visibility, WHERE clauses, etc.) without
    /// coupling the index to the storage engine.
    pub fn search_filtered<F>(&self, query: &Vector, k: usize, filter: F) -> Vec<(u64, f32)>
    where
        F: Fn(u64) -> bool,
    {
        // Same size-scaled default beam as `search` (see its doc comment).
        let auto = self
            .config
            .ef_search
            .max((self.nodes.len() / 2048).min(512));
        self.search_filtered_ef(query, k, auto, filter)
    }

    /// Like [`search_filtered`] but with an explicit base beam width `ef` for
    /// this one query (the oversampling multipliers and guaranteed-recall
    /// fallbacks apply on top of it, exactly as with the configured default).
    pub fn search_filtered_ef<F>(
        &self,
        query: &Vector,
        k: usize,
        ef: usize,
        filter: F,
    ) -> Vec<(u64, f32)>
    where
        F: Fn(u64) -> bool,
    {
        if self.nodes.is_empty() || self.entry_point.is_none() || k == 0 {
            return vec![];
        }

        // Phase 1: beam descent from top to layer 1 (see `descend_entries`)
        let entries = self.descend_entries(query, ef, k);

        // Phase 2: Oversampling search at layer 0.
        // Start with 4x oversampling and increase if needed.
        let base_ef = ef.max(k);
        for oversample in [4, 8, 16] {
            let ef = base_ef * oversample;
            let candidates = self.search_layer_multi(&entries, query, ef, 0);

            let results: Vec<(u64, f32)> = candidates
                .into_iter()
                .filter(|c| !self.deleted.contains(&c.id) && filter(c.id))
                .take(k)
                .map(|c| (c.id, c.dist))
                .collect();

            if results.len() >= k {
                return results;
            }
            if ef >= self.nodes.len() {
                // A graph traversal cannot reach disconnected layer-0
                // components no matter how large ef gets — returning short
                // results here skipped the guaranteed-recall fallback below
                // (silently dropping filter-passing matches). Stop
                // oversampling and use the fallbacks instead.
                break;
            }
        }

        // Fallback: search with ef = total nodes (brute-force through graph).
        let ef = self.nodes.len();
        let candidates = self.search_layer_multi(&entries, query, ef, 0);
        let graph_results: Vec<(u64, f32)> = candidates
            .into_iter()
            .filter(|c| !self.deleted.contains(&c.id) && filter(c.id))
            .take(k)
            .map(|c| (c.id, c.dist))
            .collect();
        if graph_results.len() >= k {
            return graph_results;
        }

        // Guaranteed-recall fallback: graph traversal may not reach every node
        // (e.g. a disconnected layer-0 component under a highly selective
        // filter), so it can return < k even when k matches exist. Linear-scan
        // all non-deleted, filter-passing nodes and return the exact top-k. This
        // is O(n) but only runs when the graph search came up short.
        let mut all: Vec<(u64, f32)> = self
            .nodes
            .iter()
            .filter(|(id, _)| !self.deleted.contains(id) && filter(**id))
            .map(|(id, node)| (*id, distance(query, &node.vector, self.config.metric)))
            .collect();
        all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        all.truncate(k);
        all
    }

    /// Mark a vector ID as deleted. It will be excluded from search results.
    /// One past the highest node id this index has ever used, counting
    /// tombstoned ids.
    ///
    /// A reopened index carries node ids that may outrun the persisted
    /// registry's allocator (delta records logged after the checkpoint hold
    /// ids the checkpoint-time registry never saw), so recovery seeds
    /// `next_node` from this floor and never below it. Starting at zero made
    /// the first post-reopen insert allocate an id the recovered graph was
    /// already using, and `insert` overwrites in place: one acknowledged
    /// vector lost per collision, silently, with the row still present in the
    /// base table.
    ///
    /// Tombstoned ids count. They are persisted, so handing one back to a
    /// new vector would file it under a standing tombstone and make it
    /// invisible to search the moment it was written.
    pub fn next_free_node_id(&self) -> u64 {
        self.nodes
            .keys()
            .chain(self.deleted.iter())
            .copied()
            .max()
            .map_or(0, |highest| highest + 1)
    }

    pub fn mark_deleted(&mut self, id: u64) {
        self.deleted.insert(id);
    }

    /// The vector stored under `id`, if the node exists. Read-only accessor
    /// for callers that need the payload before mutating the node — a
    /// transaction capturing an undo record for a delete, for instance.
    pub fn vector_of(&self, id: u64) -> Option<Vector> {
        self.nodes.get(&id).map(|n| n.vector.clone())
    }

    /// Whether `id` carries a delete tombstone.
    pub fn is_deleted(&self, id: u64) -> bool {
        self.deleted.contains(&id)
    }

    /// Number of indexed vectors (including deleted).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Number of tombstoned ids (ids never shift, so this is the drift signal
    /// between positional ids and a live scan).
    pub fn tombstone_count(&self) -> usize {
        self.deleted.len()
    }

    /// Ids of live (inserted, not tombstoned) vectors. `len` counts
    /// tombstoned nodes too, so it cannot observe a resurrected delete —
    /// which is exactly what a recovery probe needs to see.
    pub fn live_ids(&self) -> std::collections::BTreeSet<u64> {
        self.nodes
            .keys()
            .filter(|id| !self.deleted.contains(id))
            .copied()
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Dimensionality of the indexed vectors (0 if the index is empty).
    ///
    /// All vectors in an index share one dimension, so sampling any node is
    /// sufficient. Used when writing a WAL checkpoint snapshot, which records
    /// `dims` as recovery metadata.
    pub fn dims(&self) -> usize {
        self.nodes
            .values()
            .next()
            .map(|n| n.vector.dim())
            .unwrap_or(0)
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
    ///
    /// `registry` is the PK-keyed maintenance registry to persist alongside
    /// the graph (`None` for a blob without one — the pre-F1b shape).
    pub fn serialize(&self, registry: Option<&RegistrySection>) -> Vec<u8> {
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

        // Tombstones, appended after the footer behind a tag rather than
        // versioned into the header, so a blob written before this section
        // existed still loads: it ends at the footer and yields an empty set,
        // which is what it meant. Until this was written, every tombstone
        // standing at checkpoint time was dropped, and a deleted vector became
        // searchable again after reopen. Sorted, so the encoding is
        // deterministic for a given set.
        buf.extend_from_slice(&TOMBSTONE_SECTION_TAG.to_le_bytes());
        let mut tombstones: Vec<u64> = self.deleted.iter().copied().collect();
        tombstones.sort_unstable();
        buf.extend_from_slice(&(tombstones.len() as u32).to_le_bytes());
        for id in tombstones {
            buf.extend_from_slice(&id.to_le_bytes());
        }

        // PK registry, appended behind its own tag by the same reasoning as
        // the tombstones above: a blob that ends at the tombstones predates
        // the section and decodes as registry-absent (an empty registry),
        // which is what it meant. Entries sorted by pk so the encoding is
        // deterministic for a given map.
        if let Some(reg) = registry {
            buf.extend_from_slice(&REGISTRY_SECTION_TAG.to_le_bytes());
            buf.extend_from_slice(&reg.next_node.to_le_bytes());
            buf.extend_from_slice(&reg.tombstones.to_le_bytes());
            buf.extend_from_slice(&(reg.pk_to_node.len() as u32).to_le_bytes());
            let mut entries: Vec<(u64, u64)> =
                reg.pk_to_node.iter().map(|(&p, &n)| (p, n)).collect();
            entries.sort_unstable();
            for (pk, node) in entries {
                buf.extend_from_slice(&pk.to_le_bytes());
                buf.extend_from_slice(&node.to_le_bytes());
            }
        }

        buf
    }

    /// Deserialize an HNSW index from bytes, discarding any persisted PK
    /// registry. See [`Self::deserialize_with_registry`].
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        Ok(Self::deserialize_with_registry(data)?.0)
    }

    /// Deserialize an HNSW index and the PK registry persisted inside its
    /// blob, when there is one (`None` for blobs written before the registry
    /// section existed — an empty registry, which is faithful).
    pub fn deserialize_with_registry(
        data: &[u8],
    ) -> Result<(Self, Option<RegistrySection>), String> {
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

        let m = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| "truncated m field")?,
        );
        let m = m as usize;
        pos += 4;
        let ef_search = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| "truncated ef_search")?,
        );
        let ef_search = ef_search as usize;
        pos += 4;
        let num_nodes = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| "truncated num_nodes")?,
        );
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
            let id = u64::from_le_bytes(
                data[pos..pos + 8]
                    .try_into()
                    .map_err(|_| "truncated node id")?,
            );
            pos += 8;
            let dim =
                u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| "truncated dim")?);
            let dim = dim as usize;
            pos += 4;

            if pos + dim * 4 > data.len() {
                return Err("unexpected end of data reading vector".into());
            }
            let mut vec_data = Vec::with_capacity(dim);
            for _ in 0..dim {
                vec_data.push(f32::from_le_bytes(
                    data[pos..pos + 4]
                        .try_into()
                        .map_err(|_| "truncated vector element")?,
                ));
                pos += 4;
            }

            if pos + 4 > data.len() {
                return Err("unexpected end of data reading num_layers".into());
            }
            let num_layers = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| "truncated num_layers")?,
            );
            let num_layers = num_layers as usize;
            pos += 4;

            // Bound the count against the data actually present, exactly as the
            // `dim` and `nn` reads above and below already do. Without this,
            // `num_layers` was a raw u32 straight from the file feeding
            // `Vec::<Vec<u64>>::with_capacity` — u32::MAX layers asks for
            // 103 GB, and a Rust allocation failure ABORTS: no unwind, no Err,
            // no log, SIGABRT. On a startup path that is a boot crash-loop with
            // no diagnostic.
            //
            // Found by this module's own corruption test failing on Linux CI
            // while passing on macOS, which overcommits and let the 103 GB
            // reservation succeed. Same class as the counts bounded in
            // `kv::collections_wal`; the audit filed it there and it lives here
            // too. Each layer costs at least its own 4-byte neighbour count, so
            // a layer count larger than the remaining bytes / 4 cannot be real.
            if num_layers > (data.len() - pos) / 4 {
                return Err("num_layers exceeds remaining data".into());
            }
            let mut neighbors = Vec::with_capacity(num_layers);
            for _ in 0..num_layers {
                if pos + 4 > data.len() {
                    return Err("unexpected end of data reading neighbor count".into());
                }
                let nn = u32::from_le_bytes(
                    data[pos..pos + 4]
                        .try_into()
                        .map_err(|_| "truncated neighbor count")?,
                );
                let nn = nn as usize;
                pos += 4;
                if pos + nn * 8 > data.len() {
                    return Err("unexpected end of data reading neighbor ids".into());
                }
                let mut layer = Vec::with_capacity(nn);
                for _ in 0..nn {
                    layer.push(u64::from_le_bytes(
                        data[pos..pos + 8]
                            .try_into()
                            .map_err(|_| "truncated neighbor id")?,
                    ));
                    pos += 8;
                }
                neighbors.push(layer);
            }

            nodes.insert(
                id,
                HnswNode {
                    _id: id,
                    vector: Vector::new(vec_data),
                    neighbors,
                },
            );
        }

        if pos + 12 > data.len() {
            return Err("unexpected end of data reading footer".into());
        }
        let max_layer = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| "truncated max_layer")?,
        );
        let max_layer = max_layer as usize;
        pos += 4;
        let entry_raw = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| "truncated entry_point")?,
        );
        // The footer read used to end the function, so it never advanced `pos`.
        // The tombstone section below is positioned relative to it, so it must.
        pos += 8;
        let entry_point = if entry_raw == u64::MAX {
            None
        } else {
            Some(entry_raw)
        };

        // Tombstones. Absent from blobs written before this section existed, in
        // which case `pos` already sits at the end and the set is empty. Any
        // other trailing bytes are corruption, not an old file: guessing at
        // them would silently misparse a live index, which is a worse failure
        // than the resurrection bug this section fixes.
        let mut deleted = HashSet::new();
        let mut registry: Option<RegistrySection> = None;
        if pos < data.len() {
            if data.len() - pos < 8 {
                return Err(
                    "trailing bytes after the footer are too short for a tombstone section".into(),
                );
            }
            let tag = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| "truncated tombstone section tag")?,
            );
            if tag != TOMBSTONE_SECTION_TAG {
                return Err(
                    "trailing bytes after the footer do not carry the tombstone section tag".into(),
                );
            }
            pos += 4;
            let num_deleted = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| "truncated num_deleted")?,
            ) as usize;
            pos += 4;
            // Bound the count against the bytes actually present, exactly as the
            // `dim` and `num_layers` reads above already do. A raw u32 taken from
            // a file and fed to an allocation ABORTS the process on Linux -- no
            // unwind, no Err, no log, SIGABRT -- while silently succeeding on an
            // overcommitting host, so this must be checked and not merely sized.
            if num_deleted > (data.len() - pos) / 8 {
                return Err("num_deleted exceeds remaining data".into());
            }
            for _ in 0..num_deleted {
                deleted.insert(u64::from_le_bytes(
                    data[pos..pos + 8]
                        .try_into()
                        .map_err(|_| "truncated deleted id")?,
                ));
                pos += 8;
            }

            // PK registry: the one section allowed to follow the tombstones.
            // A blob that ended above predates it and stays registry-absent.
            if pos < data.len() {
                if data.len() - pos < 4 {
                    return Err(
                        "trailing bytes after the tombstone section are too short for a \
                         registry section tag"
                            .into(),
                    );
                }
                let tag = u32::from_le_bytes(
                    data[pos..pos + 4]
                        .try_into()
                        .map_err(|_| "truncated registry section tag")?,
                );
                if tag != REGISTRY_SECTION_TAG {
                    return Err(
                        "trailing bytes after the tombstone section do not carry the \
                         registry section tag"
                            .into(),
                    );
                }
                pos += 4;
                if data.len() - pos < 20 {
                    return Err("truncated registry section header".into());
                }
                let next_node = u64::from_le_bytes(
                    data[pos..pos + 8]
                        .try_into()
                        .map_err(|_| "truncated registry next_node")?,
                );
                pos += 8;
                let tombstone_count = u64::from_le_bytes(
                    data[pos..pos + 8]
                        .try_into()
                        .map_err(|_| "truncated registry tombstone count")?,
                );
                pos += 8;
                let num_entries = u32::from_le_bytes(
                    data[pos..pos + 4]
                        .try_into()
                        .map_err(|_| "truncated registry entry count")?,
                ) as usize;
                pos += 4;
                // Same bound as every other count above: each entry costs 16
                // bytes, and an unbounded `with_capacity` fed by file data
                // aborts the process on Linux.
                if num_entries > (data.len() - pos) / 16 {
                    return Err("registry entry count exceeds remaining data".into());
                }
                let mut pk_to_node = std::collections::HashMap::with_capacity(num_entries);
                for _ in 0..num_entries {
                    let pk = u64::from_le_bytes(
                        data[pos..pos + 8]
                            .try_into()
                            .map_err(|_| "truncated registry pk".to_string())?,
                    );
                    pos += 8;
                    let node = u64::from_le_bytes(
                        data[pos..pos + 8]
                            .try_into()
                            .map_err(|_| "truncated registry node".to_string())?,
                    );
                    pos += 8;
                    pk_to_node.insert(pk, node);
                }
                registry = Some(RegistrySection {
                    pk_to_node,
                    next_node,
                    tombstones: tombstone_count,
                });
                if pos != data.len() {
                    return Err("trailing bytes after the registry section".into());
                }
            }
        }

        let ml = 1.0 / (config.m as f64).ln();
        Ok((
            Self {
                config,
                nodes,
                max_layer,
                entry_point,
                ml,
                deleted,
            },
            registry,
        ))
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
#[derive(Clone)]
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
    /// Wrong-dimension training vectors are skipped with an error log rather
    /// than panicking (F-019 pattern: malformed data must not take the server
    /// down). If nothing usable remains, the index is left untrained.
    pub fn train(&mut self, vectors: &[Vec<f32>]) {
        // Owned copies: the body below consumes `vectors` both as
        // `iter().take(k).cloned()` (annotated Vec<Vec<f32>>) and as
        // `iter().enumerate()` (v: &Vec<f32>) — a Vec<&Vec<f32>> breaks both.
        let vectors: Vec<Vec<f32>> = vectors
            .iter()
            .enumerate()
            .filter(|(i, v)| {
                let ok = v.len() == self.dimension;
                if !ok {
                    tracing::error!(
                        target: "nucleus::vector",
                        "IVFFlat train: skipping vector {i} with dimension {} != {}",
                        v.len(),
                        self.dimension
                    );
                }
                ok
            })
            .map(|(_, v)| v.clone())
            .collect();
        if vectors.is_empty() {
            tracing::error!(
                target: "nucleus::vector",
                "IVFFlat train: no training vector has dimension {}; leaving index untrained",
                self.dimension
            );
            return;
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
    /// Dimension mismatch or an untrained index logs and skips (the row stays
    /// in the table, unindexed) rather than panicking the server — a panic
    /// here is a DoS reachable from plain SQL.
    pub fn add(&mut self, id: usize, vector: Vec<f32>) {
        if vector.len() != self.dimension {
            tracing::error!(
                target: "nucleus::vector",
                "IVFFlat add: vector dimension {} != index dimension {} (id {id} skipped)",
                vector.len(),
                self.dimension
            );
            return;
        }
        if self.centroids.is_empty() {
            tracing::error!(
                target: "nucleus::vector",
                "IVFFlat add: index is not trained (id {id} skipped)"
            );
            return;
        }

        let cluster = self.nearest_centroid(&vector, &self.centroids);
        self.inverted_lists[cluster].push((id, vector));
    }

    /// Search for the `k` nearest neighbors of `query`.
    ///
    /// Returns `(id, distance)` pairs sorted by ascending distance.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(usize, f32)> {
        // A dimension mismatch is a caller bug, but it must not panic the server
        // (DoS). Log and return no results.
        if query.len() != self.dimension {
            tracing::error!(
                target: "nucleus::vector",
                "IVFFlat search: query dimension {} != index dimension {}",
                query.len(),
                self.dimension
            );
            return Vec::new();
        }
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
        centroid_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

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

    /// Search for the `k` nearest neighbors that pass a filter predicate.
    ///
    /// The `filter` closure receives a vector ID and returns `true` if the
    /// vector should be included in results.
    pub fn search_filtered<F>(&self, query: &[f32], k: usize, filter: F) -> Vec<(usize, f32)>
    where
        F: Fn(usize) -> bool,
    {
        // Same guard as `search` (F-019): a dimension mismatch must not panic
        // the server. Log and return no results.
        if query.len() != self.dimension {
            tracing::error!(
                target: "nucleus::vector",
                "IVFFlat search_filtered: query dimension {} != index dimension {}",
                query.len(),
                self.dimension
            );
            return Vec::new();
        }
        if self.centroids.is_empty() {
            return Vec::new();
        }

        let mut centroid_dists: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, self.compute_distance(query, c)))
            .collect();
        centroid_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        let nprobe = self.nprobe.min(centroid_dists.len());

        let mut candidates: Vec<(usize, f32)> = Vec::new();
        for &(cluster_idx, _) in centroid_dists.iter().take(nprobe) {
            for (id, vec) in &self.inverted_lists[cluster_idx] {
                if self.deleted.contains(id) || !filter(*id) {
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
    ///
    /// Uses the SIMD-accelerated [`distance_raw`] path — no Vector allocation.
    #[inline]
    fn compute_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        distance_raw(a, b, self.metric)
    }

    /// Number of vectors stored in the index.
    pub fn len(&self) -> usize {
        self.inverted_lists.iter().map(|l| l.len()).sum()
    }

    /// Number of tombstoned ids (ids never shift, so this is the drift signal
    /// between positional ids and a live scan).
    pub fn tombstone_count(&self) -> usize {
        self.deleted.len()
    }

    /// The distance metric this index was built with.
    pub fn metric(&self) -> DistanceMetric {
        self.metric
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

/// Parallel brute-force nearest neighbor search.
///
/// Partitions the vector store across available CPU cores using
/// `std::thread::scope`. Each thread computes distances for its chunk and
/// returns local top-k results; the caller merges and takes the global top-k.
///
/// Falls back to sequential [`exact_search`] when the dataset contains fewer
/// than 1000 vectors.
pub fn par_search_brute_force(
    vectors: &[(u64, Vector)],
    query: &Vector,
    k: usize,
    metric: DistanceMetric,
) -> Vec<(u64, f32)> {
    const PAR_THRESHOLD: usize = 1000;

    if vectors.len() < PAR_THRESHOLD {
        return exact_search(vectors, query, k, metric);
    }

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let chunk_size = vectors.len().div_ceil(cpus);

    let mut merged: Vec<(u64, f32)> = std::thread::scope(|s| {
        let handles: Vec<_> = vectors
            .chunks(chunk_size)
            .map(|chunk| {
                s.spawn(move || {
                    // Compute distances for this chunk
                    let mut local: Vec<(u64, f32)> = chunk
                        .iter()
                        .map(|(id, v)| (*id, distance(v, query, metric)))
                        .collect();
                    // Keep only local top-k to reduce merge work
                    local.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
                    local.truncate(k);
                    local
                })
            })
            .collect();

        let mut all = Vec::with_capacity(cpus * k);
        for h in handles {
            all.extend(h.join().unwrap());
        }
        all
    });

    merged.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    merged.truncate(k);
    merged
}

/// Search for multiple query vectors in parallel.
///
/// Each query is independent, so they are distributed across threads using
/// `std::thread::scope`. Uses [`exact_search`] per query internally.
pub fn par_batch_search(
    vectors: &[(u64, Vector)],
    queries: &[Vector],
    k: usize,
    metric: DistanceMetric,
) -> Vec<Vec<(u64, f32)>> {
    std::thread::scope(|s| {
        let handles: Vec<_> = queries
            .iter()
            .map(|query| s.spawn(move || exact_search(vectors, query, k, metric)))
            .collect();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ============================================================================
// WAL-aware helpers
// ============================================================================

/// Encode a [`DistanceMetric`] as a single byte for WAL/persistence.
pub fn metric_to_u8(m: DistanceMetric) -> u8 {
    match m {
        DistanceMetric::L2 => 0,
        DistanceMetric::Cosine => 1,
        DistanceMetric::InnerProduct => 2,
    }
}

/// Decode a byte back to a [`DistanceMetric`] (defaults to L2 for unknown values).
pub fn metric_from_u8(b: u8) -> DistanceMetric {
    match b {
        0 => DistanceMetric::L2,
        1 => DistanceMetric::Cosine,
        2 => DistanceMetric::InnerProduct,
        _ => DistanceMetric::L2,
    }
}

impl HnswIndex {
    /// Return the distance metric configured for this index.
    pub fn metric(&self) -> DistanceMetric {
        self.config.metric
    }

    /// Return the M parameter configured for this index.
    pub fn m(&self) -> usize {
        self.config.m
    }

    /// Return the ef_search parameter configured for this index.
    pub fn ef_search(&self) -> usize {
        self.config.ef_search
    }

    /// Evaluate a batch of candidate node IDs in parallel, computing distances
    /// to the query vector. Falls back to sequential evaluation when fewer than
    /// 100 candidates are provided.
    ///
    /// Returns `(node_id, distance)` pairs sorted by ascending distance.
    pub fn par_evaluate_candidates(
        &self,
        query: &Vector,
        candidates: &[u64],
        metric: DistanceMetric,
    ) -> Vec<(u64, f32)> {
        const PAR_THRESHOLD: usize = 100;

        if candidates.len() < PAR_THRESHOLD {
            let mut results: Vec<(u64, f32)> = candidates
                .iter()
                .map(|&id| {
                    let d = if let Some(node) = self.nodes.get(&id) {
                        distance(&node.vector, query, metric)
                    } else {
                        f32::MAX
                    };
                    (id, d)
                })
                .collect();
            results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            return results;
        }

        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = candidates.len().div_ceil(cpus);

        let mut results: Vec<(u64, f32)> = std::thread::scope(|s| {
            let handles: Vec<_> = candidates
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        chunk
                            .iter()
                            .map(|&id| {
                                let d = if let Some(node) = self.nodes.get(&id) {
                                    distance(&node.vector, query, metric)
                                } else {
                                    f32::MAX
                                };
                                (id, d)
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();

            let mut merged = Vec::with_capacity(candidates.len());
            for h in handles {
                merged.extend(h.join().unwrap());
            }
            merged
        });

        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A declared count out of a corrupt file must never size an allocation.
    ///
    /// `num_layers` was a raw `u32` feeding `Vec::<Vec<u64>>::with_capacity`,
    /// so `u32::MAX` asked for 103 GB — and a Rust allocation failure ABORTS
    /// (SIGABRT, no unwind, no `Err`, no log). On the startup path that is a
    /// boot crash-loop with no diagnostic.
    ///
    /// This asserts the `Err` rather than the absence of a crash on purpose:
    /// the bug was invisible on macOS, which overcommits and let the 103 GB
    /// reservation succeed, and only aborted on Linux. A test that merely
    /// "didn't crash" would have passed on the machine that wrote it — which is
    /// exactly what happened.
    #[test]
    fn a_huge_declared_layer_count_is_rejected_not_allocated() {
        let mut data = Vec::new();
        data.push(0u8); // metric = L2
        data.extend_from_slice(&8u32.to_le_bytes()); // m
        data.extend_from_slice(&50u32.to_le_bytes()); // ef_search
        data.extend_from_slice(&1u32.to_le_bytes()); // num_nodes = 1
        data.extend_from_slice(&1u64.to_le_bytes()); // node id
        data.extend_from_slice(&0u32.to_le_bytes()); // dim = 0
        data.extend_from_slice(&u32::MAX.to_le_bytes()); // num_layers = 4.29e9

        let err = HnswIndex::deserialize(&data)
            .expect_err("a layer count larger than the whole buffer must be refused");
        assert!(
            err.contains("num_layers"),
            "the error should name the field, got: {err}"
        );
    }

    fn rand_vec(dim: usize) -> Vector {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        Vector::new((0..dim).map(|_| rng.r#gen::<f32>()).collect())
    }

    /// A deterministic vector generator, for the tests that assert a RECALL
    /// FLOOR rather than a correctness property.
    ///
    /// Approximate-nearest-neighbour recall is a statistic over the data, so
    /// with `thread_rng` those tests are a dice roll against their own
    /// threshold. `ivfflat_recall` was measured failing 4 runs in 200 — about
    /// 2% — and duly took the Full Regression gate red on an unrelated commit.
    /// A gate that fails at random is worse than no gate: this repo has already
    /// had workflows sit red for over a week because red stopped meaning
    /// anything.
    ///
    /// Seeding does not weaken the assertion. The floor still has to hold for
    /// this data, and the data is a fixed sample of the same distribution; what
    /// it removes is the test's ability to disagree with itself between two
    /// runs of identical code. If a future change genuinely degrades recall,
    /// the seeded run fails and stays failed.
    fn seeded_vecs(dim: usize, n: usize, seed: u64) -> Vec<Vector> {
        use rand::{Rng, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(seed);
        (0..n)
            .map(|_| Vector::new((0..dim).map(|_| rng.r#gen::<f32>()).collect()))
            .collect()
    }

    #[test]
    fn l2_distance_test() {
        let a = Vector::new(vec![1.0, 0.0, 0.0]);
        let b = Vector::new(vec![0.0, 1.0, 0.0]);
        let d = simd_l2_distance(&a.data, &b.data);
        assert!((d - std::f32::consts::SQRT_2).abs() < 1e-5);
    }

    #[test]
    fn cosine_distance_test() {
        let a = vec![1.0f32, 0.0];
        let b = vec![0.0f32, 1.0];
        let d = simd_cosine_distance(&a, &b);
        assert!((d - 1.0).abs() < 1e-5); // Orthogonal → distance = 1

        let c = vec![1.0f32, 0.0];
        let d2 = simd_cosine_distance(&a, &c);
        assert!(d2.abs() < 1e-5); // Same direction → distance = 0
    }

    #[test]
    fn inner_product_test() {
        let a = vec![1.0f32, 2.0, 3.0];
        let b = vec![4.0f32, 5.0, 6.0];
        let dot = simd_dot_product(&a, &b);
        assert!((dot - 32.0).abs() < 1e-5); // 1*4 + 2*5 + 3*6 = 32

        // distance() with InnerProduct negates it
        let va = Vector::new(a);
        let vb = Vector::new(b);
        let d = distance(&va, &vb, DistanceMetric::InnerProduct);
        assert!((d - (-32.0)).abs() < 1e-5);
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
    fn search_filtered_returns_all_matching_when_selective() {
        // A highly selective filter must still return every passing vector
        // (up to k), even if the HNSW graph traversal wouldn't reach them —
        // the guaranteed-recall linear-scan fallback covers that case.
        let config = HnswConfig {
            m: 8,
            m_max0: 16,
            ef_construction: 100,
            ef_search: 50,
            metric: DistanceMetric::L2,
        };
        let mut index = HnswIndex::new(config);
        let dim = 16;
        for i in 0..300u64 {
            index.insert(i, rand_vec(dim));
        }
        let allowed: HashSet<u64> = [3u64, 100, 207, 299].into_iter().collect();
        let allowed_f = allowed.clone();
        let query = rand_vec(dim);
        let results = index.search_filtered(&query, 4, move |id| allowed_f.contains(&id));
        let got: HashSet<u64> = results.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            got, allowed,
            "all filter-passing vectors must be returned regardless of graph reachability"
        );
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
    fn hnsw_filtered_search_basic() {
        let config = HnswConfig {
            m: 8,
            m_max0: 16,
            ef_construction: 100,
            ef_search: 50,
            metric: DistanceMetric::L2,
        };
        let mut index = HnswIndex::new(config);

        // Insert vectors with IDs 1-4
        index.insert(1, Vector::new(vec![1.0, 0.0, 0.0]));
        index.insert(2, Vector::new(vec![0.0, 1.0, 0.0]));
        index.insert(3, Vector::new(vec![0.9, 0.1, 0.0]));
        index.insert(4, Vector::new(vec![0.0, 0.0, 1.0]));

        let query = Vector::new(vec![1.0, 0.0, 0.0]);

        // Filter: only allow even IDs
        let results = index.search_filtered(&query, 2, |id| id % 2 == 0);
        assert!(!results.is_empty());
        // Should not contain ID 1 or 3 (odd), even though they are closest
        for (id, _) in &results {
            assert_eq!(*id % 2, 0, "filtered search returned odd ID {id}");
        }
    }

    #[test]
    fn hnsw_filtered_search_no_matches() {
        let config = HnswConfig {
            m: 8,
            m_max0: 16,
            ef_construction: 100,
            ef_search: 50,
            metric: DistanceMetric::L2,
        };
        let mut index = HnswIndex::new(config);

        index.insert(1, Vector::new(vec![1.0, 0.0]));
        index.insert(2, Vector::new(vec![0.0, 1.0]));

        let query = Vector::new(vec![1.0, 0.0]);

        // Filter rejects everything
        let results = index.search_filtered(&query, 2, |_| false);
        assert!(results.is_empty());
    }

    #[test]
    fn hnsw_filtered_search_all_pass() {
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
        index.insert(3, Vector::new(vec![0.9, 0.1, 0.0]));

        let query = Vector::new(vec![1.0, 0.0, 0.0]);

        // Filter accepts everything — same as unfiltered
        let filtered = index.search_filtered(&query, 2, |_| true);
        let unfiltered = index.search(&query, 2);
        assert_eq!(filtered.len(), unfiltered.len());
        assert_eq!(filtered[0].0, unfiltered[0].0);
    }

    #[test]
    fn ivfflat_filtered_search() {
        let mut index = IvfFlatIndex::new(2, 2, 2, DistanceMetric::L2);
        let training_data: Vec<Vec<f32>> = vec![
            vec![0.0, 0.0],
            vec![10.0, 10.0],
            vec![0.1, 0.1],
            vec![9.9, 9.9],
        ];
        index.train(&training_data);

        index.add(0, vec![0.0, 0.0]);
        index.add(1, vec![0.1, 0.1]);
        index.add(2, vec![10.0, 10.0]);
        index.add(3, vec![9.9, 9.9]);

        let query = vec![0.0, 0.0];

        // Filter: only allow IDs >= 2
        let results = index.search_filtered(&query, 2, |id| id >= 2);
        for (id, _) in &results {
            assert!(*id >= 2, "filtered IVFFlat returned id {id} < 2");
        }
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

        // Seeded for the same reason as `ivfflat_recall`: a recall floor over
        // random data is a dice roll against its own threshold. This one has
        // not been seen to fail — it measured 0 in 200 — but it is built
        // exactly the same way, and the sibling it shares that construction
        // with failed 4 in 200.
        for (i, v) in seeded_vecs(dim, n as usize, 20_260_813)
            .into_iter()
            .enumerate()
        {
            let i = i as u64;
            vectors.push((i, v.clone()));
            index.insert(i, v);
        }

        // Run search and compare with exact
        let query = seeded_vecs(dim, 1, 20_260_814).remove(0);
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

        // Seeded: this asserts a recall FLOOR, which is a statistic over the
        // data. See `seeded_vecs` — unseeded, it failed about 1 run in 50 and
        // took the Full Regression gate red on a commit that touched nothing
        // in this module.
        let training: Vec<Vec<f32>> = seeded_vecs(dim, n, 20_260_811)
            .into_iter()
            .map(|v| v.data)
            .collect();

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

        let query_vec = seeded_vecs(dim, 1, 20_260_812).remove(0);
        let query = &query_vec.data;

        let ivf_results = index.search(query, k);
        let exact_results = exact_search(&exact_data, &query_vec, k, DistanceMetric::L2);

        let ivf_ids: HashSet<usize> = ivf_results.iter().map(|(id, _)| *id).collect();
        let exact_ids: HashSet<usize> = exact_results.iter().map(|(id, _)| *id as usize).collect();

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

        let data = index.serialize(None);
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
        let data = index.serialize(None);
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

        let data = index.serialize(None);
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

    // ========================================================================
    // SIMD distance function tests
    // ========================================================================

    #[test]
    fn simd_dot_product_correctness() {
        // Hand-computed dot product
        let a = vec![1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let b = vec![8.0f32, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];
        // 1*8 + 2*7 + 3*6 + 4*5 + 5*4 + 6*3 + 7*2 + 8*1 = 120
        let dot = simd_dot_product(&a, &b);
        assert!((dot - 120.0).abs() < 1e-4, "expected 120.0, got {dot}");
    }

    #[test]
    fn simd_dot_product_non_multiple_of_8() {
        // 11 elements — exercises the remainder path (8 + 3 tail)
        let a: Vec<f32> = (1..=11).map(|x| x as f32).collect();
        let b: Vec<f32> = (11..=21).map(|x| x as f32).collect();
        let expected: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let got = simd_dot_product(&a, &b);
        assert!(
            (got - expected).abs() < 1e-2,
            "dot product mismatch: expected {expected}, got {got}"
        );
    }

    #[test]
    fn simd_l2_distance_correctness() {
        // Known L2: (1,0,0) to (0,1,0) = sqrt(2)
        let a = vec![1.0f32, 0.0, 0.0];
        let b = vec![0.0f32, 1.0, 0.0];
        let d = simd_l2_distance(&a, &b);
        assert!(
            (d - std::f32::consts::SQRT_2).abs() < 1e-5,
            "L2 mismatch: expected sqrt(2), got {d}"
        );

        // 16-dimensional (exact 8*2 chunks, no remainder)
        let a16: Vec<f32> = vec![1.0; 16];
        let b16: Vec<f32> = vec![0.0; 16];
        // sum of squares = 16 * 1.0 = 16, sqrt(16) = 4
        let d16 = simd_l2_distance(&a16, &b16);
        assert!(
            (d16 - 4.0).abs() < 1e-5,
            "L2(16d) mismatch: expected 4.0, got {d16}"
        );
    }

    #[test]
    fn simd_l2_distance_zero_vectors() {
        let a = vec![0.0f32; 32];
        let b = vec![0.0f32; 32];
        let d = simd_l2_distance(&a, &b);
        assert!(
            d.abs() < 1e-10,
            "L2 of identical zero vectors should be 0, got {d}"
        );
    }

    #[test]
    fn simd_cosine_distance_orthogonal() {
        // Orthogonal vectors → cosine distance = 1.0
        let a = vec![1.0f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let b = vec![0.0f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let d = simd_cosine_distance(&a, &b);
        assert!(
            (d - 1.0).abs() < 1e-5,
            "cosine distance of orthogonal vectors should be 1.0, got {d}"
        );
    }

    #[test]
    fn simd_cosine_distance_identical() {
        // Identical vectors → cosine distance = 0.0
        let a = vec![1.0f32, 2.0, 3.0, 4.0, 5.0];
        let d = simd_cosine_distance(&a, &a);
        assert!(
            d.abs() < 1e-5,
            "cosine distance of identical vectors should be 0.0, got {d}"
        );
    }

    #[test]
    fn simd_cosine_distance_zero_vector() {
        // Zero vector → cosine distance = 1.0 (defined by convention)
        let a = vec![0.0f32; 8];
        let b = vec![1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let d = simd_cosine_distance(&a, &b);
        assert!(
            (d - 1.0).abs() < 1e-5,
            "cosine distance with zero vector should be 1.0, got {d}"
        );
    }

    #[test]
    fn simd_matches_scalar_on_random_data() {
        // Verify SIMD results match a simple scalar implementation on
        // random data of various sizes (including non-multiples of 8).
        use rand::Rng;
        let mut rng = rand::thread_rng();
        for dim in [1, 3, 7, 8, 9, 15, 16, 31, 32, 33, 64, 100, 128, 255, 256] {
            let a: Vec<f32> = (0..dim).map(|_| rng.r#gen::<f32>() * 10.0 - 5.0).collect();
            let b: Vec<f32> = (0..dim).map(|_| rng.r#gen::<f32>() * 10.0 - 5.0).collect();

            // Scalar reference
            let scalar_dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            let scalar_l2: f32 = a
                .iter()
                .zip(b.iter())
                .map(|(x, y)| (x - y) * (x - y))
                .sum::<f32>()
                .sqrt();

            let simd_dot_val = simd_dot_product(&a, &b);
            let simd_l2_val = simd_l2_distance(&a, &b);

            // Allow slightly larger tolerance for large vectors (accumulated FP error)
            let tol = (dim as f32) * 1e-4;
            assert!(
                (simd_dot_val - scalar_dot).abs() < tol,
                "dot mismatch at dim={dim}: simd={simd_dot_val}, scalar={scalar_dot}"
            );
            assert!(
                (simd_l2_val - scalar_l2).abs() < tol,
                "l2 mismatch at dim={dim}: simd={simd_l2_val}, scalar={scalar_l2}"
            );
        }
    }

    #[test]
    fn distance_raw_matches_distance() {
        // Ensure the raw-slice convenience function matches the Vector-based one
        let a_data = vec![1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let b_data = vec![10.0f32, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];
        let va = Vector::new(a_data.clone());
        let vb = Vector::new(b_data.clone());

        for metric in [
            DistanceMetric::L2,
            DistanceMetric::Cosine,
            DistanceMetric::InnerProduct,
        ] {
            let d1 = distance(&va, &vb, metric);
            let d2 = distance_raw(&a_data, &b_data, metric);
            assert!(
                (d1 - d2).abs() < 1e-5,
                "distance vs distance_raw mismatch for {metric:?}: {d1} vs {d2}"
            );
        }
    }

    // ========================================================================
    // Parallel search tests
    // ========================================================================

    #[test]
    fn par_brute_force_matches_sequential() {
        // Parallel brute-force search must return the same top-k results
        // as sequential exact_search on a dataset above the threshold.
        let dim = 32;
        let n = 2000; // above PAR_THRESHOLD (1000)
        let k = 10;

        let vectors: Vec<(u64, Vector)> = (0..n).map(|i| (i as u64, rand_vec(dim))).collect();
        let query = rand_vec(dim);

        let seq = exact_search(&vectors, &query, k, DistanceMetric::L2);
        let par = par_search_brute_force(&vectors, &query, k, DistanceMetric::L2);

        assert_eq!(seq.len(), par.len(), "result count mismatch");
        for (s, p) in seq.iter().zip(par.iter()) {
            assert_eq!(s.0, p.0, "id mismatch: seq={}, par={}", s.0, p.0);
            assert!(
                (s.1 - p.1).abs() < 1e-6,
                "distance mismatch for id {}: seq={}, par={}",
                s.0,
                s.1,
                p.1
            );
        }
    }

    #[test]
    fn par_brute_force_small_dataset_fallback() {
        // Below the 1000-vector threshold, par_search_brute_force should
        // produce identical results to exact_search (it falls back internally).
        let dim = 16;
        let n = 50; // well below threshold
        let k = 5;

        let vectors: Vec<(u64, Vector)> = (0..n).map(|i| (i as u64, rand_vec(dim))).collect();
        let query = rand_vec(dim);

        let seq = exact_search(&vectors, &query, k, DistanceMetric::Cosine);
        let par = par_search_brute_force(&vectors, &query, k, DistanceMetric::Cosine);

        assert_eq!(seq.len(), par.len());
        for (s, p) in seq.iter().zip(par.iter()) {
            assert_eq!(s.0, p.0);
            assert!((s.1 - p.1).abs() < 1e-6);
        }
    }

    #[test]
    fn par_batch_search_independent() {
        // Multiple independent queries should each return correct results.
        let dim = 16;
        let n = 200;
        let k = 5;

        let vectors: Vec<(u64, Vector)> = (0..n).map(|i| (i as u64, rand_vec(dim))).collect();
        let queries: Vec<Vector> = (0..10).map(|_| rand_vec(dim)).collect();

        let batch_results = par_batch_search(&vectors, &queries, k, DistanceMetric::L2);

        assert_eq!(batch_results.len(), queries.len());
        for (i, query) in queries.iter().enumerate() {
            let sequential = exact_search(&vectors, query, k, DistanceMetric::L2);
            assert_eq!(
                batch_results[i].len(),
                sequential.len(),
                "query {i}: result count mismatch"
            );
            for (b, s) in batch_results[i].iter().zip(sequential.iter()) {
                assert_eq!(b.0, s.0, "query {i}: id mismatch");
                assert!((b.1 - s.1).abs() < 1e-6, "query {i}: distance mismatch");
            }
        }
    }

    #[test]
    fn par_candidate_evaluation() {
        // Parallel candidate evaluation on an HNSW index must match sequential.
        let dim = 16;
        let config = HnswConfig {
            m: 8,
            m_max0: 16,
            ef_construction: 100,
            ef_search: 50,
            metric: DistanceMetric::L2,
        };
        let mut index = HnswIndex::new(config);

        // Insert enough nodes to exceed the 100-candidate threshold
        for i in 0..200u64 {
            index.insert(i, rand_vec(dim));
        }

        let candidates: Vec<u64> = (0..200).collect();
        let query = rand_vec(dim);

        let par_results = index.par_evaluate_candidates(&query, &candidates, DistanceMetric::L2);

        // Compute sequential reference
        let mut seq_results: Vec<(u64, f32)> = candidates
            .iter()
            .map(|&id| {
                let node = index.nodes.get(&id).unwrap();
                (id, distance(&node.vector, &query, DistanceMetric::L2))
            })
            .collect();
        seq_results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        assert_eq!(par_results.len(), seq_results.len());
        for (p, s) in par_results.iter().zip(seq_results.iter()) {
            assert_eq!(p.0, s.0, "id mismatch: par={}, seq={}", p.0, s.0);
            assert!(
                (p.1 - s.1).abs() < 1e-6,
                "distance mismatch for id {}: par={}, seq={}",
                p.0,
                p.1,
                s.1
            );
        }
    }

    #[test]
    fn par_brute_force_large_dataset() {
        // 5000+ vectors with parallel search — verifies correctness at scale.
        let dim = 64;
        let n = 5000;
        let k = 20;

        let vectors: Vec<(u64, Vector)> = (0..n).map(|i| (i as u64, rand_vec(dim))).collect();
        let query = rand_vec(dim);

        let par = par_search_brute_force(&vectors, &query, k, DistanceMetric::L2);
        let seq = exact_search(&vectors, &query, k, DistanceMetric::L2);

        assert_eq!(par.len(), k);
        assert_eq!(seq.len(), k);
        for (p, s) in par.iter().zip(seq.iter()) {
            assert_eq!(p.0, s.0, "id mismatch at 5000 vectors");
            assert!((p.1 - s.1).abs() < 1e-5);
        }
    }

    /// S35 F1a: tombstones must survive a serialize/deserialize round-trip.
    ///
    /// `serialize` wrote header, nodes and footer — and never the `deleted`
    /// set every search path consults. A WAL checkpoint snapshots through it,
    /// so every tombstone standing at checkpoint time was dropped and the
    /// deleted vector resurrected on reopen. `len()` counts tombstoned nodes,
    /// so the assertion goes through `live_ids`, which can see a resurrected
    /// delete — the same observability rule as the recovery probe.
    #[test]
    fn tombstones_survive_serialize_roundtrip() {
        let config = HnswConfig {
            m: 8,
            m_max0: 16,
            ef_construction: 100,
            ef_search: 50,
            metric: DistanceMetric::L2,
        };
        let mut index = HnswIndex::new(config);
        for i in 0..24u64 {
            index.insert(i, Vector::new(vec![i as f32, 0.0, 0.0, 0.0]));
        }
        index.mark_deleted(3);
        index.mark_deleted(17);
        assert_eq!(index.live_ids().len(), 22, "fixture must hold 22 live ids");

        let round = HnswIndex::deserialize(&index.serialize(None))
            .expect("a round-trip of a tombstoned index must parse");
        assert_eq!(
            round.live_ids(),
            index.live_ids(),
            "tombstones were dropped by the round-trip: deleted vectors resurrect"
        );
    }

    /// An index serialized by a build that predates the tombstone section must
    /// still load — with an empty tombstone set, which is faithful: those
    /// bytes contain no tombstone information to recover.
    ///
    /// The format is versioned by position, not by a version byte, so the
    /// section is APPENDED after the footer and tagged. A reader that stops
    /// exactly at the footer read an old file; a reader that finds trailing
    /// bytes demands the tag match (see the next test). Hand-built here, the
    /// same way `wal::a_pre_checksum_snapshot_still_opens` exercises its
    /// legacy record: nothing writes the old layout any more.
    #[test]
    fn an_index_without_a_tombstone_section_still_loads() {
        // [metric u8][m u32][ef_search u32][num_nodes u32]
        // one node: [id u64][dim u32][f32 * dim][num_layers u32 = 0]
        // footer: [max_layer u32][entry u64]
        let mut data = Vec::new();
        data.push(0u8); // metric = L2
        data.extend_from_slice(&8u32.to_le_bytes()); // m
        data.extend_from_slice(&50u32.to_le_bytes()); // ef_search
        data.extend_from_slice(&1u32.to_le_bytes()); // num_nodes
        data.extend_from_slice(&7u64.to_le_bytes()); // node id
        data.extend_from_slice(&2u32.to_le_bytes()); // dim
        data.extend_from_slice(&1.0f32.to_le_bytes());
        data.extend_from_slice(&0.0f32.to_le_bytes());
        data.extend_from_slice(&0u32.to_le_bytes()); // num_layers = 0
        data.extend_from_slice(&0u32.to_le_bytes()); // max_layer
        data.extend_from_slice(&7u64.to_le_bytes()); // entry_point

        let index = HnswIndex::deserialize(&data).expect("the old format must still load");
        assert_eq!(index.len(), 1);
        assert_eq!(
            index.live_ids(),
            [7u64].into_iter().collect(),
            "an old-format index has no tombstones to recover; its one node is live"
        );
    }

    /// Trailing bytes after the footer that do not carry the tombstone
    /// section tag are corruption, not an old file — guessing here would
    /// silently misparse an existing index, which is worse than the bug the
    /// section fixes.
    #[test]
    fn unknown_trailing_bytes_after_the_footer_are_refused() {
        let config = HnswConfig {
            m: 8,
            m_max0: 16,
            ef_construction: 100,
            ef_search: 50,
            metric: DistanceMetric::L2,
        };
        let mut index = HnswIndex::new(config);
        index.insert(1, Vector::new(vec![1.0, 0.0, 0.0, 0.0]));
        let mut data = index.serialize(None);
        data.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

        let err = HnswIndex::deserialize(&data)
            .expect_err("trailing bytes without the section tag must be refused");
        assert!(
            err.contains("trailing"),
            "the error should name the trailing section, got: {err}"
        );
    }

    /// F1b: a PK registry serialized inside the blob round-trips — the map,
    /// the allocator floor, and the compaction counter all survive.
    #[test]
    fn a_pk_registry_round_trips_inside_the_blob() {
        let mut index = HnswIndex::new(HnswConfig::default());
        index.insert(0, Vector::new(vec![1.0, 0.0, 0.0, 0.0]));
        index.insert(5, Vector::new(vec![0.0, 1.0, 0.0, 0.0]));
        index.mark_deleted(0);

        let mut registry = RegistrySection::default();
        registry.pk_to_node.insert(1, 0);
        registry.pk_to_node.insert(2, 5);
        registry.next_node = 6;
        registry.tombstones = 3;

        let data = index.serialize(Some(&registry));
        let (_, recovered) = HnswIndex::deserialize_with_registry(&data)
            .expect("a blob with a registry section must parse");
        let recovered =
            recovered.expect("the blob carried a registry section, so one must come back");
        assert_eq!(recovered.pk_to_node.get(&1), Some(&0));
        assert_eq!(recovered.pk_to_node.get(&2), Some(&5));
        assert_eq!(recovered.next_node, 6);
        assert_eq!(recovered.tombstones, 3);
    }

    /// F1b: a blob written without a registry section (every blob predating
    /// it, and every `serialize(None)` caller) decodes as registry-absent —
    /// faithful, because those bytes carry no registry to recover.
    #[test]
    fn a_blob_without_a_registry_section_decodes_as_registry_absent() {
        let mut index = HnswIndex::new(HnswConfig::default());
        index.insert(1, Vector::new(vec![1.0, 0.0, 0.0, 0.0]));

        let data = index.serialize(None);
        let (_, registry) = HnswIndex::deserialize_with_registry(&data)
            .expect("a registry-less blob is the old format and must still load");
        assert!(
            registry.is_none(),
            "no registry section was written, so none may be invented"
        );
    }

    /// F1b: trailing bytes after the tombstone section that do not carry the
    /// registry tag stay corruption. The parser accepts exactly one more
    /// KNOWN tag there, not anything.
    #[test]
    fn unknown_trailing_bytes_after_the_tombstone_section_are_refused() {
        let mut index = HnswIndex::new(HnswConfig::default());
        index.insert(1, Vector::new(vec![1.0, 0.0, 0.0, 0.0]));
        index.mark_deleted(1);

        let mut data = index.serialize(None);
        data.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

        let err = HnswIndex::deserialize(&data).expect_err(
            "trailing bytes after the tombstones without the registry tag must be refused",
        );
        assert!(
            err.contains("registry section tag"),
            "the error should name the registry section, got: {err}"
        );
    }

    /// F1b: the entry count is bounded against the bytes actually present, so
    /// a corrupted count from a file can never feed a huge allocation.
    #[test]
    fn a_registry_entry_count_beyond_the_data_is_refused() {
        let mut index = HnswIndex::new(HnswConfig::default());
        index.insert(1, Vector::new(vec![1.0, 0.0, 0.0, 0.0]));

        let mut data = index.serialize(None);
        data.extend_from_slice(&REGISTRY_SECTION_TAG.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes()); // next_node
        data.extend_from_slice(&0u64.to_le_bytes()); // tombstones
        data.extend_from_slice(&u32::MAX.to_le_bytes()); // entry count
        // ...and then no entries at all.

        let err = HnswIndex::deserialize(&data)
            .expect_err("an entry count larger than the remaining bytes is corruption");
        assert!(
            err.contains("exceeds remaining data"),
            "the error should name the bound, got: {err}"
        );
    }

    /// Clustered corpus generator for the recall tests — points scattered
    /// around `n_clusters` centres with Gaussian jitter (sum of 4 uniforms),
    /// the embedding-like shape the vector bench (BENCH_VS_QDRANT) uses.
    /// Clustered is the trap shape: inter-cluster valleys are what an HNSW
    /// beam must be able to cross, and uniform data never exercises them.
    fn seeded_clustered_vecs(dim: usize, n: usize, n_clusters: usize, seed: u64) -> Vec<Vector> {
        use rand::{Rng, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(seed);
        let centers: Vec<Vec<f32>> = (0..n_clusters)
            .map(|_| (0..dim).map(|_| rng.r#gen::<f32>() * 2.0 - 1.0).collect())
            .collect();
        (0..n)
            .map(|_| {
                let c = &centers[rng.gen_range(0..n_clusters)];
                Vector::new(
                    (0..dim)
                        .map(|d| {
                            let jitter: f32 = (0..4).map(|_| rng.r#gen::<f32>() - 0.5).sum();
                            c[d] + jitter * 0.2
                        })
                        .collect(),
                )
            })
            .collect()
    }

    /// Building the same corpus twice must produce the same graph.
    ///
    /// Layer assignment used to draw from `rand::random()`, so every build
    /// of identical data was a different graph and recall was a per-boot
    /// lottery: over four runs of the vector bench the `ef` at which recall
    /// first hit 1.000 ranged 96 → never → 192 → 96 on byte-identical input.
    /// It is now derived from the node id (see `random_layer`), so the
    /// serialized graph — and therefore every search over it — is a pure
    /// function of (ids, vectors, insertion order). Asserted at the byte
    /// level because that is what a checkpoint persists and a replica must
    /// reproduce.
    #[test]
    fn hnsw_build_is_deterministic() {
        let corpus = seeded_clustered_vecs(32, 400, 8, 0xC0FFEE);
        let build = || {
            let mut index = HnswIndex::new(HnswConfig {
                m: 16,
                m_max0: 32,
                ef_construction: 100,
                ef_search: 64,
                metric: DistanceMetric::L2,
            });
            for (id, v) in corpus.iter().enumerate() {
                index.insert(id as u64, v.clone());
            }
            index
        };
        let a = build();
        let b = build();
        // Not byte-comparing `serialize`: it walks `self.nodes`, a HashMap,
        // so its byte order is per-process and even an identical graph
        // serializes to different bytes. The graph itself — and therefore
        // every search over it — must match exactly, distances included.
        use rand::{Rng, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(0xD_E7E7);
        for _ in 0..25 {
            let q = Vector::new((0..32).map(|_| rng.r#gen::<f32>()).collect());
            assert_eq!(
                a.search_ef(&q, 10, 64),
                b.search_ef(&q, 10, 64),
                "same corpus + ids + order must answer identically"
            );
        }
    }

    /// The clustered-recall stability gate: query-time beam descent must
    /// find the true top-10 of every in-distribution query at a modest `ef`.
    ///
    /// This is the unit-scale shape of the 2026-08-20 bench finding (one
    /// query returning NONE of its true top-10 at ef=256): greedy ef=1
    /// descent over the sparse upper layers parked layer 0's single entry in
    /// the wrong cluster, and the beam's admission filter then refused to
    /// cross the inter-cluster valley. With beam descent seeding layer 0
    /// across the whole upper-layer beam, the valley width is bounded by the
    /// beam, not by luck.
    #[test]
    fn hnsw_clustered_recall_stable_at_modest_ef() {
        let dim = 48;
        let n = 2400;
        let k = 10;
        let corpus = seeded_clustered_vecs(dim, n, 48, 0xBADC0DE);
        let mut index = HnswIndex::new(HnswConfig {
            m: 16,
            m_max0: 32,
            ef_construction: 100,
            ef_search: 64,
            metric: DistanceMetric::L2,
        });
        for (id, v) in corpus.iter().enumerate() {
            index.insert(id as u64, v.clone());
        }
        let reference: Vec<(u64, Vector)> = corpus
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, v)| (i as u64, v))
            .collect();
        // In-distribution queries: perturbed corpus points (bench shape).
        use rand::{Rng, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(0x5EED);
        let queries: Vec<Vector> = (0..40)
            .map(|_| {
                let base = &corpus[rng.gen_range(0..corpus.len())];
                Vector::new(
                    base.data
                        .iter()
                        .map(|x| x + (0..4).map(|_| rng.r#gen::<f32>() - 0.5).sum::<f32>() * 0.2)
                        .collect(),
                )
            })
            .collect();

        for ef in [32usize, 64] {
            let mut misses = Vec::new();
            for (qi, q) in queries.iter().enumerate() {
                let truth: std::collections::HashSet<u64> =
                    exact_search(&reference, q, k, DistanceMetric::L2)
                        .into_iter()
                        .map(|(id, _)| id)
                        .collect();
                let got = index.search_ef(q, k, ef);
                let hits = got.iter().filter(|(id, _)| truth.contains(id)).count();
                if hits < k {
                    misses.push((qi, hits));
                }
            }
            assert!(
                misses.is_empty(),
                "ef={ef}: {} of {} queries short of perfect recall: {misses:?}",
                misses.len(),
                queries.len()
            );
        }
    }

    /// Recall must be identical before and after a serialize/deserialize
    /// round-trip — the checkpoint path rewrites every neighbor list, and a
    /// truncation there would surface as recall loss that only exists after
    /// a reopen (invisible to every in-memory test).
    #[test]
    fn hnsw_recall_survives_serialize_roundtrip() {
        let corpus = seeded_clustered_vecs(48, 1200, 24, 0x120D_7EA7);
        let mut index = HnswIndex::new(HnswConfig {
            m: 16,
            m_max0: 32,
            ef_construction: 100,
            ef_search: 64,
            metric: DistanceMetric::L2,
        });
        for (id, v) in corpus.iter().enumerate() {
            index.insert(id as u64, v.clone());
        }
        let reloaded = HnswIndex::deserialize(&index.serialize(None)).expect("roundtrip");

        use rand::{Rng, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..25 {
            let base = &corpus[rng.gen_range(0..corpus.len())];
            let q = Vector::new(
                base.data
                    .iter()
                    .map(|x| x + (0..4).map(|_| rng.r#gen::<f32>() - 0.5).sum::<f32>() * 0.2)
                    .collect(),
            );
            assert_eq!(index.search_ef(&q, 10, 64), reloaded.search_ef(&q, 10, 64));
        }
    }

    #[test]
    fn par_batch_search_consistency() {
        // Running par_batch_search twice on the same input must produce
        // identical (deterministic) results.
        let dim = 16;
        let n = 300;
        let k = 5;

        let vectors: Vec<(u64, Vector)> = (0..n).map(|i| (i as u64, rand_vec(dim))).collect();
        let queries: Vec<Vector> = (0..5).map(|_| rand_vec(dim)).collect();

        let run1 = par_batch_search(&vectors, &queries, k, DistanceMetric::InnerProduct);
        let run2 = par_batch_search(&vectors, &queries, k, DistanceMetric::InnerProduct);

        assert_eq!(run1.len(), run2.len());
        for (r1, r2) in run1.iter().zip(run2.iter()) {
            assert_eq!(r1.len(), r2.len());
            for (a, b) in r1.iter().zip(r2.iter()) {
                assert_eq!(a.0, b.0, "determinism failure: different ids across runs");
                assert!(
                    (a.1 - b.1).abs() < 1e-6,
                    "determinism failure: different distances across runs"
                );
            }
        }
    }
}
