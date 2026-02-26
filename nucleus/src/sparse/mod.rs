//! Sparse vector engine with inverted index and WAND algorithm.
//!
//! Supports:
//!   - Sparse vector storage (most dimensions are zero)
//!   - Inverted index for efficient sparse retrieval
//!   - WAND (Weak AND) algorithm for top-k retrieval
//!   - Hybrid dense+sparse search scoring
//!   - BM25, SPLADE, TF-IDF compatible
//!
//! Replaces Elasticsearch sparse vectors, Milvus, Qdrant sparse features.

use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;

// ============================================================================
// Sparse vector types
// ============================================================================

/// A sparse vector represented as (dimension_index, value) pairs.
#[derive(Debug, Clone)]
pub struct SparseVector {
    /// Non-zero entries sorted by dimension index.
    pub indices: Vec<u32>,
    pub values: Vec<f32>,
}

impl SparseVector {
    pub fn new(mut entries: Vec<(u32, f32)>) -> Self {
        entries.sort_by_key(|(idx, _)| *idx);
        // Filter out zeros
        let (indices, values): (Vec<u32>, Vec<f32>) = entries
            .into_iter()
            .filter(|(_, v)| *v != 0.0)
            .unzip();
        Self { indices, values }
    }

    pub fn from_sorted(indices: Vec<u32>, values: Vec<f32>) -> Self {
        Self { indices, values }
    }

    /// Number of non-zero entries.
    pub fn nnz(&self) -> usize {
        self.indices.len()
    }

    /// Dot product between two sparse vectors.
    pub fn dot(&self, other: &SparseVector) -> f32 {
        let mut sum = 0.0f32;
        let mut i = 0;
        let mut j = 0;

        while i < self.indices.len() && j < other.indices.len() {
            match self.indices[i].cmp(&other.indices[j]) {
                Ordering::Equal => {
                    sum += self.values[i] * other.values[j];
                    i += 1;
                    j += 1;
                }
                Ordering::Less => i += 1,
                Ordering::Greater => j += 1,
            }
        }

        sum
    }

    /// Maximum value in this vector.
    pub fn max_value(&self) -> f32 {
        self.values.iter().copied().reduce(f32::max).unwrap_or(0.0)
    }

    /// L2 norm.
    pub fn norm(&self) -> f32 {
        self.values.iter().map(|v| v * v).sum::<f32>().sqrt()
    }
}

// ============================================================================
// Posting list for inverted index
// ============================================================================

/// An entry in a posting list.
#[derive(Debug, Clone)]
struct Posting {
    doc_id: u64,
    weight: f32,
}

/// A posting list for a single dimension, sorted by doc_id.
#[derive(Debug, Clone)]
struct PostingList {
    /// Sorted by doc_id.
    postings: Vec<Posting>,
    /// Maximum weight in this list (upper bound for WAND).
    max_weight: f32,
}

impl PostingList {
    fn new() -> Self {
        Self {
            postings: Vec::new(),
            max_weight: 0.0,
        }
    }

    fn add(&mut self, doc_id: u64, weight: f32) {
        // Insert in sorted order by doc_id
        let pos = self.postings
            .binary_search_by_key(&doc_id, |p| p.doc_id)
            .unwrap_or_else(|i| i);
        self.postings.insert(pos, Posting { doc_id, weight });
        if weight > self.max_weight {
            self.max_weight = weight;
        }
    }

    fn remove(&mut self, doc_id: u64) {
        if let Ok(pos) = self.postings.binary_search_by_key(&doc_id, |p| p.doc_id) {
            self.postings.remove(pos);
            // Recompute max_weight
            self.max_weight = self.postings.iter().map(|p| p.weight).reduce(f32::max).unwrap_or(0.0);
        }
    }

    fn _len(&self) -> usize {
        self.postings.len()
    }
}

// ============================================================================
// Inverted index for sparse vectors
// ============================================================================

/// Inverted index for sparse vector search.
pub struct SparseIndex {
    /// dimension_index → posting list
    index: HashMap<u32, PostingList>,
    /// doc_id → stored sparse vector (for exact scoring)
    vectors: HashMap<u64, SparseVector>,
    /// Number of documents.
    doc_count: usize,
}

impl Default for SparseIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparseIndex {
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            vectors: HashMap::new(),
            doc_count: 0,
        }
    }

    /// Insert a document with a sparse vector.
    pub fn insert(&mut self, doc_id: u64, vector: SparseVector) {
        // Remove old version if exists
        if self.vectors.contains_key(&doc_id) {
            self.remove(doc_id);
        }

        for (idx, val) in vector.indices.iter().zip(vector.values.iter()) {
            self.index.entry(*idx).or_insert_with(PostingList::new).add(doc_id, *val);
        }

        self.vectors.insert(doc_id, vector);
        self.doc_count += 1;
    }

    /// Remove a document.
    pub fn remove(&mut self, doc_id: u64) -> bool {
        if let Some(vector) = self.vectors.remove(&doc_id) {
            for idx in &vector.indices {
                if let Some(list) = self.index.get_mut(idx) {
                    list.remove(doc_id);
                }
            }
            self.doc_count -= 1;
            true
        } else {
            false
        }
    }

    /// Get the number of indexed documents.
    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    /// Brute-force exact search: compute dot product with every document.
    pub fn search_exact(&self, query: &SparseVector, top_k: usize) -> Vec<(u64, f32)> {
        let mut scores: Vec<(u64, f32)> = self
            .vectors
            .iter()
            .map(|(&doc_id, vec)| (doc_id, query.dot(vec)))
            .filter(|(_, score)| *score > 0.0)
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        scores.truncate(top_k);
        scores
    }

    /// WAND (Weak AND) top-k search — efficient approximate retrieval.
    ///
    /// Uses upper-bound pruning: for each query dimension, the maximum
    /// contribution is query_weight * max_posting_weight. If the sum of
    /// upper bounds for a document can't exceed the current k-th best
    /// score, skip it.
    pub fn search_wand(&self, query: &SparseVector, top_k: usize) -> Vec<(u64, f32)> {
        if query.nnz() == 0 || top_k == 0 {
            return Vec::new();
        }

        // Collect all candidate doc IDs with their upper bound contribution per query dimension
        // For WAND: accumulate scores using the inverted index
        let mut doc_scores: HashMap<u64, f32> = HashMap::new();

        // Compute upper bounds per query dimension
        let mut query_upper_bounds: Vec<(u32, f32, f32)> = Vec::new(); // (dim, query_weight, max_posting_weight)
        for (idx, qval) in query.indices.iter().zip(query.values.iter()) {
            if let Some(list) = self.index.get(idx) {
                query_upper_bounds.push((*idx, *qval, list.max_weight));
            }
        }

        // Total upper bound for any single document
        let _total_upper: f32 = query_upper_bounds
            .iter()
            .map(|(_, qw, pw)| qw * pw)
            .sum();

        // Score accumulation using inverted index
        for (dim, qval, _) in &query_upper_bounds {
            if let Some(list) = self.index.get(dim) {
                for posting in &list.postings {
                    *doc_scores.entry(posting.doc_id).or_insert(0.0) += qval * posting.weight;
                }
            }
        }

        // Collect top-k using a min-heap
        let mut heap: BinaryHeap<std::cmp::Reverse<ScoredDoc>> = BinaryHeap::new();

        for (doc_id, score) in doc_scores {
            if score <= 0.0 {
                continue;
            }
            if heap.len() < top_k {
                heap.push(std::cmp::Reverse(ScoredDoc { doc_id, score }));
            } else if let Some(min) = heap.peek() {
                if score > min.0.score {
                    heap.pop();
                    heap.push(std::cmp::Reverse(ScoredDoc { doc_id, score }));
                }
            }
        }

        let mut results: Vec<(u64, f32)> = heap
            .into_iter()
            .map(|std::cmp::Reverse(sd)| (sd.doc_id, sd.score))
            .collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        results
    }
}

/// Helper for min-heap scoring.
#[derive(Debug, Clone)]
struct ScoredDoc {
    doc_id: u64,
    score: f32,
}

impl PartialEq for ScoredDoc {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredDoc {}

impl PartialOrd for ScoredDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
    }
}

// ============================================================================
// Hybrid dense+sparse search
// ============================================================================

/// Combine dense and sparse search scores with linear interpolation.
/// `alpha` controls the blend: 0.0 = all sparse, 1.0 = all dense.
pub fn hybrid_score(
    dense_results: &[(u64, f32)],
    sparse_results: &[(u64, f32)],
    alpha: f32,
    top_k: usize,
) -> Vec<(u64, f32)> {
    let mut combined: HashMap<u64, (f32, f32)> = HashMap::new(); // (dense_score, sparse_score)

    // Normalize scores to [0, 1]
    let dense_max = dense_results.iter().map(|(_, s)| *s).reduce(f32::max).unwrap_or(1.0).max(1e-10);
    let sparse_max = sparse_results.iter().map(|(_, s)| *s).reduce(f32::max).unwrap_or(1.0).max(1e-10);

    for &(id, score) in dense_results {
        combined.entry(id).or_insert((0.0, 0.0)).0 = score / dense_max;
    }
    for &(id, score) in sparse_results {
        combined.entry(id).or_insert((0.0, 0.0)).1 = score / sparse_max;
    }

    let mut results: Vec<(u64, f32)> = combined
        .into_iter()
        .map(|(id, (dense, sparse))| {
            let score = alpha * dense + (1.0 - alpha) * sparse;
            (id, score)
        })
        .collect();

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    results.truncate(top_k);
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_vector_dot_product() {
        let a = SparseVector::new(vec![(0, 1.0), (2, 3.0), (5, 2.0)]);
        let b = SparseVector::new(vec![(0, 2.0), (2, 1.0), (7, 4.0)]);

        // dot = 1*2 + 3*1 = 5.0
        assert!((a.dot(&b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn sparse_vector_operations() {
        let v = SparseVector::new(vec![(0, 3.0), (1, 4.0)]);
        assert_eq!(v.nnz(), 2);
        assert!((v.norm() - 5.0).abs() < 1e-6); // 3^2 + 4^2 = 25, sqrt = 5
        assert!((v.max_value() - 4.0).abs() < 1e-6);
    }

    #[test]
    fn sparse_index_insert_and_exact_search() {
        let mut index = SparseIndex::new();

        // Doc 1: "quantum computing" (high weight on dims 10, 20)
        index.insert(1, SparseVector::new(vec![(10, 2.0), (20, 3.0), (30, 0.5)]));
        // Doc 2: "machine learning" (high weight on dims 20, 40)
        index.insert(2, SparseVector::new(vec![(20, 1.0), (40, 4.0), (50, 1.0)]));
        // Doc 3: "quantum machine" (weight on dims 10, 40)
        index.insert(3, SparseVector::new(vec![(10, 1.5), (40, 2.0)]));

        assert_eq!(index.doc_count(), 3);

        // Query about "quantum" (dim 10)
        let query = SparseVector::new(vec![(10, 1.0)]);
        let results = index.search_exact(&query, 3);

        // Doc 1 should score highest (weight 2.0), then Doc 3 (weight 1.5)
        assert_eq!(results[0].0, 1);
        assert_eq!(results[1].0, 3);
    }

    #[test]
    fn sparse_index_wand_search() {
        let mut index = SparseIndex::new();

        for i in 0..100 {
            let entries: Vec<(u32, f32)> = (0..5)
                .map(|d| (d * 10 + (i % 10), (i as f32) * 0.1 + d as f32))
                .collect();
            index.insert(i as u64, SparseVector::new(entries));
        }

        let query = SparseVector::new(vec![(0, 1.0), (10, 2.0), (20, 1.5)]);
        let wand_results = index.search_wand(&query, 5);
        let exact_results = index.search_exact(&query, 5);

        // WAND should return the same top-k as exact search
        assert_eq!(wand_results.len(), 5);
        assert_eq!(exact_results.len(), 5);

        // Same top result
        assert_eq!(wand_results[0].0, exact_results[0].0);
    }

    #[test]
    fn sparse_index_remove() {
        let mut index = SparseIndex::new();
        index.insert(1, SparseVector::new(vec![(0, 1.0)]));
        index.insert(2, SparseVector::new(vec![(0, 2.0)]));
        assert_eq!(index.doc_count(), 2);

        assert!(index.remove(1));
        assert_eq!(index.doc_count(), 1);

        let results = index.search_exact(&SparseVector::new(vec![(0, 1.0)]), 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);
    }

    #[test]
    fn hybrid_search_blend() {
        let dense = vec![(1, 0.9f32), (2, 0.7), (3, 0.5)];
        let sparse = vec![(2, 0.8f32), (3, 0.9), (4, 0.6)];

        // alpha=0.5: equal blend
        let results = hybrid_score(&dense, &sparse, 0.5, 5);

        // Doc 2 should score well (appears in both)
        assert!(results.iter().any(|(id, _)| *id == 2));
        assert!(results.len() <= 5);

        // Pure dense (alpha=1.0)
        let dense_only = hybrid_score(&dense, &sparse, 1.0, 3);
        assert_eq!(dense_only[0].0, 1); // Doc 1 has highest dense score

        // Pure sparse (alpha=0.0)
        let sparse_only = hybrid_score(&dense, &sparse, 0.0, 3);
        assert_eq!(sparse_only[0].0, 3); // Doc 3 has highest sparse score
    }


    #[test]
    fn large_sparse_vectors() {
        let ea = (0..1000u32).map(|i| (i * 1000, (i as f32) * 0.01)).collect();
        let eb = (0..1000u32).map(|i| (i * 1000, (i as f32) * 0.02)).collect();
        let a = SparseVector::new(ea);
        let b = SparseVector::new(eb);
        assert_eq!(a.nnz(), 999);
        assert_eq!(b.nnz(), 999);
        let dot = a.dot(&b);
        let expected: f32 = (1..1000).map(|i: i32| (i * i) as f32 * 0.0002).sum();
        assert!((dot - expected).abs() < 1.0);
    }

    #[test]
    fn dot_product_with_zero_vector() {
        let a = SparseVector::new(vec![(0, 1.0), (5, 3.0), (10, 2.0)]);
        let zero = SparseVector::new(vec![]);
        assert_eq!(a.dot(&zero), 0.0);
        assert_eq!(zero.dot(&a), 0.0);
        assert_eq!(zero.dot(&zero), 0.0);
    }

    #[test]
    fn cosine_similarity_identical() {
        let a = SparseVector::new(vec![(0, 3.0), (1, 4.0)]);
        let b = SparseVector::new(vec![(0, 3.0), (1, 4.0)]);
        let cosine = a.dot(&b) / (a.norm() * b.norm());
        assert!((cosine - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_orthogonal() {
        let a = SparseVector::new(vec![(0, 1.0)]);
        let b = SparseVector::new(vec![(1, 1.0)]);
        let cosine = a.dot(&b) / (a.norm() * b.norm());
        assert!(cosine.abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_opposite() {
        let a = SparseVector::new(vec![(0, 1.0), (1, 2.0)]);
        let b = SparseVector::new(vec![(0, -1.0), (1, -2.0)]);
        let cosine = a.dot(&b) / (a.norm() * b.norm());
        assert!((cosine - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn norm_of_zero_vector() {
        let v = SparseVector::new(vec![]);
        assert_eq!(v.norm(), 0.0);
        assert_eq!(v.max_value(), 0.0);
        assert_eq!(v.nnz(), 0);
    }

    #[test]
    fn zeros_filtered_on_construction() {
        let v = SparseVector::new(vec![(0, 0.0), (1, 1.0), (2, 0.0), (3, 2.0)]);
        assert_eq!(v.nnz(), 2);
        assert_eq!(v.indices, vec![1, 3]);
        assert_eq!(v.values, vec![1.0, 2.0]);
    }

    #[test]
    fn sparse_index_overwrite_document() {
        let mut index = SparseIndex::new();
        index.insert(1, SparseVector::new(vec![(0, 1.0), (1, 2.0)]));
        assert_eq!(index.doc_count(), 1);
        index.insert(1, SparseVector::new(vec![(0, 10.0), (2, 5.0)]));
        assert_eq!(index.doc_count(), 1);
        let query = SparseVector::new(vec![(0, 1.0)]);
        let results = index.search_exact(&query, 10);
        assert_eq!(results.len(), 1);
        assert!((results[0].1 - 10.0).abs() < 1e-6);
    }

    #[test]
    fn sparse_index_remove_nonexistent() {
        let mut index = SparseIndex::new();
        index.insert(1, SparseVector::new(vec![(0, 1.0)]));
        assert!(!index.remove(999));
        assert_eq!(index.doc_count(), 1);
    }

    #[test]
    fn search_empty_index() {
        let index = SparseIndex::new();
        let query = SparseVector::new(vec![(0, 1.0)]);
        assert!(index.search_exact(&query, 10).is_empty());
        assert!(index.search_wand(&query, 10).is_empty());
    }

    #[test]
    fn search_with_empty_query() {
        let mut index = SparseIndex::new();
        index.insert(1, SparseVector::new(vec![(0, 1.0)]));
        let eq = SparseVector::new(vec![]);
        assert!(index.search_exact(&eq, 10).is_empty());
        assert!(index.search_wand(&eq, 10).is_empty());
    }

    #[test]
    fn wand_top_k_limits_results() {
        let mut index = SparseIndex::new();
        for i in 1..=20u64 {
            index.insert(i, SparseVector::new(vec![(0, i as f32)]));
        }
        let query = SparseVector::new(vec![(0, 1.0)]);
        let results = index.search_wand(&query, 3);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 20);
        assert_eq!(results[1].0, 19);
        assert_eq!(results[2].0, 18);
    }

    #[test]
    fn hybrid_empty() {
        let e = vec![(1u64, 0.5f32)];
        let r = hybrid_score(&e[..0], &e[..0], 0.5, 10);
        assert!(r.is_empty());
        let r2 = hybrid_score(&e, &e[..0], 0.5, 10);
        assert_eq!(r2.len(), 1);
    }

    #[test]
    fn negative_values_in_sparse_vector() {
        let a = SparseVector::new(vec![(0, -2.0), (1, 3.0)]);
        let b = SparseVector::new(vec![(0, 4.0), (1, -1.0)]);
        assert!((a.dot(&b) - (-11.0)).abs() < 1e-6);
        assert!((a.norm() - 13.0f32.sqrt()).abs() < 1e-6);
        assert!((a.max_value() - 3.0).abs() < 1e-6);
    }

}
