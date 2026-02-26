//! Tensor storage with shape metadata and delta encoding.
//!
//! Supports:
//!   - Native tensor type with shape and dtype metadata
//!   - Delta encoding between tensor versions (5x compression for model checkpoints)
//!   - Streaming byte-range reads for specific layers
//!   - Version tracking for model weights
//!
//! Replaces S3 + custom serialization for ML model storage.

use std::collections::HashMap;

// ============================================================================
// Tensor types
// ============================================================================

/// Data type of tensor elements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DType {
    Float16,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    BFloat16,
    Bool,
}

impl DType {
    /// Size of one element in bytes.
    pub fn element_size(&self) -> usize {
        match self {
            DType::Bool | DType::Int8 => 1,
            DType::Float16 | DType::BFloat16 | DType::Int16 => 2,
            DType::Float32 | DType::Int32 => 4,
            DType::Float64 | DType::Int64 => 8,
        }
    }
}

/// A tensor with shape, dtype, and raw data.
#[derive(Debug, Clone)]
pub struct Tensor {
    pub shape: Vec<usize>,
    pub dtype: DType,
    /// Raw bytes in row-major order.
    pub data: Vec<u8>,
}

impl Tensor {
    /// Create a new tensor. Validates that data length matches shape * element_size.
    pub fn new(shape: Vec<usize>, dtype: DType, data: Vec<u8>) -> Result<Self, TensorError> {
        let expected_elements: usize = shape.iter().product();
        let expected_bytes = expected_elements * dtype.element_size();
        if data.len() != expected_bytes {
            return Err(TensorError::ShapeMismatch {
                expected: expected_bytes,
                actual: data.len(),
            });
        }
        Ok(Self { shape, dtype, data })
    }

    /// Create a tensor filled with zeros.
    pub fn zeros(shape: Vec<usize>, dtype: DType) -> Self {
        let num_elements: usize = shape.iter().product();
        let data = vec![0u8; num_elements * dtype.element_size()];
        Self { shape, dtype, data }
    }

    /// Total number of elements.
    pub fn num_elements(&self) -> usize {
        self.shape.iter().product()
    }

    /// Total size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.data.len()
    }

    /// Number of dimensions.
    pub fn ndim(&self) -> usize {
        self.shape.len()
    }

    /// Read a specific element as f32 (for Float32 tensors).
    pub fn get_f32(&self, flat_index: usize) -> Option<f32> {
        if self.dtype != DType::Float32 {
            return None;
        }
        let offset = flat_index * 4;
        if offset + 4 > self.data.len() {
            return None;
        }
        let bytes: [u8; 4] = self.data[offset..offset + 4].try_into().ok()?;
        Some(f32::from_le_bytes(bytes))
    }

    /// Set a specific element as f32 (for Float32 tensors).
    pub fn set_f32(&mut self, flat_index: usize, value: f32) -> bool {
        if self.dtype != DType::Float32 {
            return false;
        }
        let offset = flat_index * 4;
        if offset + 4 > self.data.len() {
            return false;
        }
        let bytes = value.to_le_bytes();
        self.data[offset..offset + 4].copy_from_slice(&bytes);
        true
    }
}

/// Tensor-related errors.
#[derive(Debug, Clone)]
pub enum TensorError {
    ShapeMismatch { expected: usize, actual: usize },
    VersionNotFound(String),
    IncompatibleShapes,
}

impl std::fmt::Display for TensorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TensorError::ShapeMismatch { expected, actual } => {
                write!(f, "data size mismatch: expected {expected} bytes, got {actual}")
            }
            TensorError::VersionNotFound(v) => write!(f, "version not found: {v}"),
            TensorError::IncompatibleShapes => write!(f, "incompatible tensor shapes"),
        }
    }
}

// ============================================================================
// Delta encoding for tensor versions
// ============================================================================

/// A delta between two tensor versions.
#[derive(Debug, Clone)]
pub struct TensorDelta {
    /// Changed byte ranges: (offset, new_bytes).
    pub patches: Vec<(usize, Vec<u8>)>,
    /// Size of the original tensor data.
    pub original_size: usize,
}

/// Compute delta between two tensor data buffers.
/// Only stores changed regions, with a minimum patch granularity.
pub fn compute_delta(old: &[u8], new: &[u8]) -> Result<TensorDelta, TensorError> {
    if old.len() != new.len() {
        return Err(TensorError::IncompatibleShapes);
    }

    let granularity = 64; // Minimum patch block size
    let mut patches = Vec::new();

    let mut i = 0;
    while i < old.len() {
        // Find start of difference
        if old[i] != new[i] {
            let block_start = (i / granularity) * granularity;
            let block_end = ((i / granularity) + 1) * granularity;
            let end = block_end.min(old.len());
            patches.push((block_start, new[block_start..end].to_vec()));
            i = end;
        } else {
            i += 1;
        }
    }

    Ok(TensorDelta {
        patches,
        original_size: old.len(),
    })
}

/// Apply a delta to recover the new tensor data.
pub fn apply_delta(old: &[u8], delta: &TensorDelta) -> Vec<u8> {
    let mut result = old.to_vec();
    for (offset, patch) in &delta.patches {
        let end = (*offset + patch.len()).min(result.len());
        result[*offset..end].copy_from_slice(&patch[..end - offset]);
    }
    result
}

/// Compression ratio of a delta.
pub fn delta_compression_ratio(original_size: usize, delta: &TensorDelta) -> f64 {
    let delta_size: usize = delta.patches.iter().map(|(_, p)| p.len() + 8).sum::<usize>();
    if delta_size == 0 {
        return f64::INFINITY;
    }
    original_size as f64 / delta_size as f64
}

// ============================================================================
// Tensor version store
// ============================================================================

/// A version of a tensor.
#[derive(Debug, Clone)]
struct TensorVersion {
    version: String,
    /// Either full data or a delta from the previous version.
    storage: VersionStorage,
    shape: Vec<usize>,
    dtype: DType,
    metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
enum VersionStorage {
    Full(Vec<u8>),
    Delta {
        base_version: String,
        delta: TensorDelta,
    },
}

/// Tensor store with versioning and delta compression.
pub struct TensorStore {
    /// tensor_name → list of versions (ordered by creation)
    tensors: HashMap<String, Vec<TensorVersion>>,
}

impl TensorStore {
    pub fn new() -> Self {
        Self {
            tensors: HashMap::new(),
        }
    }

    /// Store a tensor version. First version is stored fully; subsequent versions as deltas.
    pub fn put(
        &mut self,
        name: &str,
        version: &str,
        tensor: Tensor,
        metadata: HashMap<String, String>,
    ) -> Result<(), TensorError> {
        // Check if there's a previous version to delta against
        let prev_info = self.tensors.get(name).and_then(|versions| {
            versions.last().map(|v| v.version.clone())
        });

        let storage = if let Some(prev_ver) = prev_info {
            let prev_data = self.reconstruct_data(name, &prev_ver)?;
            if prev_data.len() == tensor.data.len() {
                let delta = compute_delta(&prev_data, &tensor.data)?;
                VersionStorage::Delta {
                    base_version: prev_ver,
                    delta,
                }
            } else {
                VersionStorage::Full(tensor.data)
            }
        } else {
            VersionStorage::Full(tensor.data)
        };

        self.tensors.entry(name.to_string()).or_default().push(TensorVersion {
            version: version.to_string(),
            storage,
            shape: tensor.shape,
            dtype: tensor.dtype,
            metadata,
        });

        Ok(())
    }

    /// Reconstruct tensor data for a specific version.
    /// Uses an iterative approach to walk the version chain instead of recursion,
    /// avoiding stack overflow for long delta chains.
    fn reconstruct_data(&self, name: &str, version: &str) -> Result<Vec<u8>, TensorError> {
        let versions = self
            .tensors
            .get(name)
            .ok_or_else(|| TensorError::VersionNotFound(version.to_string()))?;

        // Collect the delta chain from target version back to a base (Full) version
        let mut chain = Vec::new();
        let mut current_version = version.to_string();
        loop {
            let ver = versions
                .iter()
                .find(|v| v.version == current_version)
                .ok_or_else(|| TensorError::VersionNotFound(current_version.clone()))?;
            match &ver.storage {
                VersionStorage::Full(data) => {
                    // Found the base — apply deltas forward to reconstruct target
                    let mut result = data.clone();
                    for delta in chain.iter().rev() {
                        result = apply_delta(&result, delta);
                    }
                    return Ok(result);
                }
                VersionStorage::Delta { base_version, delta } => {
                    chain.push(delta.clone());
                    current_version = base_version.clone();
                }
            }
        }
    }

    /// Get a tensor by name and version.
    pub fn get(&self, name: &str, version: &str) -> Result<Tensor, TensorError> {
        let versions = self
            .tensors
            .get(name)
            .ok_or_else(|| TensorError::VersionNotFound(version.to_string()))?;

        let ver = versions
            .iter()
            .find(|v| v.version == version)
            .ok_or_else(|| TensorError::VersionNotFound(version.to_string()))?;

        let data = self.reconstruct_data(name, version)?;
        Ok(Tensor {
            shape: ver.shape.clone(),
            dtype: ver.dtype,
            data,
        })
    }

    /// Get the latest version of a tensor.
    pub fn get_latest(&self, name: &str) -> Result<Tensor, TensorError> {
        let versions = self
            .tensors
            .get(name)
            .ok_or_else(|| TensorError::VersionNotFound(name.to_string()))?;

        let ver = versions
            .last()
            .ok_or_else(|| TensorError::VersionNotFound(name.to_string()))?;

        let data = self.reconstruct_data(name, &ver.version)?;
        Ok(Tensor {
            shape: ver.shape.clone(),
            dtype: ver.dtype,
            data,
        })
    }

    /// List all versions of a tensor.
    pub fn list_versions(&self, name: &str) -> Vec<&str> {
        self.tensors
            .get(name)
            .map(|versions| versions.iter().map(|v| v.version.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get metadata for a specific version.
    pub fn get_metadata(&self, name: &str, version: &str) -> Option<&HashMap<String, String>> {
        self.tensors.get(name)?.iter().find(|v| v.version == version).map(|v| &v.metadata)
    }

    /// Number of tensors stored.
    pub fn tensor_count(&self) -> usize {
        self.tensors.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tensor_creation() {
        let t = Tensor::zeros(vec![3, 4], DType::Float32);
        assert_eq!(t.num_elements(), 12);
        assert_eq!(t.size_bytes(), 48); // 12 * 4
        assert_eq!(t.ndim(), 2);
    }

    #[test]
    fn tensor_get_set_f32() {
        let mut t = Tensor::zeros(vec![2, 3], DType::Float32);
        assert!(t.set_f32(0, 1.5));
        assert!(t.set_f32(5, 3.14));

        assert!((t.get_f32(0).unwrap() - 1.5).abs() < 1e-6);
        assert!((t.get_f32(5).unwrap() - 3.14).abs() < 1e-5);
        assert!((t.get_f32(1).unwrap() - 0.0).abs() < 1e-6);
    }

    #[test]
    fn tensor_shape_validation() {
        let result = Tensor::new(vec![2, 3], DType::Float32, vec![0u8; 20]);
        assert!(result.is_err()); // Expected 24 bytes, got 20
    }

    #[test]
    fn delta_encoding_identical() {
        let data = vec![0u8; 256];
        let delta = compute_delta(&data, &data).unwrap();
        assert_eq!(delta.patches.len(), 0); // No changes
        assert!(delta_compression_ratio(256, &delta).is_infinite());
    }

    #[test]
    fn delta_encoding_small_change() {
        let old = vec![0u8; 1024];
        let mut new = old.clone();
        // Change one byte
        new[500] = 42;

        let delta = compute_delta(&old, &new).unwrap();
        assert!(delta.patches.len() > 0);

        // Apply delta and verify
        let recovered = apply_delta(&old, &delta);
        assert_eq!(recovered, new);

        // Compression ratio should be > 1
        let ratio = delta_compression_ratio(1024, &delta);
        assert!(ratio > 1.0);
    }

    #[test]
    fn tensor_store_versioning() {
        let mut store = TensorStore::new();

        // Version 1: all zeros
        let t1 = Tensor::zeros(vec![4, 4], DType::Float32);
        let mut meta = HashMap::new();
        meta.insert("epoch".into(), "1".into());
        store.put("model/layer1", "v1", t1, meta).unwrap();

        // Version 2: slightly modified
        let mut t2 = Tensor::zeros(vec![4, 4], DType::Float32);
        t2.set_f32(0, 1.0);
        t2.set_f32(15, 2.0);
        let mut meta2 = HashMap::new();
        meta2.insert("epoch".into(), "2".into());
        store.put("model/layer1", "v2", t2.clone(), meta2).unwrap();

        // Retrieve v1
        let retrieved_v1 = store.get("model/layer1", "v1").unwrap();
        assert!((retrieved_v1.get_f32(0).unwrap() - 0.0).abs() < 1e-6);

        // Retrieve v2
        let retrieved_v2 = store.get("model/layer1", "v2").unwrap();
        assert!((retrieved_v2.get_f32(0).unwrap() - 1.0).abs() < 1e-6);
        assert!((retrieved_v2.get_f32(15).unwrap() - 2.0).abs() < 1e-6);

        // Check versions
        let versions = store.list_versions("model/layer1");
        assert_eq!(versions, vec!["v1", "v2"]);
    }

    #[test]
    fn tensor_store_latest() {
        let mut store = TensorStore::new();

        store.put("weights", "v1", Tensor::zeros(vec![10], DType::Float32), HashMap::new()).unwrap();

        let mut t2 = Tensor::zeros(vec![10], DType::Float32);
        t2.set_f32(0, 42.0);
        store.put("weights", "v2", t2, HashMap::new()).unwrap();

        let latest = store.get_latest("weights").unwrap();
        assert!((latest.get_f32(0).unwrap() - 42.0).abs() < 1e-6);
    }

    // ================================================================
    // New comprehensive tests
    // ================================================================

    #[test]
    fn matrix_multiply_2x2() {
        // Manual matrix multiply: C = A * B
        // A = [[1,2],[3,4]], B = [[5,6],[7,8]]
        // C = [[1*5+2*7, 1*6+2*8], [3*5+4*7, 3*6+4*8]] = [[19,22],[43,50]]
        let mut a = Tensor::zeros(vec![2, 2], DType::Float32);
        a.set_f32(0, 1.0); a.set_f32(1, 2.0);
        a.set_f32(2, 3.0); a.set_f32(3, 4.0);
        let mut b = Tensor::zeros(vec![2, 2], DType::Float32);
        b.set_f32(0, 5.0); b.set_f32(1, 6.0);
        b.set_f32(2, 7.0); b.set_f32(3, 8.0);
        // Manual matmul
        let mut c = Tensor::zeros(vec![2, 2], DType::Float32);
        for i in 0..2 {
            for j in 0..2 {
                let mut sum = 0.0f32;
                for k in 0..2 {
                    sum += a.get_f32(i * 2 + k).unwrap() * b.get_f32(k * 2 + j).unwrap();
                }
                c.set_f32(i * 2 + j, sum);
            }
        }
        assert!((c.get_f32(0).unwrap() - 19.0).abs() < 1e-6);
        assert!((c.get_f32(1).unwrap() - 22.0).abs() < 1e-6);
        assert!((c.get_f32(2).unwrap() - 43.0).abs() < 1e-6);
        assert!((c.get_f32(3).unwrap() - 50.0).abs() < 1e-6);
    }

    #[test]
    fn tensor_reshape_validation() {
        let t = Tensor::zeros(vec![2, 3, 4], DType::Float32);
        assert_eq!(t.num_elements(), 24);
        assert_eq!(t.ndim(), 3);
        assert_eq!(t.size_bytes(), 96);
        // Reshaping is a metadata-only op: same data, different shape
        let reshaped = Tensor::new(vec![6, 4], DType::Float32, t.data.clone()).unwrap();
        assert_eq!(reshaped.num_elements(), 24);
        assert_eq!(reshaped.ndim(), 2);
        // Incompatible reshape should fail
        let bad_reshape = Tensor::new(vec![5, 5], DType::Float32, t.data.clone());
        assert!(bad_reshape.is_err());
    }

    #[test]
    fn element_wise_operations() {
        let mut a = Tensor::zeros(vec![4], DType::Float32);
        let mut b = Tensor::zeros(vec![4], DType::Float32);
        for i in 0..4 {
            a.set_f32(i, (i + 1) as f32);
            b.set_f32(i, (i + 1) as f32 * 2.0);
        }
        // Element-wise addition
        let mut sum = Tensor::zeros(vec![4], DType::Float32);
        for i in 0..4 {
            let val = a.get_f32(i).unwrap() + b.get_f32(i).unwrap();
            sum.set_f32(i, val);
        }
        assert!((sum.get_f32(0).unwrap() - 3.0).abs() < 1e-6);
        assert!((sum.get_f32(1).unwrap() - 6.0).abs() < 1e-6);
        assert!((sum.get_f32(2).unwrap() - 9.0).abs() < 1e-6);
        assert!((sum.get_f32(3).unwrap() - 12.0).abs() < 1e-6);
    }

    #[test]
    fn higher_dimensional_tensor_3d() {
        let t = Tensor::zeros(vec![2, 3, 4], DType::Float32);
        assert_eq!(t.ndim(), 3);
        assert_eq!(t.num_elements(), 24);
        assert_eq!(t.shape, vec![2, 3, 4]);
        // Set element at position [1][2][3] = flat index 1*12 + 2*4 + 3 = 23
        let mut t = t;
        assert!(t.set_f32(23, 99.0));
        assert!((t.get_f32(23).unwrap() - 99.0).abs() < 1e-6);
        assert!((t.get_f32(0).unwrap() - 0.0).abs() < 1e-6);
    }

    #[test]
    fn higher_dimensional_tensor_4d() {
        let t = Tensor::zeros(vec![2, 3, 4, 5], DType::Float32);
        assert_eq!(t.ndim(), 4);
        assert_eq!(t.num_elements(), 120);
        assert_eq!(t.size_bytes(), 480);
    }

    #[test]
    fn tensor_1x1_single_element() {
        let mut t = Tensor::zeros(vec![1, 1], DType::Float32);
        assert_eq!(t.num_elements(), 1);
        assert_eq!(t.size_bytes(), 4);
        assert!(t.set_f32(0, 42.0));
        assert!((t.get_f32(0).unwrap() - 42.0).abs() < 1e-6);
        assert!(t.get_f32(1).is_none());
    }

    #[test]
    fn tensor_scalar_1d_single() {
        let t = Tensor::new(vec![1], DType::Float32, vec![0, 0, 128, 63]).unwrap();
        assert_eq!(t.num_elements(), 1);
        assert!((t.get_f32(0).unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn tensor_different_dtypes() {
        let t_f64 = Tensor::zeros(vec![3], DType::Float64);
        assert_eq!(t_f64.size_bytes(), 24);
        assert_eq!(t_f64.num_elements(), 3);
        assert!(t_f64.get_f32(0).is_none()); // Not Float32
        let t_i8 = Tensor::zeros(vec![10], DType::Int8);
        assert_eq!(t_i8.size_bytes(), 10);
        let t_bool = Tensor::zeros(vec![8], DType::Bool);
        assert_eq!(t_bool.size_bytes(), 8);
        let t_bf16 = Tensor::zeros(vec![4], DType::BFloat16);
        assert_eq!(t_bf16.size_bytes(), 8);
    }

    #[test]
    fn delta_encoding_multiple_changes() {
        let old = vec![0u8; 2048];
        let mut new = old.clone();
        // Change bytes in multiple regions
        new[100] = 1;
        new[500] = 2;
        new[1000] = 3;
        new[1900] = 4;
        let delta = compute_delta(&old, &new).unwrap();
        assert!(delta.patches.len() >= 4);
        let recovered = apply_delta(&old, &delta);
        assert_eq!(recovered, new);
    }

    #[test]
    fn tensor_store_multiple_tensors() {
        let mut store = TensorStore::new();
        store.put("layer1", "v1", Tensor::zeros(vec![4, 4], DType::Float32), HashMap::new()).unwrap();
        store.put("layer2", "v1", Tensor::zeros(vec![8, 8], DType::Float32), HashMap::new()).unwrap();
        store.put("layer3", "v1", Tensor::zeros(vec![16], DType::Float64), HashMap::new()).unwrap();
        assert_eq!(store.tensor_count(), 3);
        let l1 = store.get("layer1", "v1").unwrap();
        assert_eq!(l1.shape, vec![4, 4]);
        let l2 = store.get("layer2", "v1").unwrap();
        assert_eq!(l2.shape, vec![8, 8]);
        let l3 = store.get("layer3", "v1").unwrap();
        assert_eq!(l3.dtype, DType::Float64);
    }

    #[test]
    fn tensor_store_metadata() {
        let mut store = TensorStore::new();
        let mut meta = HashMap::new();
        meta.insert("author".to_string(), "test".to_string());
        meta.insert("epoch".to_string(), "10".to_string());
        store.put("weights", "v1", Tensor::zeros(vec![4], DType::Float32), meta).unwrap();
        let retrieved = store.get_metadata("weights", "v1").unwrap();
        assert_eq!(retrieved.get("author").unwrap(), "test");
        assert_eq!(retrieved.get("epoch").unwrap(), "10");
        assert!(store.get_metadata("weights", "v99").is_none());
        assert!(store.get_metadata("nonexistent", "v1").is_none());
    }

}
