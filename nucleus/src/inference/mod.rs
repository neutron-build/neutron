//! Built-in inference engine — run ML models inside the database.
//!
//! Provides a pluggable [`InferenceEngine`] trait, a [`ModelRegistry`] for
//! managing named models, and several built-in model types that require no
//! external runtime: [`LinearModel`], [`SoftmaxModel`], and
//! [`KNearestNeighbors`].  An [`EmbeddingGenerator`] turns raw text into
//! numeric vectors via bag-of-words / TF-IDF.

use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by the inference subsystem.
#[derive(Debug, Clone, PartialEq)]
pub enum InferenceError {
    /// The requested model name is not registered.
    ModelNotFound,
    /// Input vector length does not match the model's expected dimensionality.
    DimensionMismatch { expected: usize, got: usize },
    /// Catch-all for other invalid-input conditions.
    InvalidInput(String),
}

impl fmt::Display for InferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ModelNotFound => write!(f, "model not found"),
            Self::DimensionMismatch { expected, got } => {
                write!(f, "dimension mismatch: expected {expected}, got {got}")
            }
            Self::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
        }
    }
}

impl std::error::Error for InferenceError {}

// ---------------------------------------------------------------------------
// Model format & metadata
// ---------------------------------------------------------------------------

/// Supported serialisation formats for persisted models.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelFormat {
    /// ONNX format (placeholder — no runtime bundled).
    Onnx,
    /// Application-specific custom format.
    Custom,
}

/// Descriptive metadata attached to every registered model.
#[derive(Debug, Clone)]
pub struct ModelMetadata {
    pub name: String,
    pub format: ModelFormat,
    pub input_dims: Vec<usize>,
    pub output_dims: Vec<usize>,
    pub created_at: u64,
    pub description: String,
    pub version: String,
}

// ---------------------------------------------------------------------------
// InferenceEngine trait
// ---------------------------------------------------------------------------

/// Pluggable execution backend.  Implementors can wrap ONNX, TensorFlow, or
/// any other runtime; the built-in models also implement this trait directly.
pub trait InferenceEngine {
    /// Run a single forward pass.
    fn predict(&self, model_name: &str, input: &[f32]) -> Result<Vec<f32>, InferenceError>;

    /// Run a batch of forward passes, returning one output vector per input.
    fn batch_predict(
        &self,
        model_name: &str,
        inputs: &[Vec<f32>],
    ) -> Result<Vec<Vec<f32>>, InferenceError> {
        inputs
            .iter()
            .map(|inp| self.predict(model_name, inp))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Built-in models
// ---------------------------------------------------------------------------

/// A simple linear model: `y = dot(weights, x) + bias`.
#[derive(Debug, Clone)]
pub struct LinearModel {
    pub weights: Vec<f32>,
    pub bias: f32,
}

impl LinearModel {
    pub fn new(weights: Vec<f32>, bias: f32) -> Self {
        Self { weights, bias }
    }

    /// Compute `dot(weights, input) + bias`.
    pub fn predict(&self, input: &[f32]) -> Result<Vec<f32>, InferenceError> {
        if input.len() != self.weights.len() {
            return Err(InferenceError::DimensionMismatch {
                expected: self.weights.len(),
                got: input.len(),
            });
        }
        let dot: f32 = self.weights.iter().zip(input).map(|(w, x)| w * x).sum();
        Ok(vec![dot + self.bias])
    }
}

/// Multinomial softmax classifier: `y = softmax(W * x + b)`.
///
/// `weight_matrix` has shape `[num_classes][input_dim]`.
#[derive(Debug, Clone)]
pub struct SoftmaxModel {
    pub weight_matrix: Vec<Vec<f32>>,
    pub biases: Vec<f32>,
}

impl SoftmaxModel {
    pub fn new(weight_matrix: Vec<Vec<f32>>, biases: Vec<f32>) -> Self {
        Self {
            weight_matrix,
            biases,
        }
    }

    /// Compute softmax probabilities for each class.
    pub fn predict(&self, input: &[f32]) -> Result<Vec<f32>, InferenceError> {
        if self.weight_matrix.is_empty() {
            return Err(InferenceError::InvalidInput(
                "weight matrix is empty".into(),
            ));
        }
        let input_dim = self.weight_matrix[0].len();
        if input.len() != input_dim {
            return Err(InferenceError::DimensionMismatch {
                expected: input_dim,
                got: input.len(),
            });
        }

        // Compute logits = W * x + b
        let logits: Vec<f32> = self
            .weight_matrix
            .iter()
            .zip(&self.biases)
            .map(|(row, b)| {
                let dot: f32 = row.iter().zip(input).map(|(w, x)| w * x).sum();
                dot + b
            })
            .collect();

        // Numerically-stable softmax: subtract max before exp.
        let max_logit = logits
            .iter()
            .copied()
            .fold(f32::NEG_INFINITY, f32::max);
        let exps: Vec<f32> = logits.iter().map(|l| (l - max_logit).exp()).collect();
        let sum_exp: f32 = exps.iter().sum();
        Ok(exps.into_iter().map(|e| e / sum_exp).collect())
    }

    /// Return the index of the class with the highest probability.
    pub fn classify_index(&self, input: &[f32]) -> Result<usize, InferenceError> {
        let probs = self.predict(input)?;
        Ok(probs
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(i, _)| i)
            .unwrap())
    }
}

/// K-Nearest Neighbours classifier operating on L2 distance.
#[derive(Debug, Clone)]
pub struct KNearestNeighbors {
    /// Each entry is `(feature_vector, label)`.
    pub vectors: Vec<(Vec<f32>, String)>,
    pub k: usize,
}

impl KNearestNeighbors {
    pub fn new(vectors: Vec<(Vec<f32>, String)>, k: usize) -> Self {
        Self { vectors, k }
    }

    /// Squared L2 distance between two vectors.
    fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum()
    }

    /// Predict by majority vote of the `k` nearest neighbours.
    ///
    /// Returns a one-hot-style vector where the index of the winning class
    /// receives `1.0` and all others receive `0.0`.  Use [`classify`] on the
    /// registry to get the label string directly.
    pub fn predict(&self, input: &[f32]) -> Result<Vec<f32>, InferenceError> {
        if self.vectors.is_empty() {
            return Err(InferenceError::InvalidInput(
                "KNN has no stored vectors".into(),
            ));
        }
        let dim = self.vectors[0].0.len();
        if input.len() != dim {
            return Err(InferenceError::DimensionMismatch {
                expected: dim,
                got: input.len(),
            });
        }

        // Collect unique labels and assign indices.
        let mut label_set: Vec<String> = Vec::new();
        for (_, lbl) in &self.vectors {
            if !label_set.contains(lbl) {
                label_set.push(lbl.clone());
            }
        }

        // Compute distances.
        let mut dists: Vec<(f32, &str)> = self
            .vectors
            .iter()
            .map(|(v, l)| (Self::l2_sq(input, v), l.as_str()))
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // Majority vote among the k nearest.
        let mut votes: HashMap<&str, usize> = HashMap::new();
        for (_, lbl) in dists.iter().take(self.k) {
            *votes.entry(lbl).or_default() += 1;
        }
        let winner = votes
            .into_iter()
            .max_by_key(|(_, v)| *v)
            .map(|(l, _)| l)
            .unwrap();

        // Build one-hot output.
        let mut out = vec![0.0f32; label_set.len()];
        if let Some(idx) = label_set.iter().position(|l| l == winner) {
            out[idx] = 1.0;
        }
        Ok(out)
    }

    /// Return the winning class label directly.
    pub fn classify(&self, input: &[f32]) -> Result<String, InferenceError> {
        if self.vectors.is_empty() {
            return Err(InferenceError::InvalidInput(
                "KNN has no stored vectors".into(),
            ));
        }
        let dim = self.vectors[0].0.len();
        if input.len() != dim {
            return Err(InferenceError::DimensionMismatch {
                expected: dim,
                got: input.len(),
            });
        }

        let mut dists: Vec<(f32, &str)> = self
            .vectors
            .iter()
            .map(|(v, l)| (Self::l2_sq(input, v), l.as_str()))
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let mut votes: HashMap<&str, usize> = HashMap::new();
        for (_, lbl) in dists.iter().take(self.k) {
            *votes.entry(lbl).or_default() += 1;
        }
        let winner = votes
            .into_iter()
            .max_by_key(|(_, v)| *v)
            .map(|(l, _)| l)
            .unwrap();
        Ok(winner.to_string())
    }
}

// ---------------------------------------------------------------------------
// Internal enum wrapping all built-in model kinds
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ONNX Runtime model (behind feature flag — zero cost when disabled)
// ---------------------------------------------------------------------------

#[cfg(feature = "onnx")]
pub struct OnnxModel {
    /// Mutex because ort::Session::run takes &mut self.
    session: std::sync::Mutex<ort::session::Session>,
    /// Cached input name for the first input tensor.
    input_name: String,
    /// Expected dimensionality of the first input tensor (0 = dynamic).
    input_dim: usize,
}

#[cfg(feature = "onnx")]
impl OnnxModel {
    /// Extract input metadata from a session.
    fn extract_metadata(session: &ort::session::Session) -> (String, usize) {
        let input_name = session
            .inputs()
            .first()
            .map(|i| i.name().to_string())
            .unwrap_or_else(|| "input".to_string());

        let input_dim = session
            .inputs()
            .first()
            .and_then(|i| {
                if let ort::value::ValueType::Tensor { shape, .. } = i.dtype() {
                    shape.last().and_then(|&d| if d > 0 { Some(d as usize) } else { None })
                } else {
                    None
                }
            })
            .unwrap_or(0);

        (input_name, input_dim)
    }

    /// Load an ONNX model from a file path.
    pub fn from_file(path: &str) -> Result<Self, InferenceError> {
        let session = ort::session::Session::builder()
            .and_then(|b| b.with_intra_threads(1))
            .and_then(|b| b.commit_from_file(path))
            .map_err(|e| InferenceError::InvalidInput(format!("ONNX load error: {e}")))?;

        let (input_name, input_dim) = Self::extract_metadata(&session);
        Ok(Self {
            session: std::sync::Mutex::new(session),
            input_name,
            input_dim,
        })
    }

    /// Load an ONNX model from in-memory bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, InferenceError> {
        let session = ort::session::Session::builder()
            .and_then(|b| b.with_intra_threads(1))
            .and_then(|b| b.commit_from_memory(data))
            .map_err(|e| InferenceError::InvalidInput(format!("ONNX load error: {e}")))?;

        let (input_name, input_dim) = Self::extract_metadata(&session);
        Ok(Self {
            session: std::sync::Mutex::new(session),
            input_name,
            input_dim,
        })
    }

    /// Run inference. Input is shaped as `[1, input.len()]`.
    fn run_session(&self, input: &[f32]) -> Result<Vec<f32>, InferenceError> {
        let arr = ndarray::Array2::from_shape_vec((1, input.len()), input.to_vec())
            .map_err(|e| InferenceError::InvalidInput(format!("shape error: {e}")))?;
        let input_tensor = ort::value::Tensor::from_array(arr)
            .map_err(|e| InferenceError::InvalidInput(format!("tensor error: {e}")))?;

        let mut session = self.session.lock()
            .map_err(|e| InferenceError::InvalidInput(format!("session lock error: {e}")))?;
        let outputs = session
            .run(ort::inputs![input_tensor])
            .map_err(|e| InferenceError::InvalidInput(format!("ONNX run error: {e}")))?;

        let output = &outputs[0];
        let (_, data) = output
            .try_extract_tensor::<f32>()
            .map_err(|e| InferenceError::InvalidInput(format!("output extract error: {e}")))?;

        Ok(data.to_vec())
    }

    /// Run a single forward pass.
    pub fn predict(&self, input: &[f32]) -> Result<Vec<f32>, InferenceError> {
        if self.input_dim > 0 && input.len() != self.input_dim {
            return Err(InferenceError::DimensionMismatch {
                expected: self.input_dim,
                got: input.len(),
            });
        }
        self.run_session(input)
    }

    /// Batch predict — feeds multiple inputs as a single batched tensor.
    pub fn batch_predict(&self, inputs: &[Vec<f32>]) -> Result<Vec<Vec<f32>>, InferenceError> {
        if inputs.is_empty() {
            return Ok(vec![]);
        }
        let dim = inputs[0].len();
        let batch_size = inputs.len();

        let flat: Vec<f32> = inputs.iter().flat_map(|v| v.iter().copied()).collect();
        let arr = ndarray::Array2::from_shape_vec((batch_size, dim), flat)
            .map_err(|e| InferenceError::InvalidInput(format!("shape error: {e}")))?;
        let input_tensor = ort::value::Tensor::from_array(arr)
            .map_err(|e| InferenceError::InvalidInput(format!("tensor error: {e}")))?;

        let mut session = self.session.lock()
            .map_err(|e| InferenceError::InvalidInput(format!("session lock error: {e}")))?;
        let outputs = session
            .run(ort::inputs![input_tensor])
            .map_err(|e| InferenceError::InvalidInput(format!("ONNX run error: {e}")))?;

        let output = &outputs[0];
        let (_, data) = output
            .try_extract_tensor::<f32>()
            .map_err(|e| InferenceError::InvalidInput(format!("output extract error: {e}")))?;

        let out_dim = data.len() / batch_size;
        Ok(data.chunks(out_dim).map(|c| c.to_vec()).collect())
    }
}

#[cfg(feature = "onnx")]
impl fmt::Debug for OnnxModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnnxModel")
            .field("input_name", &self.input_name)
            .field("input_dim", &self.input_dim)
            .finish()
    }
}

#[derive(Debug, Clone)]
enum BuiltinModel {
    Linear(LinearModel),
    Softmax(SoftmaxModel),
    Knn(KNearestNeighbors),
}

// ---------------------------------------------------------------------------
// Model Registry
// ---------------------------------------------------------------------------

/// Central registry of named models.  Models are stored in-memory and can be
/// invoked by name through [`predict`](ModelRegistry::predict) and
/// [`batch_predict`](ModelRegistry::batch_predict).
pub struct ModelRegistry {
    models: HashMap<String, (ModelMetadata, BuiltinModel)>,
    /// Class-label tables for classification models (softmax, KNN).
    class_labels: HashMap<String, Vec<String>>,
    /// ONNX models stored separately (OnnxModel is not Clone).
    #[cfg(feature = "onnx")]
    onnx_models: HashMap<String, (ModelMetadata, OnnxModel)>,
}

impl ModelRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
            class_labels: HashMap::new(),
            #[cfg(feature = "onnx")]
            onnx_models: HashMap::new(),
        }
    }

    fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    // -- registration -------------------------------------------------------

    /// Register a [`LinearModel`].
    pub fn register_linear(&mut self, name: &str, weights: Vec<f32>, bias: f32) {
        let dim = weights.len();
        let meta = ModelMetadata {
            name: name.to_string(),
            format: ModelFormat::Custom,
            input_dims: vec![dim],
            output_dims: vec![1],
            created_at: Self::now_epoch(),
            description: "linear model".into(),
            version: "1.0".into(),
        };
        let model = BuiltinModel::Linear(LinearModel::new(weights, bias));
        self.models.insert(name.to_string(), (meta, model));
    }

    /// Register a [`SoftmaxModel`].
    ///
    /// `class_names` optionally maps each output index to a human-readable
    /// label; if `None`, labels `"0"`, `"1"`, ... are generated.
    pub fn register_softmax(
        &mut self,
        name: &str,
        weight_matrix: Vec<Vec<f32>>,
        biases: Vec<f32>,
    ) {
        self.register_softmax_with_labels(name, weight_matrix, biases, None);
    }

    /// Like [`register_softmax`](Self::register_softmax) but accepts explicit
    /// class labels.
    pub fn register_softmax_with_labels(
        &mut self,
        name: &str,
        weight_matrix: Vec<Vec<f32>>,
        biases: Vec<f32>,
        class_names: Option<Vec<String>>,
    ) {
        let num_classes = weight_matrix.len();
        let input_dim = weight_matrix.first().map(|r| r.len()).unwrap_or(0);
        let meta = ModelMetadata {
            name: name.to_string(),
            format: ModelFormat::Custom,
            input_dims: vec![input_dim],
            output_dims: vec![num_classes],
            created_at: Self::now_epoch(),
            description: "softmax classifier".into(),
            version: "1.0".into(),
        };
        let labels = class_names.unwrap_or_else(|| {
            (0..num_classes).map(|i| i.to_string()).collect()
        });
        self.class_labels.insert(name.to_string(), labels);
        let model = BuiltinModel::Softmax(SoftmaxModel::new(weight_matrix, biases));
        self.models.insert(name.to_string(), (meta, model));
    }

    /// Register a [`KNearestNeighbors`] model.
    pub fn register_knn(
        &mut self,
        name: &str,
        vectors: Vec<(Vec<f32>, String)>,
        k: usize,
    ) {
        let dim = vectors.first().map(|(v, _)| v.len()).unwrap_or(0);
        // Collect unique labels preserving insertion order.
        let mut labels: Vec<String> = Vec::new();
        for (_, lbl) in &vectors {
            if !labels.contains(lbl) {
                labels.push(lbl.clone());
            }
        }
        let num_classes = labels.len();
        let meta = ModelMetadata {
            name: name.to_string(),
            format: ModelFormat::Custom,
            input_dims: vec![dim],
            output_dims: vec![num_classes],
            created_at: Self::now_epoch(),
            description: format!("k-nearest neighbours (k={k})"),
            version: "1.0".into(),
        };
        self.class_labels.insert(name.to_string(), labels);
        let model = BuiltinModel::Knn(KNearestNeighbors::new(vectors, k));
        self.models.insert(name.to_string(), (meta, model));
    }

    // -- ONNX model registration (feature-gated) ----------------------------

    /// Register an ONNX model loaded from a file path.
    ///
    /// Only available when compiled with `--features onnx`. Has zero impact
    /// on binary size or runtime when the feature is disabled.
    #[cfg(feature = "onnx")]
    pub fn register_onnx_file(
        &mut self,
        name: &str,
        path: &str,
        description: &str,
    ) -> Result<(), InferenceError> {
        let model = OnnxModel::from_file(path)?;
        let meta = ModelMetadata {
            name: name.to_string(),
            format: ModelFormat::Onnx,
            input_dims: vec![model.input_dim],
            output_dims: vec![],
            created_at: Self::now_epoch(),
            description: description.to_string(),
            version: "1.0".into(),
        };
        self.onnx_models.insert(name.to_string(), (meta, model));
        Ok(())
    }

    /// Register an ONNX model from in-memory bytes.
    #[cfg(feature = "onnx")]
    pub fn register_onnx_bytes(
        &mut self,
        name: &str,
        data: &[u8],
        description: &str,
    ) -> Result<(), InferenceError> {
        let model = OnnxModel::from_bytes(data)?;
        let meta = ModelMetadata {
            name: name.to_string(),
            format: ModelFormat::Onnx,
            input_dims: vec![model.input_dim],
            output_dims: vec![],
            created_at: Self::now_epoch(),
            description: description.to_string(),
            version: "1.0".into(),
        };
        self.onnx_models.insert(name.to_string(), (meta, model));
        Ok(())
    }

    /// Check if a model name refers to an ONNX model.
    #[cfg(feature = "onnx")]
    pub fn is_onnx_model(&self, name: &str) -> bool {
        self.onnx_models.contains_key(name)
    }

    // -- inference -----------------------------------------------------------

    /// Single-input prediction.
    pub fn predict(&self, name: &str, input: &[f32]) -> Result<Vec<f32>, InferenceError> {
        // Check ONNX models first (when feature enabled).
        #[cfg(feature = "onnx")]
        if let Some((_, onnx)) = self.onnx_models.get(name) {
            return onnx.predict(input);
        }

        let (_, model) = self.models.get(name).ok_or(InferenceError::ModelNotFound)?;
        match model {
            BuiltinModel::Linear(m) => m.predict(input),
            BuiltinModel::Softmax(m) => m.predict(input),
            BuiltinModel::Knn(m) => m.predict(input),
        }
    }

    /// Batch prediction — runs [`predict`](Self::predict) for each input.
    /// ONNX models use true batched inference for better throughput.
    pub fn batch_predict(
        &self,
        name: &str,
        inputs: &[Vec<f32>],
    ) -> Result<Vec<Vec<f32>>, InferenceError> {
        #[cfg(feature = "onnx")]
        if let Some((_, onnx)) = self.onnx_models.get(name) {
            return onnx.batch_predict(inputs);
        }

        inputs.iter().map(|inp| self.predict(name, inp)).collect()
    }

    /// Classification convenience: returns the winning class label.
    pub fn classify(&self, name: &str, input: &[f32]) -> Result<String, InferenceError> {
        // ONNX models: run predict and return argmax index as class label.
        #[cfg(feature = "onnx")]
        if self.onnx_models.contains_key(name) {
            let output = self.predict(name, input)?;
            let (idx, _) = output
                .iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or((0, &0.0));
            let labels = self.class_labels.get(name);
            return Ok(labels
                .and_then(|l| l.get(idx))
                .cloned()
                .unwrap_or_else(|| format!("class_{idx}")));
        }

        let (_, model) = self.models.get(name).ok_or(InferenceError::ModelNotFound)?;
        match model {
            BuiltinModel::Linear(_) => {
                // For a linear model, return "positive" or "negative" based on
                // the sign of the output.
                let out = self.predict(name, input)?;
                if out[0] >= 0.0 {
                    Ok("positive".to_string())
                } else {
                    Ok("negative".to_string())
                }
            }
            BuiltinModel::Softmax(m) => {
                let idx = m.classify_index(input)?;
                let labels = self.class_labels.get(name).unwrap();
                Ok(labels[idx].clone())
            }
            BuiltinModel::Knn(m) => m.classify(input),
        }
    }

    // -- management ----------------------------------------------------------

    /// List metadata for every registered model (including ONNX).
    pub fn list_models(&self) -> Vec<&ModelMetadata> {
        #[allow(unused_mut)]
        let mut result: Vec<&ModelMetadata> = self.models.values().map(|(meta, _)| meta).collect();
        #[cfg(feature = "onnx")]
        result.extend(self.onnx_models.values().map(|(meta, _)| meta));
        result
    }

    /// Remove a model from the registry.
    pub fn unregister(&mut self, name: &str) {
        self.models.remove(name);
        self.class_labels.remove(name);
        #[cfg(feature = "onnx")]
        self.onnx_models.remove(name);
    }
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// InferenceEngine impl for ModelRegistry
// ---------------------------------------------------------------------------

impl InferenceEngine for ModelRegistry {
    fn predict(&self, model_name: &str, input: &[f32]) -> Result<Vec<f32>, InferenceError> {
        ModelRegistry::predict(self, model_name, input)
    }

    fn batch_predict(
        &self,
        model_name: &str,
        inputs: &[Vec<f32>],
    ) -> Result<Vec<Vec<f32>>, InferenceError> {
        ModelRegistry::batch_predict(self, model_name, inputs)
    }
}

// ---------------------------------------------------------------------------
// Embedding Generator
// ---------------------------------------------------------------------------

/// Bag-of-words / TF-IDF embedding generator.
///
/// Call [`build_vocabulary`](EmbeddingGenerator::build_vocabulary) first,
/// then use [`embed`](EmbeddingGenerator::embed) or
/// [`embed_tfidf`](EmbeddingGenerator::embed_tfidf) to turn text into
/// fixed-length numeric vectors.
pub struct EmbeddingGenerator {
    /// Maps each known word to its index in the output vector.
    pub vocabulary: HashMap<String, usize>,
}

impl EmbeddingGenerator {
    pub fn new() -> Self {
        Self {
            vocabulary: HashMap::new(),
        }
    }

    /// Tokenise by splitting on non-alphanumeric characters and lowercasing.
    fn tokenize(text: &str) -> Vec<String> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .map(|w| w.to_lowercase())
            .collect()
    }

    /// Build the vocabulary from a corpus of documents.  Each unique word
    /// receives a unique index (insertion order).
    pub fn build_vocabulary(&mut self, documents: &[&str]) {
        self.vocabulary.clear();
        let mut idx = 0usize;
        for doc in documents {
            for token in Self::tokenize(doc) {
                if let std::collections::hash_map::Entry::Vacant(e) = self.vocabulary.entry(token) {
                    e.insert(idx);
                    idx += 1;
                }
            }
        }
    }

    /// Term-frequency embedding: each dimension is the count of the
    /// corresponding word in `text`, divided by the total number of tokens.
    pub fn embed(&self, text: &str) -> Vec<f32> {
        let tokens = Self::tokenize(text);
        let total = tokens.len().max(1) as f32;
        let mut vec = vec![0.0f32; self.vocabulary.len()];
        for token in &tokens {
            if let Some(&idx) = self.vocabulary.get(token) {
                vec[idx] += 1.0;
            }
        }
        // Normalise to term frequency.
        for v in &mut vec {
            *v /= total;
        }
        vec
    }

    /// TF-IDF embedding.
    ///
    /// * `corpus_size` — total number of documents in the corpus.
    /// * `doc_frequencies` — for each word, the number of documents containing
    ///   that word.
    pub fn embed_tfidf(
        &self,
        text: &str,
        corpus_size: usize,
        doc_frequencies: &HashMap<String, usize>,
    ) -> Vec<f32> {
        let tokens = Self::tokenize(text);
        let total = tokens.len().max(1) as f32;
        let mut vec = vec![0.0f32; self.vocabulary.len()];
        // Count raw occurrences.
        let mut counts: HashMap<&str, f32> = HashMap::new();
        for token in &tokens {
            *counts.entry(token.as_str()).or_default() += 1.0;
        }
        for (word, &idx) in &self.vocabulary {
            let tf = counts.get(word.as_str()).copied().unwrap_or(0.0) / total;
            let df = doc_frequencies
                .get(word)
                .copied()
                .unwrap_or(0)
                .max(1) as f32;
            let idf = (corpus_size as f32 / df).ln();
            vec[idx] = tf * idf;
        }
        vec
    }
}

impl Default for EmbeddingGenerator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // 1. Linear model prediction
    #[test]
    fn test_linear_model_predict() {
        let model = LinearModel::new(vec![1.0, 2.0, 3.0], 0.5);
        // dot([1,2,3], [4,5,6]) + 0.5 = 4+10+18 + 0.5 = 32.5
        let out = model.predict(&[4.0, 5.0, 6.0]).unwrap();
        assert_eq!(out.len(), 1);
        assert!((out[0] - 32.5).abs() < 1e-5);
    }

    // 2. Softmax classification
    #[test]
    fn test_softmax_classification() {
        // Two classes, two features.
        // Class 0 weights strongly favour feature 0; class 1 favours feature 1.
        let wm = vec![vec![10.0, 0.0], vec![0.0, 10.0]];
        let biases = vec![0.0, 0.0];
        let model = SoftmaxModel::new(wm, biases);

        // Input with high feature 0 -> class 0 should dominate.
        let probs = model.predict(&[1.0, 0.0]).unwrap();
        assert!(probs[0] > 0.99);
        assert!(probs[1] < 0.01);

        // Probabilities should sum to ~1.
        let sum: f32 = probs.iter().sum();
        assert!((sum - 1.0).abs() < 1e-5);
    }

    // 3. KNN prediction
    #[test]
    fn test_knn_predict() {
        let vectors = vec![
            (vec![0.0, 0.0], "a".to_string()),
            (vec![0.1, 0.1], "a".to_string()),
            (vec![10.0, 10.0], "b".to_string()),
            (vec![10.1, 10.1], "b".to_string()),
        ];
        let knn = KNearestNeighbors::new(vectors, 3);

        // Point near the "a" cluster.
        let label = knn.classify(&[0.05, 0.05]).unwrap();
        assert_eq!(label, "a");

        // Point near the "b" cluster.
        let label = knn.classify(&[10.0, 10.0]).unwrap();
        assert_eq!(label, "b");
    }

    // 4. Model registry CRUD
    #[test]
    fn test_model_registry_crud() {
        let mut reg = ModelRegistry::new();
        assert!(reg.list_models().is_empty());

        reg.register_linear("lin1", vec![1.0, 2.0], 0.0);
        assert_eq!(reg.list_models().len(), 1);
        assert_eq!(reg.list_models()[0].name, "lin1");

        reg.register_linear("lin2", vec![3.0], 1.0);
        assert_eq!(reg.list_models().len(), 2);

        reg.unregister("lin1");
        assert_eq!(reg.list_models().len(), 1);
        assert_eq!(reg.list_models()[0].name, "lin2");

        // Predicting on an unregistered model -> ModelNotFound.
        let err = reg.predict("lin1", &[1.0]).unwrap_err();
        assert_eq!(err, InferenceError::ModelNotFound);
    }

    // 5. Batch prediction
    #[test]
    fn test_batch_predict() {
        let mut reg = ModelRegistry::new();
        reg.register_linear("lin", vec![1.0, 1.0], 0.0);

        let inputs = vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]];
        let results = reg.batch_predict("lin", &inputs).unwrap();
        assert_eq!(results.len(), 3);
        assert!((results[0][0] - 3.0).abs() < 1e-5); // 1+2
        assert!((results[1][0] - 7.0).abs() < 1e-5); // 3+4
        assert!((results[2][0] - 11.0).abs() < 1e-5); // 5+6
    }

    // 6. Embedding generation
    #[test]
    fn test_embedding_generation() {
        let mut emb_gen = EmbeddingGenerator::new();
        emb_gen.build_vocabulary(&["hello world", "world of rust"]);
        // vocabulary: hello=0, world=1, of=2, rust=3  (insertion order)
        assert_eq!(emb_gen.vocabulary.len(), 4);

        let emb = emb_gen.embed("hello hello world");
        // Counts: hello=2, world=1 => TF: hello=2/3, world=1/3
        let hello_idx = emb_gen.vocabulary["hello"];
        let world_idx = emb_gen.vocabulary["world"];
        assert!((emb[hello_idx] - 2.0 / 3.0).abs() < 1e-5);
        assert!((emb[world_idx] - 1.0 / 3.0).abs() < 1e-5);

        // TF-IDF: "hello" appears in 1 doc, "world" in 2 docs, corpus_size=2.
        let mut df = HashMap::new();
        df.insert("hello".to_string(), 1usize);
        df.insert("world".to_string(), 2usize);
        df.insert("of".to_string(), 1usize);
        df.insert("rust".to_string(), 1usize);

        let tfidf = emb_gen.embed_tfidf("hello hello world", 2, &df);
        // hello TF = 2/3, IDF = ln(2/1) ~ 0.6931
        let expected_hello = (2.0f32 / 3.0) * (2.0f32 / 1.0).ln();
        assert!((tfidf[hello_idx] - expected_hello).abs() < 1e-4);
        // world TF = 1/3, IDF = ln(2/2) = 0
        assert!(tfidf[world_idx].abs() < 1e-5);
    }

    // 7. Dimension mismatch error
    #[test]
    fn test_dimension_mismatch() {
        let model = LinearModel::new(vec![1.0, 2.0, 3.0], 0.0);
        let err = model.predict(&[1.0, 2.0]).unwrap_err();
        assert_eq!(
            err,
            InferenceError::DimensionMismatch {
                expected: 3,
                got: 2
            }
        );

        // Also via registry.
        let mut reg = ModelRegistry::new();
        reg.register_linear("m", vec![1.0, 2.0], 0.0);
        let err = reg.predict("m", &[1.0]).unwrap_err();
        assert_eq!(
            err,
            InferenceError::DimensionMismatch {
                expected: 2,
                got: 1
            }
        );
    }

    // 8. Classify via registry (softmax + KNN)
    #[test]
    fn test_classify_via_registry() {
        let mut reg = ModelRegistry::new();

        // Softmax classifier with explicit labels.
        reg.register_softmax_with_labels(
            "sentiment",
            vec![vec![10.0, 0.0], vec![0.0, 10.0]],
            vec![0.0, 0.0],
            Some(vec!["negative".into(), "positive".into()]),
        );
        let label = reg.classify("sentiment", &[0.0, 1.0]).unwrap();
        assert_eq!(label, "positive");

        // KNN classifier.
        reg.register_knn(
            "species",
            vec![
                (vec![1.0, 0.0], "cat".into()),
                (vec![1.1, 0.1], "cat".into()),
                (vec![0.0, 1.0], "dog".into()),
                (vec![0.1, 1.1], "dog".into()),
            ],
            3,
        );
        let label = reg.classify("species", &[1.0, 0.05]).unwrap();
        assert_eq!(label, "cat");
    }

    // 9. Linear model classify (positive/negative)
    #[test]
    fn test_linear_classify() {
        let mut reg = ModelRegistry::new();
        reg.register_linear("sign", vec![1.0], 0.0);

        let label = reg.classify("sign", &[5.0]).unwrap();
        assert_eq!(label, "positive");

        let label = reg.classify("sign", &[-5.0]).unwrap();
        assert_eq!(label, "negative");

        // Zero is positive
        let label = reg.classify("sign", &[0.0]).unwrap();
        assert_eq!(label, "positive");
    }

    // 10. Softmax probabilities sum to 1
    #[test]
    fn test_softmax_probabilities_sum_to_one() {
        let wm = vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
            vec![7.0, 8.0, 9.0],
        ];
        let biases = vec![0.1, 0.2, 0.3];
        let model = SoftmaxModel::new(wm, biases);

        let probs = model.predict(&[0.5, 0.3, 0.7]).unwrap();
        assert_eq!(probs.len(), 3);
        let sum: f32 = probs.iter().sum();
        assert!((sum - 1.0).abs() < 1e-5);
        // All probabilities should be non-negative
        for &p in &probs {
            assert!(p >= 0.0);
        }
    }

    // 11. KNN with ties
    #[test]
    fn test_knn_tie_breaking() {
        // Two classes with equal representation near the query point
        let vectors = vec![
            (vec![0.0], "a".to_string()),
            (vec![0.1], "b".to_string()),
        ];
        let knn = KNearestNeighbors::new(vectors, 2);
        // Should still return a valid class
        let label = knn.classify(&[0.05]).unwrap();
        assert!(label == "a" || label == "b");
    }

    // 12. Empty embedding for unknown words
    #[test]
    fn test_embedding_unknown_words() {
        let mut emb = EmbeddingGenerator::new();
        emb.build_vocabulary(&["hello world"]);

        // All unknown words → zero vector
        let vec = emb.embed("foo bar baz");
        assert!(vec.iter().all(|&v| v.abs() < 1e-10));
    }

    // 13. Empty vocabulary embedding
    #[test]
    fn test_empty_vocabulary() {
        let emb = EmbeddingGenerator::new();
        let vec = emb.embed("anything");
        assert!(vec.is_empty());
    }

    // 14. InferenceEngine trait on registry
    #[test]
    fn test_inference_engine_trait() {
        let mut reg = ModelRegistry::new();
        reg.register_linear("m", vec![2.0, 3.0], 1.0);

        // Use through trait
        let engine: &dyn InferenceEngine = &reg;
        let out = engine.predict("m", &[1.0, 1.0]).unwrap();
        assert!((out[0] - 6.0).abs() < 1e-5); // 2*1 + 3*1 + 1

        let batch = engine.batch_predict("m", &[vec![1.0, 0.0], vec![0.0, 1.0]]).unwrap();
        assert!((batch[0][0] - 3.0).abs() < 1e-5); // 2*1 + 3*0 + 1
        assert!((batch[1][0] - 4.0).abs() < 1e-5); // 2*0 + 3*1 + 1
    }

    // 15. Error display formats
    #[test]
    fn test_error_display() {
        assert_eq!(InferenceError::ModelNotFound.to_string(), "model not found");
        assert_eq!(
            InferenceError::DimensionMismatch { expected: 3, got: 2 }.to_string(),
            "dimension mismatch: expected 3, got 2"
        );
        assert_eq!(
            InferenceError::InvalidInput("bad".into()).to_string(),
            "invalid input: bad"
        );
    }

    // 16. Model metadata fields
    #[test]
    fn test_model_metadata() {
        let mut reg = ModelRegistry::new();
        reg.register_linear("my_model", vec![1.0, 2.0, 3.0], 0.5);

        let models = reg.list_models();
        assert_eq!(models.len(), 1);
        let meta = &models[0];
        assert_eq!(meta.name, "my_model");
        assert_eq!(meta.format, ModelFormat::Custom);
        assert_eq!(meta.input_dims, vec![3]);
        assert_eq!(meta.output_dims, vec![1]);
        assert!(!meta.description.is_empty());
        assert!(meta.created_at > 0);
    }
}
