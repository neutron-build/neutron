//! Quantum and scientific data types.
//!
//! Supports:
//!   - Complex number type for quantum statevectors and density matrices
//!   - Sparse matrix storage (block-sparse) for unitary/density matrices
//!   - Probability distribution type with statistical operations
//!   - Quantum measurement histogram storage
//!
//! Replaces custom HDF5/NumPy files for quantum computing workloads.

use std::collections::HashMap;

// ============================================================================
// Complex numbers
// ============================================================================

/// Complex number with f64 real and imaginary parts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Complex {
    pub re: f64,
    pub im: f64,
}

impl Complex {
    pub fn new(re: f64, im: f64) -> Self {
        Self { re, im }
    }

    pub fn from_real(re: f64) -> Self {
        Self { re, im: 0.0 }
    }

    pub fn zero() -> Self {
        Self { re: 0.0, im: 0.0 }
    }

    pub fn one() -> Self {
        Self { re: 1.0, im: 0.0 }
    }

    pub fn i() -> Self {
        Self { re: 0.0, im: 1.0 }
    }

    /// Complex conjugate.
    pub fn conj(&self) -> Self {
        Self { re: self.re, im: -self.im }
    }

    /// Magnitude (absolute value).
    pub fn abs(&self) -> f64 {
        (self.re * self.re + self.im * self.im).sqrt()
    }

    /// Squared magnitude.
    pub fn norm_sq(&self) -> f64 {
        self.re * self.re + self.im * self.im
    }

    /// Phase angle (argument).
    pub fn arg(&self) -> f64 {
        self.im.atan2(self.re)
    }

    /// Addition.
    pub fn add(&self, other: &Complex) -> Self {
        Self {
            re: self.re + other.re,
            im: self.im + other.im,
        }
    }

    /// Subtraction.
    pub fn sub(&self, other: &Complex) -> Self {
        Self {
            re: self.re - other.re,
            im: self.im - other.im,
        }
    }

    /// Multiplication.
    pub fn mul(&self, other: &Complex) -> Self {
        Self {
            re: self.re * other.re - self.im * other.im,
            im: self.re * other.im + self.im * other.re,
        }
    }

    /// Scalar multiplication.
    pub fn scale(&self, s: f64) -> Self {
        Self {
            re: self.re * s,
            im: self.im * s,
        }
    }

    /// Division.
    pub fn div(&self, other: &Complex) -> Self {
        let denom = other.norm_sq();
        Self {
            re: (self.re * other.re + self.im * other.im) / denom,
            im: (self.im * other.re - self.re * other.im) / denom,
        }
    }
}

// ============================================================================
// Sparse matrix (block-sparse for density matrices)
// ============================================================================

/// Sparse matrix in COO (coordinate) format for complex values.
#[derive(Debug, Clone)]
pub struct SparseMatrix {
    pub rows: usize,
    pub cols: usize,
    /// Non-zero entries: (row, col, value).
    pub entries: Vec<(usize, usize, Complex)>,
}

impl SparseMatrix {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            entries: Vec::new(),
        }
    }

    /// Create an identity matrix.
    pub fn identity(n: usize) -> Self {
        let entries = (0..n).map(|i| (i, i, Complex::one())).collect();
        Self {
            rows: n,
            cols: n,
            entries,
        }
    }

    /// Set a value at (row, col).
    pub fn set(&mut self, row: usize, col: usize, value: Complex) {
        // Remove existing entry if present
        self.entries.retain(|(r, c, _)| !(*r == row && *c == col));
        if value.norm_sq() > 1e-15 {
            self.entries.push((row, col, value));
        }
    }

    /// Get value at (row, col).
    pub fn get(&self, row: usize, col: usize) -> Complex {
        self.entries
            .iter()
            .find(|(r, c, _)| *r == row && *c == col)
            .map(|(_, _, v)| *v)
            .unwrap_or(Complex::zero())
    }

    /// Number of non-zero entries.
    pub fn nnz(&self) -> usize {
        self.entries.len()
    }

    /// Sparsity (fraction of zero entries).
    pub fn sparsity(&self) -> f64 {
        let total = (self.rows * self.cols) as f64;
        1.0 - self.entries.len() as f64 / total
    }

    /// Matrix-vector multiplication: result = M * v.
    pub fn matvec(&self, vec: &[Complex]) -> Vec<Complex> {
        let mut result = vec![Complex::zero(); self.rows];
        for &(row, col, ref val) in &self.entries {
            if col < vec.len() {
                result[row] = result[row].add(&val.mul(&vec[col]));
            }
        }
        result
    }

    /// Conjugate transpose (dagger).
    pub fn dagger(&self) -> Self {
        let entries = self
            .entries
            .iter()
            .map(|&(r, c, v)| (c, r, v.conj()))
            .collect();
        Self {
            rows: self.cols,
            cols: self.rows,
            entries,
        }
    }

    /// Trace (sum of diagonal elements).
    pub fn trace(&self) -> Complex {
        self.entries
            .iter()
            .filter(|(r, c, _)| r == c)
            .fold(Complex::zero(), |acc, (_, _, v)| acc.add(v))
    }
}

// ============================================================================
// Probability distributions
// ============================================================================

/// A probability distribution over discrete outcomes.
#[derive(Debug, Clone)]
pub struct Distribution {
    /// outcome → probability (must sum to ~1.0).
    pub probabilities: HashMap<String, f64>,
}

impl Distribution {
    /// Create from outcome counts (normalizes automatically).
    pub fn from_counts(counts: &HashMap<String, u64>) -> Self {
        let total: u64 = counts.values().sum();
        let probabilities = if total == 0 {
            HashMap::new()
        } else {
            counts
                .iter()
                .map(|(k, &v)| (k.clone(), v as f64 / total as f64))
                .collect()
        };
        Self { probabilities }
    }

    /// Create from explicit probabilities.
    pub fn from_probabilities(probs: HashMap<String, f64>) -> Self {
        Self { probabilities: probs }
    }

    /// Shannon entropy: H = -Σ p(x) * log2(p(x)).
    pub fn entropy(&self) -> f64 {
        self.probabilities
            .values()
            .filter(|&&p| p > 0.0)
            .map(|&p| -p * p.log2())
            .sum()
    }

    /// KL divergence: D_KL(P || Q) = Σ P(x) * log2(P(x) / Q(x)).
    pub fn kl_divergence(&self, other: &Distribution) -> f64 {
        self.probabilities
            .iter()
            .filter(|&(_, &p)| p > 0.0)
            .map(|(k, &p)| {
                let q = other.probabilities.get(k).copied().unwrap_or(1e-10);
                p * (p / q).log2()
            })
            .sum()
    }

    /// Fidelity: F(P, Q) = (Σ sqrt(P(x) * Q(x)))^2.
    pub fn fidelity(&self, other: &Distribution) -> f64 {
        let sum: f64 = self
            .probabilities
            .iter()
            .map(|(k, &p)| {
                let q = other.probabilities.get(k).copied().unwrap_or(0.0);
                (p * q).sqrt()
            })
            .sum();
        sum * sum
    }

    /// Total variation distance: TVD(P, Q) = 0.5 * Σ |P(x) - Q(x)|.
    pub fn total_variation(&self, other: &Distribution) -> f64 {
        let mut all_keys: std::collections::HashSet<&String> = self.probabilities.keys().collect();
        all_keys.extend(other.probabilities.keys());

        let sum: f64 = all_keys
            .iter()
            .map(|k| {
                let p = self.probabilities.get(*k).copied().unwrap_or(0.0);
                let q = other.probabilities.get(*k).copied().unwrap_or(0.0);
                (p - q).abs()
            })
            .sum();

        sum / 2.0
    }

    /// Most probable outcome.
    pub fn mode(&self) -> Option<(&str, f64)> {
        self.probabilities
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(k, &v)| (k.as_str(), v))
    }

    /// Number of outcomes.
    pub fn num_outcomes(&self) -> usize {
        self.probabilities.len()
    }
}

/// Quantum measurement result histogram.
#[derive(Debug, Clone)]
pub struct MeasurementHistogram {
    pub counts: HashMap<String, u64>,
    pub total_shots: u64,
    pub num_qubits: u32,
}

impl MeasurementHistogram {
    pub fn new(num_qubits: u32) -> Self {
        Self {
            counts: HashMap::new(),
            total_shots: 0,
            num_qubits,
        }
    }

    /// Record a measurement outcome.
    pub fn record(&mut self, bitstring: &str) {
        *self.counts.entry(bitstring.to_string()).or_insert(0) += 1;
        self.total_shots += 1;
    }

    /// Record multiple shots of the same outcome.
    pub fn record_n(&mut self, bitstring: &str, count: u64) {
        *self.counts.entry(bitstring.to_string()).or_insert(0) += count;
        self.total_shots += count;
    }

    /// Convert to a probability distribution.
    pub fn to_distribution(&self) -> Distribution {
        Distribution::from_counts(&self.counts)
    }

    /// Get the most frequent outcome.
    pub fn most_frequent(&self) -> Option<(&str, u64)> {
        self.counts
            .iter()
            .max_by_key(|&(_, &v)| v)
            .map(|(k, &v)| (k.as_str(), v))
    }
}

// ============================================================================
// Post-Quantum Key Exchange (ML-KEM / CRYSTALS-Kyber simulation)
// ============================================================================

/// Post-quantum key exchange algorithm identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PqAlgorithm {
    /// ML-KEM-768 (formerly CRYSTALS-Kyber-768). NIST standard.
    MlKem768,
    /// ML-KEM-1024 for higher security margin.
    MlKem1024,
}

impl std::fmt::Display for PqAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PqAlgorithm::MlKem768 => write!(f, "ML-KEM-768"),
            PqAlgorithm::MlKem1024 => write!(f, "ML-KEM-1024"),
        }
    }
}

/// Simulated public key for ML-KEM key exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PqPublicKey {
    pub algorithm: PqAlgorithm,
    pub data: Vec<u8>,
}

/// Simulated secret key for ML-KEM key exchange.
#[derive(Debug, Clone)]
pub struct PqSecretKey {
    pub algorithm: PqAlgorithm,
    pub data: Vec<u8>,
}

/// Simulated ciphertext from encapsulation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PqCiphertext {
    pub algorithm: PqAlgorithm,
    pub data: Vec<u8>,
}

/// Shared secret derived from key exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedSecret {
    pub data: Vec<u8>,
}

/// Simulated ML-KEM key exchange. In production, this would use an actual
/// ML-KEM implementation. Here we simulate the API and key sizes.
pub struct PqKeyExchange;

impl PqKeyExchange {
    /// Key generation: returns (public_key, secret_key).
    pub fn keygen(algorithm: PqAlgorithm) -> (PqPublicKey, PqSecretKey) {
        let (pk_size, sk_size) = match algorithm {
            PqAlgorithm::MlKem768 => (1184, 2400),
            PqAlgorithm::MlKem1024 => (1568, 3168),
        };
        // Simulate deterministic key material from a simple PRNG.
        let pk_data: Vec<u8> = (0..pk_size).map(|i| ((i * 37 + 13) % 256) as u8).collect();
        let sk_data: Vec<u8> = (0..sk_size).map(|i| ((i * 53 + 7) % 256) as u8).collect();
        (
            PqPublicKey { algorithm, data: pk_data },
            PqSecretKey { algorithm, data: sk_data },
        )
    }

    /// Encapsulation: given a public key, produce (ciphertext, shared_secret).
    pub fn encapsulate(pk: &PqPublicKey) -> (PqCiphertext, SharedSecret) {
        let ct_size = match pk.algorithm {
            PqAlgorithm::MlKem768 => 1088,
            PqAlgorithm::MlKem1024 => 1568,
        };
        // Derive ciphertext from public key bytes (simulated).
        let ct_data: Vec<u8> = (0..ct_size)
            .map(|i| pk.data.get(i % pk.data.len()).copied().unwrap_or(0) ^ ((i * 19) % 256) as u8)
            .collect();
        // Derive shared secret (32 bytes).
        let ss_data: Vec<u8> = (0..32)
            .map(|i| ct_data.get(i % ct_data.len()).copied().unwrap_or(0) ^ 0xAA)
            .collect();
        (
            PqCiphertext { algorithm: pk.algorithm, data: ct_data },
            SharedSecret { data: ss_data },
        )
    }

    /// Decapsulation: given a secret key and ciphertext, recover the shared secret.
    pub fn decapsulate(_sk: &PqSecretKey, ct: &PqCiphertext) -> SharedSecret {
        // In a real implementation, this would use the secret key to recover
        // the same shared secret. We simulate by reproducing the same derivation.
        let ss_data: Vec<u8> = (0..32)
            .map(|i| ct.data.get(i % ct.data.len()).copied().unwrap_or(0) ^ 0xAA)
            .collect();
        SharedSecret { data: ss_data }
    }
}

/// Hybrid key exchange mode: combines classical (ECDH) with post-quantum (ML-KEM).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HybridMode {
    /// Post-quantum only.
    PqOnly(PqAlgorithm),
    /// Classical + post-quantum hybrid (recommended).
    Hybrid(PqAlgorithm),
    /// Classical only (no PQ protection).
    ClassicalOnly,
}

/// Configuration for post-quantum TLS integration.
#[derive(Debug, Clone)]
pub struct PqTlsConfig {
    pub mode: HybridMode,
    pub allow_classical_fallback: bool,
}

impl PqTlsConfig {
    pub fn hybrid_default() -> Self {
        Self {
            mode: HybridMode::Hybrid(PqAlgorithm::MlKem768),
            allow_classical_fallback: true,
        }
    }

    pub fn pq_only() -> Self {
        Self {
            mode: HybridMode::PqOnly(PqAlgorithm::MlKem768),
            allow_classical_fallback: false,
        }
    }

    pub fn is_pq_enabled(&self) -> bool {
        !matches!(self.mode, HybridMode::ClassicalOnly)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complex_arithmetic() {
        let a = Complex::new(3.0, 4.0);
        let b = Complex::new(1.0, 2.0);

        let sum = a.add(&b);
        assert!((sum.re - 4.0).abs() < 1e-10);
        assert!((sum.im - 6.0).abs() < 1e-10);

        let prod = a.mul(&b);
        // (3+4i)(1+2i) = 3+6i+4i+8i^2 = 3+10i-8 = -5+10i
        assert!((prod.re - (-5.0)).abs() < 1e-10);
        assert!((prod.im - 10.0).abs() < 1e-10);

        assert!((a.abs() - 5.0).abs() < 1e-10); // |3+4i| = 5
        assert!((a.conj().im - (-4.0)).abs() < 1e-10);
    }

    #[test]
    fn sparse_matrix_operations() {
        let id = SparseMatrix::identity(3);
        assert_eq!(id.nnz(), 3);

        let trace = id.trace();
        assert!((trace.re - 3.0).abs() < 1e-10);

        // Matrix-vector multiply with identity
        let vec = vec![Complex::new(1.0, 0.0), Complex::new(2.0, 0.0), Complex::new(3.0, 0.0)];
        let result = id.matvec(&vec);
        assert!((result[0].re - 1.0).abs() < 1e-10);
        assert!((result[1].re - 2.0).abs() < 1e-10);
        assert!((result[2].re - 3.0).abs() < 1e-10);
    }

    #[test]
    fn sparse_matrix_dagger() {
        let mut m = SparseMatrix::new(2, 2);
        m.set(0, 1, Complex::new(0.0, 1.0)); // i at (0,1)

        let dag = m.dagger();
        let val = dag.get(1, 0);
        assert!((val.re - 0.0).abs() < 1e-10);
        assert!((val.im - (-1.0)).abs() < 1e-10); // conjugate of i = -i
    }

    #[test]
    fn distribution_entropy() {
        // Uniform distribution over 4 outcomes: H = log2(4) = 2.0
        let mut probs = HashMap::new();
        probs.insert("00".into(), 0.25);
        probs.insert("01".into(), 0.25);
        probs.insert("10".into(), 0.25);
        probs.insert("11".into(), 0.25);
        let dist = Distribution::from_probabilities(probs);
        assert!((dist.entropy() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn distribution_kl_and_fidelity() {
        let mut p_probs = HashMap::new();
        p_probs.insert("0".into(), 0.7);
        p_probs.insert("1".into(), 0.3);
        let p = Distribution::from_probabilities(p_probs);

        let mut q_probs = HashMap::new();
        q_probs.insert("0".into(), 0.5);
        q_probs.insert("1".into(), 0.5);
        let q = Distribution::from_probabilities(q_probs);

        let kl = p.kl_divergence(&q);
        assert!(kl > 0.0); // KL divergence is non-negative

        let fid = p.fidelity(&q);
        assert!(fid > 0.0 && fid <= 1.0);

        // Fidelity of identical distributions = 1.0
        let self_fid = p.fidelity(&p);
        assert!((self_fid - 1.0).abs() < 1e-10);
    }

    #[test]
    fn measurement_histogram() {
        let mut hist = MeasurementHistogram::new(2);
        hist.record_n("00", 500);
        hist.record_n("01", 250);
        hist.record_n("10", 150);
        hist.record_n("11", 100);

        assert_eq!(hist.total_shots, 1000);
        assert_eq!(hist.most_frequent().unwrap(), ("00", 500));

        let dist = hist.to_distribution();
        assert!((dist.probabilities["00"] - 0.5).abs() < 1e-10);
        assert_eq!(dist.num_outcomes(), 4);
    }

    #[test]
    fn total_variation_distance() {
        let mut p_probs = HashMap::new();
        p_probs.insert("0".into(), 1.0);
        p_probs.insert("1".into(), 0.0);
        let p = Distribution::from_probabilities(p_probs);

        let mut q_probs = HashMap::new();
        q_probs.insert("0".into(), 0.0);
        q_probs.insert("1".into(), 1.0);
        let q = Distribution::from_probabilities(q_probs);

        // Maximum TVD = 1.0 for completely disjoint distributions
        let tvd = p.total_variation(&q);
        assert!((tvd - 1.0).abs() < 1e-10);

        // TVD of same distribution = 0
        assert!((p.total_variation(&p) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn hadamard_gate_operation() {
        let inv_sqrt2 = 1.0 / 2.0_f64.sqrt();
        let mut h = SparseMatrix::new(2, 2);
        h.set(0, 0, Complex::from_real(inv_sqrt2));
        h.set(0, 1, Complex::from_real(inv_sqrt2));
        h.set(1, 0, Complex::from_real(inv_sqrt2));
        h.set(1, 1, Complex::from_real(-inv_sqrt2));
        let zero_state = vec![Complex::one(), Complex::zero()];
        let result = h.matvec(&zero_state);
        assert!((result[0].re - inv_sqrt2).abs() < 1e-10);
        assert!((result[1].re - inv_sqrt2).abs() < 1e-10);
        let one_state = vec![Complex::zero(), Complex::one()];
        let result = h.matvec(&one_state);
        assert!((result[0].re - inv_sqrt2).abs() < 1e-10);
        assert!((result[1].re - (-inv_sqrt2)).abs() < 1e-10);
    }

    #[test]
    fn pauli_x_gate() {
        let mut x = SparseMatrix::new(2, 2);
        x.set(0, 1, Complex::one());
        x.set(1, 0, Complex::one());
        let zero = vec![Complex::one(), Complex::zero()];
        let result = x.matvec(&zero);
        assert!((result[0].re).abs() < 1e-10);
        assert!((result[1].re - 1.0).abs() < 1e-10);
        let one = vec![Complex::zero(), Complex::one()];
        let result = x.matvec(&one);
        assert!((result[0].re - 1.0).abs() < 1e-10);
        assert!((result[1].re).abs() < 1e-10);
    }

    #[test]
    fn pauli_y_and_z_gates() {
        let mut y = SparseMatrix::new(2, 2);
        y.set(0, 1, Complex::new(0.0, -1.0));
        y.set(1, 0, Complex::i());
        let zero = vec![Complex::one(), Complex::zero()];
        let result = y.matvec(&zero);
        assert!((result[0].abs()) < 1e-10);
        assert!((result[1].re).abs() < 1e-10);
        assert!((result[1].im - 1.0).abs() < 1e-10);
        let mut z = SparseMatrix::new(2, 2);
        z.set(0, 0, Complex::one());
        z.set(1, 1, Complex::from_real(-1.0));
        let one = vec![Complex::zero(), Complex::one()];
        let result = z.matvec(&one);
        assert!((result[0].re).abs() < 1e-10);
        assert!((result[1].re - (-1.0)).abs() < 1e-10);
    }

    #[test]
    fn circuit_composition_hxh_equals_z() {
        let inv_sqrt2 = 1.0 / 2.0_f64.sqrt();
        let mut h = SparseMatrix::new(2, 2);
        h.set(0, 0, Complex::from_real(inv_sqrt2));
        h.set(0, 1, Complex::from_real(inv_sqrt2));
        h.set(1, 0, Complex::from_real(inv_sqrt2));
        h.set(1, 1, Complex::from_real(-inv_sqrt2));
        let mut x = SparseMatrix::new(2, 2);
        x.set(0, 1, Complex::one());
        x.set(1, 0, Complex::one());
        let zero = vec![Complex::one(), Complex::zero()];
        let step1 = h.matvec(&zero);
        let step2 = x.matvec(&step1);
        let result = h.matvec(&step2);
        assert!((result[0].re - 1.0).abs() < 1e-10);
        assert!((result[1].abs()) < 1e-10);
        let one = vec![Complex::zero(), Complex::one()];
        let step1 = h.matvec(&one);
        let step2 = x.matvec(&step1);
        let result = h.matvec(&step2);
        assert!((result[0].abs()) < 1e-10);
        assert!((result[1].re - (-1.0)).abs() < 1e-10);
    }

    #[test]
    fn measurement_distribution_convergence() {
        let mut hist = MeasurementHistogram::new(1);
        hist.record_n("0", 5000);
        hist.record_n("1", 5000);
        let dist = hist.to_distribution();
        assert!((dist.probabilities["0"] - 0.5).abs() < 1e-10);
        assert!((dist.probabilities["1"] - 0.5).abs() < 1e-10);
        assert!((dist.entropy() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn multi_qubit_cnot_gate() {
        let mut cnot = SparseMatrix::new(4, 4);
        cnot.set(0, 0, Complex::one());
        cnot.set(1, 1, Complex::one());
        cnot.set(2, 3, Complex::one());
        cnot.set(3, 2, Complex::one());
        let state_10 = vec![Complex::zero(), Complex::zero(), Complex::one(), Complex::zero()];
        let result = cnot.matvec(&state_10);
        assert!((result[0].abs()) < 1e-10);
        assert!((result[1].abs()) < 1e-10);
        assert!((result[2].abs()) < 1e-10);
        assert!((result[3].re - 1.0).abs() < 1e-10);
        let state_00 = vec![Complex::one(), Complex::zero(), Complex::zero(), Complex::zero()];
        let result = cnot.matvec(&state_00);
        assert!((result[0].re - 1.0).abs() < 1e-10);
        assert!((result[1].abs()) < 1e-10);
        assert!((result[2].abs()) < 1e-10);
        assert!((result[3].abs()) < 1e-10);
        let dag = cnot.dagger();
        for i in 0..4 {
            let mut state = vec![Complex::zero(); 4];
            state[i] = Complex::one();
            let after_cnot = cnot.matvec(&state);
            let round_trip = dag.matvec(&after_cnot);
            for j in 0..4 {
                let expected = if i == j { 1.0 } else { 0.0 };
                assert!((round_trip[j].re - expected).abs() < 1e-10);
                assert!((round_trip[j].im).abs() < 1e-10);
            }
        }
    }

    #[test]
    fn error_cases_sparse_matrix() {
        let m = SparseMatrix::new(2, 2);
        let val = m.get(5, 5);
        assert!((val.re).abs() < 1e-10);
        assert!((val.im).abs() < 1e-10);
        let mut m = SparseMatrix::new(2, 3);
        m.set(0, 0, Complex::one());
        m.set(0, 2, Complex::one());
        let short_vec = vec![Complex::from_real(5.0), Complex::from_real(10.0)];
        let result = m.matvec(&short_vec);
        assert!((result[0].re - 5.0).abs() < 1e-10);
        let empty = SparseMatrix::new(0, 0);
        assert_eq!(empty.nnz(), 0);
        assert!((empty.trace().re).abs() < 1e-10);
    }

    #[test]
    fn quantum_state_normalization_check() {
        let inv_sqrt2 = 1.0 / 2.0_f64.sqrt();
        let plus = vec![Complex::from_real(inv_sqrt2), Complex::from_real(inv_sqrt2)];
        let norm_sq: f64 = plus.iter().map(|c| c.norm_sq()).sum();
        assert!((norm_sq - 1.0).abs() < 1e-10);
        let minus = vec![Complex::from_real(inv_sqrt2), Complex::from_real(-inv_sqrt2)];
        let norm_sq: f64 = minus.iter().map(|c| c.norm_sq()).sum();
        assert!((norm_sq - 1.0).abs() < 1e-10);
        let i_state = vec![Complex::from_real(inv_sqrt2), Complex::new(0.0, inv_sqrt2)];
        let norm_sq: f64 = i_state.iter().map(|c| c.norm_sq()).sum();
        assert!((norm_sq - 1.0).abs() < 1e-10);
        let mut h = SparseMatrix::new(2, 2);
        h.set(0, 0, Complex::from_real(inv_sqrt2));
        h.set(0, 1, Complex::from_real(inv_sqrt2));
        h.set(1, 0, Complex::from_real(inv_sqrt2));
        h.set(1, 1, Complex::from_real(-inv_sqrt2));
        let result = h.matvec(&i_state);
        let norm_sq: f64 = result.iter().map(|c| c.norm_sq()).sum();
        assert!((norm_sq - 1.0).abs() < 1e-10);
    }

    // -- Post-quantum key exchange tests --

    #[test]
    fn pq_keygen_produces_correct_sizes() {
        let (pk, sk) = PqKeyExchange::keygen(PqAlgorithm::MlKem768);
        assert_eq!(pk.data.len(), 1184);
        assert_eq!(sk.data.len(), 2400);
        assert_eq!(pk.algorithm, PqAlgorithm::MlKem768);

        let (pk, sk) = PqKeyExchange::keygen(PqAlgorithm::MlKem1024);
        assert_eq!(pk.data.len(), 1568);
        assert_eq!(sk.data.len(), 3168);
    }

    #[test]
    fn pq_encapsulate_decapsulate_roundtrip() {
        let (pk, sk) = PqKeyExchange::keygen(PqAlgorithm::MlKem768);
        let (ct, ss_enc) = PqKeyExchange::encapsulate(&pk);
        let ss_dec = PqKeyExchange::decapsulate(&sk, &ct);
        assert_eq!(ss_enc, ss_dec, "shared secrets must match");
        assert_eq!(ss_enc.data.len(), 32);
    }

    #[test]
    fn pq_encapsulate_decapsulate_1024() {
        let (pk, sk) = PqKeyExchange::keygen(PqAlgorithm::MlKem1024);
        let (ct, ss_enc) = PqKeyExchange::encapsulate(&pk);
        let ss_dec = PqKeyExchange::decapsulate(&sk, &ct);
        assert_eq!(ss_enc, ss_dec);
        assert_eq!(ct.data.len(), 1568);
    }

    #[test]
    fn pq_different_keys_different_ciphertexts() {
        let (pk1, _) = PqKeyExchange::keygen(PqAlgorithm::MlKem768);
        let (pk2, _) = PqKeyExchange::keygen(PqAlgorithm::MlKem1024);
        let (ct1, _) = PqKeyExchange::encapsulate(&pk1);
        let (ct2, _) = PqKeyExchange::encapsulate(&pk2);
        // Different algorithms produce different ciphertext sizes.
        assert_ne!(ct1.data.len(), ct2.data.len());
        assert_eq!(ct1.algorithm, PqAlgorithm::MlKem768);
        assert_eq!(ct2.algorithm, PqAlgorithm::MlKem1024);
    }

    #[test]
    fn pq_algorithm_display() {
        assert_eq!(format!("{}", PqAlgorithm::MlKem768), "ML-KEM-768");
        assert_eq!(format!("{}", PqAlgorithm::MlKem1024), "ML-KEM-1024");
    }

    #[test]
    fn pq_tls_config_defaults() {
        let cfg = PqTlsConfig::hybrid_default();
        assert!(cfg.is_pq_enabled());
        assert!(cfg.allow_classical_fallback);
        assert_eq!(cfg.mode, HybridMode::Hybrid(PqAlgorithm::MlKem768));
    }

    #[test]
    fn pq_tls_config_pq_only() {
        let cfg = PqTlsConfig::pq_only();
        assert!(cfg.is_pq_enabled());
        assert!(!cfg.allow_classical_fallback);
    }

    #[test]
    fn pq_hybrid_mode_classical_only() {
        let cfg = PqTlsConfig {
            mode: HybridMode::ClassicalOnly,
            allow_classical_fallback: true,
        };
        assert!(!cfg.is_pq_enabled());
    }

    #[test]
    fn complex_division_and_phase() {
        let a = Complex::new(3.0, 4.0);
        let b = Complex::new(1.0, 2.0);
        let quot = a.div(&b);
        assert!((quot.re - 2.2).abs() < 1e-10);
        assert!((quot.im - (-0.4)).abs() < 1e-10);
        let pure_i = Complex::i();
        assert!((pure_i.arg() - std::f64::consts::FRAC_PI_2).abs() < 1e-10);
        let neg_one = Complex::from_real(-1.0);
        assert!((neg_one.arg() - std::f64::consts::PI).abs() < 1e-10);
        let scaled = a.scale(2.0);
        assert!((scaled.re - 6.0).abs() < 1e-10);
        assert!((scaled.im - 8.0).abs() < 1e-10);
    }
}
