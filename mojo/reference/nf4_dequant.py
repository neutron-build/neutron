"""NF4 (Normal Float 4-bit) dequantization — correctness oracle.

NF4 is an information-theoretically optimal 4-bit data type for
normally distributed weights. From bitsandbytes (Dettmers et al., 2023).

The key idea: 16 quantization levels are placed at quantiles of N(0,1),
so each level represents an equal probability mass. This is optimal
for Gaussian-distributed neural network weights.

The 16 NF4 values (fixed lookup table):
  [-1.0, -0.6962, -0.5251, -0.3949, -0.2844, -0.1848, -0.0911, 0.0,
   0.0796, 0.1609, 0.2461, 0.3379, 0.4407, 0.5626, 0.7230, 1.0]

Quantization process:
  1. Compute absmax per block (typically 64 elements)
  2. Normalize: x_norm = x / absmax
  3. Map each normalized value to nearest NF4 level (4-bit index)
  4. Store: 4-bit indices + FP32 absmax per block

Double quantization (optional):
  - Quantize the absmax values themselves to FP8
  - Reduces storage overhead from 0.5 bit/param to 0.127 bit/param

Tolerance: Exact (lookup table, no floating point ambiguity)
"""

import numpy as np

# The 16 NF4 quantization levels — quantiles of N(0,1)
NF4_LEVELS = np.array([
    -1.0,
    -0.6961928009986877,
    -0.5250730514526367,
    -0.39491748809814453,
    -0.28444138169288635,
    -0.18477343022823334,
    -0.09105003625154495,
    0.0,
    0.07958029955625534,
    0.16093020141124725,
    0.24611230194568634,
    0.33791524171829224,
    0.44070982933044434,
    0.5626170039176941,
    0.7229568362236023,
    1.0,
], dtype=np.float64)


def nf4_quantize(x: np.ndarray, block_size: int = 64) -> tuple:
    """Quantize FP32 weights to NF4.

    Args:
        x: Weight tensor (flattened or 1D)
        block_size: Elements per quantization block

    Returns:
        (indices, absmax): 4-bit indices and per-block absmax scales
    """
    x_flat = x.flatten().astype(np.float64)
    n = len(x_flat)

    # Pad to block_size multiple
    pad = (block_size - n % block_size) % block_size
    if pad > 0:
        x_flat = np.concatenate([x_flat, np.zeros(pad)])

    num_blocks = len(x_flat) // block_size
    x_blocks = x_flat.reshape(num_blocks, block_size)

    # Per-block absmax
    absmax = np.max(np.abs(x_blocks), axis=1)

    # Normalize to [-1, 1]
    absmax_safe = np.where(absmax == 0, 1.0, absmax)
    x_norm = x_blocks / absmax_safe[:, None]

    # Map to nearest NF4 level
    indices = np.zeros_like(x_norm, dtype=np.uint8)
    for i in range(num_blocks):
        for j in range(block_size):
            # Find nearest NF4 level
            dists = np.abs(NF4_LEVELS - x_norm[i, j])
            indices[i, j] = np.argmin(dists)

    # Trim padding
    indices = indices.flatten()[:n]

    return indices, absmax


def nf4_dequantize(
    indices: np.ndarray,
    absmax: np.ndarray,
    block_size: int = 64,
    original_shape: tuple | None = None,
) -> np.ndarray:
    """Dequantize NF4 indices back to FP32.

    Args:
        indices: 4-bit indices (0-15)
        absmax: Per-block absmax scales
        block_size: Elements per block
        original_shape: Original tensor shape (for reshape)
    """
    n = len(indices)

    # Pad to block_size multiple
    pad = (block_size - n % block_size) % block_size
    if pad > 0:
        indices = np.concatenate([indices, np.zeros(pad, dtype=np.uint8)])

    num_blocks = len(indices) // block_size
    idx_blocks = indices.reshape(num_blocks, block_size)

    # Lookup + scale
    result = np.zeros((num_blocks, block_size), dtype=np.float64)
    for i in range(num_blocks):
        for j in range(block_size):
            result[i, j] = NF4_LEVELS[idx_blocks[i, j]] * absmax[i]

    result = result.flatten()[:n].astype(np.float32)

    if original_shape is not None:
        result = result.reshape(original_shape)

    return result


def nf4_double_quantize(absmax: np.ndarray, dq_block_size: int = 256) -> tuple:
    """Double quantization: quantize the absmax values to FP8.

    Reduces per-parameter overhead from 0.5 bits to 0.127 bits.

    Returns:
        (absmax_quantized, absmax_absmax): FP8 absmax and their scales
    """
    n = len(absmax)
    pad = (dq_block_size - n % dq_block_size) % dq_block_size
    if pad > 0:
        absmax_padded = np.concatenate([absmax, np.zeros(pad)])
    else:
        absmax_padded = absmax.copy()

    num_dq_blocks = len(absmax_padded) // dq_block_size
    blocks = absmax_padded.reshape(num_dq_blocks, dq_block_size)

    absmax_absmax = np.max(np.abs(blocks), axis=1)

    # Quantize to 8-bit (simulate FP8 with int8 + scale)
    absmax_safe = np.where(absmax_absmax == 0, 1.0, absmax_absmax)
    normalized = blocks / absmax_safe[:, None]
    quantized = np.clip(np.round(normalized * 127), -127, 127).astype(np.int8)

    return quantized.flatten()[:n], absmax_absmax


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_levels():
    """NF4 levels are sorted and symmetric around 0."""
    assert len(NF4_LEVELS) == 16
    assert np.all(NF4_LEVELS[:-1] <= NF4_LEVELS[1:]), "Not sorted"
    assert NF4_LEVELS[0] == -1.0
    assert NF4_LEVELS[-1] == 1.0
    assert NF4_LEVELS[7] == 0.0
    print("  levels valid: PASS")


def _test_roundtrip():
    """Quantize → dequantize produces reasonable approximation."""
    rng = np.random.default_rng(42)
    x = rng.standard_normal(256).astype(np.float32)

    indices, absmax = nf4_quantize(x, block_size=64)
    x_hat = nf4_dequantize(indices, absmax, block_size=64)

    # NF4 should be within ~0.1 of original for normal distribution
    rmse = np.sqrt(np.mean((x - x_hat) ** 2))
    assert rmse < 0.15, f"RMSE too high: {rmse}"
    print(f"  roundtrip (RMSE={rmse:.4f}): PASS")


def _test_zero_block():
    """All-zero block doesn't crash."""
    x = np.zeros(64, dtype=np.float32)
    indices, absmax = nf4_quantize(x, block_size=64)
    x_hat = nf4_dequantize(indices, absmax, block_size=64)
    assert np.allclose(x_hat, 0.0, atol=1e-7), "Zero block failed"
    print("  zero block: PASS")


def _test_index_range():
    """All indices are 0-15 (4-bit)."""
    rng = np.random.default_rng(7)
    x = rng.standard_normal(1024).astype(np.float32)
    indices, _ = nf4_quantize(x, block_size=64)
    assert np.all(indices >= 0) and np.all(indices <= 15), "Index out of 4-bit range"
    print("  index range [0,15]: PASS")


def _test_gaussian_optimality():
    """NF4 should have lower RMSE than uniform quantization for Gaussian data."""
    rng = np.random.default_rng(99)
    x = rng.standard_normal(4096).astype(np.float32)

    # NF4 quantization
    indices, absmax = nf4_quantize(x, block_size=64)
    x_nf4 = nf4_dequantize(indices, absmax, block_size=64)
    rmse_nf4 = np.sqrt(np.mean((x - x_nf4) ** 2))

    # Uniform 4-bit quantization (16 levels, [-max, max])
    x_blocks = x.reshape(-1, 64)
    am = np.max(np.abs(x_blocks), axis=1, keepdims=True)
    am_safe = np.where(am == 0, 1.0, am)
    x_norm = x_blocks / am_safe
    uniform_levels = np.linspace(-1, 1, 16)
    idx_uniform = np.argmin(np.abs(x_norm[:, :, None] - uniform_levels[None, None, :]), axis=-1)
    x_uniform = uniform_levels[idx_uniform] * am
    rmse_uniform = np.sqrt(np.mean((x - x_uniform.flatten()) ** 2))

    assert rmse_nf4 < rmse_uniform, \
        f"NF4 ({rmse_nf4:.4f}) should beat uniform ({rmse_uniform:.4f}) for Gaussian"
    print(f"  Gaussian optimality (NF4={rmse_nf4:.4f} < Uniform={rmse_uniform:.4f}): PASS")


def _test_double_quantization():
    """Double quantization roundtrip."""
    rng = np.random.default_rng(0)
    x = rng.standard_normal(4096).astype(np.float32)

    indices, absmax = nf4_quantize(x, block_size=64)
    dq_indices, dq_scales = nf4_double_quantize(absmax, dq_block_size=256)

    # Verify shapes
    assert len(dq_indices) == len(absmax)
    print("  double quantization shapes: PASS")


if __name__ == "__main__":
    print("nf4_dequant reference tests:")
    _test_levels()
    _test_roundtrip()
    _test_zero_block()
    _test_index_range()
    _test_gaussian_optimality()
    _test_double_quantization()
    print("ALL PASSED")
