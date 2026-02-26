"""Rotary Positional Embeddings (RoPE) — correctness oracle.

RoPE encodes position by rotating pairs of dimensions in the
query/key vectors. Used in Llama, GPT-NeoX, and most modern LLMs.

The rotation is: for each pair (x_{2i}, x_{2i+1}) at position m,
  x'_{2i}   = x_{2i}   * cos(m*theta_i) - x_{2i+1} * sin(m*theta_i)
  x'_{2i+1} = x_{2i}   * sin(m*theta_i) + x_{2i+1} * cos(m*theta_i)

where theta_i = base^(-2i/d), base=10000 by default.

Reference: Su et al., "RoFormer: Enhanced Transformer with Rotary Position Embedding" (2021)

Tolerance: FP32 1e-6, FP16 1e-3
"""

import numpy as np


def compute_freqs(
    head_dim: int,
    max_seq_len: int,
    base: float = 10000.0,
) -> tuple:
    """Compute cos/sin frequency tables for RoPE.

    Args:
        head_dim: Dimension of each attention head (must be even)
        max_seq_len: Maximum sequence length
        base: Base for frequency computation

    Returns:
        (cos_table, sin_table): shape (max_seq_len, head_dim)
    """
    assert head_dim % 2 == 0, "head_dim must be even"

    # theta_i = base^(-2i/d) for i in [0, d/2)
    dim_pairs = head_dim // 2
    freqs = 1.0 / (base ** (np.arange(0, head_dim, 2, dtype=np.float64) / head_dim))

    # Outer product: positions x frequencies
    positions = np.arange(max_seq_len, dtype=np.float64)
    angles = np.outer(positions, freqs)  # (seq, d/2)

    # Duplicate for both elements of each pair
    cos_table = np.cos(angles)  # (seq, d/2)
    sin_table = np.sin(angles)  # (seq, d/2)

    return cos_table, sin_table


def apply_rope(
    x: np.ndarray,
    cos_table: np.ndarray,
    sin_table: np.ndarray,
    start_pos: int = 0,
) -> np.ndarray:
    """Apply RoPE to a tensor.

    Args:
        x: (batch, heads, seq, head_dim) or (batch, seq, heads, head_dim)
        cos_table: (max_seq, head_dim//2)
        sin_table: (max_seq, head_dim//2)
        start_pos: Starting position (for incremental decoding)

    Returns:
        Rotated tensor, same shape as x
    """
    x64 = x.astype(np.float64)
    seq_len = x64.shape[-2]
    head_dim = x64.shape[-1]

    cos = cos_table[start_pos:start_pos + seq_len]  # (seq, d/2)
    sin = sin_table[start_pos:start_pos + seq_len]  # (seq, d/2)

    # Split into pairs
    x_even = x64[..., 0::2]  # (..., seq, d/2)
    x_odd = x64[..., 1::2]   # (..., seq, d/2)

    # Broadcast cos/sin to match batch/head dims
    # cos/sin are (seq, d/2), need to broadcast to (..., seq, d/2)
    out_even = x_even * cos - x_odd * sin
    out_odd = x_even * sin + x_odd * cos

    # Interleave back
    result = np.zeros_like(x64)
    result[..., 0::2] = out_even
    result[..., 1::2] = out_odd

    return result.astype(x.dtype)


def rope_simple(
    x: np.ndarray,
    positions: np.ndarray,
    head_dim: int,
    base: float = 10000.0,
) -> np.ndarray:
    """Apply RoPE with explicit position indices (for variable-length sequences).

    Args:
        x: (..., head_dim)
        positions: (...) integer position indices, broadcastable with x[..., 0]
    """
    x64 = x.astype(np.float64)
    freqs = 1.0 / (base ** (np.arange(0, head_dim, 2, dtype=np.float64) / head_dim))

    # positions: (...), freqs: (d/2,) -> angles: (..., d/2)
    angles = positions[..., None].astype(np.float64) * freqs

    cos_angles = np.cos(angles)
    sin_angles = np.sin(angles)

    x_even = x64[..., 0::2]
    x_odd = x64[..., 1::2]

    result = np.zeros_like(x64)
    result[..., 0::2] = x_even * cos_angles - x_odd * sin_angles
    result[..., 1::2] = x_even * sin_angles + x_odd * cos_angles

    return result.astype(x.dtype)


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_freqs():
    """Frequency table properties."""
    cos_t, sin_t = compute_freqs(64, 128)
    assert cos_t.shape == (128, 32)
    assert sin_t.shape == (128, 32)

    # Position 0: cos=1, sin=0
    assert np.allclose(cos_t[0], 1.0, atol=1e-10)
    assert np.allclose(sin_t[0], 0.0, atol=1e-10)

    # cos^2 + sin^2 = 1
    assert np.allclose(cos_t ** 2 + sin_t ** 2, 1.0, atol=1e-10)
    print("  freqs: PASS")


def _test_identity_at_pos0():
    """RoPE at position 0 is identity (cos=1, sin=0)."""
    rng = np.random.default_rng(42)
    x = rng.standard_normal((1, 4, 1, 64)).astype(np.float32)
    cos_t, sin_t = compute_freqs(64, 128)

    out = apply_rope(x, cos_t, sin_t, start_pos=0)
    assert np.allclose(out, x, rtol=1e-5, atol=1e-6), "Position 0 should be identity"
    print("  identity at pos 0: PASS")


def _test_rotation_property():
    """Rotation preserves vector norm."""
    rng = np.random.default_rng(7)
    x = rng.standard_normal((2, 8, 32, 64)).astype(np.float32)
    cos_t, sin_t = compute_freqs(64, 128)

    out = apply_rope(x, cos_t, sin_t, start_pos=0)

    # Check norm preservation for each pair of dimensions
    x64 = x.astype(np.float64)
    out64 = out.astype(np.float64)
    for i in range(0, 64, 2):
        norm_in = np.sqrt(x64[..., i] ** 2 + x64[..., i + 1] ** 2)
        norm_out = np.sqrt(out64[..., i] ** 2 + out64[..., i + 1] ** 2)
        assert np.allclose(norm_in, norm_out, rtol=1e-5), f"Norm not preserved at dim {i}"

    print("  rotation preserves norm: PASS")


def _test_relative_position():
    """RoPE key property: q^T·k depends only on relative position.

    For positions m and n:
      rope(q, m)^T · rope(k, n) = f(q, k, m-n)
    """
    rng = np.random.default_rng(99)
    head_dim = 32
    cos_t, sin_t = compute_freqs(head_dim, 256)

    q = rng.standard_normal((1, 1, 1, head_dim)).astype(np.float64)
    k = rng.standard_normal((1, 1, 1, head_dim)).astype(np.float64)

    # Compute dot product at positions (5, 3) — relative distance 2
    q_rot_5 = apply_rope(q, cos_t, sin_t, start_pos=5)
    k_rot_3 = apply_rope(k, cos_t, sin_t, start_pos=3)
    dot_53 = np.sum(q_rot_5 * k_rot_3)

    # Compute dot product at positions (10, 8) — also relative distance 2
    q_rot_10 = apply_rope(q, cos_t, sin_t, start_pos=10)
    k_rot_8 = apply_rope(k, cos_t, sin_t, start_pos=8)
    dot_108 = np.sum(q_rot_10 * k_rot_8)

    assert abs(dot_53 - dot_108) < 1e-8, \
        f"Relative position violated: dot(5,3)={dot_53}, dot(10,8)={dot_108}"
    print("  relative position property: PASS")


def _test_incremental():
    """start_pos parameter for incremental decoding."""
    rng = np.random.default_rng(0)
    head_dim = 64
    cos_t, sin_t = compute_freqs(head_dim, 256)

    # Full sequence
    x_full = rng.standard_normal((1, 4, 8, head_dim)).astype(np.float32)
    out_full = apply_rope(x_full, cos_t, sin_t, start_pos=0)

    # Incremental: first 4, then next 4
    out_first = apply_rope(x_full[:, :, :4, :], cos_t, sin_t, start_pos=0)
    out_second = apply_rope(x_full[:, :, 4:, :], cos_t, sin_t, start_pos=4)

    assert np.allclose(out_full[:, :, :4, :], out_first, rtol=1e-5)
    assert np.allclose(out_full[:, :, 4:, :], out_second, rtol=1e-5)
    print("  incremental decoding: PASS")


def _test_simple_api():
    """rope_simple with explicit position indices."""
    rng = np.random.default_rng(55)
    head_dim = 32
    x = rng.standard_normal((2, 4, head_dim)).astype(np.float32)
    positions = np.array([[0, 1, 2, 3], [5, 6, 7, 8]])

    out = rope_simple(x, positions, head_dim)
    assert out.shape == x.shape
    assert np.all(np.isfinite(out))
    print("  simple API: PASS")


if __name__ == "__main__":
    print("rope reference tests:")
    _test_freqs()
    _test_identity_at_pos0()
    _test_rotation_property()
    _test_relative_position()
    _test_incremental()
    _test_simple_api()
    print("ALL PASSED")
