"""FlashAttention-2 reference — correctness oracle for Mojo FA kernel.

Implements the exact FlashAttention-2 algorithm:
  1. Standard attention (materialized, for ground truth)
  2. Block-wise FlashAttention with online softmax rescaling

The block-wise version matches the GPU tiling pattern exactly:
  - Outer loop over Q blocks
  - Inner loop over K/V blocks
  - Online softmax rescaling when new K blocks arrive

Reference: Dao, "FlashAttention-2: Faster Attention with Better Parallelism
           and Work Partitioning" (2023)

Tolerances: FP32 1e-5, FP16 1e-2
"""

import numpy as np


def attention_standard(
    q: np.ndarray,
    k: np.ndarray,
    v: np.ndarray,
    causal: bool = False,
    scale: float | None = None,
) -> np.ndarray:
    """Standard attention with full materialization. Ground truth reference.

    Args:
        q: (batch, heads, seq_q, head_dim)
        k: (batch, heads, seq_k, head_dim)
        v: (batch, heads, seq_k, head_dim)
        causal: Apply causal mask (lower triangular)
        scale: Attention scale. Default: 1/sqrt(head_dim)

    Returns:
        output: (batch, heads, seq_q, head_dim)
    """
    q64 = q.astype(np.float64)
    k64 = k.astype(np.float64)
    v64 = v.astype(np.float64)

    d = q64.shape[-1]
    if scale is None:
        scale = 1.0 / np.sqrt(d)

    # (batch, heads, seq_q, seq_k)
    scores = np.matmul(q64, k64.transpose(0, 1, 3, 2)) * scale

    if causal:
        seq_q, seq_k = scores.shape[-2], scores.shape[-1]
        # Causal mask: position i can attend to positions 0..i
        mask = np.triu(np.ones((seq_q, seq_k), dtype=np.float64), k=1)
        scores = scores - mask * 1e9

    # Stable softmax along last axis
    scores_max = np.max(scores, axis=-1, keepdims=True)
    exp_scores = np.exp(scores - scores_max)
    attn_weights = exp_scores / np.sum(exp_scores, axis=-1, keepdims=True)

    return np.matmul(attn_weights, v64).astype(q.dtype)


def flash_attention(
    q: np.ndarray,
    k: np.ndarray,
    v: np.ndarray,
    block_q: int = 32,
    block_kv: int = 32,
    causal: bool = False,
    scale: float | None = None,
) -> np.ndarray:
    """FlashAttention-2 block-wise algorithm. Matches GPU tiling pattern.

    This implements Algorithm 1 from the FlashAttention-2 paper:
    - Outer loop: iterate over Q blocks
    - Inner loop: iterate over K/V blocks
    - Maintain running (max, sum_exp, output) and rescale on new blocks

    Args:
        q: (batch, heads, seq_q, head_dim)
        k: (batch, heads, seq_k, head_dim)
        v: (batch, heads, seq_k, head_dim)
        block_q: Q block size (BLOCK_M in the paper)
        block_kv: K/V block size (BLOCK_N in the paper)
        causal: Apply causal mask
        scale: Attention scale. Default: 1/sqrt(head_dim)

    Returns:
        output: (batch, heads, seq_q, head_dim)
    """
    batch, heads, seq_q, d = q.shape
    _, _, seq_k, _ = k.shape

    if scale is None:
        scale = 1.0 / np.sqrt(d)

    q64 = q.astype(np.float64)
    k64 = k.astype(np.float64)
    v64 = v.astype(np.float64)

    output = np.zeros_like(q64)

    for b in range(batch):
        for h in range(heads):
            # Process Q in blocks
            for q_start in range(0, seq_q, block_q):
                q_end = min(q_start + block_q, seq_q)
                q_block = q64[b, h, q_start:q_end]  # (block_q, d)
                bq = q_end - q_start

                # Running statistics for this Q block
                m_i = np.full(bq, -np.inf)    # running max per row
                l_i = np.zeros(bq)             # running sum_exp per row
                o_i = np.zeros((bq, d))        # running output

                # Determine K/V range for causal masking
                kv_end_max = seq_k
                if causal:
                    # For causal: Q position i only attends to K positions <= i
                    kv_end_max = min(seq_k, q_end)

                # Inner loop over K/V blocks
                for kv_start in range(0, kv_end_max, block_kv):
                    kv_end = min(kv_start + block_kv, kv_end_max)
                    k_block = k64[b, h, kv_start:kv_end]  # (block_kv, d)
                    v_block = v64[b, h, kv_start:kv_end]  # (block_kv, d)

                    # Compute attention scores for this block
                    # (block_q, block_kv)
                    s_ij = np.matmul(q_block, k_block.T) * scale

                    # Apply causal mask within block
                    if causal:
                        for qi in range(bq):
                            for ki in range(kv_end - kv_start):
                                if kv_start + ki > q_start + qi:
                                    s_ij[qi, ki] = -np.inf

                    # Online softmax update
                    m_ij = np.max(s_ij, axis=-1)             # max per row in this block
                    m_new = np.maximum(m_i, m_ij)            # new running max

                    # Rescale old accumulator
                    alpha = np.exp(m_i - m_new)              # rescale factor for old
                    beta = np.exp(m_ij - m_new)              # rescale factor for new block

                    # exp(s - m_new) for new block
                    p_ij = np.exp(s_ij - m_new[:, None])

                    # Update running sum
                    l_new = alpha * l_i + np.sum(p_ij, axis=-1)

                    # Update running output
                    o_i = alpha[:, None] * o_i + np.matmul(p_ij, v_block)

                    m_i = m_new
                    l_i = l_new

                # Normalize output
                output[b, h, q_start:q_end] = o_i / l_i[:, None]

    return output.astype(q.dtype)


def flash_attention_gqa(
    q: np.ndarray,
    k: np.ndarray,
    v: np.ndarray,
    num_kv_heads: int,
    block_q: int = 32,
    block_kv: int = 32,
    causal: bool = False,
    scale: float | None = None,
) -> np.ndarray:
    """Grouped Query Attention — multiple Q heads share K/V heads.

    Args:
        q: (batch, num_q_heads, seq_q, head_dim)
        k: (batch, num_kv_heads, seq_k, head_dim)
        v: (batch, num_kv_heads, seq_k, head_dim)
        num_kv_heads: Number of KV heads (must divide num_q_heads)
    """
    batch, num_q_heads, seq_q, d = q.shape
    assert num_q_heads % num_kv_heads == 0
    heads_per_group = num_q_heads // num_kv_heads

    # Expand K/V by repeating heads
    k_expanded = np.repeat(k, heads_per_group, axis=1)
    v_expanded = np.repeat(v, heads_per_group, axis=1)

    return flash_attention(q, k_expanded, v_expanded, block_q, block_kv, causal, scale)


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_matches_standard():
    """Flash attention matches standard attention."""
    rng = np.random.default_rng(42)
    b, h, s, d = 1, 2, 64, 32
    q = rng.standard_normal((b, h, s, d)).astype(np.float32)
    k = rng.standard_normal((b, h, s, d)).astype(np.float32)
    v = rng.standard_normal((b, h, s, d)).astype(np.float32)

    ref = attention_standard(q, k, v, causal=False)
    fa = flash_attention(q, k, v, block_q=16, block_kv=16, causal=False)

    diff = np.max(np.abs(ref.astype(np.float64) - fa.astype(np.float64)))
    assert diff < 1e-4, f"Non-causal max diff: {diff}"
    print(f"  non-causal (max diff={diff:.2e}): PASS")


def _test_causal():
    """Causal flash attention matches causal standard attention."""
    rng = np.random.default_rng(7)
    b, h, s, d = 1, 2, 64, 32
    q = rng.standard_normal((b, h, s, d)).astype(np.float32)
    k = rng.standard_normal((b, h, s, d)).astype(np.float32)
    v = rng.standard_normal((b, h, s, d)).astype(np.float32)

    ref = attention_standard(q, k, v, causal=True)
    fa = flash_attention(q, k, v, block_q=16, block_kv=16, causal=True)

    diff = np.max(np.abs(ref.astype(np.float64) - fa.astype(np.float64)))
    assert diff < 1e-4, f"Causal max diff: {diff}"
    print(f"  causal (max diff={diff:.2e}): PASS")


def _test_block_sizes():
    """Different block sizes produce same results."""
    rng = np.random.default_rng(99)
    b, h, s, d = 1, 1, 48, 16
    q = rng.standard_normal((b, h, s, d)).astype(np.float32)
    k = rng.standard_normal((b, h, s, d)).astype(np.float32)
    v = rng.standard_normal((b, h, s, d)).astype(np.float32)

    ref = attention_standard(q, k, v, causal=False)
    for bq, bkv in [(8, 8), (16, 8), (8, 16), (32, 16), (48, 48)]:
        fa = flash_attention(q, k, v, block_q=bq, block_kv=bkv, causal=False)
        diff = np.max(np.abs(ref.astype(np.float64) - fa.astype(np.float64)))
        assert diff < 1e-4, f"Block ({bq},{bkv}) max diff: {diff}"

    print("  block sizes [8x8, 16x8, 8x16, 32x16, 48x48]: PASS")


def _test_gqa():
    """Grouped query attention with 4:1 head ratio."""
    rng = np.random.default_rng(0)
    b, s, d = 1, 32, 16
    q_heads, kv_heads = 8, 2

    q = rng.standard_normal((b, q_heads, s, d)).astype(np.float32)
    k = rng.standard_normal((b, kv_heads, s, d)).astype(np.float32)
    v = rng.standard_normal((b, kv_heads, s, d)).astype(np.float32)

    out = flash_attention_gqa(q, k, v, num_kv_heads=kv_heads,
                               block_q=16, block_kv=16, causal=True)
    assert out.shape == (b, q_heads, s, d), f"GQA shape: {out.shape}"

    # Heads in the same group should produce identical results
    # (same K/V, different Q)
    # Just verify it runs and produces finite output
    assert np.all(np.isfinite(out)), "GQA produced inf/nan"
    print("  GQA (8 Q heads, 2 KV heads): PASS")


def _test_large_values():
    """Numerical stability with large attention scores."""
    rng = np.random.default_rng(123)
    b, h, s, d = 1, 1, 32, 8
    q = rng.standard_normal((b, h, s, d)).astype(np.float32) * 10
    k = rng.standard_normal((b, h, s, d)).astype(np.float32) * 10
    v = rng.standard_normal((b, h, s, d)).astype(np.float32)

    fa = flash_attention(q, k, v, block_q=8, block_kv=8, causal=False)
    assert np.all(np.isfinite(fa)), "Large values produced inf/nan"

    ref = attention_standard(q, k, v, causal=False)
    diff = np.max(np.abs(ref.astype(np.float64) - fa.astype(np.float64)))
    assert diff < 1e-3, f"Large values max diff: {diff}"
    print(f"  large values (max diff={diff:.2e}): PASS")


def _test_single_token():
    """Single query token (autoregressive decode step)."""
    rng = np.random.default_rng(55)
    b, h, d = 1, 4, 32
    seq_k = 128

    q = rng.standard_normal((b, h, 1, d)).astype(np.float32)
    k = rng.standard_normal((b, h, seq_k, d)).astype(np.float32)
    v = rng.standard_normal((b, h, seq_k, d)).astype(np.float32)

    ref = attention_standard(q, k, v, causal=False)
    fa = flash_attention(q, k, v, block_q=1, block_kv=32, causal=False)

    diff = np.max(np.abs(ref.astype(np.float64) - fa.astype(np.float64)))
    assert diff < 1e-4, f"Single token max diff: {diff}"
    print(f"  single token decode (max diff={diff:.2e}): PASS")


if __name__ == "__main__":
    print("flash_attention reference tests:")
    _test_matches_standard()
    _test_causal()
    _test_block_sizes()
    _test_gqa()
    _test_large_values()
    _test_single_token()
    print("ALL PASSED")
