#!/usr/bin/env python3
"""
Benchmark: Triton FlashAttention
=================================
Measures the performance of FlashAttention implemented in Triton across
various sequence lengths. This serves as a baseline for comparing custom
Mojo attention kernels.

Configuration:
    --seq_lens      Sequence lengths to benchmark (default: 512 2048 8192 32768)
    --heads         Number of attention heads (default: 32)
    --head_dim      Dimension per head (default: 128)
    --batch_size    Batch size (default: 2)
    --runs          Number of timed runs per config (default: 5)
    --dtype         Data type: fp16 or bf16 (default: fp16)

Metrics:
    TFLOPS - Tera floating-point operations per second for the attention kernel.

Dependencies:
    pip install torch triton

Notes:
    - Requires an NVIDIA GPU with CUDA support.
    - Triton FlashAttention uses the reference implementation from triton's
      tutorial / flash-attention contrib. If triton.ops is unavailable, a
      minimal fused attention kernel is provided inline.
"""

import argparse
import math
import statistics
import sys
import time

# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------
try:
    import torch
except ImportError:
    print("ERROR: PyTorch is required. Install with:")
    print("  pip install torch")
    sys.exit(1)

if not torch.cuda.is_available():
    print("ERROR: CUDA is not available. This benchmark requires an NVIDIA GPU.")
    sys.exit(1)

try:
    import triton
    import triton.language as tl
except ImportError:
    print("ERROR: Triton is required. Install with:")
    print("  pip install triton")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Minimal fused attention kernel (Triton tutorial-style)
# ---------------------------------------------------------------------------
@triton.jit
def _fwd_kernel(
    Q, K, V, Out,
    stride_qb, stride_qh, stride_qm, stride_qk,
    stride_kb, stride_kh, stride_kn, stride_kk,
    stride_vb, stride_vh, stride_vn, stride_vk,
    stride_ob, stride_oh, stride_om, stride_ok,
    N_CTX,
    BLOCK_M: tl.constexpr, BLOCK_N: tl.constexpr, BLOCK_K: tl.constexpr,
):
    pid_m = tl.program_id(0)
    pid_bh = tl.program_id(1)
    num_heads = stride_qb // stride_qh  # heads per batch

    off_b = pid_bh // num_heads
    off_h = pid_bh % num_heads

    # base pointers
    q_base = Q + off_b * stride_qb + off_h * stride_qh
    k_base = K + off_b * stride_kb + off_h * stride_kh
    v_base = V + off_b * stride_vb + off_h * stride_vh
    o_base = Out + off_b * stride_ob + off_h * stride_oh

    offs_m = pid_m * BLOCK_M + tl.arange(0, BLOCK_M)
    offs_k = tl.arange(0, BLOCK_K)

    # load Q tile
    q = tl.load(q_base + offs_m[:, None] * stride_qm + offs_k[None, :] * stride_qk,
                mask=(offs_m[:, None] < N_CTX) & (offs_k[None, :] < BLOCK_K), other=0.0)

    m_i = tl.full([BLOCK_M], float("-inf"), dtype=tl.float32)
    l_i = tl.zeros([BLOCK_M], dtype=tl.float32)
    acc = tl.zeros([BLOCK_M, BLOCK_K], dtype=tl.float32)

    sm_scale = 1.0 / tl.sqrt(tl.cast(BLOCK_K, tl.float32))

    for start_n in range(0, N_CTX, BLOCK_N):
        offs_n = start_n + tl.arange(0, BLOCK_N)
        # load K tile
        k = tl.load(k_base + offs_n[None, :] * stride_kn + offs_k[:, None] * stride_kk,
                     mask=(offs_n[None, :] < N_CTX) & (offs_k[:, None] < BLOCK_K), other=0.0)
        # QK^T
        qk = tl.dot(q, k) * sm_scale
        # causal mask
        qk = tl.where(offs_m[:, None] >= offs_n[None, :], qk, float("-inf"))
        # online softmax
        m_new = tl.maximum(m_i, tl.max(qk, axis=1))
        alpha = tl.exp(m_i - m_new)
        p = tl.exp(qk - m_new[:, None])
        l_i = l_i * alpha + tl.sum(p, axis=1)
        acc = acc * alpha[:, None]
        # load V tile
        v = tl.load(v_base + offs_n[:, None] * stride_vn + offs_k[None, :] * stride_vk,
                     mask=(offs_n[:, None] < N_CTX) & (offs_k[None, :] < BLOCK_K), other=0.0)
        acc += tl.dot(p.to(v.dtype), v)
        m_i = m_new

    acc = acc / l_i[:, None]
    # store
    tl.store(o_base + offs_m[:, None] * stride_om + offs_k[None, :] * stride_ok,
             acc.to(q.dtype),
             mask=(offs_m[:, None] < N_CTX) & (offs_k[None, :] < BLOCK_K))


def triton_flash_attention(q, k, v):
    """Run the Triton fused attention kernel."""
    B, H, N, D = q.shape
    out = torch.empty_like(q)

    BLOCK_M = 64
    BLOCK_N = 64
    BLOCK_K = D

    grid = (math.ceil(N / BLOCK_M), B * H)

    _fwd_kernel[grid](
        q, k, v, out,
        q.stride(0), q.stride(1), q.stride(2), q.stride(3),
        k.stride(0), k.stride(1), k.stride(2), k.stride(3),
        v.stride(0), v.stride(1), v.stride(2), v.stride(3),
        out.stride(0), out.stride(1), out.stride(2), out.stride(3),
        N,
        BLOCK_M=BLOCK_M, BLOCK_N=BLOCK_N, BLOCK_K=BLOCK_K,
    )
    return out


# ---------------------------------------------------------------------------
# FLOP calculation for causal self-attention
# ---------------------------------------------------------------------------
def attention_flops(batch, heads, seq_len, head_dim, causal=True):
    """Return total FLOPs for one forward pass of multi-head attention.

    QK^T: 2 * B * H * N * N * D
    softmax: ~5 * B * H * N * N  (exp, sub, div, sum, mul)
    PV:  2 * B * H * N * N * D

    For causal, multiply by 0.5 (lower triangle only).
    """
    qk_flops = 2 * batch * heads * seq_len * seq_len * head_dim
    pv_flops = 2 * batch * heads * seq_len * seq_len * head_dim
    softmax_flops = 5 * batch * heads * seq_len * seq_len
    total = qk_flops + pv_flops + softmax_flops
    if causal:
        total = total // 2
    return total


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------
def benchmark_one(batch, heads, seq_len, head_dim, dtype, runs):
    torch_dtype = torch.float16 if dtype == "fp16" else torch.bfloat16
    device = torch.device("cuda")

    q = torch.randn(batch, heads, seq_len, head_dim, dtype=torch_dtype, device=device)
    k = torch.randn_like(q)
    v = torch.randn_like(q)

    # Warmup
    for _ in range(3):
        _ = triton_flash_attention(q, k, v)
    torch.cuda.synchronize()

    times = []
    for _ in range(runs):
        torch.cuda.synchronize()
        t0 = time.perf_counter()
        _ = triton_flash_attention(q, k, v)
        torch.cuda.synchronize()
        t1 = time.perf_counter()
        times.append(t1 - t0)

    median_s = statistics.median(times)
    stddev_s = statistics.stdev(times) if len(times) > 1 else 0.0
    flops = attention_flops(batch, heads, seq_len, head_dim, causal=True)
    tflops = (flops / median_s) / 1e12

    return median_s, stddev_s, tflops


def main():
    parser = argparse.ArgumentParser(description="Triton FlashAttention Benchmark")
    parser.add_argument("--seq_lens", type=int, nargs="+", default=[512, 2048, 8192, 32768])
    parser.add_argument("--heads", type=int, default=32)
    parser.add_argument("--head_dim", type=int, default=128)
    parser.add_argument("--batch_size", type=int, default=2)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--dtype", choices=["fp16", "bf16"], default="fp16")
    args = parser.parse_args()

    gpu_name = torch.cuda.get_device_name(0)
    print(f"GPU: {gpu_name}")
    print(f"Config: heads={args.heads}, head_dim={args.head_dim}, batch={args.batch_size}, "
          f"dtype={args.dtype}, runs={args.runs}")
    print("=" * 80)

    for seq_len in args.seq_lens:
        try:
            median_s, stddev_s, tflops = benchmark_one(
                args.batch_size, args.heads, seq_len, args.head_dim, args.dtype, args.runs
            )
            print(f"BENCHMARK: triton_flash_attention | seq_len: {seq_len} | "
                  f"time: {median_s*1000:.2f} ms (std {stddev_s*1000:.2f} ms) | "
                  f"tflops: {tflops:.2f} TFLOPS")
        except torch.cuda.OutOfMemoryError:
            print(f"BENCHMARK: triton_flash_attention | seq_len: {seq_len} | OOM")
            torch.cuda.empty_cache()
        except Exception as e:
            print(f"BENCHMARK: triton_flash_attention | seq_len: {seq_len} | ERROR: {e}")


if __name__ == "__main__":
    main()
