#!/usr/bin/env python3
"""
Benchmark: cuDNN Matrix Multiplication (via PyTorch)
=====================================================
Measures cuDNN-backed matrix multiplication performance through PyTorch's
torch.mm, which dispatches to cuBLAS/cuDNN under the hood. This represents
the best-case vendor-optimized GEMM performance on NVIDIA hardware.

Configuration:
    --sizes     Matrix sizes (square NxN) to benchmark (default: 1024 2048 4096 8192)
    --dtypes    Data types to benchmark (default: fp16 fp32)
    --runs      Number of timed runs per config (default: 5)

Metrics:
    TFLOPS - Tera floating-point operations per second.
    For NxN matmul: FLOPs = 2 * N^3 (multiply-accumulate).

Dependencies:
    pip install torch

Notes:
    - Requires an NVIDIA GPU with CUDA support.
    - cuDNN must be available (bundled with PyTorch CUDA builds).
    - torch.backends.cudnn.benchmark is enabled for best performance.
    - FP16 matmul uses tensor cores on Volta+ GPUs.
"""

import argparse
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
    print("  For CUDA support: pip install torch --index-url https://download.pytorch.org/whl/cu121")
    sys.exit(1)

if not torch.cuda.is_available():
    print("ERROR: CUDA is not available. This benchmark requires an NVIDIA GPU.")
    sys.exit(1)

if not torch.backends.cudnn.is_available():
    print("WARNING: cuDNN is not available. Results may not reflect cuDNN performance.")


def matmul_flops(n):
    """FLOPs for square NxN matrix multiply: 2*N^3 (fused multiply-add)."""
    return 2 * (n ** 3)


def benchmark_matmul(n, dtype, runs, device):
    """Benchmark a single NxN matmul configuration."""
    torch_dtype = {
        "fp16": torch.float16,
        "fp32": torch.float32,
        "bf16": torch.bfloat16,
        "tf32": torch.float32,  # TF32 uses float32 tensors with tf32 mode
    }[dtype]

    # Enable TF32 if requested
    old_tf32_matmul = torch.backends.cuda.matmul.allow_tf32
    old_tf32_cudnn = torch.backends.cudnn.allow_tf32
    if dtype == "tf32":
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
    elif dtype == "fp32":
        torch.backends.cuda.matmul.allow_tf32 = False
        torch.backends.cudnn.allow_tf32 = False

    a = torch.randn(n, n, dtype=torch_dtype, device=device)
    b = torch.randn(n, n, dtype=torch_dtype, device=device)

    # Warmup (important for cuDNN autotuning)
    for _ in range(5):
        _ = torch.mm(a, b)
    torch.cuda.synchronize()

    times = []
    for _ in range(runs):
        torch.cuda.synchronize()
        t0 = time.perf_counter()
        _ = torch.mm(a, b)
        torch.cuda.synchronize()
        t1 = time.perf_counter()
        times.append(t1 - t0)

    # Restore TF32 settings
    torch.backends.cuda.matmul.allow_tf32 = old_tf32_matmul
    torch.backends.cudnn.allow_tf32 = old_tf32_cudnn

    median_s = statistics.median(times)
    stddev_s = statistics.stdev(times) if len(times) > 1 else 0.0
    flops = matmul_flops(n)
    tflops = (flops / median_s) / 1e12

    return median_s, stddev_s, tflops


def main():
    parser = argparse.ArgumentParser(description="cuDNN Matrix Multiplication Benchmark")
    parser.add_argument("--sizes", type=int, nargs="+", default=[1024, 2048, 4096, 8192])
    parser.add_argument("--dtypes", nargs="+", default=["fp16", "fp32"],
                        choices=["fp16", "fp32", "bf16", "tf32"])
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    device = torch.device("cuda")
    gpu_name = torch.cuda.get_device_name(0)
    cudnn_ver = torch.backends.cudnn.version() if torch.backends.cudnn.is_available() else "N/A"

    print(f"GPU: {gpu_name}")
    print(f"cuDNN version: {cudnn_ver}")
    print(f"PyTorch version: {torch.__version__}")
    print(f"Config: sizes={args.sizes}, dtypes={args.dtypes}, runs={args.runs}")

    # Enable cuDNN benchmark mode for optimal kernel selection
    torch.backends.cudnn.benchmark = True
    print("cuDNN benchmark mode: enabled")
    print("=" * 80)

    for dtype in args.dtypes:
        for n in args.sizes:
            try:
                median_s, stddev_s, tflops = benchmark_matmul(n, dtype, args.runs, device)
                print(f"BENCHMARK: cudnn_matmul | size: {n}x{n} | dtype: {dtype} | "
                      f"time: {median_s*1000:.2f} ms (std {stddev_s*1000:.2f} ms) | "
                      f"tflops: {tflops:.2f} TFLOPS")
            except torch.cuda.OutOfMemoryError:
                print(f"BENCHMARK: cudnn_matmul | size: {n}x{n} | dtype: {dtype} | OOM")
                torch.cuda.empty_cache()
            except Exception as e:
                print(f"BENCHMARK: cudnn_matmul | size: {n}x{n} | dtype: {dtype} | ERROR: {e}")


if __name__ == "__main__":
    main()
