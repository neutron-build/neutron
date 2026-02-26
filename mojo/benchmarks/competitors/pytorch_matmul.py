#!/usr/bin/env python3
"""
Benchmark: PyTorch Eager Matrix Multiplication
================================================
Measures PyTorch eager-mode matrix multiplication performance as a TFLOPS
baseline. This uses torch.matmul directly (no torch.compile, no custom
kernels) to establish the floor performance that any custom kernel should beat.

Configuration:
    --sizes     Matrix sizes (square NxN) to benchmark (default: 1024 2048 4096 8192)
    --dtypes    Data types to benchmark (default: fp16 fp32)
    --runs      Number of timed runs per config (default: 5)
    --warmup    Number of warmup iterations (default: 10)

Metrics:
    TFLOPS - Tera floating-point operations per second.
    For NxN matmul: FLOPs = 2 * N^3 (multiply-accumulate).

Dependencies:
    pip install torch

Notes:
    - Requires an NVIDIA GPU with CUDA support.
    - Unlike cudnn_matmul.py, this explicitly uses torch.matmul (not torch.mm)
      and represents the most common user-facing API.
    - TF32 is disabled for FP32 to get true FP32 numbers. Use --dtypes tf32
      to measure TF32 performance.
    - CUDA warmup is critical for stable measurements; the default 10 iterations
      ensures GPU clocks are boosted and caches are warm.
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
    print("")
    print("For CUDA support:")
    print("  pip install torch --index-url https://download.pytorch.org/whl/cu121")
    sys.exit(1)

if not torch.cuda.is_available():
    print("ERROR: CUDA is not available. This benchmark requires an NVIDIA GPU.")
    print("")
    print("If you have an NVIDIA GPU, ensure:")
    print("  1. NVIDIA drivers are installed")
    print("  2. PyTorch is installed with CUDA support:")
    print("     pip install torch --index-url https://download.pytorch.org/whl/cu121")
    sys.exit(1)


def matmul_flops(n):
    """FLOPs for square NxN matrix multiply: 2*N^3."""
    return 2 * (n ** 3)


def benchmark_matmul(n, dtype, runs, warmup, device):
    """Benchmark a single NxN matmul configuration.

    Returns (median_seconds, stddev_seconds, tflops).
    """
    dtype_map = {
        "fp16": torch.float16,
        "fp32": torch.float32,
        "bf16": torch.bfloat16,
        "tf32": torch.float32,
    }
    torch_dtype = dtype_map[dtype]

    # Configure TF32 behavior
    old_tf32_matmul = torch.backends.cuda.matmul.allow_tf32
    old_tf32_cudnn = torch.backends.cudnn.allow_tf32
    if dtype == "tf32":
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
    elif dtype == "fp32":
        # Disable TF32 for true FP32 measurement
        torch.backends.cuda.matmul.allow_tf32 = False
        torch.backends.cudnn.allow_tf32 = False

    # Allocate matrices
    a = torch.randn(n, n, dtype=torch_dtype, device=device)
    b = torch.randn(n, n, dtype=torch_dtype, device=device)

    # Warmup: critical for GPU clock boosting and cache warming
    for _ in range(warmup):
        _ = torch.matmul(a, b)
    torch.cuda.synchronize()

    # Timed runs
    times = []
    for _ in range(runs):
        torch.cuda.synchronize()
        t0 = time.perf_counter()
        _ = torch.matmul(a, b)
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
    parser = argparse.ArgumentParser(description="PyTorch Eager Matmul Benchmark")
    parser.add_argument("--sizes", type=int, nargs="+", default=[1024, 2048, 4096, 8192])
    parser.add_argument("--dtypes", nargs="+", default=["fp16", "fp32"],
                        choices=["fp16", "fp32", "bf16", "tf32"])
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=10)
    args = parser.parse_args()

    device = torch.device("cuda")
    gpu_name = torch.cuda.get_device_name(0)
    gpu_mem = torch.cuda.get_device_properties(0).total_mem / (1024**3)

    print(f"GPU: {gpu_name} ({gpu_mem:.1f} GB)")
    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA version: {torch.version.cuda}")
    print(f"cuDNN version: {torch.backends.cudnn.version() if torch.backends.cudnn.is_available() else 'N/A'}")
    print(f"Config: sizes={args.sizes}, dtypes={args.dtypes}, runs={args.runs}, warmup={args.warmup}")
    print("=" * 80)

    for dtype in args.dtypes:
        for n in args.sizes:
            try:
                median_s, stddev_s, tflops = benchmark_matmul(n, dtype, args.runs, args.warmup, device)

                # Memory estimate: 3 matrices (A, B, C) of NxN
                bytes_per_elem = 2 if dtype in ("fp16", "bf16") else 4
                mem_gb = 3 * n * n * bytes_per_elem / (1024**3)

                print(f"BENCHMARK: pytorch_matmul | size: {n}x{n} | dtype: {dtype} | "
                      f"time: {median_s*1000:.2f} ms (std {stddev_s*1000:.2f} ms) | "
                      f"tflops: {tflops:.2f} TFLOPS | mem: {mem_gb:.2f} GB")
            except torch.cuda.OutOfMemoryError:
                print(f"BENCHMARK: pytorch_matmul | size: {n}x{n} | dtype: {dtype} | OOM")
                torch.cuda.empty_cache()
            except Exception as e:
                print(f"BENCHMARK: pytorch_matmul | size: {n}x{n} | dtype: {dtype} | ERROR: {e}")

    # Summary comparison
    print("")
    print("=" * 80)
    print("Notes:")
    print("  - FP16 uses tensor cores on Volta+ (V100, A100, H100, etc.)")
    print("  - FP32 with TF32 disabled is true IEEE FP32 (slower on Ampere+)")
    print("  - Use --dtypes tf32 to measure TF32 mode (Ampere+ default)")
    print("  - These are EAGER mode numbers; torch.compile may be faster")


if __name__ == "__main__":
    main()
