# ===----------------------------------------------------------------------=== #
# Benchmark: Softmax — numerically stable softmax
# ===----------------------------------------------------------------------=== #

"""Benchmarks softmax at various sizes.

Run: mojo run bench_softmax.mojo
Compares against: benchmarks/competitors/triton_fa.py (softmax component)

Sizes tested:
  1D: 128, 1024, 8192, 32768
  2D: (32, 128), (32, 1024), (32, 8192) — simulates attention row softmax
DType: float32 (Sprint 1 — CPU only)
"""

from time import now

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import softmax


fn bench_softmax_1d(size: Int, warmup: Int = 2, iters: Int = 20) raises:
    """Benchmark 1D softmax."""
    var x = Tensor[DType.float32].rand(Shape(size))

    for _ in range(warmup):
        var s = softmax(x)

    var total_ns: Int = 0
    for _ in range(iters):
        var t0 = now()
        var s = softmax(x)
        var t1 = now()
        total_ns += t1 - t0

    var avg_ms = Float64(total_ns) / Float64(iters) / 1_000_000.0
    print("softmax_1d n=" + str(size) + ": " + str(avg_ms) + " ms")


fn bench_softmax_2d(rows: Int, cols: Int, warmup: Int = 2, iters: Int = 20) raises:
    """Benchmark 2D softmax along last axis."""
    var x = Tensor[DType.float32].rand(Shape(rows, cols))

    for _ in range(warmup):
        var s = softmax(x, axis=-1)

    var total_ns: Int = 0
    for _ in range(iters):
        var t0 = now()
        var s = softmax(x, axis=-1)
        var t1 = now()
        total_ns += t1 - t0

    var avg_ms = Float64(total_ns) / Float64(iters) / 1_000_000.0
    print(
        "softmax_2d (" + str(rows) + "," + str(cols) + "): "
        + str(avg_ms) + " ms"
    )


fn main() raises:
    print("=" * 60)
    print("Neutron Mojo — Softmax Benchmark (CPU, float32)")
    print("=" * 60)

    print("\n--- 1D ---")
    bench_softmax_1d(128)
    bench_softmax_1d(1024)
    bench_softmax_1d(8192)
    bench_softmax_1d(32768)

    print("\n--- 2D (attention-like) ---")
    bench_softmax_2d(32, 128)
    bench_softmax_2d(32, 1024)
    bench_softmax_2d(32, 8192)

    print("Done.")
