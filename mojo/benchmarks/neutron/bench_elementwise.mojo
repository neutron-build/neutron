# ===----------------------------------------------------------------------=== #
# Benchmark: Elementwise operations — SIMD vectorized add/mul
# ===----------------------------------------------------------------------=== #

"""Benchmarks elementwise add and mul at various sizes.

Run: mojo run bench_elementwise.mojo
Measures: GB/s throughput (memory-bandwidth-bound)

Sizes tested: 1K, 10K, 100K, 1M, 10M elements
DType: float32 (Sprint 1 — CPU only)
"""

from time import now

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import add, mul


fn bench_op(name: String, size: Int, warmup: Int = 2, iters: Int = 20) raises:
    """Benchmark an elementwise op."""
    var a = Tensor[DType.float32].rand(Shape(size))
    var b = Tensor[DType.float32].rand(Shape(size))

    # Warmup
    for _ in range(warmup):
        var c = add(a, b)

    # Timed iterations — add
    var total_ns: Int = 0
    for _ in range(iters):
        var t0 = now()
        var c = add(a, b)
        var t1 = now()
        total_ns += t1 - t0

    var avg_ms = Float64(total_ns) / Float64(iters) / 1_000_000.0
    # 3 arrays * size * 4 bytes (read a, read b, write c)
    var bytes = 3.0 * Float64(size) * 4.0
    var gbps = bytes / (avg_ms / 1000.0) / 1e9

    print(
        name + " n=" + str(size)
        + ": " + str(avg_ms) + " ms"
        + " (" + str(gbps) + " GB/s)"
    )


fn main() raises:
    print("=" * 60)
    print("Neutron Mojo — Elementwise Benchmark (CPU, float32)")
    print("=" * 60)

    var sizes = List[Int]()
    sizes.append(1_000)
    sizes.append(10_000)
    sizes.append(100_000)
    sizes.append(1_000_000)
    sizes.append(10_000_000)

    for i in range(len(sizes)):
        bench_op("add", sizes[i])

    print("Done.")
