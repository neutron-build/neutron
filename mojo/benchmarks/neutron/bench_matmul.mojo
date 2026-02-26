# ===----------------------------------------------------------------------=== #
# Benchmark: Matrix multiplication — neutron_mojo tiled matmul
# ===----------------------------------------------------------------------=== #

"""Benchmarks our tiled matmul at various sizes.

Run: mojo run bench_matmul.mojo
Compares against: benchmarks/competitors/pytorch_matmul.py, cudnn_matmul.py

Sizes tested: 128, 256, 512, 1024, 2048, 4096
DType: float32 (Sprint 1 — CPU only)
"""

from time import now

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import matmul


fn bench_matmul(M: Int, K: Int, N: Int, warmup: Int = 2, iters: Int = 10) raises:
    """Benchmark matmul at a given size."""
    var a = Tensor[DType.float32].rand(Shape(M, K))
    var b = Tensor[DType.float32].rand(Shape(K, N))

    # Warmup
    for _ in range(warmup):
        var c = matmul(a, b)

    # Timed iterations
    var total_ns: Int = 0
    for _ in range(iters):
        var t0 = now()
        var c = matmul(a, b)
        var t1 = now()
        total_ns += t1 - t0

    var avg_ms = Float64(total_ns) / Float64(iters) / 1_000_000.0
    var flops = 2.0 * Float64(M) * Float64(N) * Float64(K)
    var gflops = flops / (avg_ms / 1000.0) / 1e9

    print(
        str(M) + "x" + str(K) + " @ " + str(K) + "x" + str(N)
        + ": " + str(avg_ms) + " ms"
        + " (" + str(gflops) + " GFLOPS)"
    )


fn main() raises:
    print("=" * 60)
    print("Neutron Mojo — Matmul Benchmark (CPU, float32)")
    print("=" * 60)

    var sizes = List[Int]()
    sizes.append(128)
    sizes.append(256)
    sizes.append(512)
    sizes.append(1024)
    sizes.append(2048)

    for i in range(len(sizes)):
        var n = sizes[i]
        bench_matmul(n, n, n)

    print("Done.")
