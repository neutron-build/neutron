"""MJ-K01: all five attention paths against independent two-token softmax."""
from std.math import sqrt, exp, abs, isfinite
from std.testing import assert_true
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.kv_cache import KVCache
from neutron_mojo.nn.q_kv_cache import Q8KVCache, q8_attention_single_head
from neutron_mojo.nn.fused_attention import fused_attention_head, fused_q8_attention_head
from neutron_mojo.nn.sliding_window import SlidingWindowKVCache, sliding_window_attention_head, windowed_fused_attention_head


def near(value: Float32, expected: Float32) raises:
    assert_true(isfinite(value))
    assert_true(abs(value - expected) < 0.00001, String(value) + " vs " + String(expected))


def main() raises:
    var dimensions: List[Int] = [1, 3, 12, 16, 32, 64, 128]
    for di in range(len(dimensions)):
        var dim = dimensions[di]
        var cache = KVCache(4, 1, dim)
        var quantized = Q8KVCache(4, 1, dim)
        var sliding = SlidingWindowKVCache(4, 1, dim)
        var query = Tensor[DType.float32].ones(Shape(dim))
        for pos in range(2):
            var key = Tensor[DType.float32].full(Shape(dim), Float32(pos))
            var value = Tensor[DType.float32].full(Shape(dim), Float32(pos))
            cache.append_kv(key, value, num_new_tokens=1)
            quantized.append_kv(key, value, num_new_tokens=1)
            sliding.append_kv(key, value)
        # Scores 0 and dim/sqrt(dim); values 0 and 1. Uniform per-head
        # quantization is exact here, so all five paths share the same oracle.
        var expected = Float32(1) / (1 + Float32(exp(-sqrt(Float64(dim)))))
        var a = q8_attention_single_head(query, quantized, 0, 0, dim)
        var b = fused_attention_head(query, cache, 0, dim, 1)
        var c = fused_q8_attention_head(query, quantized, 0, dim, 1)
        var d = sliding_window_attention_head(query, sliding, 0, dim)
        var e = windowed_fused_attention_head(query, cache, 0, dim, 1, 4)
        for i in range(dim):
            near(a.get(i), expected)
            near(b.get(i), expected)
            near(c.get(i), expected)
            near(d.get(i), expected)
            near(e.get(i), expected)
    print("MJ-K01: 7 head dimensions x 5 attention paths PASS")
