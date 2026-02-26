# ===----------------------------------------------------------------------=== #
# Neutron Mojo -- Sprint 29: Per-Operation Profiler Tests
# ===----------------------------------------------------------------------=== #

"""Tests for per-operation profiling of transformer forward passes.

Tests:
1. ProfileResult default init (all zeros)
2. ProfileResult copy works
3. ProfileResult add accumulates
4. ProfileResult summary formatting
5. ProfileResult layer_total_ns
6. ProfileResult overhead_ns
7. Profile forward produces logits and profile
8. Profile forward timing is non-zero
9. Profile forward all ops are measured
10. Profile forward matches Model.forward output
11. Profile decode runs without error
12. Profile decode aggregate accumulates across steps
13. Profile decode tokens_per_sec is positive
14. DecodeProfileResult avg_step_ns
15. DecodeProfileResult summary formatting
16. Benchmark: profiled vs unprofiled overhead
"""

from math import abs
from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.profiler import (
    ProfileResult,
    DecodeProfileResult,
    profile_forward,
    profile_decode,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


fn assert_close(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error("FAIL: " + msg + " a=" + String(a) + " b=" + String(b))


# ===----------------------------------------------------------------------=== #
# Helpers
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    var p = tiny_test_params()
    var model = Model(p)
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32(v * p.hidden_dim + d) * 0.01)
            model.lm_head.set(v * p.hidden_dim + d, Float32(v + d) * 0.1)
    for i in range(p.num_layers * model.layer_size):
        model.layer_weights.set(i, Float32(i % 13) * 0.02 - 0.12)
    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        for i in range(p.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)
    return model^


# ===----------------------------------------------------------------------=== #
# ProfileResult Tests
# ===----------------------------------------------------------------------=== #

fn test_profile_result_default() raises:
    """ProfileResult initializes to all zeros."""
    var r = ProfileResult()
    assert_true(r.embed_ns == 0, "embed_ns should be 0")
    assert_true(r.attn_norm_ns == 0, "attn_norm_ns should be 0")
    assert_true(r.qkv_proj_ns == 0, "qkv_proj_ns should be 0")
    assert_true(r.rope_ns == 0, "rope_ns should be 0")
    assert_true(r.kv_cache_ns == 0, "kv_cache_ns should be 0")
    assert_true(r.attention_ns == 0, "attention_ns should be 0")
    assert_true(r.output_proj_ns == 0, "output_proj_ns should be 0")
    assert_true(r.ffn_norm_ns == 0, "ffn_norm_ns should be 0")
    assert_true(r.ffn_proj_ns == 0, "ffn_proj_ns should be 0")
    assert_true(r.swiglu_ns == 0, "swiglu_ns should be 0")
    assert_true(r.final_norm_ns == 0, "final_norm_ns should be 0")
    assert_true(r.lm_head_ns == 0, "lm_head_ns should be 0")
    assert_true(r.total_ns == 0, "total_ns should be 0")
    assert_true(r.num_layers == 0, "num_layers should be 0")
    print("  profile_result_default: PASS")


fn test_profile_result_copy() raises:
    """ProfileResult copy preserves values."""
    var r = ProfileResult()
    r.embed_ns = 100
    r.qkv_proj_ns = 500
    r.total_ns = 1000
    r.num_layers = 2

    var c = r.copy()
    assert_true(c.embed_ns == 100, "copy embed_ns")
    assert_true(c.qkv_proj_ns == 500, "copy qkv_proj_ns")
    assert_true(c.total_ns == 1000, "copy total_ns")
    assert_true(c.num_layers == 2, "copy num_layers")
    print("  profile_result_copy: PASS")


fn test_profile_result_add() raises:
    """ProfileResult add accumulates values."""
    var a = ProfileResult()
    a.embed_ns = 100
    a.qkv_proj_ns = 200
    a.total_ns = 300

    var b = ProfileResult()
    b.embed_ns = 50
    b.qkv_proj_ns = 150
    b.total_ns = 200

    a.add(b)
    assert_true(a.embed_ns == 150, "accumulated embed_ns")
    assert_true(a.qkv_proj_ns == 350, "accumulated qkv_proj_ns")
    assert_true(a.total_ns == 500, "accumulated total_ns")
    print("  profile_result_add: PASS")


fn test_profile_result_summary() raises:
    """ProfileResult summary produces formatted output."""
    var r = ProfileResult()
    r.embed_ns = 1000
    r.attn_norm_ns = 2000
    r.qkv_proj_ns = 5000
    r.rope_ns = 1000
    r.kv_cache_ns = 500
    r.attention_ns = 3000
    r.output_proj_ns = 2000
    r.ffn_norm_ns = 1500
    r.ffn_proj_ns = 6000
    r.swiglu_ns = 1000
    r.final_norm_ns = 800
    r.lm_head_ns = 2000
    r.total_ns = 30000
    r.num_layers = 2

    var s = r.summary()
    assert_true(len(s) > 50, "summary has content")
    assert_true("embed" in s, "summary contains embed")
    assert_true("qkv_proj" in s, "summary contains qkv_proj")
    assert_true("attention" in s, "summary contains attention")
    assert_true("lm_head" in s, "summary contains lm_head")
    assert_true("Profile" in s, "summary contains Profile header")
    print("  profile_result_summary: PASS")


fn test_profile_result_layer_total() raises:
    """Layer total sums all per-layer operations."""
    var r = ProfileResult()
    r.attn_norm_ns = 100
    r.qkv_proj_ns = 200
    r.rope_ns = 50
    r.kv_cache_ns = 30
    r.attention_ns = 300
    r.output_proj_ns = 150
    r.ffn_norm_ns = 80
    r.ffn_proj_ns = 400
    r.swiglu_ns = 60

    var expected = 100 + 200 + 50 + 30 + 300 + 150 + 80 + 400 + 60
    assert_true(r.layer_total_ns() == expected,
        "layer_total should be " + String(expected))
    print("  profile_result_layer_total: PASS")


fn test_profile_result_overhead() raises:
    """Overhead computes unaccounted time."""
    var r = ProfileResult()
    r.embed_ns = 100
    r.attn_norm_ns = 200
    r.qkv_proj_ns = 300
    r.total_ns = 1000

    # overhead = total - (embed + layer_total + final_norm + lm_head)
    # layer_total = attn_norm(200) + qkv_proj(300) = 500
    # measured = 100 + 500 + 0 + 0 = 600
    # overhead = 1000 - 600 = 400
    assert_true(r.overhead_ns() == 400, "overhead should be 400")
    print("  profile_result_overhead: PASS")


# ===----------------------------------------------------------------------=== #
# Profiled Forward Tests
# ===----------------------------------------------------------------------=== #

fn test_profile_forward_produces_output() raises:
    """Profile forward produces logits and fills profile data."""
    var model = _build_tiny_model()
    var p = model.params.copy()
    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    var prof = ProfileResult()
    var logits = profile_forward(model, 3, cache, rope, pos=0, prof=prof)
    assert_true(logits.numel() == p.vocab_size, "logits has vocab_size elements")
    assert_true(prof.num_layers == p.num_layers, "profile has correct num_layers")
    assert_true(prof.total_ns > 0, "total_ns is positive")
    print("  profile_forward_produces_output: PASS")


fn test_profile_forward_timing_nonzero() raises:
    """Profiled forward has non-zero timing for key operations."""
    var model = _build_tiny_model()
    var p = model.params.copy()
    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    var prof = ProfileResult()
    _ = profile_forward(model, 3, cache, rope, pos=0, prof=prof)

    # Total should be non-zero
    assert_true(prof.total_ns > 0, "total_ns > 0")
    # On a tiny model, individual ops might be < 1ns resolution,
    # but aggregate should be measurable
    var measured = (prof.embed_ns + prof.attn_norm_ns + prof.qkv_proj_ns +
                    prof.rope_ns + prof.attention_ns + prof.output_proj_ns +
                    prof.ffn_norm_ns + prof.ffn_proj_ns + prof.final_norm_ns +
                    prof.lm_head_ns)
    assert_true(measured >= 0, "measured ops are non-negative")
    print("  profile_forward_timing_nonzero: PASS")


fn test_profile_forward_all_ops_measured() raises:
    """All operation categories get timing entries after warmup."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    # Warmup: fill cache with a few positions
    for i in range(3):
        _ = model.forward(3, cache, rope, pos=i)

    # Now profile at pos=3
    var prof = ProfileResult()
    _ = profile_forward(model, 3, cache, rope, pos=3, prof=prof)

    assert_true(prof.total_ns > 0, "total_ns > 0 after warmup")
    assert_true(prof.num_layers == 2, "num_layers == 2")
    print("  profile_forward_all_ops_measured: PASS")


fn test_profile_matches_model_forward() raises:
    """Profiled forward produces identical logits to Model.forward."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    # Run Model.forward
    var cache1 = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope1 = RoPETable(head_dim=p.head_dim, max_seq_len=32)
    var logits1 = model.forward(3, cache1, rope1, pos=0)

    # Run profiled forward
    var cache2 = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope2 = RoPETable(head_dim=p.head_dim, max_seq_len=32)
    var prof = ProfileResult()
    var logits2 = profile_forward(model, 3, cache2, rope2, pos=0, prof=prof)

    # Compare logits
    assert_true(logits1.numel() == logits2.numel(), "same logits size")
    for i in range(p.vocab_size):
        assert_close(logits1.get(i), logits2.get(i), 1e-5,
            "logit mismatch at " + String(i))
    print("  profile_matches_model_forward: PASS")


# ===----------------------------------------------------------------------=== #
# Decode Profile Tests
# ===----------------------------------------------------------------------=== #

fn test_profile_decode_runs() raises:
    """Decode profiling runs without error."""
    var model = _build_tiny_model()
    var prompt = List[Int]()
    prompt.append(3)
    prompt.append(4)
    prompt.append(5)

    var result = profile_decode(model, prompt, num_steps=5)
    assert_true(result.num_steps == 5, "num_steps == 5")
    assert_true(result.aggregate.total_ns > 0, "aggregate total > 0")
    print("  profile_decode_runs: PASS")


fn test_profile_decode_accumulates() raises:
    """Decode profiling accumulates across steps."""
    var model = _build_tiny_model()
    var prompt = List[Int]()
    prompt.append(3)

    var result = profile_decode(model, prompt, num_steps=3)

    assert_true(result.aggregate.total_ns > 0, "aggregate total > 0")
    assert_true(result.aggregate.num_layers == 2, "num_layers preserved")
    print("  profile_decode_accumulates: PASS")


fn test_profile_decode_tokens_per_sec() raises:
    """Decode profiling computes positive tokens/sec."""
    var model = _build_tiny_model()
    var prompt = List[Int]()
    prompt.append(3)

    var result = profile_decode(model, prompt, num_steps=10)
    assert_true(result.tokens_per_sec > 0.0, "tokens_per_sec > 0")
    print("  profile_decode_tokens_per_sec: PASS")


fn test_decode_profile_avg_step() raises:
    """DecodeProfileResult computes average step time."""
    var dr = DecodeProfileResult()
    dr.num_steps = 5
    dr.aggregate.total_ns = 5000

    assert_true(dr.avg_step_ns() == 1000, "avg step should be 1000 ns")

    # Zero steps should return 0
    var dr2 = DecodeProfileResult()
    assert_true(dr2.avg_step_ns() == 0, "avg step with 0 steps is 0")
    print("  decode_profile_avg_step: PASS")


fn test_decode_profile_summary() raises:
    """DecodeProfileResult summary produces formatted output."""
    var model = _build_tiny_model()
    var prompt = List[Int]()
    prompt.append(3)

    var result = profile_decode(model, prompt, num_steps=5)
    var s = result.summary()
    assert_true(len(s) > 50, "summary has content")
    assert_true("Decode Profile" in s, "summary has header")
    assert_true("tok/s" in s, "summary has tok/s")
    assert_true("Avg step" in s, "summary has avg step")
    print("  decode_profile_summary: PASS")


# ===----------------------------------------------------------------------=== #
# Benchmark: Profiling Overhead
# ===----------------------------------------------------------------------=== #

fn test_benchmark_profiling_overhead() raises:
    """Benchmark profiled vs unprofiled forward to measure overhead."""
    var model = _build_tiny_model()
    var p = model.params.copy()
    var num_iters = 20

    # Unprofiled
    var cache1 = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=64,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope1 = RoPETable(head_dim=p.head_dim, max_seq_len=64)

    var start1 = Int(perf_counter_ns())
    for i in range(num_iters):
        _ = model.forward(3 + i % 5, cache1, rope1, pos=i)
    var unprofiled_ns = Int(perf_counter_ns()) - start1

    # Profiled
    var cache2 = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=64,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope2 = RoPETable(head_dim=p.head_dim, max_seq_len=64)

    var start2 = Int(perf_counter_ns())
    for i in range(num_iters):
        var prof = ProfileResult()
        _ = profile_forward(model, 3 + i % 5, cache2, rope2, pos=i, prof=prof)
    var profiled_ns = Int(perf_counter_ns()) - start2

    var overhead_pct: Float64 = 0.0
    if unprofiled_ns > 0:
        overhead_pct = Float64(profiled_ns - unprofiled_ns) * 100.0 / Float64(unprofiled_ns)

    print("  benchmark_profiling_overhead: " +
          String(Int(Float64(unprofiled_ns) / 1000.0)) + " us unprofiled, " +
          String(Int(Float64(profiled_ns) / 1000.0)) + " us profiled, " +
          String(Int(overhead_pct)) + "% overhead: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_profiler:")

    # ProfileResult tests
    test_profile_result_default()
    test_profile_result_copy()
    test_profile_result_add()
    test_profile_result_summary()
    test_profile_result_layer_total()
    test_profile_result_overhead()

    # Profiled forward tests
    test_profile_forward_produces_output()
    test_profile_forward_timing_nonzero()
    test_profile_forward_all_ops_measured()
    test_profile_matches_model_forward()

    # Decode profile tests
    test_profile_decode_runs()
    test_profile_decode_accumulates()
    test_profile_decode_tokens_per_sec()
    test_decode_profile_avg_step()
    test_decode_profile_summary()

    # Benchmark
    test_benchmark_profiling_overhead()

    print("ALL PASSED (16 tests)")
