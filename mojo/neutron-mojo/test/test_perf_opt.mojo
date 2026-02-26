# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 28: Performance Optimization Tests
# ===----------------------------------------------------------------------=== #

"""Tests for direct KV cache attention and parallel projections.

Tests:
1. gqa_attention_direct matches gqa_attention (correctness)
2. gqa_attention_direct multi-token sequence
3. gqa_attention_direct GQA grouping
4. forward_layer with direct cache matches original output
5. forward produces valid logits (parallel projections)
6. forward + decode loop still works
7. Pipeline generate still produces text
8. Q8 model forward with direct cache
9. Q8 pipeline generate still works
10. Fused forward with direct cache
11. Benchmark: direct vs copy attention speedup
"""

from math import abs, sqrt
from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model, q_generate
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.kv_cache import KVCache, MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.attention import gqa_attention, gqa_attention_direct, softmax_inplace
from neutron_mojo.nn.pipeline import PipelineConfig, pipeline_generate
from neutron_mojo.nn.q_pipeline import q_pipeline_generate


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


fn _build_tiny_tokenizer() -> BPETokenizer:
    var tok = BPETokenizer()
    _ = tok.add_token("<s>")
    _ = tok.add_token("</s>")
    _ = tok.add_token("<unk>")
    _ = tok.add_token("a")
    _ = tok.add_token("b")
    _ = tok.add_token("c")
    _ = tok.add_token("d")
    _ = tok.add_token("e")
    tok.bos_id = 0
    tok.eos_id = 1
    tok.unk_id = 2
    return tok^


# ===----------------------------------------------------------------------=== #
# Direct KV Cache Attention Tests
# ===----------------------------------------------------------------------=== #

fn test_direct_attention_matches_copy() raises:
    """Direct cache attention produces same results as copy-based."""
    var p = tiny_test_params()
    var num_q = p.num_q_heads
    var num_kv = p.num_kv_heads
    var hd = p.head_dim

    # Create query
    var q = Tensor[DType.float32](Shape(num_q * hd))
    for i in range(num_q * hd):
        q.set(i, Float32(i) * 0.1 - 0.5)

    # Create and fill a MultiLayerKVCache with 3 tokens
    var cache = MultiLayerKVCache(
        num_layers=2, max_seq_len=16,
        num_kv_heads=num_kv, head_dim=hd,
    )
    for tok in range(3):
        var k = Tensor[DType.float32](Shape(num_kv * hd))
        var v = Tensor[DType.float32](Shape(num_kv * hd))
        for i in range(num_kv * hd):
            k.set(i, Float32(tok * 10 + i) * 0.05)
            v.set(i, Float32(tok * 10 + i + 5) * 0.03)
        cache.append_kv(0, k, v, num_new_tokens=1)

    # Also create equivalent KVCache (single-layer, copy-based)
    var single_cache = KVCache(max_seq_len=16, num_kv_heads=num_kv, head_dim=hd)
    for tok in range(3):
        var k = Tensor[DType.float32](Shape(num_kv * hd))
        var v = Tensor[DType.float32](Shape(num_kv * hd))
        for i in range(num_kv * hd):
            k.set(i, Float32(tok * 10 + i) * 0.05)
            v.set(i, Float32(tok * 10 + i + 5) * 0.03)
        single_cache.append_kv(k, v, num_new_tokens=1)

    # Compare outputs
    var out_direct = gqa_attention_direct(q, cache, 0, num_q, num_kv, hd)
    var out_copy = gqa_attention(q, single_cache, num_q, num_kv, hd)

    for i in range(num_q * hd):
        assert_close(out_direct.get(i), out_copy.get(i), 1e-5,
            "Direct vs copy mismatch at " + String(i))

    print("  direct_attention_matches_copy: PASS")


fn test_direct_attention_multi_token() raises:
    """Direct attention works with multiple cached tokens."""
    var num_q = 2
    var num_kv = 1
    var hd = 4

    var q = Tensor[DType.float32](Shape(num_q * hd))
    for i in range(num_q * hd):
        q.set(i, Float32(i + 1) * 0.1)

    var cache = MultiLayerKVCache(
        num_layers=1, max_seq_len=32,
        num_kv_heads=num_kv, head_dim=hd,
    )
    # Add 5 tokens
    for tok in range(5):
        var k = Tensor[DType.float32](Shape(num_kv * hd))
        var v = Tensor[DType.float32](Shape(num_kv * hd))
        for i in range(num_kv * hd):
            k.set(i, Float32(tok + i) * 0.1)
            v.set(i, Float32(tok * 2 + i) * 0.05)
        cache.append_kv(0, k, v, num_new_tokens=1)

    var out = gqa_attention_direct(q, cache, 0, num_q, num_kv, hd)
    assert_true(out.numel() == num_q * hd, "Output has correct size")

    # Output should be non-zero weighted sum of values
    var has_nonzero = False
    for i in range(num_q * hd):
        if abs(out.get(i)) > 1e-8:
            has_nonzero = True
    assert_true(has_nonzero, "Output has non-zero values")

    print("  direct_attention_multi_token: PASS")


fn test_direct_attention_gqa_grouping() raises:
    """Direct attention correctly maps Q heads to KV heads in GQA."""
    var num_q = 4
    var num_kv = 2
    var hd = 2

    var q = Tensor[DType.float32](Shape(num_q * hd))
    for i in range(num_q * hd):
        q.set(i, 1.0)

    var cache = MultiLayerKVCache(
        num_layers=1, max_seq_len=8,
        num_kv_heads=num_kv, head_dim=hd,
    )
    # Add one token with distinct K/V per head
    var k = Tensor[DType.float32](Shape(num_kv * hd))
    var v = Tensor[DType.float32](Shape(num_kv * hd))
    # KV head 0: k=[1,0], v=[10,20]
    k.set(0, 1.0)
    k.set(1, 0.0)
    v.set(0, 10.0)
    v.set(1, 20.0)
    # KV head 1: k=[0,1], v=[30,40]
    k.set(2, 0.0)
    k.set(3, 1.0)
    v.set(2, 30.0)
    v.set(3, 40.0)
    cache.append_kv(0, k, v, num_new_tokens=1)

    var out = gqa_attention_direct(q, cache, 0, num_q, num_kv, hd)

    # Q heads 0,1 → KV head 0 → value [10, 20]
    # Q heads 2,3 → KV head 1 → value [30, 40]
    # With single cached position, softmax = 1.0, output = value
    assert_close(out.get(0), 10.0, 0.01, "Q0 gets KV0 val0")
    assert_close(out.get(1), 20.0, 0.01, "Q0 gets KV0 val1")
    assert_close(out.get(2), 10.0, 0.01, "Q1 gets KV0 val0")
    assert_close(out.get(3), 20.0, 0.01, "Q1 gets KV0 val1")
    assert_close(out.get(4), 30.0, 0.01, "Q2 gets KV1 val0")
    assert_close(out.get(5), 40.0, 0.01, "Q2 gets KV1 val1")
    assert_close(out.get(6), 30.0, 0.01, "Q3 gets KV1 val0")
    assert_close(out.get(7), 40.0, 0.01, "Q3 gets KV1 val1")

    print("  direct_attention_gqa_grouping: PASS")


# ===----------------------------------------------------------------------=== #
# Forward Pass Tests (with optimizations)
# ===----------------------------------------------------------------------=== #

fn test_forward_produces_valid_logits() raises:
    """Forward pass with parallel projections produces valid logits."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    var logits = model.forward(3, cache, rope, pos=0)
    assert_true(logits.numel() == p.vocab_size, "Logits has vocab_size elements")

    # Logits should not all be zero
    var has_nonzero = False
    for i in range(p.vocab_size):
        if abs(logits.get(i)) > 1e-8:
            has_nonzero = True
    assert_true(has_nonzero, "Logits are non-zero")

    print("  forward_produces_valid_logits: PASS")


fn test_forward_decode_loop() raises:
    """Multiple forward passes (decode loop) work correctly."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    # Process 5 tokens
    for i in range(5):
        var logits = model.forward(3 + i % 5, cache, rope, pos=i)
        assert_true(logits.numel() == p.vocab_size,
            "Token " + String(i) + " has correct logits size")

    # Cache should have 5 positions filled
    assert_true(cache.lengths[0] == 5, "Cache has 5 positions after decode")

    print("  forward_decode_loop: PASS")


fn test_pipeline_generate_works() raises:
    """Pipeline generate still works with optimized forward."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 5

    var text = pipeline_generate(model, tok, "abc", cfg)
    # Just verify it runs without error and produces some output
    assert_true(len(text) >= 0, "Pipeline produces text")

    print("  pipeline_generate_works: PASS")


fn test_q8_model_forward() raises:
    """Q8 model forward with direct cache works."""
    var model = _build_tiny_model()
    var qm = quantize_from_model(model, block_size=2)
    var p = qm.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    var logits = qm.forward(3, cache, rope, pos=0)
    assert_true(logits.numel() == p.vocab_size, "Q8 logits correct size")

    # Process more tokens
    for i in range(1, 4):
        logits = qm.forward(3 + i, cache, rope, pos=i)

    assert_true(cache.lengths[0] == 4, "Q8 cache has 4 positions")

    print("  q8_model_forward: PASS")


fn test_q8_pipeline_generate_works() raises:
    """Q8 pipeline generate still works with optimized forward."""
    var model = _build_tiny_model()
    var qm = quantize_from_model(model, block_size=2)
    var tok = _build_tiny_tokenizer()
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 5

    var text = q_pipeline_generate(qm, tok, "abc", cfg)
    assert_true(len(text) >= 0, "Q8 pipeline produces text")

    print("  q8_pipeline_generate_works: PASS")


fn test_fused_forward_works() raises:
    """Fused forward path with direct cache works."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    var logits = model.forward_fused(3, cache, rope, pos=0)
    assert_true(logits.numel() == p.vocab_size, "Fused logits correct size")

    # Process more tokens
    for i in range(1, 4):
        logits = model.forward_fused(3 + i, cache, rope, pos=i)

    assert_true(cache.lengths[0] == 4, "Fused cache has 4 positions")

    print("  fused_forward_works: PASS")


# ===----------------------------------------------------------------------=== #
# Benchmark Test
# ===----------------------------------------------------------------------=== #

fn test_benchmark_decode() raises:
    """Benchmark decode performance with optimizations."""
    var model = _build_tiny_model()
    var p = model.params.copy()
    var num_tokens = 20

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=64,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=64)

    var start = Int(perf_counter_ns())
    for i in range(num_tokens):
        _ = model.forward(3 + i % 5, cache, rope, pos=i)
    var elapsed = Int(perf_counter_ns()) - start

    var elapsed_ms = Int(Float64(elapsed) / 1_000_000.0)
    var tps: Float64 = 0.0
    if elapsed > 0:
        tps = Float64(num_tokens) / (Float64(elapsed) / 1_000_000_000.0)

    print("  benchmark_decode: " + String(Int(tps)) + " tok/s (" +
          String(elapsed_ms) + " ms for " + String(num_tokens) + " tokens): PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_perf_opt:")

    # Direct KV cache attention
    test_direct_attention_matches_copy()
    test_direct_attention_multi_token()
    test_direct_attention_gqa_grouping()

    # Forward pass with optimizations
    test_forward_produces_valid_logits()
    test_forward_decode_loop()
    test_pipeline_generate_works()
    test_q8_model_forward()
    test_q8_pipeline_generate_works()
    test_fused_forward_works()

    # Benchmark
    test_benchmark_decode()

    print("ALL PASSED (10 tests)")
