# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Benchmark and Memory Tests
# ===----------------------------------------------------------------------=== #

"""Tests for memory estimation, model info, and benchmark harness."""

from neutron_mojo.nn.bench import (
    MemoryEstimate,
    estimate_memory,
    ModelInfo,
    model_info,
    BenchmarkResult,
    benchmark_inference,
    benchmark_prefill_comparison,
)
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


# ===----------------------------------------------------------------------=== #
# Helpers
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    var params = tiny_test_params()
    var model = Model(params)
    var total = model.layer_weights.numel()
    for i in range(total):
        model.layer_weights.set(i, Float32(0.01) * Float32(i % 7 - 3))
    var embed_total = model.embed.numel()
    for i in range(embed_total):
        model.embed.set(i, Float32(0.01) * Float32(i % 5 - 2))
    for i in range(model.final_norm.numel()):
        model.final_norm.set(i, 1.0)
    var lm_total = model.lm_head.numel()
    for i in range(lm_total):
        model.lm_head.set(i, Float32(0.01) * Float32(i % 11 - 5))
    return model^


fn _build_tiny_tokenizer() -> BPETokenizer:
    var tok = BPETokenizer()
    _ = tok.add_special_token("<bos>", "bos")
    _ = tok.add_special_token("<eos>", "eos")
    _ = tok.add_special_token("<unk>", "unk")
    for i in range(5):
        _ = tok.add_token(chr(97 + i))
    tok.unk_id = 2
    return tok^


# ===----------------------------------------------------------------------=== #
# Memory Estimation Tests
# ===----------------------------------------------------------------------=== #

fn test_memory_estimate_creation() raises:
    """Test MemoryEstimate default construction."""
    var est = MemoryEstimate()
    assert_true(est.total_bytes == 0, "Default total should be 0")
    assert_true(est.total_mb() == 0.0, "Default MB should be 0")
    print("  memory_estimate_creation: PASS")


fn test_estimate_memory_tiny() raises:
    """Test memory estimation for tiny model."""
    var params = tiny_test_params()
    var est = estimate_memory(params)

    assert_true(est.embed_bytes > 0, "Embed bytes should be > 0")
    assert_true(est.layer_weights_bytes > 0, "Layer weights should be > 0")
    assert_true(est.lm_head_bytes > 0, "LM head should be > 0")
    assert_true(est.kv_cache_bytes > 0, "KV cache should be > 0")
    assert_true(est.model_params_bytes > 0, "Model params should be > 0")
    assert_true(est.total_bytes > 0, "Total should be > 0")

    # Verify model params = embed + layers + lm_head
    assert_true(
        est.model_params_bytes == est.embed_bytes + est.layer_weights_bytes + est.lm_head_bytes,
        "Model params should sum correctly",
    )

    print("  estimate_memory_tiny: PASS")


fn test_estimate_memory_batch() raises:
    """Test that batch size scales KV cache memory."""
    var params = tiny_test_params()
    var est1 = estimate_memory(params, batch_size=1)
    var est4 = estimate_memory(params, batch_size=4)

    # KV cache should scale with batch size
    assert_true(est4.kv_cache_bytes == 4 * est1.kv_cache_bytes,
               "KV cache should scale 4x with batch_size=4")

    # Model params should be same
    assert_true(est4.model_params_bytes == est1.model_params_bytes,
               "Model params should not change with batch size")

    print("  estimate_memory_batch: PASS")


fn test_estimate_memory_seq_len() raises:
    """Test that sequence length scales KV cache memory."""
    var params = tiny_test_params()
    var est_256 = estimate_memory(params, seq_len=256)
    var est_512 = estimate_memory(params, seq_len=512)

    assert_true(est_512.kv_cache_bytes == 2 * est_256.kv_cache_bytes,
               "KV cache should scale 2x with double seq_len")

    print("  estimate_memory_seq_len: PASS")


fn test_estimate_memory_q8() raises:
    """Test Q8 (1 byte per param) memory estimation."""
    var params = tiny_test_params()
    var est_fp32 = estimate_memory(params, bytes_per_param=4)
    var est_q8 = estimate_memory(params, bytes_per_param=1)

    # Q8 model params should be 1/4 of FP32
    assert_true(est_q8.model_params_bytes == est_fp32.model_params_bytes // 4,
               "Q8 should use 1/4 model memory")

    print("  estimate_memory_q8: PASS")


fn test_estimate_memory_realistic() raises:
    """Test memory estimation for a realistic-sized model config."""
    var params = ModelParams()  # Default: 32-layer, 4096 hidden
    var est = estimate_memory(params, batch_size=1, seq_len=2048)

    # Verify reasonable values
    # Embed: 32000 * 4096 * 4 = 524MB
    # Layers: 32 * layer_weight_count * 4 bytes
    # Total should be in the GB range
    assert_true(est.total_mb() > 100.0, "Realistic model should use > 100 MB")
    assert_true(est.model_mb() > 100.0, "Model params should use > 100 MB")

    print("  estimate_memory_realistic: PASS")


# ===----------------------------------------------------------------------=== #
# Model Info Tests
# ===----------------------------------------------------------------------=== #

fn test_model_info_tiny() raises:
    """Test model info for tiny model."""
    var params = tiny_test_params()
    var info = model_info(params)

    assert_true(info.num_layers == 2, "Should have 2 layers")
    assert_true(info.vocab_size == 8, "Vocab should be 8")
    assert_true(info.hidden_dim == 4, "Hidden dim should be 4")
    assert_true(info.num_q_heads == 2, "Q heads should be 2")
    assert_true(info.num_kv_heads == 1, "KV heads should be 1")
    assert_true(info.is_gqa(), "Should be GQA (1 KV < 2 Q)")
    assert_true(info.gqa_ratio() == 2, "GQA ratio should be 2")
    assert_true(info.total_params > 0, "Should have params")
    assert_true(info.total_params_millions > 0.0, "Should have > 0M params")

    print("  model_info_tiny: PASS")


fn test_model_info_summary() raises:
    """Test model info summary string."""
    var params = tiny_test_params()
    var info = model_info(params)
    var s = info.summary()

    assert_true(len(s) > 50, "Summary should be non-trivial")
    # Check key content is present
    assert_true(s.find("Layers: 2") >= 0, "Should mention 2 layers")
    assert_true(s.find("GQA") >= 0, "Should mention GQA")

    print("  model_info_summary: PASS")


fn test_model_info_non_gqa() raises:
    """Test model info for non-GQA model (MHA)."""
    var params = ModelParams()
    params.num_q_heads = 8
    params.num_kv_heads = 8  # MHA: same Q and KV heads
    var info = model_info(params)

    assert_true(not info.is_gqa(), "Should not be GQA when heads match")
    assert_true(info.gqa_ratio() == 1, "GQA ratio should be 1 for MHA")

    print("  model_info_non_gqa: PASS")


# ===----------------------------------------------------------------------=== #
# Benchmark Tests
# ===----------------------------------------------------------------------=== #

fn test_benchmark_result_creation() raises:
    """Test BenchmarkResult default values."""
    var r = BenchmarkResult()
    assert_true(r.prefill_tokens == 0, "Default prefill should be 0")
    assert_true(r.decode_tokens == 0, "Default decode should be 0")
    assert_true(r.prefill_tokens_per_sec == 0.0, "Default tps should be 0")
    print("  benchmark_result_creation: PASS")


fn test_benchmark_inference_batch() raises:
    """Test benchmark with batch prefill."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var result = benchmark_inference(model, tok, "ab", max_tokens=3, use_batch_prefill=True)

    assert_true(result.prefill_tokens > 0, "Should have prefill tokens")
    assert_true(result.prefill_ns > 0, "Prefill should take > 0 ns")
    assert_true(result.decode_ns > 0, "Decode should take > 0 ns")
    assert_true(result.total_ns > 0, "Total should take > 0 ns")
    assert_true(result.prefill_tokens_per_sec > 0.0, "Prefill tps should be > 0")

    print("  benchmark_inference_batch: PASS")


fn test_benchmark_inference_sequential() raises:
    """Test benchmark with sequential prefill."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var result = benchmark_inference(model, tok, "ab", max_tokens=3, use_batch_prefill=False)

    assert_true(result.prefill_tokens > 0, "Should have prefill tokens")
    assert_true(result.prefill_ns > 0, "Prefill should take > 0 ns")
    assert_true(result.total_ns > 0, "Total should take > 0 ns")

    print("  benchmark_inference_sequential: PASS")


fn test_benchmark_result_summary() raises:
    """Test benchmark result summary formatting."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var result = benchmark_inference(model, tok, "ab", max_tokens=2)
    var s = result.summary()

    assert_true(len(s) > 50, "Summary should be non-trivial")
    assert_true(s.find("Prefill") >= 0, "Should mention Prefill")
    assert_true(s.find("Decode") >= 0, "Should mention Decode")
    assert_true(s.find("tok/s") >= 0, "Should show tok/s")

    print("  benchmark_result_summary: PASS")


fn test_benchmark_comparison() raises:
    """Test prefill comparison utility."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var s = benchmark_prefill_comparison(model, tok, "ab")

    assert_true(len(s) > 50, "Comparison should be non-trivial")
    assert_true(s.find("Sequential") >= 0, "Should mention Sequential")
    assert_true(s.find("Batch") >= 0, "Should mention Batch")
    assert_true(s.find("Speedup") >= 0, "Should mention Speedup")

    print("  benchmark_comparison: PASS")


fn test_memory_estimate_copy() raises:
    """Test MemoryEstimate copy semantics."""
    var est1 = estimate_memory(tiny_test_params())
    var est2 = est1.copy()

    assert_true(est2.total_bytes == est1.total_bytes, "Copy should match")
    assert_true(est2.model_params_bytes == est1.model_params_bytes, "Copy model params should match")

    print("  memory_estimate_copy: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_bench:")

    # Memory estimation
    test_memory_estimate_creation()
    test_estimate_memory_tiny()
    test_estimate_memory_batch()
    test_estimate_memory_seq_len()
    test_estimate_memory_q8()
    test_estimate_memory_realistic()
    test_memory_estimate_copy()

    # Model info
    test_model_info_tiny()
    test_model_info_summary()
    test_model_info_non_gqa()

    # Benchmark
    test_benchmark_result_creation()
    test_benchmark_inference_batch()
    test_benchmark_inference_sequential()
    test_benchmark_result_summary()
    test_benchmark_comparison()

    print("ALL PASSED (16 tests)")
