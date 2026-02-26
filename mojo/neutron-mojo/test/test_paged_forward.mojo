# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Paged Forward Pass Tests
# ===----------------------------------------------------------------------=== #

"""Tests for paged forward pass: FP32, Q8, Q4 models with PagedKVCache."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model, q_generate
from neutron_mojo.nn.q4_model import Q4Model, quantize_from_model_q4, q4_generate
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.paged_kv_cache import PagedKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.paged_forward import (
    paged_forward,
    paged_generate,
    paged_q8_forward,
    paged_q8_generate,
    paged_q4_forward,
    paged_q4_generate,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn _build_model() -> Model:
    """Build tiny model with non-trivial weights."""
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
# FP32 Paged Forward Tests
# ===----------------------------------------------------------------------=== #

fn test_paged_forward_produces_logits() raises:
    """Test that paged forward produces valid logits."""
    var model = _build_model()
    var p = model.params.copy()

    var cache = PagedKVCache(
        max_pages=64, page_size=4,
        num_layers=p.num_layers,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=16)

    var logits = paged_forward(model, 1, cache, rope, pos=0)
    assert_true(logits.numel() == p.vocab_size, "logits size = vocab_size")

    # Cache should have 1 entry per layer
    for layer in range(p.num_layers):
        assert_true(cache.seq_len(layer) == 1, "cache has 1 entry")

    print("  paged_forward_produces_logits: PASS")


fn test_paged_forward_matches_contiguous() raises:
    """Verify paged forward matches contiguous forward."""
    var model = _build_model()
    var p = model.params.copy()

    # Contiguous path
    var cache_c = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=16,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope_c = RoPETable(head_dim=p.head_dim, max_seq_len=16)
    var logits_c = model.forward(1, cache_c, rope_c, pos=0)
    logits_c = model.forward(3, cache_c, rope_c, pos=1)

    # Paged path
    var cache_p = PagedKVCache(
        max_pages=64, page_size=4,
        num_layers=p.num_layers,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope_p = RoPETable(head_dim=p.head_dim, max_seq_len=16)
    var logits_p = paged_forward(model, 1, cache_p, rope_p, pos=0)
    logits_p = paged_forward(model, 3, cache_p, rope_p, pos=1)

    # Logits should match within tolerance
    for i in range(p.vocab_size):
        assert_near(logits_p.get(i), logits_c.get(i), 1e-4,
            "logit[" + String(i) + "]")

    print("  paged_forward_matches_contiguous: PASS")


fn test_paged_generate() raises:
    """Test paged generation produces valid tokens."""
    var model = _build_model()
    var p = model.params.copy()

    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)

    var tokens = paged_generate(model, prompt, max_new_tokens=3, max_pages=64, page_size=4)
    assert_true(len(tokens) == 3, "generated 3 tokens")
    for i in range(len(tokens)):
        assert_true(tokens[i] >= 0 and tokens[i] < p.vocab_size, "valid token")

    print("  paged_generate: PASS")


fn test_paged_vs_contiguous_generation() raises:
    """Verify paged generation matches contiguous generation."""
    var model = _build_model()

    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)

    var contiguous_tokens = generate(model, prompt, max_new_tokens=3)
    var paged_tokens = paged_generate(model, prompt, max_new_tokens=3, max_pages=64, page_size=4)

    assert_true(len(contiguous_tokens) == 3, "contiguous generated 3")
    assert_true(len(paged_tokens) == 3, "paged generated 3")

    for i in range(3):
        assert_true(contiguous_tokens[i] == paged_tokens[i],
            "token[" + String(i) + "] matches: c=" + String(contiguous_tokens[i]) +
            " p=" + String(paged_tokens[i]))

    print("  paged_vs_contiguous_generation: PASS")


# ===----------------------------------------------------------------------=== #
# Q8 Paged Forward Tests
# ===----------------------------------------------------------------------=== #

fn test_paged_q8_forward() raises:
    """Test Q8 paged forward produces valid logits."""
    var model = _build_model()
    var p = model.params.copy()
    var qm = quantize_from_model(model, block_size=2)

    var cache = PagedKVCache(
        max_pages=64, page_size=4,
        num_layers=p.num_layers,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=16)

    var logits = paged_q8_forward(qm, 1, cache, rope, pos=0)
    assert_true(logits.numel() == p.vocab_size, "q8 logits size")

    print("  paged_q8_forward: PASS")


fn test_paged_q8_generate() raises:
    """Test Q8 paged generation."""
    var model = _build_model()
    var p = model.params.copy()
    var qm = quantize_from_model(model, block_size=2)

    var prompt = List[Int]()
    prompt.append(1)

    var tokens = paged_q8_generate(qm, prompt, max_new_tokens=2, max_pages=64, page_size=4)
    assert_true(len(tokens) == 2, "q8 generated 2 tokens")
    for i in range(len(tokens)):
        assert_true(tokens[i] >= 0 and tokens[i] < p.vocab_size, "q8 valid token")

    print("  paged_q8_generate: PASS")


fn test_paged_q8_matches_contiguous() raises:
    """Verify Q8 paged generation matches contiguous Q8 generation."""
    var model = _build_model()
    var qm = quantize_from_model(model, block_size=2)

    var prompt = List[Int]()
    prompt.append(2)
    prompt.append(4)

    var contiguous = q_generate(qm, prompt, max_new_tokens=3)
    var paged = paged_q8_generate(qm, prompt, max_new_tokens=3, max_pages=64, page_size=4)

    assert_true(len(contiguous) == 3 and len(paged) == 3, "both generated 3")
    for i in range(3):
        assert_true(contiguous[i] == paged[i],
            "q8 token[" + String(i) + "] matches")

    print("  paged_q8_matches_contiguous: PASS")


# ===----------------------------------------------------------------------=== #
# Q4 Paged Forward Tests
# ===----------------------------------------------------------------------=== #

fn test_paged_q4_forward() raises:
    """Test Q4 paged forward produces valid logits."""
    var model = _build_model()
    var p = model.params.copy()
    var q4m = quantize_from_model_q4(model, block_size=2)

    var cache = PagedKVCache(
        max_pages=64, page_size=4,
        num_layers=p.num_layers,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=16)

    var logits = paged_q4_forward(q4m, 1, cache, rope, pos=0)
    assert_true(logits.numel() == p.vocab_size, "q4 logits size")

    print("  paged_q4_forward: PASS")


fn test_paged_q4_generate() raises:
    """Test Q4 paged generation."""
    var model = _build_model()
    var p = model.params.copy()
    var q4m = quantize_from_model_q4(model, block_size=2)

    var prompt = List[Int]()
    prompt.append(1)

    var tokens = paged_q4_generate(q4m, prompt, max_new_tokens=2, max_pages=64, page_size=4)
    assert_true(len(tokens) == 2, "q4 generated 2 tokens")
    for i in range(len(tokens)):
        assert_true(tokens[i] >= 0 and tokens[i] < p.vocab_size, "q4 valid token")

    print("  paged_q4_generate: PASS")


fn test_paged_q4_matches_contiguous() raises:
    """Verify Q4 paged generation matches contiguous Q4 generation."""
    var model = _build_model()
    var q4m = quantize_from_model_q4(model, block_size=2)

    var prompt = List[Int]()
    prompt.append(3)
    prompt.append(5)

    var contiguous = q4_generate(q4m, prompt, max_new_tokens=3)
    var paged = paged_q4_generate(q4m, prompt, max_new_tokens=3, max_pages=64, page_size=4)

    assert_true(len(contiguous) == 3 and len(paged) == 3, "both generated 3")
    for i in range(3):
        assert_true(contiguous[i] == paged[i],
            "q4 token[" + String(i) + "] matches")

    print("  paged_q4_matches_contiguous: PASS")


# ===----------------------------------------------------------------------=== #
# Memory Efficiency Test
# ===----------------------------------------------------------------------=== #

fn test_paged_memory_efficiency() raises:
    """Verify paged cache uses less memory for short sequences."""
    var p = tiny_test_params()
    var max_seq = 64

    # Contiguous: pre-allocates full max_seq_len
    var contiguous_bytes = max_seq * p.num_kv_heads * p.head_dim * 4 * 2 * p.num_layers

    # Paged: only allocates pages on demand
    var page_size = 4
    var cache_p = PagedKVCache(
        max_pages=32, page_size=page_size,
        num_layers=p.num_layers,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )

    # After 5 tokens, paged uses ceil(5/4) = 2 pages per layer
    assert_true(cache_p.total_pages_used() == 0, "no pages used initially")

    var model = _build_model()
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=max_seq)
    for i in range(5):
        _ = paged_forward(model, i % p.vocab_size, cache_p, rope, pos=i)

    var pages_used = cache_p.total_pages_used()
    var paged_bytes = cache_p.used_memory_bytes()

    assert_true(pages_used == 4, "2 pages/layer * 2 layers = 4 pages")
    assert_true(paged_bytes < contiguous_bytes,
        "paged (" + String(paged_bytes) + " B) < contiguous (" + String(contiguous_bytes) + " B)")

    var savings = Float32(1.0) - Float32(paged_bytes) / Float32(contiguous_bytes)
    print("  Memory: contiguous=" + String(contiguous_bytes) + "B, paged=" + String(paged_bytes) + "B")
    print("  Savings: " + String(Int(savings * 100.0)) + "%")

    print("  paged_memory_efficiency: PASS")


fn main() raises:
    print("test_paged_forward:")

    # FP32
    test_paged_forward_produces_logits()
    test_paged_forward_matches_contiguous()
    test_paged_generate()
    test_paged_vs_contiguous_generation()

    # Q8
    test_paged_q8_forward()
    test_paged_q8_generate()
    test_paged_q8_matches_contiguous()

    # Q4
    test_paged_q4_forward()
    test_paged_q4_generate()
    test_paged_q4_matches_contiguous()

    # Memory
    test_paged_memory_efficiency()

    print("ALL PASSED")
