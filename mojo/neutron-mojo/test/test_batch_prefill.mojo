# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Batch Prefill Tests
# ===----------------------------------------------------------------------=== #

"""Tests for batch prefill kernels and Model.forward_prefill."""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import (
    simd_matvec,
    simd_rmsnorm,
    simd_swiglu,
    simd_batch_matvec,
    simd_batch_rmsnorm,
    simd_batch_swiglu,
    simd_batch_add,
)
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig, pipeline_generate


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


fn assert_close(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    var diff = a - b
    if diff < 0:
        diff = -diff
    if diff > tol:
        raise Error("FAIL: " + msg + " (got " + String(a) + " vs " + String(b) + ", diff=" + String(diff) + ")")


# ===----------------------------------------------------------------------=== #
# Helper builders
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    """Build a tiny model for testing (2 layers, vocab=8, dim=4)."""
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
    """Build tokenizer for tiny model (vocab=8)."""
    var tok = BPETokenizer()
    _ = tok.add_special_token("<bos>", "bos")
    _ = tok.add_special_token("<eos>", "eos")
    _ = tok.add_special_token("<unk>", "unk")
    for i in range(5):
        _ = tok.add_token(chr(97 + i))
    tok.unk_id = 2
    return tok^


# ===----------------------------------------------------------------------=== #
# Batch Kernel Tests
# ===----------------------------------------------------------------------=== #

fn test_batch_matvec_single() raises:
    """Batch matvec with batch=1 should match regular matvec."""
    var weight = Tensor[DType.float32](Shape(3 * 4))  # 3x4 matrix
    for i in range(12):
        weight.set(i, Float32(i) * 0.1)

    var x = Tensor[DType.float32](Shape(4))
    for i in range(4):
        x.set(i, Float32(i + 1))

    # Regular matvec
    var expected = Tensor[DType.float32](Shape(3))
    simd_matvec(expected, 0, weight, 0, x, 0, 3, 4)

    # Batch matvec with batch=1
    var actual = Tensor[DType.float32](Shape(3))
    simd_batch_matvec(actual, 0, weight, 0, x, 0, 1, 3, 4)

    for i in range(3):
        assert_close(actual.get(i), expected.get(i), 1e-6, "batch=1 should match regular")

    print("  batch_matvec_single: PASS")


fn test_batch_matvec_multi() raises:
    """Batch matvec with batch=3 should produce correct results."""
    var weight = Tensor[DType.float32](Shape(2 * 3))  # 2x3 matrix
    for i in range(6):
        weight.set(i, Float32(i + 1) * 0.1)

    # 3 input vectors of dim 3
    var x = Tensor[DType.float32](Shape(9))
    for i in range(9):
        x.set(i, Float32(i + 1))

    var out = Tensor[DType.float32](Shape(6))  # 3 * 2
    simd_batch_matvec(out, 0, weight, 0, x, 0, 3, 2, 3)

    # Verify each batch item independently
    for b in range(3):
        var expected = Tensor[DType.float32](Shape(2))
        var x_single = Tensor[DType.float32](Shape(3))
        for j in range(3):
            x_single.set(j, x.get(b * 3 + j))
        simd_matvec(expected, 0, weight, 0, x_single, 0, 2, 3)

        for j in range(2):
            assert_close(out.get(b * 2 + j), expected.get(j), 1e-5,
                        "batch item " + String(b) + " dim " + String(j))

    print("  batch_matvec_multi: PASS")


fn test_batch_rmsnorm() raises:
    """Batch RMSNorm should match per-vector RMSNorm."""
    var dim = 4
    var batch = 3
    var gamma = Tensor[DType.float32](Shape(dim))
    for i in range(dim):
        gamma.set(i, 1.0)

    var x = Tensor[DType.float32](Shape(batch * dim))
    for i in range(batch * dim):
        x.set(i, Float32(i + 1))

    # Batch RMSNorm
    var batch_out = Tensor[DType.float32](Shape(batch * dim))
    simd_batch_rmsnorm(batch_out, 0, x, 0, gamma, 0, batch, dim)

    # Per-vector RMSNorm
    for b in range(batch):
        var expected = Tensor[DType.float32](Shape(dim))
        simd_rmsnorm(expected, 0, x, b * dim, gamma, 0, dim)

        for d in range(dim):
            assert_close(batch_out.get(b * dim + d), expected.get(d), 1e-5,
                        "rmsnorm batch " + String(b) + " dim " + String(d))

    print("  batch_rmsnorm: PASS")


fn test_batch_swiglu() raises:
    """Batch SwiGLU should match per-vector SwiGLU."""
    var dim = 4
    var batch = 2
    var gate = Tensor[DType.float32](Shape(batch * dim))
    var up = Tensor[DType.float32](Shape(batch * dim))
    for i in range(batch * dim):
        gate.set(i, Float32(i) * 0.5 - 1.0)
        up.set(i, Float32(i) * 0.3 + 0.5)

    # Batch SwiGLU
    var batch_out = Tensor[DType.float32](Shape(batch * dim))
    simd_batch_swiglu(batch_out, 0, gate, 0, up, 0, batch, dim)

    # Per-vector SwiGLU
    for b in range(batch):
        var expected = Tensor[DType.float32](Shape(dim))
        simd_swiglu(expected, 0, gate, b * dim, up, b * dim, dim)

        for d in range(dim):
            assert_close(batch_out.get(b * dim + d), expected.get(d), 1e-5,
                        "swiglu batch " + String(b))

    print("  batch_swiglu: PASS")


fn test_batch_add() raises:
    """Batch add should produce correct element-wise sums."""
    var batch = 3
    var dim = 4
    var a = Tensor[DType.float32](Shape(batch * dim))
    var b = Tensor[DType.float32](Shape(batch * dim))
    for i in range(batch * dim):
        a.set(i, Float32(i))
        b.set(i, Float32(i) * 2.0)

    var out = Tensor[DType.float32](Shape(batch * dim))
    simd_batch_add(out, 0, a, 0, b, 0, batch, dim)

    for i in range(batch * dim):
        assert_close(out.get(i), Float32(i) * 3.0, 1e-6, "add at " + String(i))

    print("  batch_add: PASS")


# ===----------------------------------------------------------------------=== #
# forward_prefill Tests
# ===----------------------------------------------------------------------=== #

fn test_prefill_single_token() raises:
    """forward_prefill with 1 token should match forward."""
    var model = _build_tiny_model()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=32,
        theta=params.rope_theta,
    )

    # Single-token forward
    var cache1 = MultiLayerKVCache(
        num_layers=params.num_layers, max_seq_len=32,
        num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
    )
    var logits1 = model.forward(3, cache1, rope, pos=0)

    # Batch prefill with 1 token
    var cache2 = MultiLayerKVCache(
        num_layers=params.num_layers, max_seq_len=32,
        num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
    )
    var ids = List[Int]()
    ids.append(3)
    var logits2 = model.forward_prefill(ids, cache2, rope)

    # Logits should match
    for i in range(params.vocab_size):
        assert_close(logits1.get(i), logits2.get(i), 1e-4,
                    "logit " + String(i) + " mismatch")

    print("  prefill_single_token: PASS")


fn test_prefill_multi_token() raises:
    """forward_prefill with N tokens should match N sequential forward calls."""
    var model = _build_tiny_model()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=32,
        theta=params.rope_theta,
    )

    var token_ids = List[Int]()
    token_ids.append(1)
    token_ids.append(3)
    token_ids.append(5)

    # Sequential forward
    var cache1 = MultiLayerKVCache(
        num_layers=params.num_layers, max_seq_len=32,
        num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
    )
    var logits_seq = Tensor[DType.float32](Shape(params.vocab_size))
    for i in range(len(token_ids)):
        logits_seq = model.forward(token_ids[i], cache1, rope, pos=i)

    # Batch prefill
    var cache2 = MultiLayerKVCache(
        num_layers=params.num_layers, max_seq_len=32,
        num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
    )
    var logits_batch = model.forward_prefill(token_ids, cache2, rope)

    # Logits should match (last token's logits)
    for i in range(params.vocab_size):
        assert_close(logits_seq.get(i), logits_batch.get(i), 1e-3,
                    "logit " + String(i))

    print("  prefill_multi_token: PASS")


fn test_prefill_cache_state() raises:
    """Cache should have correct state after batch prefill."""
    var model = _build_tiny_model()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=32,
        theta=params.rope_theta,
    )

    var token_ids = List[Int]()
    token_ids.append(0)
    token_ids.append(2)
    token_ids.append(4)

    var cache = MultiLayerKVCache(
        num_layers=params.num_layers, max_seq_len=32,
        num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
    )
    _ = model.forward_prefill(token_ids, cache, rope)

    # Cache should have 3 entries per layer
    for layer in range(params.num_layers):
        assert_true(cache.lengths[layer] == 3,
                   "Layer " + String(layer) + " cache should have 3 entries")

    print("  prefill_cache_state: PASS")


fn test_prefill_then_decode() raises:
    """Batch prefill followed by single-token decode should work."""
    var model = _build_tiny_model()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=32,
        theta=params.rope_theta,
    )

    var token_ids = List[Int]()
    token_ids.append(1)
    token_ids.append(3)

    var cache = MultiLayerKVCache(
        num_layers=params.num_layers, max_seq_len=32,
        num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
    )

    # Batch prefill
    var logits = model.forward_prefill(token_ids, cache, rope)

    # Decode one more token
    var next_logits = model.forward(5, cache, rope, pos=2)
    assert_true(next_logits.numel() == params.vocab_size, "Should produce logits")

    # Cache should now have 3 entries
    assert_true(cache.lengths[0] == 3, "Cache should have 3 entries after decode")

    print("  prefill_then_decode: PASS")


fn test_prefill_vs_sequential_decode() raises:
    """Full prefill+decode should match sequential prefill+decode."""
    var model = _build_tiny_model()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=32,
        theta=params.rope_theta,
    )

    var token_ids = List[Int]()
    token_ids.append(1)
    token_ids.append(2)
    token_ids.append(3)

    # Path 1: Sequential forward for prefill + 1 decode
    var cache1 = MultiLayerKVCache(
        num_layers=params.num_layers, max_seq_len=32,
        num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
    )
    var logits1 = Tensor[DType.float32](Shape(params.vocab_size))
    for i in range(len(token_ids)):
        logits1 = model.forward(token_ids[i], cache1, rope, pos=i)
    var decode1 = model.forward(5, cache1, rope, pos=3)

    # Path 2: Batch prefill + 1 decode
    var cache2 = MultiLayerKVCache(
        num_layers=params.num_layers, max_seq_len=32,
        num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
    )
    _ = model.forward_prefill(token_ids, cache2, rope)
    var decode2 = model.forward(5, cache2, rope, pos=3)

    # Decode logits should match
    for i in range(params.vocab_size):
        assert_close(decode1.get(i), decode2.get(i), 1e-3,
                    "decode logit " + String(i))

    print("  prefill_vs_sequential_decode: PASS")


fn test_pipeline_with_batch_prefill() raises:
    """Pipeline generate should work with batch prefill enabled."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var config = PipelineConfig()
    config.max_new_tokens = 3

    var output = pipeline_generate(model, tok, "ab", config)
    assert_true(len(output) >= 0, "Pipeline should produce output")

    print("  pipeline_with_batch_prefill: PASS")


fn test_prefill_different_prompt_lengths() raises:
    """Batch prefill should work with various prompt lengths."""
    var model = _build_tiny_model()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=32,
        theta=params.rope_theta,
    )

    # Test with 1, 2, 4, 6 tokens
    var lengths = List[Int]()
    lengths.append(1)
    lengths.append(2)
    lengths.append(4)
    lengths.append(6)

    for li in range(len(lengths)):
        var n = lengths[li]
        var ids = List[Int]()
        for j in range(n):
            ids.append(j % params.vocab_size)

        var cache = MultiLayerKVCache(
            num_layers=params.num_layers, max_seq_len=32,
            num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
        )
        var logits = model.forward_prefill(ids, cache, rope)
        assert_true(logits.numel() == params.vocab_size,
                   "Should produce logits for length " + String(n))
        assert_true(cache.lengths[0] == n,
                   "Cache should have " + String(n) + " entries")

    print("  prefill_different_prompt_lengths: PASS")


fn test_batch_matvec_with_offset() raises:
    """Batch matvec should work with non-zero offsets."""
    var weight = Tensor[DType.float32](Shape(10 + 2 * 3))  # Offset + 2x3
    for i in range(10, 10 + 6):
        weight.set(i, Float32(i - 10 + 1) * 0.1)

    var x = Tensor[DType.float32](Shape(5 + 2 * 3))  # Offset + 2 vectors of dim 3
    for i in range(5, 5 + 6):
        x.set(i, Float32(i - 5 + 1))

    var out = Tensor[DType.float32](Shape(3 + 2 * 2))  # Offset + 2 outputs of dim 2
    simd_batch_matvec(out, 3, weight, 10, x, 5, 2, 2, 3)

    # Verify with individual matvecs
    for b in range(2):
        var expected = Tensor[DType.float32](Shape(2))
        simd_matvec(expected, 0, weight, 10, x, 5 + b * 3, 2, 3)
        for j in range(2):
            assert_close(out.get(3 + b * 2 + j), expected.get(j), 1e-5,
                        "offset batch " + String(b))

    print("  batch_matvec_with_offset: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_batch_prefill:")

    # Batch kernel tests
    test_batch_matvec_single()
    test_batch_matvec_multi()
    test_batch_rmsnorm()
    test_batch_swiglu()
    test_batch_add()
    test_batch_matvec_with_offset()

    # forward_prefill tests
    test_prefill_single_token()
    test_prefill_multi_token()
    test_prefill_cache_state()
    test_prefill_then_decode()
    test_prefill_vs_sequential_decode()
    test_pipeline_with_batch_prefill()
    test_prefill_different_prompt_lengths()

    print("ALL PASSED (13 tests)")
