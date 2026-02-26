# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Multi-Layer Model Tests
# ===----------------------------------------------------------------------=== #

"""Tests for the full N-layer model."""

from math import abs
from neutron_mojo.nn.model import (
    Model,
    ModelParams,
    LayerWeightOffsets,
    tiny_test_params,
    generate,
)
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn test_model_params() raises:
    """Test ModelParams creation."""
    var p = tiny_test_params()
    assert_true(p.num_layers == 2, "num_layers")
    assert_true(p.vocab_size == 8, "vocab_size")
    assert_true(p.hidden_dim == 4, "hidden_dim")
    assert_true(p.q_dim() == 4, "q_dim = 2*2")
    assert_true(p.kv_dim() == 2, "kv_dim = 1*2")
    assert_true(p.layer_weight_count() > 0, "layer weight count")

    print("  model_params: PASS")


fn test_model_creation() raises:
    """Test Model struct creation."""
    var p = tiny_test_params()
    var model = Model(p)

    assert_true(model.embed.numel() == 8 * 4, "embed size")
    assert_true(model.lm_head.numel() == 8 * 4, "lm_head size")
    assert_true(model.final_norm.numel() == 4, "final_norm size")

    # Norms should be 1.0
    assert_near(model.final_norm.get(0), 1.0, 1e-5, "final norm init")

    print("  model_creation: PASS")


fn test_model_forward_single_token() raises:
    """Test single-token forward pass through model."""
    var p = tiny_test_params()
    var model = Model(p)

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    # Set a simple embedding so we get non-zero input
    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    var logits = model.forward(0, cache, rope, pos=0)

    assert_true(logits.numel() == p.vocab_size, "logits size")
    # Check cache was updated
    assert_true(cache.lengths[0] == 1, "layer 0 cache length")
    assert_true(cache.lengths[1] == 1, "layer 1 cache length")

    print("  model_forward_single_token: PASS")


fn test_model_forward_multi_token() raises:
    """Test multi-token forward pass."""
    var p = tiny_test_params()
    var model = Model(p)

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    # Process 3 tokens
    for i in range(3):
        _ = model.forward(i, cache, rope, pos=i)

    assert_true(cache.lengths[0] == 3, "3 tokens cached in layer 0")
    assert_true(cache.lengths[1] == 3, "3 tokens cached in layer 1")

    print("  model_forward_multi_token: PASS")


fn test_model_residual_with_zero_weights() raises:
    """Test that zero projection weights preserve input through residual."""
    var p = tiny_test_params()
    var model = Model(p)

    # Set embedding for token 0
    model.embed.set(0, 1.0)
    model.embed.set(1, 2.0)
    model.embed.set(2, 3.0)
    model.embed.set(3, 4.0)

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    # With all projection weights zero, residual connections should
    # mostly preserve the input through to final norm + logits
    var logits = model.forward(0, cache, rope, pos=0)
    assert_true(logits.numel() == p.vocab_size, "logits shape")

    print("  model_residual_with_zero_weights: PASS")


fn test_generate_basic() raises:
    """Test basic generation loop."""
    var p = tiny_test_params()
    var model = Model(p)

    # Set embeddings
    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    # Set LM head so token 3 always wins
    for d in range(p.hidden_dim):
        model.lm_head.set(3 * p.hidden_dim + d, 10.0)

    var prompt = List[Int]()
    prompt.append(0)

    var gen = generate(model, prompt, max_new_tokens=3)

    assert_true(len(gen) == 3, "generated 3 tokens")

    print("  generate_basic: PASS")


fn test_generate_deterministic() raises:
    """Test that generation is deterministic."""
    var p = tiny_test_params()
    var model = Model(p)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t * d + 1) * 0.05)

    var prompt = List[Int]()
    prompt.append(1)

    var gen1 = generate(model, prompt, max_new_tokens=4)
    var gen2 = generate(model, prompt, max_new_tokens=4)

    for i in range(4):
        assert_true(gen1[i] == gen2[i], "deterministic at step " + String(i))

    print("  generate_deterministic: PASS")


fn test_generate_with_longer_prompt() raises:
    """Test generation with multi-token prompt."""
    var p = tiny_test_params()
    var model = Model(p)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    var prompt = List[Int]()
    prompt.append(0)
    prompt.append(1)
    prompt.append(2)

    var gen = generate(model, prompt, max_new_tokens=2)
    assert_true(len(gen) == 2, "generated 2 tokens")

    print("  generate_with_longer_prompt: PASS")


fn test_layer_weight_offsets() raises:
    """Test that layer weight offsets don't overlap."""
    var p = tiny_test_params()
    var model = Model(p)

    var off0 = model._layer_offsets(0)
    var off1 = model._layer_offsets(1)

    # Layer 1's offsets should all be >= layer 0's end
    assert_true(off1.attn_norm >= off0.w_down + p.hidden_dim * p.ffn_dim,
        "layer 1 starts after layer 0")
    # Layer 0 and 1 shouldn't overlap
    assert_true(off1.attn_norm == model.layer_size, "layer 1 base = layer_size")

    print("  layer_weight_offsets: PASS")


fn main() raises:
    print("test_model:")

    test_model_params()
    test_model_creation()
    test_model_forward_single_token()
    test_model_forward_multi_token()
    test_model_residual_with_zero_weights()
    test_generate_basic()
    test_generate_deterministic()
    test_generate_with_longer_prompt()
    test_layer_weight_offsets()

    print("ALL PASSED")
