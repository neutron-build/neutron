# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Causal LM Tests
# ===----------------------------------------------------------------------=== #

"""Tests for causal language model and generation."""

from math import abs
from neutron_mojo.nn.causal_lm import (
    CausalLMWeights,
    embed_token,
    compute_logits,
    argmax,
    apply_temperature,
    top_k_filter,
    generate_greedy_one_layer,
)
from neutron_mojo.nn.transformer import TransformerWeights
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


fn test_embed_token() raises:
    """Test embedding lookup."""
    # vocab_size=3, hidden_dim=2
    var embed = Tensor[DType.float32](Shape(3, 2))
    # Token 0: [1, 2]
    embed.set(0, 1.0)
    embed.set(1, 2.0)
    # Token 1: [3, 4]
    embed.set(2, 3.0)
    embed.set(3, 4.0)
    # Token 2: [5, 6]
    embed.set(4, 5.0)
    embed.set(5, 6.0)

    var e0 = embed_token(embed, 0, 2)
    assert_near(e0.get(0), 1.0, 1e-5, "embed token 0")
    assert_near(e0.get(1), 2.0, 1e-5, "embed token 0")

    var e2 = embed_token(embed, 2, 2)
    assert_near(e2.get(0), 5.0, 1e-5, "embed token 2")
    assert_near(e2.get(1), 6.0, 1e-5, "embed token 2")

    print("  embed_token: PASS")


fn test_compute_logits() raises:
    """Test logit computation."""
    var hidden = Tensor[DType.float32](Shape(2))
    hidden.set(0, 1.0)
    hidden.set(1, 2.0)

    # lm_head: [3, 2] → 3 vocab items
    var lm_head = Tensor[DType.float32](Shape(3, 2))
    lm_head.set(0, 1.0)
    lm_head.set(1, 0.0)
    lm_head.set(2, 0.0)
    lm_head.set(3, 1.0)
    lm_head.set(4, 1.0)
    lm_head.set(5, 1.0)

    var logits = compute_logits(hidden, lm_head, 3, 2)
    # logit[0] = 1*1 + 0*2 = 1
    # logit[1] = 0*1 + 1*2 = 2
    # logit[2] = 1*1 + 1*2 = 3
    assert_near(logits.get(0), 1.0, 1e-5, "logit 0")
    assert_near(logits.get(1), 2.0, 1e-5, "logit 1")
    assert_near(logits.get(2), 3.0, 1e-5, "logit 2")

    print("  compute_logits: PASS")


fn test_argmax() raises:
    """Test argmax."""
    var logits = Tensor[DType.float32](Shape(5))
    logits.set(0, 1.0)
    logits.set(1, 5.0)
    logits.set(2, 3.0)
    logits.set(3, 2.0)
    logits.set(4, 4.0)

    var idx = argmax(logits, 5)
    assert_true(idx == 1, "argmax should be 1")

    print("  argmax: PASS")


fn test_argmax_first_element() raises:
    """Test argmax when maximum is first element."""
    var logits = Tensor[DType.float32](Shape(3))
    logits.set(0, 10.0)
    logits.set(1, 5.0)
    logits.set(2, 1.0)

    var idx = argmax(logits, 3)
    assert_true(idx == 0, "argmax should be 0")

    print("  argmax_first_element: PASS")


fn test_temperature_scaling() raises:
    """Test temperature scaling."""
    var logits = Tensor[DType.float32](Shape(3))
    logits.set(0, 2.0)
    logits.set(1, 4.0)
    logits.set(2, 6.0)

    apply_temperature(logits, 3, 2.0)
    assert_near(logits.get(0), 1.0, 1e-5, "temp/2 = 1.0")
    assert_near(logits.get(1), 2.0, 1e-5, "temp/2 = 2.0")
    assert_near(logits.get(2), 3.0, 1e-5, "temp/2 = 3.0")

    print("  temperature_scaling: PASS")


fn test_top_k_filter() raises:
    """Test top-k filtering."""
    var logits = Tensor[DType.float32](Shape(5))
    logits.set(0, 1.0)
    logits.set(1, 5.0)
    logits.set(2, 3.0)
    logits.set(3, 2.0)
    logits.set(4, 4.0)

    top_k_filter(logits, 5, 2)

    # Top 2 are indices 1 (5.0) and 4 (4.0)
    assert_near(logits.get(1), 5.0, 1e-5, "top-k keeps idx 1")
    assert_near(logits.get(4), 4.0, 1e-5, "top-k keeps idx 4")
    # Others should be masked to -inf
    assert_true(logits.get(0) < -1e20, "idx 0 filtered")
    assert_true(logits.get(2) < -1e20, "idx 2 filtered")
    assert_true(logits.get(3) < -1e20, "idx 3 filtered")

    print("  top_k_filter: PASS")


fn test_causal_lm_weights_creation() raises:
    """Test CausalLMWeights initialization."""
    var w = CausalLMWeights(
        num_layers=2,
        vocab_size=100,
        hidden_dim=8,
        num_q_heads=4,
        num_kv_heads=2,
        head_dim=2,
        ffn_dim=16,
    )

    assert_true(w.embed.numel() == 800, "embed size 100*8")
    assert_true(w.lm_head.numel() == 800, "lm_head size 100*8")
    assert_true(w.final_norm.numel() == 8, "final norm size")
    assert_near(w.final_norm.get(0), 1.0, 1e-5, "norm init")
    assert_true(w.num_layers == 2, "num layers")

    print("  causal_lm_weights_creation: PASS")


fn test_generate_greedy_deterministic() raises:
    """Test that greedy generation is deterministic.

    With a tiny model where the LM head has one clearly dominant logit,
    the output should always pick the same token.
    """
    # Tiny model: vocab=4, hidden=2, 1 layer, 1 Q head, 1 KV head, head_dim=2
    var vocab_size = 4
    var hidden_dim = 2

    var model = CausalLMWeights(
        num_layers=1,
        vocab_size=vocab_size,
        hidden_dim=hidden_dim,
        num_q_heads=1,
        num_kv_heads=1,
        head_dim=2,
        ffn_dim=4,
    )

    # Set embedding: each token maps to a distinct vector
    model.embed.set(0, 1.0)   # token 0 → [1, 0]
    model.embed.set(1, 0.0)
    model.embed.set(2, 0.0)   # token 1 → [0, 1]
    model.embed.set(3, 1.0)
    model.embed.set(4, 1.0)   # token 2 → [1, 1]
    model.embed.set(5, 1.0)
    model.embed.set(6, -1.0)  # token 3 → [-1, -1]
    model.embed.set(7, -1.0)

    # Set LM head: token 2 always gets highest logit for any input
    # lm_head[2] = [10, 10] — large dot product with any positive vector
    model.lm_head.set(4, 10.0)
    model.lm_head.set(5, 10.0)

    var layer = TransformerWeights(hidden_dim, 1, 1, 2, 4)

    var prompt = List[Int]()
    prompt.append(0)

    var gen1 = generate_greedy_one_layer(prompt, model, layer, max_new_tokens=3)
    var gen2 = generate_greedy_one_layer(prompt, model, layer, max_new_tokens=3)

    # Should be deterministic
    assert_true(len(gen1) == 3, "generated 3 tokens")
    for i in range(3):
        assert_true(gen1[i] == gen2[i], "deterministic generation")

    print("  generate_greedy_deterministic: PASS")


fn test_generate_output_length() raises:
    """Test that generation produces correct number of tokens."""
    var model = CausalLMWeights(
        num_layers=1,
        vocab_size=4,
        hidden_dim=2,
        num_q_heads=1,
        num_kv_heads=1,
        head_dim=2,
        ffn_dim=4,
    )
    var layer = TransformerWeights(2, 1, 1, 2, 4)

    var prompt = List[Int]()
    prompt.append(0)
    prompt.append(1)

    var gen = generate_greedy_one_layer(prompt, model, layer, max_new_tokens=5)
    assert_true(len(gen) == 5, "should generate exactly 5 tokens")

    print("  generate_output_length: PASS")


fn main() raises:
    print("test_causal_lm:")

    test_embed_token()
    test_compute_logits()
    test_argmax()
    test_argmax_first_element()
    test_temperature_scaling()
    test_top_k_filter()
    test_causal_lm_weights_creation()
    test_generate_greedy_deterministic()
    test_generate_output_length()

    print("ALL PASSED")
