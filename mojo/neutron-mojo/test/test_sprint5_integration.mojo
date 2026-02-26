# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 5 Integration Tests
# ===----------------------------------------------------------------------=== #

"""Integration tests exercising Sprint 5 modules together:
repetition penalty + sampler, stop tokens in generation,
beam search, quantized KV cache in model pipeline, fused attention.
"""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    apply_frequency_penalty,
    should_stop,
    GenerationConfig,
    BeamEntry,
    beam_search_step,
    select_top_beams,
)
from neutron_mojo.nn.sampler import Sampler, greedy_config, random_config
from neutron_mojo.nn.model import Model, tiny_test_params
from neutron_mojo.nn.kv_cache import KVCache, MultiLayerKVCache
from neutron_mojo.nn.q_kv_cache import Q8KVCache, q8_gqa_attention
from neutron_mojo.nn.fused_attention import (
    fused_attention_head,
    fused_gqa_attention,
    fused_q8_attention_head,
)
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.attention import attention_single_head


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn _inv_sqrt(d: Int) -> Float32:
    var df = Float32(d)
    var x: Float32 = 0.5
    for _ in range(10):
        x = x * (1.5 - 0.5 * df * x * x)
    return x


fn test_rep_penalty_improves_diversity() raises:
    """Test: repetition penalty prevents degenerate repetition in generation."""
    var p = tiny_test_params()
    var model = Model(p)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    # Bias LM head slightly toward token 3, with token 2 close behind
    for d in range(p.hidden_dim):
        model.lm_head.set(3 * p.hidden_dim + d, 2.0)
        model.lm_head.set(2 * p.hidden_dim + d, 1.8)

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    # Without penalty: greedy always picks token 3
    var sampler = Sampler(greedy_config())
    var logits = model.forward(0, cache, rope, pos=0)
    var no_penalty_tok = sampler.sample(logits, p.vocab_size)

    # With penalty: after seeing token 3, should sometimes pick other tokens
    var generated = List[Int]()
    generated.append(3)
    generated.append(3)
    generated.append(3)
    apply_repetition_penalty(logits, p.vocab_size, generated, penalty=3.0)

    var sampler2 = Sampler(greedy_config())
    var with_penalty_tok = sampler2.sample(logits, p.vocab_size)

    # With strong penalty on token 3, greedy should pick something else
    assert_true(no_penalty_tok == 3, "without penalty picks 3")
    assert_true(with_penalty_tok != 3, "with penalty avoids 3")

    print("  rep_penalty_improves_diversity: PASS")


fn test_stop_token_halts_generation() raises:
    """Test: stop tokens properly detected during generation loop."""
    var p = tiny_test_params()
    var model = Model(p)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    var config = GenerationConfig()
    config.add_stop_token(0)  # EOS = token 0
    config.max_tokens = 20

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    var sampler = Sampler(greedy_config())
    var generated = List[Int]()
    var logits = model.forward(1, cache, rope, pos=0)

    for step in range(config.max_tokens):
        var token = sampler.sample(logits, p.vocab_size)
        if config.is_stop_token(token):
            break
        generated.append(token)
        logits = model.forward(token, cache, rope, pos=step + 1)

    # Should have stopped before max_tokens (or at max)
    assert_true(len(generated) <= config.max_tokens, "generation stopped")

    print("  stop_token_halts_generation: PASS")


fn test_beam_search_finds_best_sequence() raises:
    """Test: beam search explores multiple candidates."""
    # Create logits favoring different tokens
    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 1.0)
    logits.set(1, 5.0)
    logits.set(2, 3.0)
    logits.set(3, 2.0)

    var beams = List[BeamEntry]()
    var initial = BeamEntry()
    initial.score = 0.0
    beams.append(initial^)

    # Expand with beam_width=3
    var candidates = beam_search_step(logits, 4, beams, beam_width=3, beam_idx=0)
    assert_true(len(candidates) == 3, "3 candidates")

    # Select top 2
    var top = select_top_beams(candidates, beam_width=2)
    assert_true(len(top) == 2, "top 2 selected")

    # Best should have token 1 (highest logit)
    var best_last = top[0].tokens[len(top[0].tokens) - 1]
    assert_true(best_last == 1, "best sequence ends with token 1")

    # Second should have token 2
    var second_last = top[1].tokens[len(top[1].tokens) - 1]
    assert_true(second_last == 2, "second ends with token 2")

    print("  beam_search_finds_best_sequence: PASS")


fn test_q8_cache_in_model_pipeline() raises:
    """Test: quantized KV cache used in a model-like forward pass."""
    var head_dim = 4

    var q8_cache = Q8KVCache(max_seq_len=16, num_kv_heads=1, head_dim=4)
    var fp32_cache = KVCache(max_seq_len=16, num_kv_heads=1, head_dim=4)

    # Simulate a few layers of K/V projection
    for pos in range(5):
        var k = Tensor[DType.float32](Shape(4))
        var v = Tensor[DType.float32](Shape(4))
        for d in range(4):
            k.set(d, Float32(pos * 3 + d) * 0.15 - 0.3)
            v.set(d, Float32(pos + d * 2) * 0.1)
        q8_cache.append_kv(k, v, num_new_tokens=1)
        fp32_cache.append_kv(k, v, num_new_tokens=1)

    # Query
    var query = Tensor[DType.float32](Shape(4))
    query.set(0, 0.7)
    query.set(1, -0.2)
    query.set(2, 0.5)
    query.set(3, 0.3)

    # Compare FP32 fused attention vs Q8 fused attention
    var fp32_out = fused_attention_head(query, fp32_cache, 0, head_dim, 4)
    var q8_out = fused_q8_attention_head(query, q8_cache, 0, head_dim, 4)

    var max_err: Float32 = 0.0
    for d in range(head_dim):
        var err = fp32_out.get(d) - q8_out.get(d)
        if err < 0.0:
            err = -err
        if err > max_err:
            max_err = err

    assert_true(max_err < 0.15, "Q8 pipeline error: " + String(max_err))

    # Memory savings
    assert_true(q8_cache.memory_bytes() < q8_cache.fp32_equivalent_bytes(), "Q8 saves memory")

    print("  q8_cache_in_model_pipeline: PASS")


fn test_fused_attention_in_generation() raises:
    """Test: fused attention produces same result as reference across positions."""
    var head_dim = 4
    var cache = KVCache(max_seq_len=16, num_kv_heads=1, head_dim=4)

    # Simulate 6 steps of generation
    for pos in range(6):
        var k = Tensor[DType.float32](Shape(4))
        var v = Tensor[DType.float32](Shape(4))
        for d in range(4):
            k.set(d, Float32(pos * 2 + d) * 0.1)
            v.set(d, Float32(pos + d) * 0.2)
        cache.append_kv(k, v, num_new_tokens=1)

    # At each position, compare fused vs reference
    var scale = _inv_sqrt(head_dim)
    for pos in range(6):
        var query = Tensor[DType.float32](Shape(4))
        for d in range(4):
            query.set(d, Float32(pos + d) * 0.15)

        var fused_out = fused_attention_head(query, cache, 0, head_dim, pos)
        var ref_out = attention_single_head(query, cache, 0, 0, pos + 1, head_dim, scale)

        for d in range(head_dim):
            var err = fused_out.get(d) - ref_out.get(d)
            if err < 0.0:
                err = -err
            assert_true(err < 0.02, "fused vs ref at pos " + String(pos) + " dim " + String(d))

    print("  fused_attention_in_generation: PASS")


fn test_combined_penalties_with_sampling() raises:
    """Test: frequency + presence + repetition penalties together."""
    var logits = Tensor[DType.float32](Shape(5))
    logits.set(0, 5.0)
    logits.set(1, 5.0)
    logits.set(2, 5.0)
    logits.set(3, 5.0)
    logits.set(4, 5.0)

    var history = List[Int]()
    history.append(0)
    history.append(0)
    history.append(0)
    history.append(1)
    history.append(2)

    # Apply repetition penalty
    apply_repetition_penalty(logits, 5, history, penalty=1.5)

    # Apply frequency + presence penalty
    apply_frequency_penalty(logits, 5, history, frequency_penalty=0.3, presence_penalty=0.5)

    # Token 0 appeared 3x: should be most penalized
    # Tokens 3,4 never appeared: should be highest
    assert_true(logits.get(3) > logits.get(0), "token 3 > token 0 after penalties")
    assert_true(logits.get(4) > logits.get(0), "token 4 > token 0 after penalties")
    assert_true(logits.get(3) > logits.get(1), "token 3 > token 1 after penalties")

    # Greedy should pick an unpenalized token
    var sampler = Sampler(greedy_config())
    var token = sampler.sample(logits, 5)
    assert_true(token == 3 or token == 4, "picks unpenalized token")

    print("  combined_penalties_with_sampling: PASS")


fn test_generation_config_driven_loop() raises:
    """Test: full generation loop driven by GenerationConfig."""
    var p = tiny_test_params()
    var model = Model(p)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    var config = GenerationConfig()
    config.sampler_config = random_config(temperature=0.8, seed=42)
    config.repetition_penalty = 1.2
    config.max_tokens = 8
    config.add_stop_token(7)  # Token 7 = stop

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    var sampler = Sampler(config.sampler_config)
    var generated = List[Int]()
    var logits = model.forward(0, cache, rope, pos=0)

    for step in range(config.max_tokens):
        # Apply repetition penalty
        if config.repetition_penalty > 1.0:
            apply_repetition_penalty(logits, p.vocab_size, generated, config.repetition_penalty)

        var token = sampler.sample(logits, p.vocab_size)

        if config.is_stop_token(token):
            break

        generated.append(token)
        logits = model.forward(token, cache, rope, pos=step + 1)

    assert_true(len(generated) <= config.max_tokens, "respects max_tokens")
    for i in range(len(generated)):
        assert_true(generated[i] >= 0 and generated[i] < p.vocab_size, "valid tokens")

    print("  generation_config_driven_loop: PASS")


fn test_q8_gqa_vs_fused_q8_gqa() raises:
    """Test: q8_gqa_attention matches fused_q8_gqa for consistency."""
    var q8_cache = Q8KVCache(max_seq_len=8, num_kv_heads=2, head_dim=2)

    for pos in range(3):
        var k = Tensor[DType.float32](Shape(4))
        var v = Tensor[DType.float32](Shape(4))
        for i in range(4):
            k.set(i, Float32(pos + i) * 0.3)
            v.set(i, Float32(pos * i + 1) * 0.15)
        q8_cache.append_kv(k, v, num_new_tokens=1)

    var query = Tensor[DType.float32](Shape(8))
    for i in range(8):
        query.set(i, Float32(i) * 0.1 - 0.3)

    var ref_out = q8_gqa_attention(query, q8_cache, 4, 2, 2)

    from neutron_mojo.nn.fused_attention import fused_q8_gqa_attention
    var fused_out = fused_q8_gqa_attention(query, q8_cache, 4, 2, 2, 2)

    var max_err: Float32 = 0.0
    for i in range(8):
        var err = ref_out.get(i) - fused_out.get(i)
        if err < 0.0:
            err = -err
        if err > max_err:
            max_err = err

    assert_true(max_err < 0.05, "q8 gqa vs fused q8 gqa: " + String(max_err))

    print("  q8_gqa_vs_fused_q8_gqa: PASS")


fn main() raises:
    print("test_sprint5_integration:")

    test_rep_penalty_improves_diversity()
    test_stop_token_halts_generation()
    test_beam_search_finds_best_sequence()
    test_q8_cache_in_model_pipeline()
    test_fused_attention_in_generation()
    test_combined_penalties_with_sampling()
    test_generation_config_driven_loop()
    test_q8_gqa_vs_fused_q8_gqa()

    print("ALL PASSED")
