# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 4 Integration Tests
# ===----------------------------------------------------------------------=== #

"""Integration tests exercising Sprint 4 modules together:
tokenizer → quantized linear → multi-layer model → sampler.
"""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import rmsnorm
from neutron_mojo.nn.tokenizer import BPETokenizer, build_test_tokenizer
from neutron_mojo.nn.quantized_linear import (
    Q8Weight,
    quantize_weight_q8,
    q8_linear,
    quantization_error,
)
from neutron_mojo.nn.model import (
    Model,
    ModelParams,
    tiny_test_params,
    generate,
)
from neutron_mojo.nn.sampler import (
    LCG,
    SamplerConfig,
    Sampler,
    greedy_config,
    creative_config,
    random_config,
)
from neutron_mojo.nn.transformer import linear
from neutron_mojo.nn.causal_lm import embed_token, compute_logits, argmax
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn test_tokenizer_to_model_pipeline() raises:
    """Test: tokenize text → feed token IDs to model → generate."""
    var tok = build_test_tokenizer()

    # Encode some text
    var tokens = tok.encode("the")
    assert_true(len(tokens) > 0, "tokenizer produced tokens")

    # Feed tokens through model
    var p = tiny_test_params()
    var model = Model(p)

    # Set embeddings so we get non-zero activations
    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    # Use first token from tokenizer (clamped to model vocab)
    var first_tok = tokens[0]
    if first_tok >= p.vocab_size:
        first_tok = 0

    var prompt = List[Int]()
    prompt.append(first_tok)

    var gen = generate(model, prompt, max_new_tokens=2)
    assert_true(len(gen) == 2, "generated 2 tokens from tokenized input")

    print("  tokenizer_to_model_pipeline: PASS")


fn test_quantized_projection_in_model() raises:
    """Test: quantize a weight matrix → use Q8 linear as substitute for FP32."""
    # Create a small weight matrix [4, 8]
    var w = Tensor[DType.float32](Shape(4, 8))
    for i in range(32):
        w.set(i, Float32(i) * 0.1 - 1.6)

    var x = Tensor[DType.float32](Shape(8))
    for i in range(8):
        x.set(i, Float32(i + 1) * 0.25)

    # FP32 reference
    var y_ref = linear(x, w)

    # Q8 version
    var qw = quantize_weight_q8(w, 4, 8, block_size=4)
    var y_q8 = q8_linear(x, qw)

    # Should be close
    var err = quantization_error(y_ref, y_q8, 4)
    assert_true(err < 0.2, "Q8 projection error: " + String(err))

    print("  quantized_projection_in_model: PASS")


fn test_sampler_with_model_logits() raises:
    """Test: run model forward → feed logits to sampler."""
    var p = tiny_test_params()
    var model = Model(p)

    # Set embeddings
    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    # Set LM head to make token 2 clearly preferred
    for d in range(p.hidden_dim):
        model.lm_head.set(2 * p.hidden_dim + d, 5.0)

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    var logits = model.forward(0, cache, rope, pos=0)

    # Greedy sampler should pick argmax
    var greedy = Sampler(greedy_config())
    var token_greedy = greedy.sample(logits, p.vocab_size)
    var expected = argmax(logits, p.vocab_size)
    assert_true(token_greedy == expected, "greedy sampler matches argmax")

    # Temperature sampler should produce valid token
    var temp = Sampler(random_config(temperature=0.8, seed=42))
    var token_temp = temp.sample(logits, p.vocab_size)
    assert_true(token_temp >= 0 and token_temp < p.vocab_size, "temp sampler valid token")

    print("  sampler_with_model_logits: PASS")


fn test_full_generation_with_sampler() raises:
    """Test: model generate loop using sampler for token selection."""
    var p = tiny_test_params()
    var model = Model(p)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t * d + 1) * 0.05)

    # Use sampler inside a manual generation loop
    var config = random_config(temperature=0.5, seed=123)
    var sampler = Sampler(config)

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    var generated = List[Int]()
    var logits = model.forward(0, cache, rope, pos=0)

    for step in range(5):
        var token = sampler.sample(logits, p.vocab_size)
        assert_true(token >= 0 and token < p.vocab_size, "valid token at step " + String(step))
        generated.append(token)
        logits = model.forward(token, cache, rope, pos=step + 1)

    assert_true(len(generated) == 5, "generated 5 tokens with sampler")

    print("  full_generation_with_sampler: PASS")


fn test_sampler_reproducibility_in_generation() raises:
    """Test: same seed produces same generation sequence."""
    var p = tiny_test_params()
    var model = Model(p)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    # Run 1
    var cache1 = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope1 = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)
    var sampler1 = Sampler(random_config(temperature=0.8, seed=42))

    var seq1 = List[Int]()
    var logits1 = model.forward(0, cache1, rope1, pos=0)
    for step in range(4):
        var tok = sampler1.sample(logits1, p.vocab_size)
        seq1.append(tok)
        logits1 = model.forward(tok, cache1, rope1, pos=step + 1)

    # Run 2 — same seed
    var cache2 = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope2 = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)
    var sampler2 = Sampler(random_config(temperature=0.8, seed=42))

    var seq2 = List[Int]()
    var logits2 = model.forward(0, cache2, rope2, pos=0)
    for step in range(4):
        var tok = sampler2.sample(logits2, p.vocab_size)
        seq2.append(tok)
        logits2 = model.forward(tok, cache2, rope2, pos=step + 1)

    for i in range(4):
        assert_true(seq1[i] == seq2[i], "reproducible gen at step " + String(i))

    print("  sampler_reproducibility_in_generation: PASS")


fn test_top_k_with_model() raises:
    """Test: top-k sampling with model-generated logits."""
    var p = tiny_test_params()
    var model = Model(p)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    # Set LM head to create peaked distribution
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.lm_head.set(v * p.hidden_dim + d, Float32(v) * 2.0)

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    var logits = model.forward(0, cache, rope, pos=0)

    # Top-k=2 should only sample from top 2 tokens
    var config = random_config(temperature=1.0, seed=42)
    config.top_k = 2
    var sampler = Sampler(config)

    for _ in range(20):
        # Create a fresh sampler each time with same progressing state
        var tok = sampler.sample(logits, p.vocab_size)
        assert_true(tok >= 0 and tok < p.vocab_size, "top-k token in vocab range")

    print("  top_k_with_model: PASS")


fn test_tokenizer_roundtrip_with_ids() raises:
    """Test: encode → decode roundtrip preserves text."""
    var tok = build_test_tokenizer()

    # Encode then decode
    var text = "the"
    var ids = tok.encode(text)
    assert_true(len(ids) > 0, "encode produced IDs")

    var decoded = tok.decode(ids)
    assert_true(decoded == text, "roundtrip: '" + decoded + "' == '" + text + "'")

    print("  tokenizer_roundtrip_with_ids: PASS")


fn test_multi_layer_model_deterministic() raises:
    """Test: multi-layer model produces deterministic outputs."""
    var p = tiny_test_params()
    var model = Model(p)

    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    var prompt = List[Int]()
    prompt.append(1)

    var gen1 = generate(model, prompt, max_new_tokens=3)
    var gen2 = generate(model, prompt, max_new_tokens=3)

    for i in range(3):
        assert_true(gen1[i] == gen2[i], "deterministic gen at " + String(i))

    print("  multi_layer_model_deterministic: PASS")


fn test_q8_preserves_model_quality() raises:
    """Test: Q8 quantized linear gives close results to FP32 for model-like weights."""
    # Simulate a model projection: hidden_dim=4 → ffn_dim=8
    var w = Tensor[DType.float32](Shape(8, 4))
    for i in range(32):
        w.set(i, Float32(i % 7) * 0.3 - 0.9)

    # Simulate a hidden state
    var h = Tensor[DType.float32](Shape(4))
    h.set(0, 0.5)
    h.set(1, -0.3)
    h.set(2, 1.2)
    h.set(3, -0.8)

    var y_fp32 = linear(h, w)
    var qw = quantize_weight_q8(w, 8, 4, block_size=4)
    var q_out = q8_linear(h, qw)

    var err = quantization_error(y_fp32, q_out, 8)
    assert_true(err < 0.15, "Q8 model-like projection error: " + String(err))

    print("  q8_preserves_model_quality: PASS")


fn test_end_to_end_pipeline() raises:
    """Test: full pipeline tokenize → model → sampler → detokenize."""
    var tok = build_test_tokenizer()

    # Encode input
    var input_ids = tok.encode("in")
    assert_true(len(input_ids) > 0, "input encoded")

    # Create model
    var p = tiny_test_params()
    var model = Model(p)
    for t in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(t * p.hidden_dim + d, Float32(t + d) * 0.1)

    # Forward through model with first token (clamped to vocab)
    var first_id = input_ids[0]
    if first_id >= p.vocab_size:
        first_id = 0

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=p.max_seq_len)

    var logits = model.forward(first_id, cache, rope, pos=0)

    # Sample with different strategies
    var greedy = Sampler(greedy_config())
    var greedy_tok = greedy.sample(logits, p.vocab_size)
    assert_true(greedy_tok >= 0 and greedy_tok < p.vocab_size, "greedy output valid")

    var creative = Sampler(creative_config())
    var creative_tok = creative.sample(logits, p.vocab_size)
    assert_true(creative_tok >= 0 and creative_tok < p.vocab_size, "creative output valid")

    print("  end_to_end_pipeline: PASS")


fn main() raises:
    print("test_sprint4_integration:")

    test_tokenizer_to_model_pipeline()
    test_quantized_projection_in_model()
    test_sampler_with_model_logits()
    test_full_generation_with_sampler()
    test_sampler_reproducibility_in_generation()
    test_top_k_with_model()
    test_tokenizer_roundtrip_with_ids()
    test_multi_layer_model_deterministic()
    test_q8_preserves_model_quality()
    test_end_to_end_pipeline()

    print("ALL PASSED")
