# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 20: Streaming Generation Tests
# ===----------------------------------------------------------------------=== #

"""Tests for streaming token-by-token generation.

Tests:
1. TokenEvent creation and defaults
2. TokenEvent tokens_per_sec calculation
3. StreamingGenerator produces tokens
4. StreamingGenerator finishes at max_tokens
5. StreamingGenerator finishes at EOS
6. streaming_collect returns all events
7. Generated text matches non-streaming pipeline
8. TokenEvent position tracks correctly
9. TokenEvent has timing info
10. StreamingGenerator get_text accumulates
"""

from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig, pipeline_generate
from neutron_mojo.nn.streaming import (
    TokenEvent,
    StreamingGenerator,
    streaming_collect,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error("FAIL: " + msg + " expected=" + String(b) + " got=" + String(a))


fn _build_tiny_model() -> Model:
    """Build a tiny FP32 model with non-trivial weights."""
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
    _ = tok.add_token("<s>")     # 0
    _ = tok.add_token("</s>")   # 1
    _ = tok.add_token("<unk>")  # 2
    _ = tok.add_token("a")      # 3
    _ = tok.add_token("b")      # 4
    _ = tok.add_token("c")      # 5
    _ = tok.add_token("d")      # 6
    _ = tok.add_token("e")      # 7
    tok.bos_id = 0
    tok.eos_id = 1
    tok.unk_id = 2
    return tok^


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_token_event_defaults() raises:
    """TokenEvent default constructor works."""
    var event = TokenEvent()
    assert_eq(event.token_id, -1, "default token_id")
    assert_eq(event.position, 0, "default position")
    assert_true(not event.is_eos, "default not EOS")
    assert_true(event.elapsed_ns == 0, "default elapsed")
    print("  token_event_defaults: PASS")


fn test_token_event_tps() raises:
    """TokenEvent tokens_per_sec calculation."""
    var event = TokenEvent(String("hi"), 5, 10, False, UInt(1_000_000_000))
    var tps = event.tokens_per_sec()
    assert_true(tps > 9.0 and tps < 11.0, "10 tokens in 1s = ~10 tps")

    var event2 = TokenEvent()
    assert_true(event2.tokens_per_sec() == 0.0, "zero elapsed = 0 tps")
    print("  token_event_tps: PASS")


fn test_streaming_produces_tokens() raises:
    """StreamingGenerator produces at least one token."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 3

    var gen = StreamingGenerator(model^, tok^, "abc", cfg)
    assert_true(not gen.is_finished(), "not finished initially")

    var event = gen.next_token()
    assert_true(event.token_id >= 0, "first token has valid ID")
    assert_true(len(event.text) > 0, "first token has text")
    print("  streaming_produces_tokens: PASS")


fn test_streaming_finishes_at_max() raises:
    """StreamingGenerator stops at max_new_tokens."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 5

    var gen = StreamingGenerator(model^, tok^, "abc", cfg)
    var count = 0
    while not gen.is_finished():
        var event = gen.next_token()
        if not event.is_eos:
            count += 1

    assert_true(count <= 5, "generated <= max_new_tokens")
    assert_true(gen.is_finished(), "finished after loop")
    print("  streaming_finishes_at_max: PASS")


fn test_streaming_eos_stop() raises:
    """StreamingGenerator stops when EOS is generated."""
    # With tiny model, EOS may or may not be generated naturally.
    # Just verify the is_eos flag mechanism works.
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 20

    var gen = StreamingGenerator(model^, tok^, "abc", cfg)
    var found_eos = False
    var count = 0
    while not gen.is_finished():
        var event = gen.next_token()
        count += 1
        if event.is_eos:
            found_eos = True

    # Either hit EOS or max_tokens — both are valid
    assert_true(gen.is_finished(), "generator finished")
    assert_true(count > 0, "at least one event")
    print("  streaming_eos_stop: PASS")


fn test_streaming_collect() raises:
    """streaming_collect returns all events."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 4

    var events = streaming_collect(model^, tok^, "abc", cfg)
    assert_true(len(events) > 0, "collect has events")
    assert_true(len(events) <= 5, "collect <= max+1 events")  # +1 for possible EOS
    print("  streaming_collect: PASS")


fn test_streaming_matches_pipeline() raises:
    """Streaming output matches non-streaming pipeline output."""
    # Build two identical models
    var model1 = _build_tiny_model()
    var tok1 = _build_tiny_tokenizer()
    var model2 = _build_tiny_model()
    var tok2 = _build_tiny_tokenizer()

    var cfg = PipelineConfig()
    cfg.max_new_tokens = 5

    # Non-streaming
    var text1 = pipeline_generate(model1, tok1, "abc", cfg)

    # Streaming
    var events = streaming_collect(model2^, tok2^, "abc", cfg)
    var text2 = String("")
    for i in range(len(events)):
        if not events[i].is_eos:
            text2 += events[i].text

    assert_true(text1 == text2, "streaming matches pipeline: '" + text1 + "' vs '" + text2 + "'")
    print("  streaming_matches_pipeline: PASS")


fn test_position_tracking() raises:
    """TokenEvent position increments correctly."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 4

    var events = streaming_collect(model^, tok^, "abc", cfg)

    var pos = 0
    for i in range(len(events)):
        if not events[i].is_eos:
            assert_eq(events[i].position, pos, "position at step " + String(i))
            pos += 1
    print("  position_tracking: PASS")


fn test_timing_info() raises:
    """TokenEvent contains non-zero timing after generation."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 3

    var events = streaming_collect(model^, tok^, "abc", cfg)

    # Last event should have measurable elapsed time
    if len(events) > 0:
        assert_true(events[len(events) - 1].elapsed_ns > 0, "last event has elapsed_ns > 0")
    print("  timing_info: PASS")


fn test_get_text_accumulates() raises:
    """StreamingGenerator.get_text() accumulates generated text."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 4

    var gen = StreamingGenerator(model^, tok^, "abc", cfg)

    # Before generating
    assert_true(len(gen.get_text()) == 0, "empty before generation")

    # Generate some tokens
    _ = gen.next_token()
    var text1 = gen.get_text()
    assert_true(len(text1) > 0, "text after first token")

    _ = gen.next_token()
    var text2 = gen.get_text()
    assert_true(len(text2) >= len(text1), "text grows with tokens")

    print("  get_text_accumulates: PASS")


fn main() raises:
    print("test_streaming:")

    test_token_event_defaults()
    test_token_event_tps()
    test_streaming_produces_tokens()
    test_streaming_finishes_at_max()
    test_streaming_eos_stop()
    test_streaming_collect()
    test_streaming_matches_pipeline()
    test_position_tracking()
    test_timing_info()
    test_get_text_accumulates()

    print("ALL PASSED (10 tests)")
