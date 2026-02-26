# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Generation Utilities Tests
# ===----------------------------------------------------------------------=== #

"""Tests for repetition penalty, stop tokens, and beam search."""

from math import abs
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    apply_frequency_penalty,
    should_stop,
    GenerationConfig,
    BeamEntry,
    beam_search_step,
    select_top_beams,
)
from neutron_mojo.nn.sampler import Sampler, random_config, greedy_config
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


fn test_repetition_penalty_basic() raises:
    """Test repetition penalty reduces repeated token scores."""
    var logits = Tensor[DType.float32](Shape(5))
    logits.set(0, 2.0)
    logits.set(1, 3.0)
    logits.set(2, 1.0)
    logits.set(3, 4.0)
    logits.set(4, 0.5)

    var history = List[Int]()
    history.append(1)
    history.append(3)

    var original_1 = logits.get(1)
    var original_3 = logits.get(3)

    apply_repetition_penalty(logits, 5, history, penalty=1.5)

    # Token 1 and 3 should be reduced (positive logits divided by penalty)
    assert_true(logits.get(1) < original_1, "token 1 penalized")
    assert_true(logits.get(3) < original_3, "token 3 penalized")
    assert_near(logits.get(1), 3.0 / 1.5, 0.01, "token 1 value")
    assert_near(logits.get(3), 4.0 / 1.5, 0.01, "token 3 value")

    # Unpenalized tokens unchanged
    assert_near(logits.get(0), 2.0, 0.01, "token 0 unchanged")
    assert_near(logits.get(2), 1.0, 0.01, "token 2 unchanged")

    print("  repetition_penalty_basic: PASS")


fn test_repetition_penalty_negative_logits() raises:
    """Test repetition penalty with negative logits (multiplied, not divided)."""
    var logits = Tensor[DType.float32](Shape(3))
    logits.set(0, -2.0)
    logits.set(1, 1.0)
    logits.set(2, -3.0)

    var history = List[Int]()
    history.append(0)
    history.append(2)

    apply_repetition_penalty(logits, 3, history, penalty=2.0)

    # Negative logits multiplied by penalty (more negative)
    assert_near(logits.get(0), -4.0, 0.01, "neg logit * penalty")
    assert_near(logits.get(2), -6.0, 0.01, "neg logit * penalty")
    assert_near(logits.get(1), 1.0, 0.01, "positive unchanged")

    print("  repetition_penalty_negative_logits: PASS")


fn test_repetition_penalty_no_effect_at_1() raises:
    """Test penalty=1.0 has no effect."""
    var logits = Tensor[DType.float32](Shape(3))
    logits.set(0, 1.0)
    logits.set(1, 2.0)
    logits.set(2, 3.0)

    var history = List[Int]()
    history.append(0)
    history.append(1)

    apply_repetition_penalty(logits, 3, history, penalty=1.0)

    assert_near(logits.get(0), 1.0, 0.01, "no effect at 1.0")
    assert_near(logits.get(1), 2.0, 0.01, "no effect at 1.0")

    print("  repetition_penalty_no_effect_at_1: PASS")


fn test_frequency_penalty() raises:
    """Test frequency penalty scales with occurrence count."""
    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 5.0)
    logits.set(1, 5.0)
    logits.set(2, 5.0)
    logits.set(3, 5.0)

    var history = List[Int]()
    history.append(1)
    history.append(1)
    history.append(1)
    history.append(2)

    apply_frequency_penalty(logits, 4, history, frequency_penalty=0.5, presence_penalty=0.0)

    # Token 1 appeared 3 times: 5.0 - 0.5*3 = 3.5
    assert_near(logits.get(1), 3.5, 0.01, "freq penalty token 1")
    # Token 2 appeared 1 time: 5.0 - 0.5*1 = 4.5
    assert_near(logits.get(2), 4.5, 0.01, "freq penalty token 2")
    # Token 0 never appeared: unchanged
    assert_near(logits.get(0), 5.0, 0.01, "no penalty token 0")
    # Token 3 never appeared: unchanged
    assert_near(logits.get(3), 5.0, 0.01, "no penalty token 3")

    print("  frequency_penalty: PASS")


fn test_presence_penalty() raises:
    """Test presence penalty applies flat penalty per unique token."""
    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 5.0)
    logits.set(1, 5.0)
    logits.set(2, 5.0)
    logits.set(3, 5.0)

    var history = List[Int]()
    history.append(1)
    history.append(1)
    history.append(1)
    history.append(2)

    apply_frequency_penalty(logits, 4, history, frequency_penalty=0.0, presence_penalty=1.0)

    # Token 1 appeared: 5.0 - 1.0 = 4.0
    assert_near(logits.get(1), 4.0, 0.01, "presence penalty token 1")
    # Token 2 appeared: 5.0 - 1.0 = 4.0
    assert_near(logits.get(2), 4.0, 0.01, "presence penalty token 2")
    # Token 0 didn't appear: unchanged
    assert_near(logits.get(0), 5.0, 0.01, "no presence penalty")

    print("  presence_penalty: PASS")


fn test_should_stop() raises:
    """Test stop token detection."""
    var stop_tokens = List[Int]()
    stop_tokens.append(2)
    stop_tokens.append(5)
    stop_tokens.append(99)

    assert_true(should_stop(2, stop_tokens), "2 is stop token")
    assert_true(should_stop(5, stop_tokens), "5 is stop token")
    assert_true(should_stop(99, stop_tokens), "99 is stop token")
    assert_true(not should_stop(0, stop_tokens), "0 is not stop token")
    assert_true(not should_stop(3, stop_tokens), "3 is not stop token")

    print("  should_stop: PASS")


fn test_generation_config() raises:
    """Test GenerationConfig struct."""
    var config = GenerationConfig()
    config.repetition_penalty = 1.2
    config.frequency_penalty = 0.5
    config.presence_penalty = 0.3
    config.max_tokens = 100

    config.add_stop_token(0)
    config.add_stop_token(2)

    assert_true(config.num_stop_tokens == 2, "2 stop tokens")
    assert_true(config.is_stop_token(0), "0 is stop")
    assert_true(config.is_stop_token(2), "2 is stop")
    assert_true(not config.is_stop_token(1), "1 is not stop")

    var stops = config.get_stop_tokens()
    assert_true(len(stops) == 2, "stop list len")
    assert_true(stops[0] == 0, "stop 0")
    assert_true(stops[1] == 2, "stop 1")

    print("  generation_config: PASS")


fn test_generation_config_copy() raises:
    """Test GenerationConfig is copyable."""
    var config = GenerationConfig()
    config.repetition_penalty = 1.5
    config.add_stop_token(42)

    var copy = config.copy()
    assert_near(copy.repetition_penalty, 1.5, 0.01, "copy rep penalty")
    assert_true(copy.is_stop_token(42), "copy has stop token")

    print("  generation_config_copy: PASS")


fn test_beam_entry() raises:
    """Test BeamEntry struct."""
    var entry = BeamEntry()
    entry.tokens.append(1)
    entry.tokens.append(3)
    entry.score = -0.5

    assert_true(len(entry.tokens) == 2, "beam tokens")
    assert_near(entry.score, -0.5, 0.01, "beam score")
    assert_true(not entry.finished, "not finished")

    # Test copy
    var copy = entry.copy()
    assert_true(len(copy.tokens) == 2, "copy tokens")
    assert_true(copy.tokens[0] == 1, "copy tok 0")

    print("  beam_entry: PASS")


fn test_beam_search_step() raises:
    """Test beam search expansion step."""
    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 1.0)
    logits.set(1, 5.0)
    logits.set(2, 3.0)
    logits.set(3, 2.0)

    var beams = List[BeamEntry]()
    var initial = BeamEntry()
    initial.tokens.append(0)
    initial.score = 0.0
    beams.append(initial^)

    var candidates = beam_search_step(logits, 4, beams, beam_width=2, beam_idx=0)

    assert_true(len(candidates) == 2, "2 candidates")
    # First candidate should have highest scoring token (1)
    var last_tok_0 = candidates[0].tokens[len(candidates[0].tokens) - 1]
    assert_true(last_tok_0 == 1, "best candidate picks token 1")
    # Second candidate should be token 2 (next highest)
    var last_tok_1 = candidates[1].tokens[len(candidates[1].tokens) - 1]
    assert_true(last_tok_1 == 2, "second picks token 2")

    print("  beam_search_step: PASS")


fn test_select_top_beams() raises:
    """Test selecting top beams by score."""
    var candidates = List[BeamEntry]()

    var b0 = BeamEntry()
    b0.score = -2.0
    b0.tokens.append(0)
    candidates.append(b0^)

    var b1 = BeamEntry()
    b1.score = -0.5
    b1.tokens.append(1)
    candidates.append(b1^)

    var b2 = BeamEntry()
    b2.score = -1.0
    b2.tokens.append(2)
    candidates.append(b2^)

    var b3 = BeamEntry()
    b3.score = -3.0
    b3.tokens.append(3)
    candidates.append(b3^)

    var top = select_top_beams(candidates, beam_width=2)
    assert_true(len(top) == 2, "top 2 beams")
    assert_near(top[0].score, -0.5, 0.01, "best score")
    assert_near(top[1].score, -1.0, 0.01, "second score")

    print("  select_top_beams: PASS")


fn test_repetition_penalty_changes_sampling() raises:
    """Test that repetition penalty actually changes which token is sampled."""
    # Logits slightly favor token 1
    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 4.9)
    logits.set(1, 5.0)
    logits.set(2, 4.8)
    logits.set(3, 4.7)

    # Without penalty, greedy picks token 1
    var sampler = Sampler(greedy_config())
    var tok_before = sampler.sample(logits, 4)
    assert_true(tok_before == 1, "greedy picks 1")

    # Apply heavy penalty to token 1
    var history = List[Int]()
    history.append(1)
    apply_repetition_penalty(logits, 4, history, penalty=5.0)

    # Now greedy should pick something else
    var sampler2 = Sampler(greedy_config())
    var tok_after = sampler2.sample(logits, 4)
    assert_true(tok_after != 1, "penalty shifts away from 1")

    print("  repetition_penalty_changes_sampling: PASS")


fn main() raises:
    print("test_generation:")

    test_repetition_penalty_basic()
    test_repetition_penalty_negative_logits()
    test_repetition_penalty_no_effect_at_1()
    test_frequency_penalty()
    test_presence_penalty()
    test_should_stop()
    test_generation_config()
    test_generation_config_copy()
    test_beam_entry()
    test_beam_search_step()
    test_select_top_beams()
    test_repetition_penalty_changes_sampling()

    print("ALL PASSED")
