# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sampler Tests
# ===----------------------------------------------------------------------=== #

"""Tests for sampling strategies."""

from math import abs
from neutron_mojo.nn.sampler import (
    LCG,
    SamplerConfig,
    Sampler,
    greedy_config,
    creative_config,
    random_config,
)
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


fn test_lcg_deterministic() raises:
    """Test LCG produces deterministic sequence."""
    var rng1 = LCG(seed=123)
    var rng2 = LCG(seed=123)

    for _ in range(10):
        assert_true(rng1.next_int() == rng2.next_int(), "deterministic")

    print("  lcg_deterministic: PASS")


fn test_lcg_different_seeds() raises:
    """Test different seeds produce different sequences."""
    var rng1 = LCG(seed=1)
    var rng2 = LCG(seed=2)

    var same_count = 0
    for _ in range(10):
        if rng1.next_int() == rng2.next_int():
            same_count += 1

    assert_true(same_count < 5, "different seeds differ")

    print("  lcg_different_seeds: PASS")


fn test_lcg_float_range() raises:
    """Test next_float produces values in [0, 1)."""
    var rng = LCG(seed=42)
    for _ in range(100):
        var f = rng.next_float()
        assert_true(f >= 0.0 and f < 1.0, "float in [0,1)")

    print("  lcg_float_range: PASS")


fn test_greedy_sampling() raises:
    """Test greedy (temperature=0) always picks argmax."""
    var config = greedy_config()
    var sampler = Sampler(config)

    var logits = Tensor[DType.float32](Shape(5))
    logits.set(0, 1.0)
    logits.set(1, 5.0)
    logits.set(2, 3.0)
    logits.set(3, 2.0)
    logits.set(4, 4.0)

    var token = sampler.sample(logits, 5)
    assert_true(token == 1, "greedy picks argmax")

    print("  greedy_sampling: PASS")


fn test_greedy_deterministic() raises:
    """Test greedy sampling is deterministic."""
    var config = greedy_config()

    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 2.0)
    logits.set(1, 7.0)
    logits.set(2, 5.0)
    logits.set(3, 1.0)

    var s1 = Sampler(config)
    var s2 = Sampler(config)
    assert_true(s1.sample(logits, 4) == s2.sample(logits, 4), "deterministic")

    print("  greedy_deterministic: PASS")


fn test_temperature_sampling() raises:
    """Test that temperature > 0 enables sampling."""
    var config = random_config(temperature=1.0, seed=42)
    var sampler = Sampler(config)

    # Create peaked logits
    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 10.0)
    logits.set(1, 9.0)
    logits.set(2, 8.0)
    logits.set(3, 7.0)

    # Sample many times — should mostly get 0 but occasionally others
    var counts = Tensor[DType.float32](Shape(4))
    for i in range(4):
        counts.set(i, 0.0)

    for _ in range(100):
        var tok = sampler.sample(logits, 4)
        counts.set(tok, counts.get(tok) + 1.0)

    # Token 0 should be most common
    assert_true(counts.get(0) > counts.get(3), "highest logit sampled most")

    print("  temperature_sampling: PASS")


fn test_top_k_filtering() raises:
    """Test top-k limits the candidate set."""
    var config = random_config(temperature=1.0, seed=42)
    config.top_k = 2

    var sampler = Sampler(config)

    var logits = Tensor[DType.float32](Shape(5))
    logits.set(0, 1.0)
    logits.set(1, 10.0)
    logits.set(2, 2.0)
    logits.set(3, 9.0)
    logits.set(4, 3.0)

    # Sample many times — should only see tokens 1 and 3
    var saw_other = False
    for _ in range(50):
        var tok = sampler.sample(logits, 5)
        if tok != 1 and tok != 3:
            saw_other = True

    assert_true(not saw_other, "top-k=2 only samples from top 2")

    print("  top_k_filtering: PASS")


fn test_top_p_filtering() raises:
    """Test top-p (nucleus) sampling."""
    var config = random_config(temperature=1.0, seed=42)
    config.top_p = 0.5  # Only keep tokens summing to 50% probability

    var sampler = Sampler(config)

    # Create logits where one token dominates
    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 10.0)   # This will have >90% probability
    logits.set(1, 1.0)
    logits.set(2, 1.0)
    logits.set(3, 1.0)

    # With top_p=0.5, only token 0 should be sampled
    for _ in range(20):
        var tok = sampler.sample(logits, 4)
        assert_true(tok == 0, "top_p=0.5 with dominant token")

    print("  top_p_filtering: PASS")


fn test_config_presets() raises:
    """Test config presets."""
    var g = greedy_config()
    assert_true(g.temperature <= 0.0, "greedy temp=0")

    var c = creative_config()
    assert_near(c.temperature, 0.8, 0.01, "creative temp")
    assert_true(c.top_k == 40, "creative top_k")
    assert_near(c.top_p, 0.9, 0.01, "creative top_p")

    var r = random_config(temperature=1.5, seed=99)
    assert_near(r.temperature, 1.5, 0.01, "random temp")
    assert_true(r.seed == 99, "random seed")

    print("  config_presets: PASS")


fn test_sampler_reproducible() raises:
    """Test that same seed produces same sequence."""
    var config = random_config(temperature=0.8, seed=12345)

    var logits = Tensor[DType.float32](Shape(10))
    for i in range(10):
        logits.set(i, Float32(i) * 0.5)

    var s1 = Sampler(config)
    var s2 = Sampler(config)

    var seq1 = List[Int]()
    var seq2 = List[Int]()
    for _ in range(20):
        seq1.append(s1.sample(logits, 10))
        seq2.append(s2.sample(logits, 10))

    for i in range(20):
        assert_true(seq1[i] == seq2[i], "reproducible at step " + String(i))

    print("  sampler_reproducible: PASS")


fn test_high_temperature_diversity() raises:
    """Test that high temperature increases diversity."""
    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 5.0)
    logits.set(1, 4.0)
    logits.set(2, 3.0)
    logits.set(3, 2.0)

    # Low temperature — should be very peaked
    var low_config = random_config(temperature=0.1, seed=42)
    var low_sampler = Sampler(low_config)
    var low_unique = Tensor[DType.float32](Shape(4))
    for i in range(4):
        low_unique.set(i, 0.0)
    for _ in range(50):
        var tok = low_sampler.sample(logits, 4)
        low_unique.set(tok, 1.0)

    var low_count = 0
    for i in range(4):
        if low_unique.get(i) > 0.5:
            low_count += 1

    # High temperature — should hit more tokens
    var high_config = random_config(temperature=2.0, seed=42)
    var high_sampler = Sampler(high_config)
    var high_unique = Tensor[DType.float32](Shape(4))
    for i in range(4):
        high_unique.set(i, 0.0)
    for _ in range(50):
        var tok = high_sampler.sample(logits, 4)
        high_unique.set(tok, 1.0)

    var high_count = 0
    for i in range(4):
        if high_unique.get(i) > 0.5:
            high_count += 1

    assert_true(high_count >= low_count, "high temp more diverse")

    print("  high_temperature_diversity: PASS")


fn main() raises:
    print("test_sampler:")

    test_lcg_deterministic()
    test_lcg_different_seeds()
    test_lcg_float_range()
    test_greedy_sampling()
    test_greedy_deterministic()
    test_temperature_sampling()
    test_top_k_filtering()
    test_top_p_filtering()
    test_config_presets()
    test_sampler_reproducible()
    test_high_temperature_diversity()

    print("ALL PASSED")
