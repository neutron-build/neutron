# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sampling Strategies
# ===----------------------------------------------------------------------=== #

"""Token sampling strategies for autoregressive generation.

Supports greedy, top-k, top-p (nucleus), and temperature-scaled sampling.
Uses a simple LCG PRNG for reproducible random sampling.
"""

from math import exp
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# PRNG (Linear Congruential Generator)
# ===----------------------------------------------------------------------=== #

struct LCG(Copyable, Movable):
    """Simple LCG pseudo-random number generator.

    Parameters from Numerical Recipes (period 2^32).
    """
    var state: Int

    fn __init__(out self, seed: Int = 42):
        self.state = seed

    fn __copyinit__(out self, existing: Self):
        self.state = existing.state

    fn __moveinit__(out self, deinit other: Self):
        self.state = other.state

    fn next_int(mut self) -> Int:
        """Generate next pseudo-random integer."""
        # LCG: state = (a * state + c) mod m
        # Using Numerical Recipes constants
        self.state = (1664525 * self.state + 1013904223) & 0x7FFFFFFF
        return self.state

    fn next_float(mut self) -> Float32:
        """Generate next uniform random float in [0, 1)."""
        return Float32(self.next_int()) / Float32(0x7FFFFFFF)


# ===----------------------------------------------------------------------=== #
# Sampler Config
# ===----------------------------------------------------------------------=== #

struct SamplerConfig(Copyable, Movable):
    """Configuration for token sampling."""
    var temperature: Float32    # >1 = more random, <1 = more peaked, 0 = greedy
    var top_k: Int              # 0 = disabled
    var top_p: Float32          # 1.0 = disabled, 0.9 = nucleus sampling
    var seed: Int               # PRNG seed

    fn __init__(out self):
        """Default: greedy decoding."""
        self.temperature = 0.0
        self.top_k = 0
        self.top_p = 1.0
        self.seed = 42

    fn __copyinit__(out self, existing: Self):
        self.temperature = existing.temperature
        self.top_k = existing.top_k
        self.top_p = existing.top_p
        self.seed = existing.seed

    fn __moveinit__(out self, deinit other: Self):
        self.temperature = other.temperature
        self.top_k = other.top_k
        self.top_p = other.top_p
        self.seed = other.seed


fn greedy_config() -> SamplerConfig:
    """Greedy decoding config."""
    return SamplerConfig()


fn creative_config() -> SamplerConfig:
    """Creative sampling: temperature=0.8, top_p=0.9, top_k=40."""
    var c = SamplerConfig()
    c.temperature = 0.8
    c.top_k = 40
    c.top_p = 0.9
    return c^


fn random_config(temperature: Float32 = 1.0, seed: Int = 42) -> SamplerConfig:
    """Temperature sampling config."""
    var c = SamplerConfig()
    c.temperature = temperature
    c.seed = seed
    return c^


# ===----------------------------------------------------------------------=== #
# Sampler
# ===----------------------------------------------------------------------=== #

struct Sampler(Movable):
    """Token sampler with configurable strategy."""
    var config: SamplerConfig
    var rng: LCG

    fn __init__(out self, config: SamplerConfig):
        self.config = config.copy()
        self.rng = LCG(config.seed)

    fn __moveinit__(out self, deinit other: Self):
        self.config = other.config.copy()
        self.rng = other.rng^

    fn sample(mut self, logits: Tensor[DType.float32], vocab_size: Int) raises -> Int:
        """Sample a token from logits.

        Applies temperature, top-k, top-p, then samples.

        Args:
            logits: Raw logits [vocab_size].
            vocab_size: Number of tokens.

        Returns:
            Sampled token ID.
        """
        # Greedy: just argmax
        if self.config.temperature <= 0.0:
            return self._argmax(logits, vocab_size)

        # Make a working copy of logits
        var scores = Tensor[DType.float32](Shape(vocab_size))
        for i in range(vocab_size):
            scores.set(i, logits.get(i))

        # Temperature scaling
        if self.config.temperature != 1.0:
            for i in range(vocab_size):
                scores.set(i, scores.get(i) / self.config.temperature)

        # Top-k filtering
        if self.config.top_k > 0 and self.config.top_k < vocab_size:
            self._top_k_filter(scores, vocab_size)

        # Softmax
        self._softmax(scores, vocab_size)

        # Top-p (nucleus) filtering
        if self.config.top_p < 1.0:
            self._top_p_filter(scores, vocab_size)

        # Sample from probability distribution
        return self._categorical_sample(scores, vocab_size)

    fn _argmax(self, logits: Tensor[DType.float32], size: Int) -> Int:
        """Return index of maximum value."""
        var best = 0
        var best_val = logits.get(0)
        for i in range(1, size):
            var v = logits.get(i)
            if v > best_val:
                best_val = v
                best = i
        return best

    fn _softmax(self, mut scores: Tensor[DType.float32], size: Int):
        """Apply softmax in-place."""
        var max_val = scores.get(0)
        for i in range(1, size):
            var v = scores.get(i)
            if v > max_val:
                max_val = v

        var sum_exp: Float32 = 0.0
        for i in range(size):
            var e = Float32(exp(Float64(scores.get(i) - max_val)))
            scores.set(i, e)
            sum_exp += e

        if sum_exp > 0.0:
            for i in range(size):
                scores.set(i, scores.get(i) / sum_exp)

    fn _top_k_filter(self, mut scores: Tensor[DType.float32], size: Int):
        """Zero out all but top-k logits."""
        var k = self.config.top_k
        var neg_inf: Float32 = -1e30

        # Find k-th largest value using k passes
        var used = Tensor[DType.float32](Shape(size))
        for j in range(size):
            used.set(j, 0.0)

        for _ in range(k):
            var best_idx = -1
            var best_val: Float32 = -1e30
            for j in range(size):
                if used.get(j) == 0.0 and scores.get(j) > best_val:
                    best_val = scores.get(j)
                    best_idx = j
            if best_idx >= 0:
                used.set(best_idx, 1.0)

        for j in range(size):
            if used.get(j) == 0.0:
                scores.set(j, neg_inf)

    fn _top_p_filter(self, mut probs: Tensor[DType.float32], size: Int):
        """Apply nucleus (top-p) filtering on probability distribution.

        Zeroes out tokens whose cumulative probability exceeds top_p.
        """
        # Build sorted indices by probability (descending)
        # Simple O(n^2) sort is fine for small vocabs
        var indices = Tensor[DType.float32](Shape(size))
        var sorted_probs = Tensor[DType.float32](Shape(size))
        var used = Tensor[DType.float32](Shape(size))
        for i in range(size):
            used.set(i, 0.0)

        for rank in range(size):
            var best_idx = -1
            var best_val: Float32 = -1.0
            for j in range(size):
                if used.get(j) == 0.0 and probs.get(j) > best_val:
                    best_val = probs.get(j)
                    best_idx = j
            if best_idx >= 0:
                indices.set(rank, Float32(best_idx))
                sorted_probs.set(rank, best_val)
                used.set(best_idx, 1.0)

        # Find cutoff
        var cumsum: Float32 = 0.0
        var cutoff_rank = size
        for rank in range(size):
            cumsum += sorted_probs.get(rank)
            if cumsum >= self.config.top_p:
                cutoff_rank = rank + 1
                break

        # Zero out tokens below cutoff
        var keep = Tensor[DType.float32](Shape(size))
        for i in range(size):
            keep.set(i, 0.0)
        for rank in range(cutoff_rank):
            var idx = Int(indices.get(rank))
            keep.set(idx, 1.0)

        # Renormalize
        var new_sum: Float32 = 0.0
        for i in range(size):
            if keep.get(i) == 0.0:
                probs.set(i, 0.0)
            else:
                new_sum += probs.get(i)

        if new_sum > 0.0:
            for i in range(size):
                probs.set(i, probs.get(i) / new_sum)

    fn _categorical_sample(mut self, probs: Tensor[DType.float32], size: Int) -> Int:
        """Sample from a categorical distribution."""
        var u = self.rng.next_float()
        var cumsum: Float32 = 0.0
        for i in range(size):
            cumsum += probs.get(i)
            if u < cumsum:
                return i
        return size - 1  # fallback to last token
