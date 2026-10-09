# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sampling Strategies
# ===----------------------------------------------------------------------=== #

"""Token sampling strategies for autoregressive generation.

Supports greedy, top-k, top-p (nucleus), and temperature-scaled sampling.
Uses a simple LCG PRNG for reproducible random sampling.
"""

from std.math import exp, isfinite
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.io.binary_reader import _u32_to_f32


# ===----------------------------------------------------------------------=== #
# PRNG (Linear Congruential Generator)
# ===----------------------------------------------------------------------=== #

struct LCG(Copyable, Movable, ImplicitlyCopyable):
    """Simple LCG pseudo-random number generator.

    Parameters from Numerical Recipes (period 2^32).
    """
    var state: Int

    def __init__(out self, seed: Int = 42):
        self.state = seed & 0x7FFFFFFF

    def __init__(out self, *, copy: Self):
        self.state = copy.state

    def __init__(out self, *, deinit move: Self):
        self.state = move.state^

    def next_int(mut self) -> Int:
        """Generate next pseudo-random integer."""
        # LCG: state = (a * state + c) mod m
        # Using Numerical Recipes constants
        self.state = (1664525 * (self.state & 0x7FFFFFFF) + 1013904223) & 0x7FFFFFFF
        return self.state

    def next_float(mut self) -> Float32:
        """Generate next uniform random float in [0, 1)."""
        return Float32(self.next_int() >> 7) / Float32(16777216)


# ===----------------------------------------------------------------------=== #
# Sampler Config
# ===----------------------------------------------------------------------=== #

struct SamplerConfig(Copyable, Movable, ImplicitlyCopyable):
    """Configuration for token sampling."""
    var temperature: Float32    # >1 = more random, <1 = more peaked, 0 = greedy
    var top_k: Int              # 0 = disabled
    var top_p: Float32          # 1.0 = disabled, 0.9 = nucleus sampling
    var seed: Int               # PRNG seed

    def __init__(out self):
        """Default: greedy decoding."""
        self.temperature = 0.0
        self.top_k = 0
        self.top_p = 1.0
        self.seed = 42

    def __init__(out self, *, copy: Self):
        self.temperature = copy.temperature
        self.top_k = copy.top_k
        self.top_p = copy.top_p
        self.seed = copy.seed

    def __init__(out self, *, deinit move: Self):
        self.temperature = move.temperature^
        self.top_k = move.top_k^
        self.top_p = move.top_p^
        self.seed = move.seed^
    def validate(self, vocab_size: Int) raises:
        if vocab_size <= 0 or not isfinite(self.temperature) or self.temperature < 0 or self.top_k < 0 or not isfinite(self.top_p) or self.top_p <= 0 or self.top_p > 1:
            raise Error("Invalid sampling configuration")



def greedy_config() -> SamplerConfig:
    """Greedy decoding config."""
    return SamplerConfig()


def creative_config() -> SamplerConfig:
    """Creative sampling: temperature=0.8, top_p=0.9, top_k=40."""
    var c = SamplerConfig()
    c.temperature = 0.8
    c.top_k = 40
    c.top_p = 0.9
    return c^


def random_config(temperature: Float32 = 1.0, seed: Int = 42) -> SamplerConfig:
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

    def __init__(out self, config: SamplerConfig):
        self.config = config.copy()
        self.rng = LCG(config.seed)

    def __init__(out self, *, deinit move: Self):
        self.config = move.config.copy()
        self.rng = move.rng^

    def sample(mut self, logits: Tensor[DType.float32], vocab_size: Int) raises -> Int:
        """Sample a token from logits.

        Applies temperature, top-k, top-p, then samples.

        Args:
            logits: Raw logits [vocab_size].
            vocab_size: Number of tokens.

        Returns:
            Sampled token ID.
        """
        self.config.validate(vocab_size)
        if logits.numel() != vocab_size:
            raise Error("Sampler logits size mismatch")
        var has_finite = False
        var maximum = Float32(0)
        for i in range(vocab_size):
            var value = logits.data_ptr()[i]
            if isfinite(value):
                if not has_finite or value > maximum:
                    maximum = value
                has_finite = True
            elif not (value < 0):
                raise Error("Sampler refuses NaN or positive-infinite logits")
        if not has_finite:
            raise Error("Sampler has no selectable logit")
        # Greedy: just argmax
        if self.config.temperature <= 0.0:
            return self._argmax(logits, vocab_size)

        # Subtract in Float64 before scaling, so extreme finite logits or
        # a small temperature cannot create positive infinity / inf-inf.
        var scores = Tensor[DType.float32](Shape(vocab_size))
        for i in range(vocab_size):
            scores.set(i, Float32((Float64(logits.data_ptr()[i]) - Float64(maximum)) / Float64(self.config.temperature)))

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

    def _argmax(self, logits: Tensor[DType.float32], size: Int) -> Int:
        """Return index of maximum value."""
        var best = 0
        var best_val = logits.get(0)
        for i in range(1, size):
            var v = logits.get(i)
            if v > best_val:
                best_val = v
                best = i
        return best

    def _softmax(self, mut scores: Tensor[DType.float32], size: Int):
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

    def _top_k_filter(self, mut scores: Tensor[DType.float32], size: Int) raises:
        """Zero out all but top-k logits."""
        var k = self.config.top_k
        var neg_inf = _u32_to_f32(0xFF800000)

        # Find k-th largest value using k passes
        var used = Tensor[DType.float32](Shape(size))
        for j in range(size):
            used.set(j, 0.0)

        for _ in range(k):
            var best_idx = -1
            var best_val = neg_inf
            for j in range(size):
                if used.get(j) == 0.0 and scores.get(j) > best_val:
                    best_val = scores.get(j)
                    best_idx = j
            if best_idx >= 0:
                used.set(best_idx, 1.0)

        for j in range(size):
            if used.get(j) == 0.0:
                scores.set(j, neg_inf)

    def _top_p_filter(self, mut probs: Tensor[DType.float32], size: Int) raises:
        """Apply nucleus (top-p) filtering on probability distribution.

        Zeroes out tokens whose cumulative probability exceeds top_p.
        """
        # Build sorted indices by probability (descending)
        # Simple O(n^2) sort is fine for small vocabs
        var indices = List[Int]()
        for _ in range(size):
            indices.append(-1)
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
                indices[rank] = best_idx
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
            var idx = indices[rank]
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

    def _categorical_sample(mut self, probs: Tensor[DType.float32], size: Int) -> Int:
        """Sample from a categorical distribution."""
        var total = Float64(0)
        var last_positive = 0
        for i in range(size):
            var probability = Float64(probs.data_ptr()[i])
            total += probability
            if probability > 0:
                last_positive = i
        var threshold = Float64(self.rng.next_float()) * total
        var cumsum = Float64(0)
        for i in range(size):
            cumsum += Float64(probs.data_ptr()[i])
            if threshold < cumsum:
                return i
        return last_positive
