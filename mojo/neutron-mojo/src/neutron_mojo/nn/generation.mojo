# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Generation Utilities
# ===----------------------------------------------------------------------=== #

"""Generation utilities: repetition penalty, stop tokens, beam search.

Extends the core sampler with production-quality generation features.
"""

from math import log, exp
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.sampler import Sampler, SamplerConfig, greedy_config


# ===----------------------------------------------------------------------=== #
# Repetition Penalty
# ===----------------------------------------------------------------------=== #

fn apply_repetition_penalty(
    mut logits: Tensor[DType.float32],
    vocab_size: Int,
    generated_tokens: List[Int],
    penalty: Float32,
):
    """Apply repetition penalty to logits based on previously generated tokens.

    For each token that has appeared in generated_tokens, divides positive
    logits by penalty and multiplies negative logits by penalty.
    This is the method from the CTRL paper (Keskar et al., 2019).

    Args:
        logits: Raw logits [vocab_size], modified in-place.
        vocab_size: Vocabulary size.
        generated_tokens: Previously generated token IDs.
        penalty: Repetition penalty factor (1.0 = no penalty, >1.0 = penalize).
    """
    if penalty <= 1.0:
        return

    for i in range(len(generated_tokens)):
        var tok = generated_tokens[i]
        if tok >= 0 and tok < vocab_size:
            var score = logits.get(tok)
            if score > 0.0:
                logits.set(tok, score / penalty)
            else:
                logits.set(tok, score * penalty)


fn apply_frequency_penalty(
    mut logits: Tensor[DType.float32],
    vocab_size: Int,
    generated_tokens: List[Int],
    frequency_penalty: Float32,
    presence_penalty: Float32,
):
    """Apply frequency and presence penalties (OpenAI-style).

    frequency_penalty: Penalizes tokens proportional to their count.
    presence_penalty: Penalizes tokens that have appeared at all.

    Formula: logit -= frequency_penalty * count + presence_penalty * (count > 0)

    Args:
        logits: Raw logits [vocab_size], modified in-place.
        vocab_size: Vocabulary size.
        generated_tokens: Previously generated token IDs.
        frequency_penalty: Penalty per occurrence (0.0 = disabled).
        presence_penalty: Flat penalty if token appeared (0.0 = disabled).
    """
    if frequency_penalty == 0.0 and presence_penalty == 0.0:
        return

    # Count occurrences
    var counts = Tensor[DType.float32](Shape(vocab_size))
    for i in range(vocab_size):
        counts.set(i, 0.0)

    for i in range(len(generated_tokens)):
        var tok = generated_tokens[i]
        if tok >= 0 and tok < vocab_size:
            counts.set(tok, counts.get(tok) + 1.0)

    for i in range(vocab_size):
        var c = counts.get(i)
        if c > 0.0:
            var penalty = frequency_penalty * c
            if presence_penalty != 0.0:
                penalty += presence_penalty
            logits.set(i, logits.get(i) - penalty)


# ===----------------------------------------------------------------------=== #
# Stop Tokens
# ===----------------------------------------------------------------------=== #

fn should_stop(token: Int, stop_tokens: List[Int]) -> Bool:
    """Check if a generated token is a stop token.

    Args:
        token: The generated token ID.
        stop_tokens: List of token IDs that signal generation should stop.

    Returns:
        True if the token is a stop token.
    """
    for i in range(len(stop_tokens)):
        if token == stop_tokens[i]:
            return True
    return False


# ===----------------------------------------------------------------------=== #
# Generation Config
# ===----------------------------------------------------------------------=== #

struct GenerationConfig(Copyable, Movable):
    """Full generation configuration combining sampling + penalties + stopping."""
    var sampler_config: SamplerConfig
    var repetition_penalty: Float32
    var frequency_penalty: Float32
    var presence_penalty: Float32
    var max_tokens: Int
    var num_stop_tokens: Int
    var stop_token_0: Int
    var stop_token_1: Int
    var stop_token_2: Int
    var stop_token_3: Int

    fn __init__(out self):
        self.sampler_config = SamplerConfig()
        self.repetition_penalty = 1.0
        self.frequency_penalty = 0.0
        self.presence_penalty = 0.0
        self.max_tokens = 256
        self.num_stop_tokens = 0
        self.stop_token_0 = -1
        self.stop_token_1 = -1
        self.stop_token_2 = -1
        self.stop_token_3 = -1

    fn __copyinit__(out self, existing: Self):
        self.sampler_config = existing.sampler_config.copy()
        self.repetition_penalty = existing.repetition_penalty
        self.frequency_penalty = existing.frequency_penalty
        self.presence_penalty = existing.presence_penalty
        self.max_tokens = existing.max_tokens
        self.num_stop_tokens = existing.num_stop_tokens
        self.stop_token_0 = existing.stop_token_0
        self.stop_token_1 = existing.stop_token_1
        self.stop_token_2 = existing.stop_token_2
        self.stop_token_3 = existing.stop_token_3

    fn __moveinit__(out self, deinit other: Self):
        self.sampler_config = other.sampler_config.copy()
        self.repetition_penalty = other.repetition_penalty
        self.frequency_penalty = other.frequency_penalty
        self.presence_penalty = other.presence_penalty
        self.max_tokens = other.max_tokens
        self.num_stop_tokens = other.num_stop_tokens
        self.stop_token_0 = other.stop_token_0
        self.stop_token_1 = other.stop_token_1
        self.stop_token_2 = other.stop_token_2
        self.stop_token_3 = other.stop_token_3

    fn add_stop_token(mut self, token_id: Int):
        """Add a stop token (up to 4)."""
        if self.num_stop_tokens == 0:
            self.stop_token_0 = token_id
        elif self.num_stop_tokens == 1:
            self.stop_token_1 = token_id
        elif self.num_stop_tokens == 2:
            self.stop_token_2 = token_id
        elif self.num_stop_tokens == 3:
            self.stop_token_3 = token_id
        if self.num_stop_tokens < 4:
            self.num_stop_tokens += 1

    fn get_stop_tokens(self) -> List[Int]:
        """Get stop tokens as a list."""
        var result = List[Int]()
        if self.num_stop_tokens > 0:
            result.append(self.stop_token_0)
        if self.num_stop_tokens > 1:
            result.append(self.stop_token_1)
        if self.num_stop_tokens > 2:
            result.append(self.stop_token_2)
        if self.num_stop_tokens > 3:
            result.append(self.stop_token_3)
        return result^

    fn is_stop_token(self, token: Int) -> Bool:
        """Check if token is a stop token."""
        if self.num_stop_tokens > 0 and token == self.stop_token_0:
            return True
        if self.num_stop_tokens > 1 and token == self.stop_token_1:
            return True
        if self.num_stop_tokens > 2 and token == self.stop_token_2:
            return True
        if self.num_stop_tokens > 3 and token == self.stop_token_3:
            return True
        return False


# ===----------------------------------------------------------------------=== #
# Beam Search
# ===----------------------------------------------------------------------=== #

struct BeamEntry(Copyable, Movable):
    """A single beam hypothesis."""
    var tokens: List[Int]
    var score: Float32
    var finished: Bool

    fn __init__(out self):
        self.tokens = List[Int]()
        self.score = 0.0
        self.finished = False

    fn __copyinit__(out self, existing: Self):
        self.tokens = List[Int]()
        for i in range(len(existing.tokens)):
            self.tokens.append(existing.tokens[i])
        self.score = existing.score
        self.finished = existing.finished

    fn __moveinit__(out self, deinit other: Self):
        self.tokens = other.tokens^
        self.score = other.score
        self.finished = other.finished


fn beam_search_step(
    logits: Tensor[DType.float32],
    vocab_size: Int,
    beams: List[BeamEntry],
    beam_width: Int,
    beam_idx: Int,
) -> List[BeamEntry]:
    """Expand one beam by considering all next tokens, return top candidates.

    Args:
        logits: Logits for this beam's current state [vocab_size].
        vocab_size: Vocabulary size.
        beams: Current beam entries (we expand beams[beam_idx]).
        beam_width: Number of candidates to keep.
        beam_idx: Which beam to expand.

    Returns:
        List of beam_width best expansions.
    """
    # Compute log-softmax for scoring
    var max_logit: Float32 = logits.get(0)
    for i in range(1, vocab_size):
        var v = logits.get(i)
        if v > max_logit:
            max_logit = v

    var log_sum_exp: Float32 = 0.0
    for i in range(vocab_size):
        log_sum_exp += Float32(exp(Float64(logits.get(i) - max_logit)))
    var log_norm = max_logit + Float32(log(Float64(log_sum_exp)))

    # Find top beam_width tokens
    var used = Tensor[DType.float32](Shape(vocab_size))
    for i in range(vocab_size):
        used.set(i, 0.0)

    var candidates = List[BeamEntry]()
    var parent = beams[beam_idx].copy()

    var k = beam_width
    if k > vocab_size:
        k = vocab_size

    for _ in range(k):
        var best_tok = -1
        var best_log_prob: Float32 = -1e30
        for j in range(vocab_size):
            if used.get(j) == 0.0:
                var lp = logits.get(j) - log_norm
                if lp > best_log_prob:
                    best_log_prob = lp
                    best_tok = j
        if best_tok >= 0:
            used.set(best_tok, 1.0)
            var entry = BeamEntry()
            for t in range(len(parent.tokens)):
                entry.tokens.append(parent.tokens[t])
            entry.tokens.append(best_tok)
            entry.score = parent.score + best_log_prob
            candidates.append(entry^)

    return candidates^


fn select_top_beams(
    candidates: List[BeamEntry],
    beam_width: Int,
) -> List[BeamEntry]:
    """Select the top beam_width entries by score.

    Args:
        candidates: All candidate beam entries.
        beam_width: How many to keep.

    Returns:
        Top beam_width entries sorted by score descending.
    """
    var result = List[BeamEntry]()
    var used = List[Int]()
    for _ in range(len(candidates)):
        used.append(0)

    var k = beam_width
    if k > len(candidates):
        k = len(candidates)

    for _ in range(k):
        var best_idx = -1
        var best_score: Float32 = -1e30
        for j in range(len(candidates)):
            if used[j] == 0 and candidates[j].score > best_score:
                best_score = candidates[j].score
                best_idx = j
        if best_idx >= 0:
            used[best_idx] = 1
            result.append(candidates[best_idx].copy())

    return result^
