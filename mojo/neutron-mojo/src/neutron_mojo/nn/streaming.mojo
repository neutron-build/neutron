# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Streaming Token Generation
# ===----------------------------------------------------------------------=== #

"""Iterator-based streaming generation for token-by-token output.

Instead of buffering all tokens and returning a string, StreamingGenerator
produces tokens one at a time via next_token(). This enables:
- Real-time output display (CLI, UI)
- Early stopping based on content
- Grammar-constrained generation with per-token FSM updates
- Token-level timing/profiling

Usage:
    var gen = StreamingGenerator(model, tokenizer, "prompt", config)
    while not gen.is_finished():
        var event = gen.next_token()
        print(event.text, end="")
"""

from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.sampler import Sampler, SamplerConfig
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    apply_frequency_penalty,
    should_stop,
)
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig, _apply_template


# ===----------------------------------------------------------------------=== #
# Token Event
# ===----------------------------------------------------------------------=== #

struct TokenEvent(Copyable, Movable):
    """A single token emission from streaming generation.

    Contains the decoded text, token ID, and timing information.
    """
    var text: String          # Decoded token text
    var token_id: Int         # Raw token ID
    var position: Int         # Position in the sequence (0-indexed from generation start)
    var is_eos: Bool          # Whether this is the end-of-sequence token
    var elapsed_ns: UInt       # Nanoseconds since generation started

    fn __init__(out self):
        self.text = String("")
        self.token_id = -1
        self.position = 0
        self.is_eos = False
        self.elapsed_ns = UInt(0)

    fn __init__(out self, text: String, token_id: Int, position: Int,
                is_eos: Bool, elapsed_ns: UInt):
        self.text = text
        self.token_id = token_id
        self.position = position
        self.is_eos = is_eos
        self.elapsed_ns = elapsed_ns

    fn __copyinit__(out self, existing: Self):
        self.text = existing.text
        self.token_id = existing.token_id
        self.position = existing.position
        self.is_eos = existing.is_eos
        self.elapsed_ns = existing.elapsed_ns

    fn __moveinit__(out self, deinit other: Self):
        self.text = other.text^
        self.token_id = other.token_id
        self.position = other.position
        self.is_eos = other.is_eos
        self.elapsed_ns = other.elapsed_ns

    fn tokens_per_sec(self) -> Float64:
        """Compute tokens/sec based on elapsed time and position."""
        if self.elapsed_ns == 0 or self.position <= 0:
            return 0.0
        return Float64(self.position) / (Float64(Int(self.elapsed_ns)) / 1_000_000_000.0)


# ===----------------------------------------------------------------------=== #
# Streaming Generator
# ===----------------------------------------------------------------------=== #

struct StreamingGenerator(Movable):
    """Iterator-style streaming generator for FP32 models.

    Call next_token() repeatedly until is_finished() returns True.
    Each call runs one forward pass, samples a token, and returns a TokenEvent.
    """
    var model: Model
    var tokenizer: BPETokenizer
    var config: PipelineConfig
    var cache: MultiLayerKVCache
    var rope: RoPETable
    var sampler: Sampler
    var logits: Tensor[DType.float32]
    var generated: List[Int]
    var stop_tokens: List[Int]
    var input_len: Int
    var step: Int
    var finished: Bool
    var prefilled: Bool
    var input_ids: List[Int]
    var start_ns: UInt

    fn __init__(out self, var model: Model, var tokenizer: BPETokenizer,
                prompt: String, config: PipelineConfig) raises:
        """Initialize streaming generator with model and prompt.

        Encodes the prompt but does NOT prefill yet. Prefill happens
        on the first call to next_token().

        Args:
            model: FP32 language model (consumed).
            tokenizer: BPE tokenizer (consumed).
            prompt: Input text prompt.
            config: Pipeline configuration.
        """
        var p = model.params.copy()
        self.config = PipelineConfig()
        self.config.max_new_tokens = config.max_new_tokens
        self.config.sampler_config = config.sampler_config.copy()
        self.config.repetition_penalty = config.repetition_penalty
        self.config.frequency_penalty = config.frequency_penalty
        self.config.presence_penalty = config.presence_penalty
        self.config.add_bos = config.add_bos
        self.config.chat_template = config.chat_template
        self.config.system_prompt = config.system_prompt
        self.config.use_q8_cache = config.use_q8_cache

        # Apply template and encode
        var formatted = _apply_template(prompt, self.config)
        self.input_ids = tokenizer.encode_with_special(formatted, add_bos=self.config.add_bos)
        self.input_len = len(self.input_ids)

        var total_len = self.input_len + self.config.max_new_tokens

        self.cache = MultiLayerKVCache(
            num_layers=p.num_layers, max_seq_len=total_len,
            num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
        )
        self.rope = RoPETable(
            head_dim=p.head_dim, max_seq_len=total_len, theta=p.rope_theta,
        )
        self.sampler = Sampler(self.config.sampler_config)
        self.logits = Tensor[DType.float32](Shape(p.vocab_size))
        self.generated = List[Int]()

        self.stop_tokens = List[Int]()
        if tokenizer.eos_id >= 0:
            self.stop_tokens.append(tokenizer.eos_id)

        self.model = model^
        self.tokenizer = tokenizer^
        self.step = 0
        self.finished = False
        self.prefilled = False
        self.start_ns = UInt(0)

    fn __moveinit__(out self, deinit other: Self):
        self.model = other.model^
        self.tokenizer = other.tokenizer^
        self.config = other.config^
        self.cache = other.cache^
        self.rope = other.rope^
        self.sampler = other.sampler^
        self.logits = other.logits^
        self.generated = other.generated^
        self.stop_tokens = other.stop_tokens^
        self.input_ids = other.input_ids^
        self.input_len = other.input_len
        self.step = other.step
        self.finished = other.finished
        self.prefilled = other.prefilled
        self.start_ns = other.start_ns

    fn is_finished(self) -> Bool:
        """Check if generation is complete."""
        return self.finished

    fn tokens_generated(self) -> Int:
        """Number of tokens generated so far."""
        return len(self.generated)

    fn get_text(self) -> String:
        """Get all generated text so far."""
        return self.tokenizer.decode(self.generated)

    fn next_token(mut self) raises -> TokenEvent:
        """Generate and return the next token.

        On first call, performs prefill of all prompt tokens.
        Then samples one token per call.

        Returns:
            TokenEvent with the generated token's text, ID, and timing.
        """
        if self.finished:
            return TokenEvent(String(""), -1, self.step, True, UInt(0))

        var p = self.model.params.copy()

        # Prefill on first call
        if not self.prefilled:
            self.start_ns = perf_counter_ns()
            for i in range(self.input_len):
                self.logits = self.model.forward(
                    self.input_ids[i], self.cache, self.rope, pos=i,
                )
            self.prefilled = True

        # Apply penalties
        if self.config.repetition_penalty > 1.0:
            apply_repetition_penalty(
                self.logits, p.vocab_size, self.generated,
                self.config.repetition_penalty,
            )
        if self.config.frequency_penalty != 0.0 or self.config.presence_penalty != 0.0:
            apply_frequency_penalty(
                self.logits, p.vocab_size, self.generated,
                self.config.frequency_penalty, self.config.presence_penalty,
            )

        # Sample
        var next_tok = self.sampler.sample(self.logits, p.vocab_size)

        # Check stop
        if should_stop(next_tok, self.stop_tokens):
            self.finished = True
            var elapsed = perf_counter_ns() - self.start_ns
            return TokenEvent(String(""), next_tok, self.step, True, elapsed)

        # Record and advance
        self.generated.append(next_tok)
        var text = self.tokenizer.decode_single(next_tok)
        var elapsed = perf_counter_ns() - self.start_ns

        var event = TokenEvent(text, next_tok, self.step, False, elapsed)
        self.step += 1

        # Check max tokens
        if self.step >= self.config.max_new_tokens:
            self.finished = True
        else:
            # Forward pass for next position
            var pos = self.input_len + self.step - 1
            self.logits = self.model.forward(next_tok, self.cache, self.rope, pos=pos)

        return event^


# ===----------------------------------------------------------------------=== #
# Convenience: Collect all tokens
# ===----------------------------------------------------------------------=== #

fn streaming_collect(
    var model: Model,
    var tokenizer: BPETokenizer,
    prompt: String,
    config: PipelineConfig,
) raises -> List[TokenEvent]:
    """Generate all tokens and collect them as a list of events.

    Useful for testing or when you need both streaming events and final text.

    Args:
        model: FP32 language model (consumed).
        tokenizer: BPE tokenizer (consumed).
        prompt: Input text prompt.
        config: Pipeline configuration.

    Returns:
        List of TokenEvent for each generated token.
    """
    var gen = StreamingGenerator(model^, tokenizer^, prompt, config)
    var events = List[TokenEvent]()

    while not gen.is_finished():
        var event = gen.next_token()
        events.append(event^)

    return events^
