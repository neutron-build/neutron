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

from std.time import perf_counter_ns
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

def _utf8_complete_prefix(bytes: List[UInt8]) raises -> Int:
    """Validate UTF-8 and retain an incomplete final code point for later tokens."""
    var p = 0
    while p < len(bytes):
        var lead = Int(bytes[p])
        var width = 1
        if lead >= 194 and lead <= 223:
            width = 2
        elif lead >= 224 and lead <= 239:
            width = 3
        elif lead >= 240 and lead <= 244:
            width = 4
        elif lead >= 128:
            raise Error("Generated token bytes are invalid UTF-8")
        var available = min(width, len(bytes) - p)
        for j in range(1, available):
            var tail = Int(bytes[p + j])
            if tail < 128 or tail > 191:
                raise Error("Generated token bytes are invalid UTF-8")
            if j == 1 and ((lead == 224 and tail < 160) or (lead == 237 and tail >= 160) or (lead == 240 and tail < 144) or (lead == 244 and tail >= 144)):
                raise Error("Generated UTF-8 contains an invalid code point")
        if available < width:
            return p
        p += width
    return p


struct TokenEvent(Copyable, Movable, ImplicitlyCopyable):
    """A single token emission from streaming generation.

    Contains the decoded text, token ID, and timing information.
    """
    var text: String          # Decoded token text
    var token_id: Int         # Raw token ID
    var position: Int         # Position in the sequence (0-indexed from generation start)
    var is_eos: Bool          # Whether this is the end-of-sequence token
    var elapsed_ns: UInt       # Nanoseconds since generation started

    def __init__(out self):
        self.text = String("")
        self.token_id = -1
        self.position = 0
        self.is_eos = False
        self.elapsed_ns = UInt(0)

    def __init__(out self, text: String, token_id: Int, position: Int,
                is_eos: Bool, elapsed_ns: UInt):
        self.text = text
        self.token_id = token_id
        self.position = position
        self.is_eos = is_eos
        self.elapsed_ns = elapsed_ns

    def __init__(out self, *, copy: Self):
        self.text = copy.text
        self.token_id = copy.token_id
        self.position = copy.position
        self.is_eos = copy.is_eos
        self.elapsed_ns = copy.elapsed_ns

    def __init__(out self, *, deinit move: Self):
        self.text = move.text^
        self.token_id = move.token_id^
        self.position = move.position^
        self.is_eos = move.is_eos^
        self.elapsed_ns = move.elapsed_ns^

    def tokens_per_sec(self) -> Float64:
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
    var emitted_bytes: Int
    var step: Int
    var finished: Bool
    var prefilled: Bool
    var input_ids: List[Int]
    var start_ns: UInt

    def __init__(out self, var model: Model, var tokenizer: BPETokenizer,
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
        self.emitted_bytes = 0

        var total_len = self.config.validate_request(self.input_ids, p, p.max_seq_len)

        self.cache = MultiLayerKVCache(
            num_layers=p.num_layers, max_seq_len=total_len,
            num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
        )
        self.rope = RoPETable(
            head_dim=p.head_dim, max_seq_len=total_len, theta=p.rope_theta, rotary_dim=p.rotary_dim(),
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
        self.finished = self.config.max_new_tokens == 0
        self.prefilled = False
        self.start_ns = UInt(0)

    def __init__(out self, *, deinit move: Self):
        self.model = move.model^
        self.tokenizer = move.tokenizer^
        self.config = move.config^
        self.cache = move.cache^
        self.rope = move.rope^
        self.sampler = move.sampler^
        self.logits = move.logits^
        self.generated = move.generated^
        self.stop_tokens = move.stop_tokens^
        self.input_ids = move.input_ids^
        self.input_len = move.input_len^
        self.emitted_bytes = move.emitted_bytes^
        self.step = move.step^
        self.finished = move.finished^
        self.prefilled = move.prefilled^
        self.start_ns = move.start_ns^

    def is_finished(self) -> Bool:
        """Check if generation is complete."""
        return self.finished

    def tokens_generated(self) -> Int:
        """Number of tokens generated so far."""
        return len(self.generated)

    def get_text(self) raises -> String:
        """Get all generated text so far."""
        return self.tokenizer.decode(self.generated)

    def next_token(mut self) raises -> TokenEvent:
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
            self.start_ns = UInt(perf_counter_ns())
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
            var bytes = self.tokenizer.decode_bytes(self.generated)
            if _utf8_complete_prefix(bytes) != len(bytes):
                raise Error("Generation ended with incomplete UTF-8")
            self.finished = True
            var elapsed = UInt(perf_counter_ns()) - self.start_ns
            return TokenEvent(String(""), next_tok, self.step, True, elapsed)

        # Record and advance
        self.generated.append(next_tok)
        var bytes = self.tokenizer.decode_bytes(self.generated)
        var complete = _utf8_complete_prefix(bytes)
        if self.step + 1 >= self.config.max_new_tokens and complete != len(bytes):
            raise Error("Generation budget ended with incomplete UTF-8")
        var emitted = List[UInt8]()
        for i in range(self.emitted_bytes, complete):
            emitted.append(bytes[i])
        var text = String(from_utf8=emitted)
        self.emitted_bytes = complete
        var elapsed = UInt(perf_counter_ns()) - self.start_ns

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

def streaming_collect(
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
