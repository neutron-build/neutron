# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Unified Generation Pipeline
# ===----------------------------------------------------------------------=== #

"""Text-in → text-out generation pipeline.

Combines Model + BPETokenizer + Sampler + penalties into a single
`pipeline_generate()` call. Supports chat templates for instruct models.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.q_kv_cache import MultiLayerQ8KVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.sampler import Sampler, SamplerConfig
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    apply_frequency_penalty,
    should_stop,
)
from neutron_mojo.nn.tokenizer import BPETokenizer


# ===----------------------------------------------------------------------=== #
# Pipeline Config
# ===----------------------------------------------------------------------=== #

struct PipelineConfig(Copyable, Movable):
    """Configuration for the generation pipeline."""
    var max_new_tokens: Int
    var sampler_config: SamplerConfig
    var repetition_penalty: Float32
    var frequency_penalty: Float32
    var presence_penalty: Float32
    var add_bos: Bool
    var chat_template: String  # "none", "llama", "chatml"
    var system_prompt: String
    var use_q8_cache: Bool  # Use Q8-quantized KV cache (~4x memory reduction)

    fn __init__(out self):
        self.max_new_tokens = 128
        self.sampler_config = SamplerConfig()
        self.repetition_penalty = 1.0
        self.frequency_penalty = 0.0
        self.presence_penalty = 0.0
        self.add_bos = True
        self.chat_template = String("none")
        self.system_prompt = String("")
        self.use_q8_cache = False

    fn __copyinit__(out self, existing: Self):
        self.max_new_tokens = existing.max_new_tokens
        self.sampler_config = existing.sampler_config.copy()
        self.repetition_penalty = existing.repetition_penalty
        self.frequency_penalty = existing.frequency_penalty
        self.presence_penalty = existing.presence_penalty
        self.add_bos = existing.add_bos
        self.chat_template = existing.chat_template
        self.system_prompt = existing.system_prompt
        self.use_q8_cache = existing.use_q8_cache

    fn __moveinit__(out self, deinit other: Self):
        self.max_new_tokens = other.max_new_tokens
        self.sampler_config = other.sampler_config.copy()
        self.repetition_penalty = other.repetition_penalty
        self.frequency_penalty = other.frequency_penalty
        self.presence_penalty = other.presence_penalty
        self.add_bos = other.add_bos
        self.chat_template = other.chat_template^
        self.system_prompt = other.system_prompt^
        self.use_q8_cache = other.use_q8_cache


# ===----------------------------------------------------------------------=== #
# Chat Template Formatting
# ===----------------------------------------------------------------------=== #

fn format_llama(prompt: String, system_prompt: String) -> String:
    """Format prompt using Llama instruct template.

    Args:
        prompt: User message.
        system_prompt: Optional system message.

    Returns:
        Formatted prompt string.
    """
    if len(system_prompt) > 0:
        return "<<SYS>>\n" + system_prompt + "\n<</SYS>>\n\n[INST] " + prompt + " [/INST]"
    return "[INST] " + prompt + " [/INST]"


fn format_chatml(prompt: String, system_prompt: String) -> String:
    """Format prompt using ChatML template.

    Args:
        prompt: User message.
        system_prompt: Optional system message.

    Returns:
        Formatted prompt string.
    """
    var result = String("")
    if len(system_prompt) > 0:
        result += "<|im_start|>system\n" + system_prompt + "<|im_end|>\n"
    result += "<|im_start|>user\n" + prompt + "<|im_end|>\n<|im_start|>assistant\n"
    return result^


fn _apply_template(prompt: String, config: PipelineConfig) -> String:
    """Apply chat template based on config.

    Args:
        prompt: Raw user prompt.
        config: Pipeline config with template selection.

    Returns:
        Formatted prompt (or original if template="none").
    """
    if config.chat_template == "llama":
        return format_llama(prompt, config.system_prompt)
    elif config.chat_template == "chatml":
        return format_chatml(prompt, config.system_prompt)
    return prompt


# ===----------------------------------------------------------------------=== #
# Pipeline Generate
# ===----------------------------------------------------------------------=== #

fn pipeline_generate(
    model: Model,
    tokenizer: BPETokenizer,
    prompt: String,
    config: PipelineConfig,
) raises -> String:
    """Generate text from a prompt using the full pipeline.

    Steps:
        1. Apply chat template if configured
        2. Encode prompt with tokenizer
        3. Create KV cache and RoPE table
        4. Prefill all prompt tokens
        5. Decode loop with penalties and sampling
        6. Decode generated tokens back to text

    Args:
        model: The language model.
        tokenizer: BPE tokenizer.
        prompt: Input text prompt.
        config: Pipeline configuration.

    Returns:
        Generated text string.
    """
    var p = model.params.copy()

    # 1. Apply chat template
    var formatted = _apply_template(prompt, config)

    # 2. Encode
    var input_ids = tokenizer.encode_with_special(formatted, add_bos=config.add_bos)

    # 3. Create infrastructure
    var total_len = len(input_ids) + config.max_new_tokens
    var rope = RoPETable(
        head_dim=p.head_dim,
        max_seq_len=total_len,
        theta=p.rope_theta,
    )
    var sampler = Sampler(config.sampler_config)

    # 4. Create cache and prefill
    var logits = Tensor[DType.float32](Shape(p.vocab_size))

    if config.use_q8_cache:
        # Q8-quantized KV cache path (~4x memory reduction)
        var q8cache = MultiLayerQ8KVCache(
            num_layers=p.num_layers, max_seq_len=total_len,
            num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
        )
        for i in range(len(input_ids)):
            logits = model.forward_q8cache(input_ids[i], q8cache, rope, pos=i)

        # 5. Decode loop
        var generated = List[Int]()
        var stop_tokens = List[Int]()
        if tokenizer.eos_id >= 0:
            stop_tokens.append(tokenizer.eos_id)

        for step in range(config.max_new_tokens):
            if config.repetition_penalty > 1.0:
                apply_repetition_penalty(logits, p.vocab_size, generated, config.repetition_penalty)
            if config.frequency_penalty != 0.0 or config.presence_penalty != 0.0:
                apply_frequency_penalty(logits, p.vocab_size, generated, config.frequency_penalty, config.presence_penalty)
            var next_token = sampler.sample(logits, p.vocab_size)
            if should_stop(next_token, stop_tokens):
                break
            generated.append(next_token)
            var pos = len(input_ids) + step
            logits = model.forward_q8cache(next_token, q8cache, rope, pos=pos)

        return tokenizer.decode(generated)
    else:
        # FP32 KV cache path — uses batch prefill for prompt tokens
        var cache = MultiLayerKVCache(
            num_layers=p.num_layers, max_seq_len=total_len,
            num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
        )

        # 4. Batch prefill: process all prompt tokens at once
        logits = model.forward_prefill(input_ids, cache, rope)

        # 5. Decode loop (single-token autoregressive)
        var generated = List[Int]()
        var stop_tokens = List[Int]()
        if tokenizer.eos_id >= 0:
            stop_tokens.append(tokenizer.eos_id)

        for step in range(config.max_new_tokens):
            if config.repetition_penalty > 1.0:
                apply_repetition_penalty(logits, p.vocab_size, generated, config.repetition_penalty)
            if config.frequency_penalty != 0.0 or config.presence_penalty != 0.0:
                apply_frequency_penalty(logits, p.vocab_size, generated, config.frequency_penalty, config.presence_penalty)
            var next_token = sampler.sample(logits, p.vocab_size)
            if should_stop(next_token, stop_tokens):
                break
            generated.append(next_token)
            var pos = len(input_ids) + step
            logits = model.forward(next_token, cache, rope, pos=pos)

        return tokenizer.decode(generated)


# ===----------------------------------------------------------------------=== #
# Helper Configs
# ===----------------------------------------------------------------------=== #

fn default_pipeline_config() -> PipelineConfig:
    """Create a default pipeline config (greedy, no template, 128 tokens).

    Returns:
        PipelineConfig with defaults.
    """
    return PipelineConfig()


fn chat_pipeline_config(template: String) -> PipelineConfig:
    """Create a chat-oriented pipeline config.

    Uses temperature=0.7, top_p=0.9, top_k=40, repetition_penalty=1.1.

    Args:
        template: Chat template name ("llama" or "chatml").

    Returns:
        Configured PipelineConfig.
    """
    var config = PipelineConfig()
    config.sampler_config.temperature = 0.7
    config.sampler_config.top_p = 0.9
    config.sampler_config.top_k = 40
    config.repetition_penalty = 1.1
    config.chat_template = template
    return config^
