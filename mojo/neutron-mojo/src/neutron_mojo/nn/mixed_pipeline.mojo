# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Mixed Precision Pipeline
# ===----------------------------------------------------------------------=== #

"""Text-in -> text-out generation pipeline for MixedQuantModel.

Mirrors q_pipeline.mojo but uses MixedQuantModel for per-layer Q8/Q4/FP32
inference. Reuses PipelineConfig, chat template formatting, and penalty
functions from the existing pipeline infrastructure.

Usage:
    var mixed = auto_quantize(model)
    var text = mixed_pipeline_generate(mixed, tokenizer, "Hello", config)
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.mixed_quant import MixedQuantModel
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.sampler import Sampler, SamplerConfig
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    apply_frequency_penalty,
    should_stop,
)
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig


# ===----------------------------------------------------------------------=== #
# Chat Template (duplicated from pipeline.mojo to avoid coupling)
# ===----------------------------------------------------------------------=== #

fn _apply_template(prompt: String, config: PipelineConfig) -> String:
    """Apply chat template based on config."""
    if config.chat_template == "llama":
        return _format_llama(prompt, config.system_prompt)
    elif config.chat_template == "chatml":
        return _format_chatml(prompt, config.system_prompt)
    return prompt


fn _format_llama(prompt: String, system_prompt: String) -> String:
    if len(system_prompt) > 0:
        return "<<SYS>>\n" + system_prompt + "\n<</SYS>>\n\n[INST] " + prompt + " [/INST]"
    return "[INST] " + prompt + " [/INST]"


fn _format_chatml(prompt: String, system_prompt: String) -> String:
    var result = String("")
    if len(system_prompt) > 0:
        result += "<|im_start|>system\n" + system_prompt + "<|im_end|>\n"
    result += "<|im_start|>user\n" + prompt + "<|im_end|>\n<|im_start|>assistant\n"
    return result^


# ===----------------------------------------------------------------------=== #
# Mixed Precision Pipeline Generate
# ===----------------------------------------------------------------------=== #

fn mixed_pipeline_generate(
    model: MixedQuantModel,
    tokenizer: BPETokenizer,
    prompt: String,
    config: PipelineConfig,
) raises -> String:
    """Generate text from a prompt using the mixed-precision pipeline.

    Steps:
        1. Apply chat template if configured
        2. Encode prompt with tokenizer
        3. Create KV cache and RoPE table
        4. Prefill all prompt tokens
        5. Decode loop with penalties and sampling
        6. Decode generated tokens back to text

    Args:
        model: Mixed-precision model (per-layer Q8/Q4/FP32).
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
    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=total_len,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var sampler = Sampler(config.sampler_config)

    # 4. Prefill
    var logits = Tensor[DType.float32](Shape(p.vocab_size))
    for i in range(len(input_ids)):
        logits = model.forward(input_ids[i], cache, rope, pos=i)

    # 5. Decode loop
    var generated = List[Int]()
    var stop_tokens = List[Int]()
    if tokenizer.eos_id >= 0:
        stop_tokens.append(tokenizer.eos_id)

    for step in range(config.max_new_tokens):
        if config.repetition_penalty > 1.0:
            apply_repetition_penalty(
                logits, p.vocab_size, generated, config.repetition_penalty
            )
        if config.frequency_penalty != 0.0 or config.presence_penalty != 0.0:
            apply_frequency_penalty(
                logits, p.vocab_size, generated,
                config.frequency_penalty, config.presence_penalty,
            )
        var next_token = sampler.sample(logits, p.vocab_size)
        if should_stop(next_token, stop_tokens):
            break
        generated.append(next_token)
        var pos = len(input_ids) + step
        logits = model.forward(next_token, cache, rope, pos=pos)

    # 6. Decode
    return tokenizer.decode(generated)
