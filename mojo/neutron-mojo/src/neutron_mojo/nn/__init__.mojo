# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Neural Network Components
# ===----------------------------------------------------------------------=== #

"""Neural network building blocks for transformer inference."""

from neutron_mojo.nn.rope import RoPETable, apply_rope, apply_rope_single_head, apply_rope_batch
from neutron_mojo.nn.kv_cache import KVCache, MultiLayerKVCache
from neutron_mojo.nn.attention import (
    gqa_attention,
    gqa_attention_direct,
    paged_gqa_attention,
    gqa_attention_prefill,
    mha_attention,
    attention_single_head,
    softmax_inplace,
    apply_causal_mask,
)
from neutron_mojo.nn.paged_kv_cache import (
    PageAllocator,
    PageTable,
    PagedKVCache,
)
from neutron_mojo.nn.paged_forward import (
    paged_forward,
    paged_forward_layer,
    paged_generate,
    paged_q8_forward,
    paged_q8_forward_layer,
    paged_q8_generate,
    paged_q4_forward,
    paged_q4_forward_layer,
    paged_q4_generate,
)
from neutron_mojo.nn.transformer import (
    linear,
    TransformerWeights,
    transformer_block,
)
from neutron_mojo.nn.causal_lm import (
    CausalLMWeights,
    embed_token,
    compute_logits,
    argmax,
    apply_temperature,
    top_k_filter,
    generate_greedy_one_layer,
)
from neutron_mojo.nn.tokenizer import (
    MergeRule,
    BPETokenizer,
    build_byte_level_vocab,
    build_test_tokenizer,
    MergePair,
    _parse_merge_rule,
    load_gguf_tokenizer,
    load_vocab_file,
)
from neutron_mojo.nn.quantized_linear import (
    Q8Weight,
    Q4Weight,
    quantize_weight_q8,
    quantize_weight_q4,
    q8_linear,
    q4_linear,
    quantization_error,
)
from neutron_mojo.nn.model import (
    Model,
    ModelParams,
    LayerWeightOffsets,
    tiny_test_params,
    generate,
)
from neutron_mojo.nn.sampler import (
    LCG,
    SamplerConfig,
    Sampler,
    greedy_config,
    creative_config,
    random_config,
)
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    apply_frequency_penalty,
    should_stop,
    GenerationConfig,
    BeamEntry,
    beam_search_step,
    select_top_beams,
)
from neutron_mojo.nn.q_kv_cache import (
    Q8KVCache,
    MultiLayerQ8KVCache,
    QuantResult,
    quantize_vector_q8,
    q8_attention_single_head,
    q8_gqa_attention,
)
from neutron_mojo.nn.fused_attention import (
    fused_attention_head,
    fused_gqa_attention,
    fused_q8_attention_head,
    fused_q8_gqa_attention,
)
from neutron_mojo.nn.sliding_window import (
    SlidingWindowKVCache,
    sliding_window_attention_head,
    sliding_window_gqa_attention,
    windowed_fused_attention_head,
)
from neutron_mojo.nn.moe import (
    MoEConfig,
    MoERouter,
    RoutingResult,
    ExpertWeights,
    expert_ffn,
    moe_forward,
    compute_load_balance_loss,
)
from neutron_mojo.nn.lora import (
    LoRAConfig,
    LoRAWeight,
    lora_forward,
    lora_linear,
    merge_lora,
    unmerge_lora,
)
from neutron_mojo.nn.speculative import (
    SpeculativeResult,
    compute_probs,
    draft_greedy,
    verify_tokens,
    sample_from_probs,
    AcceptanceTracker,
)
from neutron_mojo.nn.q_model import (
    QuantizedModel,
    LayerScaleOffsets,
    quantize_from_model,
    q_generate,
)
from neutron_mojo.nn.pipeline import (
    PipelineConfig,
    pipeline_generate,
    format_llama,
    format_chatml,
    default_pipeline_config,
    chat_pipeline_config,
)
from neutron_mojo.nn.q_pipeline import (
    q_pipeline_generate,
)
from neutron_mojo.nn.q4_model import (
    Q4Model,
    quantize_from_model_q4,
    q4_generate,
)
from neutron_mojo.nn.q4_pipeline import (
    q4_pipeline_generate,
)
from neutron_mojo.nn.prefix_cache import (
    PrefixCache,
    PrefixCacheEntry,
    PrefixMatch,
    hash_token_sequence,
    tokens_match,
)
from neutron_mojo.nn.streaming import (
    TokenEvent,
    StreamingGenerator,
    streaming_collect,
)
from neutron_mojo.nn.grammar import (
    GrammarState,
    JsonFSM,
    apply_grammar_mask,
    apply_grammar_mask_full,
    advance_fsm,
    is_digit,
    is_whitespace,
    is_hex,
)
from neutron_mojo.nn.conversation import (
    ChatMessage,
    ConversationSession,
    format_conversation_llama,
    format_conversation_chatml,
    format_conversation,
    conversation_generate,
)
from neutron_mojo.nn.bench import (
    MemoryEstimate,
    estimate_memory,
    ModelInfo,
    model_info,
    BenchmarkResult,
    benchmark_inference,
    benchmark_prefill_comparison,
)
from neutron_mojo.nn.profiler import (
    ProfileResult,
    DecodeProfileResult,
    profile_forward,
    profile_decode,
)
from neutron_mojo.nn.eviction import (
    EvictionPolicy,
    AttentionScoreTracker,
    no_eviction,
    streaming_policy,
    h2o_policy,
    streaming_evict_layer,
    streaming_evict,
    h2o_compact_layer,
    h2o_evict,
    should_evict,
    evict_if_needed,
)
from neutron_mojo.nn.mixed_quant import (
    LayerSensitivity,
    MixedQuantModel,
    measure_layer_sensitivity,
    analyze_sensitivity,
    auto_calibrate,
    auto_quantize,
    quantize_mixed,
    mixed_generate,
)
from neutron_mojo.nn.mixed_pipeline import (
    mixed_pipeline_generate,
)
