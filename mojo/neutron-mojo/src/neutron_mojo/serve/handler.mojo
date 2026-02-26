# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Inference Request Handler
# ===----------------------------------------------------------------------=== #

"""Request/response types and handler for model inference serving.

Core serving logic: takes an InferenceRequest, runs the pipeline,
and returns an InferenceResponse with generated text and stats.

Supports both FP32 Model and Q8 QuantizedModel inference.
"""

from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.q_model import QuantizedModel
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig, pipeline_generate
from neutron_mojo.nn.q_pipeline import q_pipeline_generate
from neutron_mojo.nn.sampler import SamplerConfig
from neutron_mojo.nn.conversation import ConversationSession, conversation_generate


# ===----------------------------------------------------------------------=== #
# Request / Response
# ===----------------------------------------------------------------------=== #

struct InferenceRequest(Copyable, Movable):
    """Inference request with all generation parameters."""
    var prompt: String
    var max_tokens: Int
    var temperature: Float32
    var top_k: Int
    var top_p: Float32
    var repetition_penalty: Float32
    var frequency_penalty: Float32
    var presence_penalty: Float32
    var chat_template: String
    var system_prompt: String
    var use_q8_cache: Bool
    var request_id: String

    fn __init__(out self):
        self.prompt = String("")
        self.max_tokens = 128
        self.temperature = 1.0
        self.top_k = 0
        self.top_p = 1.0
        self.repetition_penalty = 1.0
        self.frequency_penalty = 0.0
        self.presence_penalty = 0.0
        self.chat_template = String("none")
        self.system_prompt = String("")
        self.use_q8_cache = False
        self.request_id = String("")

    fn __init__(out self, prompt: String):
        self.prompt = prompt
        self.max_tokens = 128
        self.temperature = 1.0
        self.top_k = 0
        self.top_p = 1.0
        self.repetition_penalty = 1.0
        self.frequency_penalty = 0.0
        self.presence_penalty = 0.0
        self.chat_template = String("none")
        self.system_prompt = String("")
        self.use_q8_cache = False
        self.request_id = String("")

    fn __copyinit__(out self, existing: Self):
        self.prompt = existing.prompt
        self.max_tokens = existing.max_tokens
        self.temperature = existing.temperature
        self.top_k = existing.top_k
        self.top_p = existing.top_p
        self.repetition_penalty = existing.repetition_penalty
        self.frequency_penalty = existing.frequency_penalty
        self.presence_penalty = existing.presence_penalty
        self.chat_template = existing.chat_template
        self.system_prompt = existing.system_prompt
        self.use_q8_cache = existing.use_q8_cache
        self.request_id = existing.request_id

    fn __moveinit__(out self, deinit other: Self):
        self.prompt = other.prompt^
        self.max_tokens = other.max_tokens
        self.temperature = other.temperature
        self.top_k = other.top_k
        self.top_p = other.top_p
        self.repetition_penalty = other.repetition_penalty
        self.frequency_penalty = other.frequency_penalty
        self.presence_penalty = other.presence_penalty
        self.chat_template = other.chat_template^
        self.system_prompt = other.system_prompt^
        self.use_q8_cache = other.use_q8_cache
        self.request_id = other.request_id^

    fn to_pipeline_config(self) -> PipelineConfig:
        """Convert request parameters to PipelineConfig."""
        var cfg = PipelineConfig()
        cfg.max_new_tokens = self.max_tokens
        cfg.repetition_penalty = self.repetition_penalty
        cfg.frequency_penalty = self.frequency_penalty
        cfg.presence_penalty = self.presence_penalty
        cfg.chat_template = self.chat_template
        cfg.system_prompt = self.system_prompt
        cfg.use_q8_cache = self.use_q8_cache

        var sc = SamplerConfig()
        sc.temperature = self.temperature
        sc.top_k = self.top_k
        sc.top_p = self.top_p
        cfg.sampler_config = sc.copy()

        return cfg^


struct InferenceResponse(Copyable, Movable):
    """Inference response with generated text and performance stats."""
    var text: String
    var request_id: String
    var tokens_generated: Int
    var prompt_tokens: Int
    var elapsed_ms: Int
    var tokens_per_sec: Float64
    var error: String

    fn __init__(out self):
        self.text = String("")
        self.request_id = String("")
        self.tokens_generated = 0
        self.prompt_tokens = 0
        self.elapsed_ms = 0
        self.tokens_per_sec = 0.0
        self.error = String("")

    fn __copyinit__(out self, existing: Self):
        self.text = existing.text
        self.request_id = existing.request_id
        self.tokens_generated = existing.tokens_generated
        self.prompt_tokens = existing.prompt_tokens
        self.elapsed_ms = existing.elapsed_ms
        self.tokens_per_sec = existing.tokens_per_sec
        self.error = existing.error

    fn __moveinit__(out self, deinit other: Self):
        self.text = other.text^
        self.request_id = other.request_id^
        self.tokens_generated = other.tokens_generated
        self.prompt_tokens = other.prompt_tokens
        self.elapsed_ms = other.elapsed_ms
        self.tokens_per_sec = other.tokens_per_sec
        self.error = other.error^

    fn is_error(self) -> Bool:
        """Check if response contains an error."""
        return len(self.error) > 0


fn make_success_response(text: String, request_id: String, tokens: Int,
                         prompt_tokens: Int, elapsed_ms: Int,
                         tps: Float64) -> InferenceResponse:
    """Create a successful response."""
    var resp = InferenceResponse()
    resp.text = text
    resp.request_id = request_id
    resp.tokens_generated = tokens
    resp.prompt_tokens = prompt_tokens
    resp.elapsed_ms = elapsed_ms
    resp.tokens_per_sec = tps
    return resp^


fn make_error_response(msg: String, request_id: String) -> InferenceResponse:
    """Create an error response."""
    var resp = InferenceResponse()
    resp.error = msg
    resp.request_id = request_id
    return resp^


# ===----------------------------------------------------------------------=== #
# FP32 Handler
# ===----------------------------------------------------------------------=== #

fn handle_inference_request(
    model: Model,
    tokenizer: BPETokenizer,
    request: InferenceRequest,
) raises -> InferenceResponse:
    """Handle an inference request using FP32 model.

    Args:
        model: FP32 language model.
        tokenizer: BPE tokenizer.
        request: Inference request with prompt and parameters.

    Returns:
        InferenceResponse with generated text and stats.
    """
    var cfg = request.to_pipeline_config()

    # Count prompt tokens for stats
    var prompt_ids = tokenizer.encode_with_special(request.prompt, add_bos=True)
    var prompt_token_count = len(prompt_ids)

    var start = perf_counter_ns()
    var text = pipeline_generate(model, tokenizer, request.prompt, cfg)
    var elapsed_ns = perf_counter_ns() - start

    var elapsed_ms = Int(Float64(elapsed_ns) / 1_000_000.0)
    var gen_tokens = request.max_tokens  # Upper bound estimate
    var tps: Float64 = 0.0
    if elapsed_ns > 0:
        tps = Float64(gen_tokens) / (Float64(elapsed_ns) / 1_000_000_000.0)

    return make_success_response(
        text, request.request_id, gen_tokens,
        prompt_token_count, elapsed_ms, tps,
    )


# ===----------------------------------------------------------------------=== #
# Q8 Handler
# ===----------------------------------------------------------------------=== #

fn handle_q8_inference_request(
    model: QuantizedModel,
    tokenizer: BPETokenizer,
    request: InferenceRequest,
) raises -> InferenceResponse:
    """Handle an inference request using Q8 quantized model.

    Args:
        model: Q8 quantized language model.
        tokenizer: BPE tokenizer.
        request: Inference request with prompt and parameters.

    Returns:
        InferenceResponse with generated text and stats.
    """
    var cfg = request.to_pipeline_config()

    var prompt_ids = tokenizer.encode_with_special(request.prompt, add_bos=True)
    var prompt_token_count = len(prompt_ids)

    var start = perf_counter_ns()
    var text = q_pipeline_generate(model, tokenizer, request.prompt, cfg)
    var elapsed_ns = perf_counter_ns() - start

    var elapsed_ms = Int(Float64(elapsed_ns) / 1_000_000.0)
    var gen_tokens = request.max_tokens
    var tps: Float64 = 0.0
    if elapsed_ns > 0:
        tps = Float64(gen_tokens) / (Float64(elapsed_ns) / 1_000_000_000.0)

    return make_success_response(
        text, request.request_id, gen_tokens,
        prompt_token_count, elapsed_ms, tps,
    )


# ===----------------------------------------------------------------------=== #
# Batch Handler
# ===----------------------------------------------------------------------=== #

fn handle_batch_requests(
    model: Model,
    tokenizer: BPETokenizer,
    requests: List[InferenceRequest],
) raises -> List[InferenceResponse]:
    """Process a batch of inference requests sequentially.

    Args:
        model: FP32 language model.
        tokenizer: BPE tokenizer.
        requests: List of inference requests.

    Returns:
        List of responses, one per request.
    """
    var responses = List[InferenceResponse]()

    for i in range(len(requests)):
        var resp = handle_inference_request(model, tokenizer, requests[i])
        responses.append(resp^)

    return responses^


fn handle_q8_batch_requests(
    model: QuantizedModel,
    tokenizer: BPETokenizer,
    requests: List[InferenceRequest],
) raises -> List[InferenceResponse]:
    """Process a batch of Q8 inference requests sequentially.

    Args:
        model: Q8 quantized language model.
        tokenizer: BPE tokenizer.
        requests: List of inference requests.

    Returns:
        List of responses, one per request.
    """
    var responses = List[InferenceResponse]()

    for i in range(len(requests)):
        var resp = handle_q8_inference_request(model, tokenizer, requests[i])
        responses.append(resp^)

    return responses^


# ===----------------------------------------------------------------------=== #
# Conversation Handler
# ===----------------------------------------------------------------------=== #

fn handle_conversation_request(
    model: Model,
    tokenizer: BPETokenizer,
    mut session: ConversationSession,
    request: InferenceRequest,
) raises -> InferenceResponse:
    """Handle a multi-turn conversation request.

    Adds the request prompt as a user message, generates a response
    using the full conversation history, and adds the response as
    an assistant message.

    Args:
        model: FP32 language model.
        tokenizer: BPE tokenizer.
        session: Conversation session (modified in-place with new messages).
        request: Inference request with the user's latest message.

    Returns:
        InferenceResponse with generated text and stats.
    """
    # Add user message to session
    session.add_user_message(request.prompt)

    var cfg = request.to_pipeline_config()

    var prompt_ids = tokenizer.encode_with_special(request.prompt, add_bos=True)
    var prompt_token_count = len(prompt_ids)

    var start = perf_counter_ns()
    var text = conversation_generate(model, tokenizer, session, cfg)
    var elapsed_ns = perf_counter_ns() - start

    # Add assistant reply to session
    session.add_assistant_message(text)

    var elapsed_ms = Int(Float64(elapsed_ns) / 1_000_000.0)
    var gen_tokens = request.max_tokens
    var tps: Float64 = 0.0
    if elapsed_ns > 0:
        tps = Float64(gen_tokens) / (Float64(elapsed_ns) / 1_000_000_000.0)

    return make_success_response(
        text, request.request_id, gen_tokens,
        prompt_token_count, elapsed_ms, tps,
    )
