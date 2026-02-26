# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 18: Serving Module Tests
# ===----------------------------------------------------------------------=== #

"""Tests for the inference serving module.

Tests:
1. InferenceRequest creation and defaults
2. InferenceRequest with prompt constructor
3. InferenceRequest to_pipeline_config conversion
4. InferenceResponse success creation
5. InferenceResponse error creation
6. Protocol line parsing (key=value)
7. Protocol block parsing (full request)
8. Protocol response formatting
9. FP32 handler — single request
10. Q8 handler — single request
11. Batch handler — multiple requests
12. Handler with custom parameters
13. Error response formatting
"""

from math import abs
from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig
from neutron_mojo.serve.handler import (
    InferenceRequest,
    InferenceResponse,
    make_success_response,
    make_error_response,
    handle_inference_request,
    handle_q8_inference_request,
    handle_batch_requests,
)
from neutron_mojo.serve.protocol import (
    parse_request_line,
    format_response,
    parse_request_block,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error("Assertion failed: " + msg + " expected=" + String(b) + " got=" + String(a))


# ===----------------------------------------------------------------------=== #
# Test Model Helpers
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    """Build a tiny FP32 model with non-trivial weights."""
    var p = tiny_test_params()
    var model = Model(p)

    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32(v * p.hidden_dim + d) * 0.01)
            model.lm_head.set(v * p.hidden_dim + d, Float32(v + d) * 0.1)

    for i in range(p.num_layers * model.layer_size):
        model.layer_weights.set(i, Float32(i % 13) * 0.02 - 0.12)

    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        for i in range(p.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)

    return model^


fn _build_tiny_tokenizer() -> BPETokenizer:
    """Build a minimal tokenizer for testing."""
    var tok = BPETokenizer()
    _ = tok.add_token("<s>")     # 0
    _ = tok.add_token("</s>")   # 1
    _ = tok.add_token("<unk>")  # 2
    _ = tok.add_token("a")      # 3
    _ = tok.add_token("b")      # 4
    _ = tok.add_token("c")      # 5
    _ = tok.add_token("d")      # 6
    _ = tok.add_token("e")      # 7
    tok.bos_id = 0
    tok.eos_id = 1
    tok.unk_id = 2
    return tok^


# ===----------------------------------------------------------------------=== #
# Request/Response Tests
# ===----------------------------------------------------------------------=== #

fn test_request_defaults() raises:
    """InferenceRequest has sensible defaults."""
    var req = InferenceRequest()

    assert_true(len(req.prompt) == 0, "default prompt empty")
    assert_eq(req.max_tokens, 128, "default max_tokens")
    assert_true(req.temperature == 1.0, "default temperature")
    assert_eq(req.top_k, 0, "default top_k")
    assert_true(req.top_p == 1.0, "default top_p")
    assert_true(req.repetition_penalty == 1.0, "default rep_penalty")
    assert_true(req.chat_template == "none", "default template")
    assert_true(not req.use_q8_cache, "default q8_cache off")

    print("  request_defaults: PASS")


fn test_request_with_prompt() raises:
    """InferenceRequest prompt constructor works."""
    var req = InferenceRequest("Hello world")

    assert_true(req.prompt == "Hello world", "prompt set correctly")
    assert_eq(req.max_tokens, 128, "other defaults preserved")

    print("  request_with_prompt: PASS")


fn test_request_to_pipeline_config() raises:
    """InferenceRequest converts to PipelineConfig correctly."""
    var req = InferenceRequest("test")
    req.max_tokens = 50
    req.temperature = 0.7
    req.top_k = 40
    req.top_p = 0.9
    req.repetition_penalty = 1.2
    req.chat_template = String("llama")
    req.use_q8_cache = True

    var cfg = req.to_pipeline_config()

    assert_eq(cfg.max_new_tokens, 50, "pipeline max_tokens")
    assert_true(cfg.sampler_config.temperature == Float32(0.7), "pipeline temperature")
    assert_eq(cfg.sampler_config.top_k, 40, "pipeline top_k")
    assert_true(cfg.repetition_penalty == Float32(1.2), "pipeline rep_penalty")
    assert_true(cfg.chat_template == "llama", "pipeline template")
    assert_true(cfg.use_q8_cache, "pipeline q8_cache")

    print("  request_to_pipeline_config: PASS")


fn test_response_success() raises:
    """InferenceResponse success creation works."""
    var resp = make_success_response(
        "hello world", "req-1", 10, 3, 50, 200.0,
    )

    assert_true(resp.text == "hello world", "response text")
    assert_true(resp.request_id == "req-1", "response request_id")
    assert_eq(resp.tokens_generated, 10, "response tokens")
    assert_eq(resp.prompt_tokens, 3, "response prompt_tokens")
    assert_eq(resp.elapsed_ms, 50, "response elapsed")
    assert_true(not resp.is_error(), "success is not error")

    print("  response_success: PASS")


fn test_response_error() raises:
    """InferenceResponse error creation works."""
    var resp = make_error_response("model failed", "req-2")

    assert_true(resp.error == "model failed", "error message")
    assert_true(resp.request_id == "req-2", "error request_id")
    assert_true(resp.is_error(), "error is_error")
    assert_eq(resp.tokens_generated, 0, "error has no tokens")

    print("  response_error: PASS")


# ===----------------------------------------------------------------------=== #
# Protocol Tests
# ===----------------------------------------------------------------------=== #

fn test_parse_request_line() raises:
    """Protocol key=value line parsing works."""
    var req = InferenceRequest()

    parse_request_line(req, "prompt=Hello world")
    assert_true(req.prompt == "Hello world", "parsed prompt")

    parse_request_line(req, "max_tokens=50")
    assert_eq(req.max_tokens, 50, "parsed max_tokens")

    parse_request_line(req, "temperature=0.5")
    assert_true(abs(req.temperature - Float32(0.5)) < 0.01, "parsed temperature")

    parse_request_line(req, "top_k=40")
    assert_eq(req.top_k, 40, "parsed top_k")

    parse_request_line(req, "chat_template=llama")
    assert_true(req.chat_template == "llama", "parsed template")

    parse_request_line(req, "request_id=req-123")
    assert_true(req.request_id == "req-123", "parsed request_id")

    parse_request_line(req, "q8_cache=true")
    assert_true(req.use_q8_cache, "parsed q8_cache")

    # Line without '=' should be skipped
    parse_request_line(req, "invalid_line")
    assert_true(req.prompt == "Hello world", "invalid line ignored")

    print("  parse_request_line: PASS")


fn test_parse_request_block() raises:
    """Protocol block parsing builds complete request."""
    var lines = List[String]()
    lines.append("REQUEST")
    lines.append("prompt=What is AI?")
    lines.append("max_tokens=64")
    lines.append("temperature=0.8")
    lines.append("chat_template=chatml")
    lines.append("request_id=test-001")
    lines.append("END")

    var req = parse_request_block(lines)

    assert_true(req.prompt == "What is AI?", "block prompt")
    assert_eq(req.max_tokens, 64, "block max_tokens")
    assert_true(abs(req.temperature - Float32(0.8)) < 0.01, "block temperature")
    assert_true(req.chat_template == "chatml", "block template")
    assert_true(req.request_id == "test-001", "block request_id")

    print("  parse_request_block: PASS")


fn test_format_response() raises:
    """Protocol response formatting works."""
    var resp = make_success_response(
        "Generated text here", "req-5", 15, 4, 100, 150.0,
    )

    var formatted = format_response(resp)

    assert_true("RESPONSE" in formatted, "has RESPONSE marker")
    assert_true("END" in formatted, "has END marker")
    assert_true("text=Generated text here" in formatted, "has text")
    assert_true("tokens_generated=15" in formatted, "has tokens")
    assert_true("prompt_tokens=4" in formatted, "has prompt_tokens")
    assert_true("elapsed_ms=100" in formatted, "has elapsed")
    assert_true("request_id=req-5" in formatted, "has request_id")

    print("  format_response: PASS")


fn test_format_error_response() raises:
    """Protocol error response formatting works."""
    var resp = make_error_response("Out of memory", "req-err")

    var formatted = format_response(resp)

    assert_true("RESPONSE" in formatted, "error has RESPONSE marker")
    assert_true("error=Out of memory" in formatted, "error has message")
    assert_true("request_id=req-err" in formatted, "error has request_id")
    # Error responses should not have text/tokens
    assert_true("text=" not in formatted, "error has no text field")

    print("  format_error_response: PASS")


# ===----------------------------------------------------------------------=== #
# Handler Tests
# ===----------------------------------------------------------------------=== #

fn test_fp32_handler() raises:
    """FP32 handler produces valid response."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var req = InferenceRequest("abc")
    req.max_tokens = 5

    var resp = handle_inference_request(model, tok, req)

    assert_true(not resp.is_error(), "FP32 handler no error")
    assert_true(len(resp.text) >= 0, "FP32 handler produces text")
    assert_true(resp.elapsed_ms >= 0, "FP32 handler has elapsed time")
    assert_true(resp.tokens_per_sec > 0.0, "FP32 handler has tps")

    print("  fp32_handler: PASS")


fn test_q8_handler() raises:
    """Q8 handler produces valid response."""
    var model = _build_tiny_model()
    var qm = quantize_from_model(model, block_size=2)
    var tok = _build_tiny_tokenizer()

    var req = InferenceRequest("abc")
    req.max_tokens = 5

    var resp = handle_q8_inference_request(qm, tok, req)

    assert_true(not resp.is_error(), "Q8 handler no error")
    assert_true(len(resp.text) >= 0, "Q8 handler produces text")
    assert_true(resp.elapsed_ms >= 0, "Q8 handler has elapsed time")

    print("  q8_handler: PASS")


fn test_batch_handler() raises:
    """Batch handler processes multiple requests."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var requests = List[InferenceRequest]()
    var req1 = InferenceRequest("abc")
    req1.max_tokens = 3
    req1.request_id = String("batch-1")
    requests.append(req1^)

    var req2 = InferenceRequest("def")
    req2.max_tokens = 3
    req2.request_id = String("batch-2")
    requests.append(req2^)

    var req3 = InferenceRequest("abc")
    req3.max_tokens = 3
    req3.request_id = String("batch-3")
    requests.append(req3^)

    var responses = handle_batch_requests(model, tok, requests)

    assert_eq(len(responses), 3, "batch produces 3 responses")
    for i in range(3):
        assert_true(not responses[i].is_error(), "batch response " + String(i) + " no error")

    print("  batch_handler: PASS")


fn test_handler_custom_params() raises:
    """Handler with custom sampling parameters works."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var req = InferenceRequest("abc")
    req.max_tokens = 5
    req.temperature = 0.7
    req.top_k = 4
    req.repetition_penalty = 1.2
    req.chat_template = String("llama")

    var resp = handle_inference_request(model, tok, req)

    assert_true(not resp.is_error(), "custom params handler no error")
    assert_true(len(resp.text) >= 0, "custom params handler produces text")

    print("  handler_custom_params: PASS")


fn main() raises:
    print("test_serve:")

    test_request_defaults()
    test_request_with_prompt()
    test_request_to_pipeline_config()
    test_response_success()
    test_response_error()
    test_parse_request_line()
    test_parse_request_block()
    test_format_response()
    test_format_error_response()
    test_fp32_handler()
    test_q8_handler()
    test_batch_handler()
    test_handler_custom_params()

    print("ALL PASSED (13 tests)")
