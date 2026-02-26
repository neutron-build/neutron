# ===----------------------------------------------------------------------=== #
# Neutron Mojo — HTTP API Tests
# ===----------------------------------------------------------------------=== #

"""Tests for HTTP API server types and JSON formatting."""

from neutron_mojo.serve.http import (
    ChatMessage, ChatCompletionRequest, ChatCompletionResponse,
    format_chat_response, format_models_response, format_health_response,
    format_error_response, format_sse_event, format_sse_done,
    parse_chat_request,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn contains(haystack: String, needle: String) -> Bool:
    """Check if haystack contains needle."""
    if len(needle) > len(haystack):
        return False
    for i in range(len(haystack) - len(needle) + 1):
        var found = True
        for j in range(len(needle)):
            if ord(haystack[byte=i + j]) != ord(needle[byte=j]):
                found = False
                break
        if found:
            return True
    return False


fn test_chat_message() raises:
    """ChatMessage creation and copy."""
    var msg = ChatMessage("user", "Hello")
    assert_true(msg.role == "user", "Role should be user")
    assert_true(msg.content == "Hello", "Content should be Hello")
    var copy = msg.copy()
    assert_true(copy.role == "user", "Copy role")
    assert_true(copy.content == "Hello", "Copy content")
    print("  chat_message: PASS")


fn test_chat_request() raises:
    """ChatCompletionRequest creation."""
    var req = ChatCompletionRequest()
    req.add_message("user", "Test prompt")
    assert_true(len(req.messages) == 1, "Should have 1 message")
    assert_true(req.messages[0].copy().content == "Test prompt", "Content")
    assert_true(req.max_tokens == 256, "Default max_tokens")
    assert_true(req.stream == False, "Default no streaming")
    print("  chat_request: PASS")


fn test_format_chat_response() raises:
    """Format response as JSON."""
    var resp = ChatCompletionResponse("Hello world")
    resp.prompt_tokens = 5
    resp.completion_tokens = 2
    var json = format_chat_response(resp)
    assert_true(contains(json, '"chat.completion"'), "Has object type")
    assert_true(contains(json, '"Hello world"'), "Has content")
    assert_true(contains(json, '"stop"'), "Has finish_reason")
    assert_true(contains(json, '"prompt_tokens":5'), "Has prompt tokens")
    assert_true(contains(json, '"total_tokens":7'), "Has total tokens")
    print("  format_chat_response: PASS")


fn test_format_models_response() raises:
    """Format models list."""
    var json = format_models_response()
    assert_true(contains(json, '"neutron-mojo"'), "Has model name")
    assert_true(contains(json, '"list"'), "Has list object")
    print("  format_models_response: PASS")


fn test_format_health_response() raises:
    """Format health check."""
    var json = format_health_response()
    assert_true(contains(json, '"ok"'), "Has ok status")
    print("  format_health_response: PASS")


fn test_format_error_response() raises:
    """Format error response."""
    var json = format_error_response("Model not found", 404)
    assert_true(contains(json, "Model not found"), "Has error message")
    assert_true(contains(json, "404"), "Has error code")
    print("  format_error_response: PASS")


fn test_format_sse_event() raises:
    """Format SSE event."""
    var sse = format_sse_event("token")
    assert_true(contains(sse, "data: "), "Has data prefix")
    assert_true(contains(sse, '"token"'), "Has token content")
    assert_true(contains(sse, "delta"), "Has delta field")
    print("  format_sse_event: PASS")


fn test_format_sse_done() raises:
    """Format SSE done event."""
    var done = format_sse_done()
    assert_true(contains(done, "[DONE]"), "Has DONE marker")
    print("  format_sse_done: PASS")


fn test_parse_chat_request() raises:
    """Parse chat request from JSON."""
    var json = '{"model":"gpt-4","messages":[{"role":"user","content":"Hello AI"}],"max_tokens":100}'
    var req = parse_chat_request(json)
    assert_true(req.model == "gpt-4", "Parsed model name")
    assert_true(req.max_tokens == 100, "Parsed max_tokens")
    assert_true(len(req.messages) == 1, "Parsed 1 message")
    assert_true(req.messages[0].copy().content == "Hello AI", "Parsed content")
    assert_true(req.stream == False, "Not streaming")
    print("  parse_chat_request: PASS")


fn test_parse_chat_request_streaming() raises:
    """Parse streaming chat request."""
    var json = '{"model":"test","messages":[{"role":"user","content":"Hi"}],"stream":true}'
    var req = parse_chat_request(json)
    assert_true(req.stream == True, "Streaming enabled")
    print("  parse_chat_request_streaming: PASS")


fn main() raises:
    print("test_http")
    test_chat_message()
    test_chat_request()
    test_format_chat_response()
    test_format_models_response()
    test_format_health_response()
    test_format_error_response()
    test_format_sse_event()
    test_format_sse_done()
    test_parse_chat_request()
    test_parse_chat_request_streaming()
    print("All 10 HTTP tests passed!")
