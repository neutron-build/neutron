# ===----------------------------------------------------------------------=== #
# Neutron Mojo — HTTP Streaming / SSE Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Server-Sent Events streaming integration with HTTP API."""

from neutron_mojo.serve.http import (
    format_sse_event, format_sse_done, format_chat_response,
    ChatCompletionResponse, ChatCompletionRequest,
    parse_chat_request,
)
from neutron_mojo.nn.streaming import TokenEvent


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


fn test_sse_event_format() raises:
    """SSE event has correct data: prefix and JSON structure."""
    var sse = format_sse_event("Hello")
    assert_true(contains(sse, "data: "), "Should start with data: prefix")
    assert_true(contains(sse, '"Hello"'), "Should contain token text")
    assert_true(contains(sse, "delta"), "Should have delta field")
    assert_true(contains(sse, "content"), "Should have content field")
    assert_true(contains(sse, "choices"), "Should have choices array")
    print("  sse_event_format: PASS")


fn test_sse_done_format() raises:
    """SSE done event has [DONE] marker."""
    var done = format_sse_done()
    assert_true(contains(done, "data: [DONE]"), "Should have data: [DONE]")
    assert_true(contains(done, "\n\n"), "Should end with double newline")
    print("  sse_done_format: PASS")


fn test_sse_event_double_newline() raises:
    """SSE events end with double newline per spec."""
    var sse = format_sse_event("token")
    # Count trailing newlines
    var n = len(sse)
    assert_true(n >= 2, "Event should have at least 2 chars")
    assert_true(ord(sse[byte=n - 1]) == ord('\n'), "Last char should be newline")
    assert_true(ord(sse[byte=n - 2]) == ord('\n'), "Second-to-last should be newline")
    print("  sse_event_double_newline: PASS")


fn test_sse_escape_special_chars() raises:
    """SSE event escapes special JSON characters."""
    var sse = format_sse_event('say "hello"')
    assert_true(contains(sse, '\\"hello\\"'), "Should escape quotes")
    print("  sse_escape_special_chars: PASS")


fn test_sse_multi_token_stream() raises:
    """Simulate multi-token streaming output."""
    var tokens = List[String]()
    tokens.append("Hello")
    tokens.append(" world")
    tokens.append("!")

    var stream_output = String("")
    for i in range(len(tokens)):
        stream_output += format_sse_event(tokens[i])
    stream_output += format_sse_done()

    # All tokens should be in the stream
    assert_true(contains(stream_output, '"Hello"'), "Has first token")
    assert_true(contains(stream_output, '" world"'), "Has second token")
    assert_true(contains(stream_output, '"!"'), "Has third token")
    assert_true(contains(stream_output, "[DONE]"), "Has DONE marker")
    print("  sse_multi_token_stream: PASS")


fn test_parse_streaming_request() raises:
    """Parse a streaming chat request."""
    var json = '{"model":"test","messages":[{"role":"user","content":"Hi"}],"stream":true}'
    var req = parse_chat_request(json)
    assert_true(req.stream == True, "Should detect streaming")
    assert_true(req.model == "test", "Model name")
    print("  parse_streaming_request: PASS")


fn test_non_streaming_still_works() raises:
    """Non-streaming request still works after adding streaming support."""
    var json = '{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}],"max_tokens":50}'
    var req = parse_chat_request(json)
    assert_true(req.stream == False, "Should not be streaming")
    assert_true(req.max_tokens == 50, "Max tokens parsed")

    # Non-streaming response format still works
    var resp = ChatCompletionResponse("test output")
    resp.prompt_tokens = 3
    resp.completion_tokens = 2
    var json_out = format_chat_response(resp)
    assert_true(contains(json_out, '"test output"'), "Has content")
    assert_true(contains(json_out, '"total_tokens":5'), "Has total")
    print("  non_streaming_still_works: PASS")


fn test_token_event_to_sse() raises:
    """Convert TokenEvent to SSE format."""
    var event = TokenEvent(String("world"), 42, 1, False, UInt(1000))
    var sse = format_sse_event(event.text)
    assert_true(contains(sse, '"world"'), "SSE should contain token text")
    assert_true(contains(sse, "data: "), "SSE should have data prefix")
    print("  token_event_to_sse: PASS")


fn main() raises:
    print("test_http_streaming")
    test_sse_event_format()
    test_sse_done_format()
    test_sse_event_double_newline()
    test_sse_escape_special_chars()
    test_sse_multi_token_stream()
    test_parse_streaming_request()
    test_non_streaming_still_works()
    test_token_event_to_sse()
    print("All 8 HTTP streaming tests passed!")
