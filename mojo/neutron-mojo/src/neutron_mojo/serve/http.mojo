# ===----------------------------------------------------------------------=== #
# Neutron Mojo — HTTP API Server
# ===----------------------------------------------------------------------=== #

"""OpenAI-compatible HTTP API server using Python's http.server as transport.

All inference runs in pure Mojo. Python is used only for HTTP transport
(parsing requests, sending responses).
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# Request/Response types (pure Mojo, no Python dependency)
# ===----------------------------------------------------------------------=== #

struct ChatMessage(Copyable, Movable):
    """A single chat message."""
    var role: String
    var content: String

    fn __init__(out self, role: String, content: String):
        self.role = role
        self.content = content

    fn __copyinit__(out self, existing: Self):
        self.role = existing.role
        self.content = existing.content

    fn __moveinit__(out self, deinit other: Self):
        self.role = other.role
        self.content = other.content

    fn copy(self) -> ChatMessage:
        return ChatMessage(self.role, self.content)


struct ChatCompletionRequest(Movable):
    """OpenAI-compatible chat completion request."""
    var model: String
    var messages: List[ChatMessage]
    var max_tokens: Int
    var temperature: Float32
    var stream: Bool

    fn __init__(out self):
        self.model = "default"
        self.messages = List[ChatMessage]()
        self.max_tokens = 256
        self.temperature = 1.0
        self.stream = False

    fn __moveinit__(out self, deinit other: Self):
        self.model = other.model
        self.messages = other.messages^
        self.max_tokens = other.max_tokens
        self.temperature = other.temperature
        self.stream = other.stream

    fn add_message(mut self, role: String, content: String):
        self.messages.append(ChatMessage(role, content))


struct ChatCompletionResponse(Movable):
    """OpenAI-compatible chat completion response."""
    var id: String
    var model: String
    var content: String
    var finish_reason: String
    var prompt_tokens: Int
    var completion_tokens: Int

    fn __init__(out self, content: String):
        self.id = "chatcmpl-neutron"
        self.model = "neutron-mojo"
        self.content = content
        self.finish_reason = "stop"
        self.prompt_tokens = 0
        self.completion_tokens = 0

    fn __moveinit__(out self, deinit other: Self):
        self.id = other.id
        self.model = other.model
        self.content = other.content
        self.finish_reason = other.finish_reason
        self.prompt_tokens = other.prompt_tokens
        self.completion_tokens = other.completion_tokens


# ===----------------------------------------------------------------------=== #
# JSON formatting
# ===----------------------------------------------------------------------=== #

fn _escape_json_string(s: String) -> String:
    """Escape special characters for JSON string value."""
    var out = String("")
    for i in range(len(s)):
        var c = ord(s[byte=i])
        if c == ord('"'):
            out += '\\"'
        elif c == ord('\\'):
            out += '\\\\'
        elif c == ord('\n'):
            out += '\\n'
        elif c == ord('\r'):
            out += '\\r'
        elif c == ord('\t'):
            out += '\\t'
        else:
            # Direct byte append — safe for ASCII
            out += chr(Int(c))
    return out^


fn format_chat_response(resp: ChatCompletionResponse) -> String:
    """Format response as OpenAI-compatible JSON."""
    var json = String('{"id":"')
    json += resp.id
    json += '","object":"chat.completion","model":"'
    json += resp.model
    json += '","choices":[{"index":0,"message":{"role":"assistant","content":"'
    json += _escape_json_string(resp.content)
    json += '"},"finish_reason":"'
    json += resp.finish_reason
    json += '"}],"usage":{"prompt_tokens":'
    json += String(resp.prompt_tokens)
    json += ',"completion_tokens":'
    json += String(resp.completion_tokens)
    json += ',"total_tokens":'
    json += String(resp.prompt_tokens + resp.completion_tokens)
    json += "}}"
    return json^


fn format_models_response() -> String:
    """Format /v1/models response."""
    return '{"object":"list","data":[{"id":"neutron-mojo","object":"model","owned_by":"neutron"}]}'


fn format_health_response() -> String:
    """Format /health response."""
    return '{"status":"ok"}'


fn format_error_response(message: String, code: Int) -> String:
    """Format error response."""
    var json = String('{"error":{"message":"')
    json += _escape_json_string(message)
    json += '","type":"invalid_request_error","code":'
    json += String(code)
    json += "}}"
    return json^


fn format_sse_event(content: String) -> String:
    """Format a Server-Sent Event for streaming."""
    var json = String('data: {"choices":[{"delta":{"content":"')
    json += _escape_json_string(content)
    json += '"}}]}\n\n'
    return json^


fn format_sse_done() -> String:
    """Format the final SSE DONE event."""
    return "data: [DONE]\n\n"


# ===----------------------------------------------------------------------=== #
# Simple JSON request parser
# ===----------------------------------------------------------------------=== #

fn _find_string_value(json: String, key: String) -> String:
    """Find a string value for a given key in JSON. Simple parser."""
    var search = '"' + key + '":"'
    var idx = 0
    for i in range(len(json) - len(search)):
        var found = True
        for j in range(len(search)):
            if ord(json[byte=i + j]) != ord(search[byte=j]):
                found = False
                break
        if found:
            idx = i + len(search)
            break

    if idx == 0:
        return ""

    var end = idx
    while end < len(json) and ord(json[byte=end]) != ord('"'):
        end += 1
    var result = String("")
    for i in range(idx, end):
        result += chr(Int(ord(json[byte=i])))
    return result^


fn _find_int_value(json: String, key: String, default: Int) -> Int:
    """Find an integer value for a given key in JSON."""
    var search = '"' + key + '":'
    var idx = 0
    for i in range(len(json) - len(search)):
        var found = True
        for j in range(len(search)):
            if ord(json[byte=i + j]) != ord(search[byte=j]):
                found = False
                break
        if found:
            idx = i + len(search)
            break

    if idx == 0:
        return default

    # Skip whitespace
    while idx < len(json) and (ord(json[byte=idx]) == ord(' ') or ord(json[byte=idx]) == ord('\t')):
        idx += 1

    # Parse integer
    var result = 0
    var negative = False
    if idx < len(json) and ord(json[byte=idx]) == ord('-'):
        negative = True
        idx += 1
    while idx < len(json) and ord(json[byte=idx]) >= ord('0') and ord(json[byte=idx]) <= ord('9'):
        result = result * 10 + Int(ord(json[byte=idx])) - Int(ord('0'))
        idx += 1
    if negative:
        result = -result
    return result


fn parse_chat_request(json: String) -> ChatCompletionRequest:
    """Parse a chat completion request from JSON.

    Simple parser that extracts model, max_tokens, temperature, stream,
    and the last message content. Full message array parsing is deferred.
    """
    var req = ChatCompletionRequest()
    var model = _find_string_value(json, "model")
    if len(model) > 0:
        req.model = model
    req.max_tokens = _find_int_value(json, "max_tokens", 256)

    # Extract last message content (simplified: finds last "content" value)
    var content = _find_string_value(json, "content")
    if len(content) > 0:
        req.add_message("user", content)

    # Check for stream flag
    var stream_search = '"stream":true'
    for i in range(len(json) - len(stream_search)):
        var found = True
        for j in range(len(stream_search)):
            if ord(json[byte=i + j]) != ord(stream_search[byte=j]):
                found = False
                break
        if found:
            req.stream = True
            break

    return req^
