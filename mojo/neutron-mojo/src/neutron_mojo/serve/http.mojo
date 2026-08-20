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

struct ChatMessage(Copyable, Movable, ImplicitlyCopyable):
    """A single chat message."""
    var role: String
    var content: String

    def __init__(out self, role: String, content: String):
        self.role = role
        self.content = content

    def __init__(out self, *, copy: Self):
        self.role = copy.role
        self.content = copy.content

    def __init__(out self, *, deinit move: Self):
        self.role = move.role^
        self.content = move.content^

    def copy(self) -> ChatMessage:
        return ChatMessage(self.role, self.content)


struct ChatCompletionRequest(Movable):
    """OpenAI-compatible chat completion request."""
    var model: String
    var messages: List[ChatMessage]
    var max_tokens: Int
    var temperature: Float32
    var stream: Bool

    def __init__(out self):
        self.model = "default"
        self.messages = List[ChatMessage]()
        self.max_tokens = 256
        self.temperature = 1.0
        self.stream = False

    def __init__(out self, *, deinit move: Self):
        self.model = move.model^
        self.messages = move.messages^
        self.max_tokens = move.max_tokens^
        self.temperature = move.temperature^
        self.stream = move.stream^

    def add_message(mut self, role: String, content: String):
        self.messages.append(ChatMessage(role, content))


struct ChatCompletionResponse(Movable):
    """OpenAI-compatible chat completion response."""
    var id: String
    var model: String
    var content: String
    var finish_reason: String
    var prompt_tokens: Int
    var completion_tokens: Int

    def __init__(out self, content: String):
        self.id = "chatcmpl-neutron"
        self.model = "neutron-mojo"
        self.content = content
        self.finish_reason = "stop"
        self.prompt_tokens = 0
        self.completion_tokens = 0

    def __init__(out self, *, deinit move: Self):
        self.id = move.id^
        self.model = move.model^
        self.content = move.content^
        self.finish_reason = move.finish_reason^
        self.prompt_tokens = move.prompt_tokens^
        self.completion_tokens = move.completion_tokens^


# ===----------------------------------------------------------------------=== #
# JSON formatting
# ===----------------------------------------------------------------------=== #

def _escape_json_string(s: String) -> String:
    """Escape special characters for JSON string value."""
    var out = String("")
    for i in range(s.byte_length()):
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


def format_chat_response(resp: ChatCompletionResponse) -> String:
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


def format_models_response() -> String:
    """Format /v1/models response."""
    return '{"object":"list","data":[{"id":"neutron-mojo","object":"model","owned_by":"neutron"}]}'


def format_health_response() -> String:
    """Format /health response."""
    return '{"status":"ok"}'


def format_error_response(message: String, code: Int) -> String:
    """Format error response."""
    var json = String('{"error":{"message":"')
    json += _escape_json_string(message)
    json += '","type":"invalid_request_error","code":'
    json += String(code)
    json += "}}"
    return json^


def format_sse_event(content: String) -> String:
    """Format a Server-Sent Event for streaming."""
    var json = String('data: {"choices":[{"delta":{"content":"')
    json += _escape_json_string(content)
    json += '"}}]}\n\n'
    return json^


def format_sse_done() -> String:
    """Format the final SSE DONE event."""
    return "data: [DONE]\n\n"


# ===----------------------------------------------------------------------=== #
# Simple JSON request parser
# ===----------------------------------------------------------------------=== #

def _find_string_value(json: String, key: String) -> String:
    """Find a string value for a given key in JSON. Simple parser."""
    var search = '"' + key + '":"'
    var idx = 0
    for i in range(json.byte_length() - search.byte_length()):
        var found = True
        for j in range(search.byte_length()):
            if ord(json[byte=i + j]) != ord(search[byte=j]):
                found = False
                break
        if found:
            idx = i + search.byte_length()
            break

    if idx == 0:
        return ""

    var end = idx
    while end < json.byte_length() and ord(json[byte=end]) != ord('"'):
        end += 1
    var result = String("")
    for i in range(idx, end):
        result += chr(Int(ord(json[byte=i])))
    return result^


def _find_int_value(json: String, key: String, default: Int) -> Int:
    """Find an integer value for a given key in JSON."""
    var search = '"' + key + '":'
    var idx = 0
    for i in range(json.byte_length() - search.byte_length()):
        var found = True
        for j in range(search.byte_length()):
            if ord(json[byte=i + j]) != ord(search[byte=j]):
                found = False
                break
        if found:
            idx = i + search.byte_length()
            break

    if idx == 0:
        return default

    # Skip whitespace
    while idx < json.byte_length() and (ord(json[byte=idx]) == ord(' ') or ord(json[byte=idx]) == ord('\t')):
        idx += 1

    # Parse integer
    var result = 0
    var negative = False
    if idx < json.byte_length() and ord(json[byte=idx]) == ord('-'):
        negative = True
        idx += 1
    while idx < json.byte_length() and ord(json[byte=idx]) >= ord('0') and ord(json[byte=idx]) <= ord('9'):
        result = result * 10 + Int(ord(json[byte=idx])) - Int(ord('0'))
        idx += 1
    if negative:
        result = -result
    return result


def parse_chat_request(json: String) -> ChatCompletionRequest:
    """Parse a chat completion request from JSON.

    Simple parser that extracts model, max_tokens, temperature, stream,
    and the last message content. Full message array parsing is deferred.
    """
    var req = ChatCompletionRequest()
    var model = _find_string_value(json, "model")
    if model.byte_length() > 0:
        req.model = model
    req.max_tokens = _find_int_value(json, "max_tokens", 256)

    # Extract last message content (simplified: finds last "content" value)
    var content = _find_string_value(json, "content")
    if content.byte_length() > 0:
        req.add_message("user", content)

    # Check for stream flag
    var stream_search = '"stream":true'
    for i in range(json.byte_length() - stream_search.byte_length()):
        var found = True
        for j in range(stream_search.byte_length()):
            if ord(json[byte=i + j]) != ord(stream_search[byte=j]):
                found = False
                break
        if found:
            req.stream = True
            break

    return req^
