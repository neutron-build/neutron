# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Text Protocol for Serving
# ===----------------------------------------------------------------------=== #

"""Simple key=value text protocol for stdin/stdout serving.

Protocol format:
  Request:
    REQUEST
    prompt=Hello world
    max_tokens=128
    temperature=0.7
    top_k=40
    ...
    END

  Response:
    RESPONSE
    request_id=abc123
    text=generated text here
    tokens_generated=42
    prompt_tokens=5
    elapsed_ms=123
    tokens_per_sec=341
    END

  Error Response:
    RESPONSE
    request_id=abc123
    error=Something went wrong
    END

This protocol is designed to be:
1. Easy to parse without a JSON library
2. Human-readable for debugging
3. Pipe-friendly for integration with HTTP frameworks
"""

from neutron_mojo.serve.handler import InferenceRequest, InferenceResponse


# ===----------------------------------------------------------------------=== #
# Request Parsing
# ===----------------------------------------------------------------------=== #

fn _parse_int_or(value: String, fallback: Int) -> Int:
    """Parse an integer and return fallback if parsing fails."""
    try:
        return atol(value)
    except:
        return fallback


fn _parse_float_or(value: String, fallback: Float32) -> Float32:
    """Parse a float and return fallback if parsing fails."""
    try:
        return Float32(atof(value))
    except:
        return fallback

fn parse_request_line(mut request: InferenceRequest, line: String):
    """Parse a single key=value line into an InferenceRequest field.

    Args:
        request: Request to populate.
        line: Line in "key=value" format.
    """
    # Find first '='
    var eq_pos = -1
    for i in range(len(line)):
        if String(line[byte=i]) == "=":
            eq_pos = i
            break

    if eq_pos < 0:
        return  # No '=' found, skip

    var key = String(line[0:eq_pos])
    var value = String(line[eq_pos + 1:len(line)])

    if key == "prompt":
        request.prompt = value
    elif key == "max_tokens":
        request.max_tokens = _parse_int_or(value, request.max_tokens)
    elif key == "temperature":
        request.temperature = _parse_float_or(value, request.temperature)
    elif key == "top_k":
        request.top_k = _parse_int_or(value, request.top_k)
    elif key == "top_p":
        request.top_p = _parse_float_or(value, request.top_p)
    elif key == "repetition_penalty":
        request.repetition_penalty = _parse_float_or(value, request.repetition_penalty)
    elif key == "frequency_penalty":
        request.frequency_penalty = _parse_float_or(value, request.frequency_penalty)
    elif key == "presence_penalty":
        request.presence_penalty = _parse_float_or(value, request.presence_penalty)
    elif key == "chat_template":
        request.chat_template = value
    elif key == "system_prompt":
        request.system_prompt = value
    elif key == "request_id":
        request.request_id = value
    elif key == "q8_cache":
        request.use_q8_cache = value == "true" or value == "1"
    elif key == "session_id":
        request.request_id = value  # Reuse request_id field for session tracking


fn parse_request_block(lines: List[String]) -> InferenceRequest:
    """Parse a list of key=value lines into an InferenceRequest.

    Expects lines between REQUEST and END markers (markers excluded).

    Args:
        lines: List of "key=value" strings.

    Returns:
        Populated InferenceRequest.
    """
    var request = InferenceRequest()
    for i in range(len(lines)):
        var line = lines[i]
        if len(line) > 0 and line != "REQUEST" and line != "END":
            parse_request_line(request, line)
    return request^


# ===----------------------------------------------------------------------=== #
# Response Formatting
# ===----------------------------------------------------------------------=== #

fn format_response(response: InferenceResponse) -> String:
    """Format an InferenceResponse as protocol text block.

    Args:
        response: Response to format.

    Returns:
        Multi-line string in protocol format.
    """
    var result = String("RESPONSE\n")

    if len(response.request_id) > 0:
        result += "request_id=" + response.request_id + "\n"

    if response.is_error():
        result += "error=" + response.error + "\n"
    else:
        result += "text=" + response.text + "\n"
        result += "tokens_generated=" + String(response.tokens_generated) + "\n"
        result += "prompt_tokens=" + String(response.prompt_tokens) + "\n"
        result += "elapsed_ms=" + String(response.elapsed_ms) + "\n"
        result += "tokens_per_sec=" + String(Int(response.tokens_per_sec)) + "\n"

    result += "END\n"
    return result^


# ===----------------------------------------------------------------------=== #
# Helpers
# ===----------------------------------------------------------------------=== #

fn _strip_trailing_newline(s: String) -> String:
    """Remove trailing newline/carriage return from a string."""
    var end = len(s)
    while end > 0:
        var c = String(s[byte=end - 1])
        if c == "\n" or c == "\r":
            end -= 1
        else:
            break
    if end == len(s):
        return s
    return String(s[0:end])
