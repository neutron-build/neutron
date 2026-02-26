# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Minimal JSON Parser
# ===----------------------------------------------------------------------=== #

"""Minimal JSON parser for SafeTensors header metadata.

Only supports the subset of JSON needed for SafeTensors:
- Top-level object {...}
- String values "..."
- Integer values
- Integer arrays [1, 2, 3]
- Nested objects (for tensor info)

NOT a general-purpose JSON parser.
"""

from collections import Dict
from neutron_mojo.io.safetensors import TensorInfo


# ===----------------------------------------------------------------------=== #
# Parse Result Structs (no tuple returns in Mojo)
# ===----------------------------------------------------------------------=== #

struct StringParseResult(Movable):
    """Result of parsing a JSON string."""
    var value: String
    var pos: Int

    fn __init__(out self, value: String, pos: Int):
        self.value = value
        self.pos = pos

    fn __moveinit__(out self, deinit other: Self):
        self.value = other.value^
        self.pos = other.pos


struct IntParseResult(Movable):
    """Result of parsing a JSON integer."""
    var value: Int
    var pos: Int

    fn __init__(out self, value: Int, pos: Int):
        self.value = value
        self.pos = pos

    fn __moveinit__(out self, deinit other: Self):
        self.value = other.value
        self.pos = other.pos


struct IntArrayParseResult(Movable):
    """Result of parsing a JSON integer array."""
    var values: List[Int]
    var pos: Int

    fn __init__(out self, var values: List[Int], pos: Int):
        self.values = values^
        self.pos = pos

    fn __moveinit__(out self, deinit other: Self):
        self.values = other.values^
        self.pos = other.pos


# ===----------------------------------------------------------------------=== #
# JSON Primitives
# ===----------------------------------------------------------------------=== #

fn json_skip_whitespace(s: String, pos: Int) -> Int:
    """Skip whitespace characters (space, tab, newline, CR).

    Args:
        s: JSON string.
        pos: Current position.

    Returns:
        Position after whitespace.
    """
    var p = pos
    var n = len(s)
    while p < n:
        var c = ord(s[byte=p])
        if c == 32 or c == 9 or c == 10 or c == 13:  # space, tab, LF, CR
            p += 1
        else:
            break
    return p


fn json_parse_string(s: String, pos: Int) raises -> StringParseResult:
    """Parse a JSON string starting at pos (must be at opening quote).

    Handles basic escapes: \\", \\\\, \\n, \\t

    Args:
        s: JSON string.
        pos: Position of opening quote.

    Returns:
        StringParseResult with value and position after closing quote.
    """
    if pos >= len(s) or ord(s[byte=pos]) != 34:  # '"'
        raise Error("Expected '\"' at position " + String(pos))

    var p = pos + 1
    var result = String("")
    var n = len(s)

    while p < n:
        var c = ord(s[byte=p])
        if c == 34:  # closing quote
            return StringParseResult(result^, p + 1)
        elif c == 92:  # backslash
            p += 1
            if p >= n:
                raise Error("Unexpected end after backslash")
            var ec = ord(s[byte=p])
            if ec == 34:
                result += chr(34)
            elif ec == 92:
                result += chr(92)
            elif ec == 110:  # 'n'
                result += chr(10)
            elif ec == 116:  # 't'
                result += chr(9)
            else:
                result += chr(ec)
            p += 1
        else:
            result += chr(c)
            p += 1

    raise Error("Unterminated string")


fn json_parse_int(s: String, pos: Int) raises -> IntParseResult:
    """Parse a JSON integer starting at pos.

    Args:
        s: JSON string.
        pos: Start position.

    Returns:
        IntParseResult with value and position after the number.
    """
    var p = pos
    var negative = False
    var n = len(s)

    if p < n and ord(s[byte=p]) == 45:  # '-'
        negative = True
        p += 1

    if p >= n or ord(s[byte=p]) < 48 or ord(s[byte=p]) > 57:
        raise Error("Expected digit at position " + String(p))

    var value = 0
    while p < n:
        var c = ord(s[byte=p])
        if c >= 48 and c <= 57:
            value = value * 10 + (c - 48)
            p += 1
        else:
            break

    if negative:
        value = -value

    return IntParseResult(value, p)


fn json_parse_int_array(s: String, pos: Int) raises -> IntArrayParseResult:
    """Parse a JSON integer array: [1, 2, 3]

    Args:
        s: JSON string.
        pos: Position of opening bracket.

    Returns:
        IntArrayParseResult with values and position after closing bracket.
    """
    if pos >= len(s) or ord(s[byte=pos]) != 91:  # '['
        raise Error("Expected '[' at position " + String(pos))

    var p = pos + 1
    var values = List[Int]()
    var n = len(s)

    p = json_skip_whitespace(s, p)

    # Empty array
    if p < n and ord(s[byte=p]) == 93:  # ']'
        return IntArrayParseResult(values^, p + 1)

    # Parse elements
    while p < n:
        p = json_skip_whitespace(s, p)
        var result = json_parse_int(s, p)
        values.append(result.value)
        p = result.pos

        p = json_skip_whitespace(s, p)
        if p < n and ord(s[byte=p]) == 44:  # ','
            p += 1
        elif p < n and ord(s[byte=p]) == 93:  # ']'
            return IntArrayParseResult(values^, p + 1)
        else:
            raise Error("Expected ',' or ']' in array")

    raise Error("Unterminated array")


# ===----------------------------------------------------------------------=== #
# SafeTensors Header Parser
# ===----------------------------------------------------------------------=== #

fn _skip_json_value(s: String, pos: Int) raises -> Int:
    """Skip a JSON value (string, number, object, array, bool, null).

    Args:
        s: JSON string.
        pos: Start of value.

    Returns:
        Position after the value.
    """
    var p = json_skip_whitespace(s, pos)
    if p >= len(s):
        raise Error("Unexpected end of JSON")

    var c = ord(s[byte=p])

    if c == 34:  # string
        var r = json_parse_string(s, p)
        return r.pos
    elif c == 123:  # '{'
        return _skip_json_object(s, p)
    elif c == 91:  # '['
        return _skip_json_array(s, p)
    elif c == 116 or c == 102:  # 'true' or 'false'
        if c == 116:
            return p + 4
        return p + 5
    elif c == 110:  # 'null'
        return p + 4
    elif c == 45 or (c >= 48 and c <= 57):  # number
        return _skip_json_number(s, p)
    else:
        raise Error("Unexpected character in JSON at " + String(p))


fn _skip_json_object(s: String, pos: Int) raises -> Int:
    """Skip a JSON object {...}."""
    var p = pos + 1
    var n = len(s)
    p = json_skip_whitespace(s, p)
    if p < n and ord(s[byte=p]) == 125:  # '}'
        return p + 1
    while p < n:
        p = json_skip_whitespace(s, p)
        var kr = json_parse_string(s, p)
        p = kr.pos
        p = json_skip_whitespace(s, p)
        if p < n and ord(s[byte=p]) == 58:  # ':'
            p += 1
        p = _skip_json_value(s, p)
        p = json_skip_whitespace(s, p)
        if p < n and ord(s[byte=p]) == 44:
            p += 1
        elif p < n and ord(s[byte=p]) == 125:
            return p + 1
        else:
            raise Error("Expected ',' or '}' in object")
    raise Error("Unterminated object")


fn _skip_json_array(s: String, pos: Int) raises -> Int:
    """Skip a JSON array [...]."""
    var p = pos + 1
    var n = len(s)
    p = json_skip_whitespace(s, p)
    if p < n and ord(s[byte=p]) == 93:  # ']'
        return p + 1
    while p < n:
        p = _skip_json_value(s, p)
        p = json_skip_whitespace(s, p)
        if p < n and ord(s[byte=p]) == 44:
            p += 1
        elif p < n and ord(s[byte=p]) == 93:
            return p + 1
        else:
            raise Error("Expected ',' or ']' in array")
    raise Error("Unterminated array")


fn _skip_json_number(s: String, pos: Int) -> Int:
    """Skip a JSON number (int or float)."""
    var p = pos
    var n = len(s)
    if p < n and ord(s[byte=p]) == 45:
        p += 1
    while p < n:
        var c = ord(s[byte=p])
        if (c >= 48 and c <= 57) or c == 46 or c == 101 or c == 69 or c == 43 or c == 45:
            p += 1
        else:
            break
    return p


fn parse_safetensors_header(json: String) raises -> Dict[String, TensorInfo]:
    """Parse SafeTensors JSON header to extract tensor metadata.

    Expected format:
    {
        "__metadata__": {...},  // skipped
        "tensor_name": {
            "dtype": "F32",
            "shape": [4096, 4096],
            "data_offsets": [0, 67108864]
        },
        ...
    }

    Args:
        json: JSON header string.

    Returns:
        Dict mapping tensor names to TensorInfo.
    """
    var result = Dict[String, TensorInfo]()
    var p = json_skip_whitespace(json, 0)
    var n = len(json)

    if p >= n or ord(json[byte=p]) != 123:  # '{'
        raise Error("Expected '{' at start of header")
    p += 1

    while p < n:
        p = json_skip_whitespace(json, p)

        if ord(json[byte=p]) == 125:  # '}'
            break

        # Parse key
        var key_result = json_parse_string(json, p)
        var key = key_result.value
        p = key_result.pos

        # Skip ':'
        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 58:
            p += 1

        p = json_skip_whitespace(json, p)

        # Skip __metadata__
        if key == "__metadata__":
            p = _skip_json_value(json, p)
        else:
            # Parse tensor info object
            var parse_result = _parse_tensor_info_object(json, p)
            p = parse_result.end_pos
            result[key] = parse_result.info.copy()

        # Skip comma
        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 44:
            p += 1

    return result^


struct _TensorInfoParseResult(Movable):
    """Internal result for parsing tensor info."""
    var info: TensorInfo
    var end_pos: Int

    fn __init__(out self, var info: TensorInfo, end_pos: Int):
        self.info = info^
        self.end_pos = end_pos

    fn __moveinit__(out self, deinit other: Self):
        self.info = other.info^
        self.end_pos = other.end_pos


fn _parse_tensor_info_object(
    json: String, pos: Int
) raises -> _TensorInfoParseResult:
    """Parse a tensor info JSON object: {"dtype":"F32","shape":[...],"data_offsets":[...]}"""
    var info = TensorInfo()
    var p = pos

    if ord(json[byte=p]) != 123:  # '{'
        raise Error("Expected '{' for tensor info")
    p += 1

    var n = len(json)
    while p < n:
        p = json_skip_whitespace(json, p)
        if ord(json[byte=p]) == 125:  # '}'
            p += 1
            break

        var kr = json_parse_string(json, p)
        var field = kr.value
        p = kr.pos

        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 58:
            p += 1
        p = json_skip_whitespace(json, p)

        if field == "dtype":
            var vr = json_parse_string(json, p)
            info.dtype = vr.value
            p = vr.pos
        elif field == "shape":
            var ar = json_parse_int_array(json, p)
            info.shape = ar.values.copy()
            p = ar.pos
        elif field == "data_offsets":
            var ar = json_parse_int_array(json, p)
            if len(ar.values) >= 2:
                info.data_offset_start = ar.values[0]
                info.data_offset_end = ar.values[1]
            p = ar.pos
        else:
            p = _skip_json_value(json, p)

        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 44:
            p += 1

    return _TensorInfoParseResult(info^, p)


# ===----------------------------------------------------------------------=== #
# Weight Map Parser (for model.safetensors.index.json)
# ===----------------------------------------------------------------------=== #

fn parse_weight_map(json: String) raises -> Dict[String, String]:
    """Parse a weight map JSON: {"weight_map": {"name": "shard_file", ...}}

    Extracts the weight_map object from a SafeTensors index JSON.
    Also handles a bare dict of string->string (no wrapping object).

    Expected format:
    {
        "metadata": {"total_size": 12345},
        "weight_map": {
            "model.embed_tokens.weight": "model-00001-of-00003.safetensors",
            ...
        }
    }

    Args:
        json: JSON string containing the index.

    Returns:
        Dict mapping tensor names to shard filenames.
    """
    var result = Dict[String, String]()
    var p = json_skip_whitespace(json, 0)
    var n = len(json)

    if p >= n or ord(json[byte=p]) != 123:  # '{'
        raise Error("Expected '{' at start of index JSON")
    p += 1

    while p < n:
        p = json_skip_whitespace(json, p)
        if p >= n or ord(json[byte=p]) == 125:  # '}'
            break

        # Parse key
        var key_result = json_parse_string(json, p)
        var key = key_result.value
        p = key_result.pos

        # Skip ':'
        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 58:
            p += 1
        p = json_skip_whitespace(json, p)

        if key == "weight_map":
            # Parse the inner string->string dict
            result = _parse_string_dict(json, p)
            # Skip past the object to continue
            p = _skip_json_object(json, p)
        else:
            # Skip other values (metadata, etc.)
            p = _skip_json_value(json, p)

        # Skip comma
        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 44:
            p += 1

    return result^


fn _parse_string_dict(json: String, pos: Int) raises -> Dict[String, String]:
    """Parse a JSON object of string->string pairs.

    Args:
        json: JSON string.
        pos: Position of opening '{'.

    Returns:
        Dict[String, String] with all key-value pairs.
    """
    var result = Dict[String, String]()
    var p = pos
    var n = len(json)

    if p >= n or ord(json[byte=p]) != 123:  # '{'
        raise Error("Expected '{' for string dict")
    p += 1

    while p < n:
        p = json_skip_whitespace(json, p)
        if p >= n or ord(json[byte=p]) == 125:  # '}'
            break

        # Parse key
        var kr = json_parse_string(json, p)
        p = kr.pos

        # Skip ':'
        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 58:
            p += 1
        p = json_skip_whitespace(json, p)

        # Parse value (string)
        var vr = json_parse_string(json, p)
        p = vr.pos

        result[kr.value] = vr.value

        # Skip comma
        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 44:
            p += 1

    return result^


fn parse_config_json(json: String) raises -> Dict[String, Int]:
    """Parse a minimal HuggingFace config.json into string->int dict.

    Extracts only integer-valued fields (sufficient for model dimensions).
    Ignores string, float, array, and nested object fields.

    Args:
        json: JSON string of config.json.

    Returns:
        Dict of field name to integer value.
    """
    var result = Dict[String, Int]()
    var p = json_skip_whitespace(json, 0)
    var n = len(json)

    if p >= n or ord(json[byte=p]) != 123:
        raise Error("Expected '{' at start of config JSON")
    p += 1

    while p < n:
        p = json_skip_whitespace(json, p)
        if p >= n or ord(json[byte=p]) == 125:
            break

        var kr = json_parse_string(json, p)
        p = kr.pos

        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 58:
            p += 1
        p = json_skip_whitespace(json, p)

        # Try to parse as int; skip if not a number
        if p < n:
            var c = ord(json[byte=p])
            if c == 45 or (c >= 48 and c <= 57):  # number
                var ir = json_parse_int(json, p)
                result[kr.value] = ir.value
                p = ir.pos
            else:
                p = _skip_json_value(json, p)

        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 44:
            p += 1

    return result^
