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

from std.collections import Dict
from neutron_mojo.io.safetensors import TensorInfo


# ===----------------------------------------------------------------------=== #
# Parse Result Structs (no tuple returns in Mojo)
# ===----------------------------------------------------------------------=== #

struct StringParseResult(Movable):
    """Result of parsing a JSON string."""
    var value: String
    var pos: Int

    def __init__(out self, value: String, pos: Int):
        self.value = value
        self.pos = pos

    def __init__(out self, *, deinit move: Self):
        self.value = move.value^
        self.pos = move.pos^


struct IntParseResult(Movable):
    """Result of parsing a JSON integer."""
    var value: Int
    var pos: Int

    def __init__(out self, value: Int, pos: Int):
        self.value = value
        self.pos = pos

    def __init__(out self, *, deinit move: Self):
        self.value = move.value^
        self.pos = move.pos^


struct IntArrayParseResult(Movable):
    """Result of parsing a JSON integer array."""
    var values: List[Int]
    var pos: Int

    def __init__(out self, var values: List[Int], pos: Int):
        self.values = values^
        self.pos = pos

    def __init__(out self, *, deinit move: Self):
        self.values = move.values^
        self.pos = move.pos^


# ===----------------------------------------------------------------------=== #
# JSON Primitives
# ===----------------------------------------------------------------------=== #

def json_skip_whitespace(s: String, pos: Int) raises -> Int:
    """Skip whitespace characters (space, tab, newline, CR).

    Args:
        s: JSON string.
        pos: Current position.

    Returns:
        Position after whitespace.
    """
    if pos < 0 or pos > s.byte_length():
        raise Error("JSON cursor outside byte span")
    var p = pos
    var n = s.byte_length()
    while p < n:
        var c = ord(s[byte=p])
        if c == 32 or c == 9 or c == 10 or c == 13:  # space, tab, LF, CR
            p += 1
        else:
            break
    return p


def _json_hex4(s: String, pos: Int) raises -> Int:
    if pos < 0 or pos > s.byte_length() or 4 > s.byte_length() - pos:
        raise Error("Truncated JSON unicode escape")
    var value = 0
    for i in range(4):
        var c = ord(s[byte=pos+i])
        var digit = -1
        if c >= 48 and c <= 57:
            digit = c - 48
        elif c >= 65 and c <= 70:
            digit = c - 55
        elif c >= 97 and c <= 102:
            digit = c - 87
        if digit < 0:
            raise Error("Invalid JSON unicode escape")
        value = value * 16 + digit
    return value


def json_parse_string(s: String, pos: Int) raises -> StringParseResult:
    """Decode complete UTF-8 and JSON escapes, including surrogate pairs."""
    if pos < 0 or pos >= s.byte_length() or ord(s[byte=pos]) != 34:
        raise Error("Expected JSON string")
    var bytes = List[UInt8]()
    var p = pos + 1
    var n = s.byte_length()
    while p < n:
        var c = ord(s[byte=p])
        p += 1
        if c == 34:
            return StringParseResult(String(from_utf8=bytes), p)
        if c < 32:
            raise Error("Raw control in JSON string")
        if c != 92:
            bytes.append(UInt8(c))
            continue
        if p >= n:
            raise Error("Truncated JSON escape")
        var e = ord(s[byte=p])
        p += 1
        if e == 34 or e == 92 or e == 47:
            bytes.append(UInt8(e))
        elif e == 98:
            bytes.append(8)
        elif e == 102:
            bytes.append(12)
        elif e == 110:
            bytes.append(10)
        elif e == 114:
            bytes.append(13)
        elif e == 116:
            bytes.append(9)
        elif e == 117:
            var cp = _json_hex4(s,p)
            p += 4
            if cp >= 0xD800 and cp <= 0xDBFF:
                if p + 2 > n or ord(s[byte=p]) != 92 or ord(s[byte=p+1]) != 117:
                    raise Error("Unpaired JSON high surrogate")
                var low = _json_hex4(s,p+2)
                if low < 0xDC00 or low > 0xDFFF:
                    raise Error("Invalid JSON low surrogate")
                p += 6
                cp = 0x10000 + (cp-0xD800)*1024 + low-0xDC00
            elif cp >= 0xDC00 and cp <= 0xDFFF:
                raise Error("Unpaired JSON low surrogate")
            var encoded = chr(cp).as_bytes()
            for i in range(len(encoded)):
                bytes.append(encoded[i])
        else:
            raise Error("Unsupported JSON escape")
    raise Error("Unterminated JSON string")


def json_parse_int(s: String, pos: Int) raises -> IntParseResult:
    """Parse a JSON integer starting at pos.

    Args:
        s: JSON string.
        pos: Start position.

    Returns:
        IntParseResult with value and position after the number.
    """
    if pos < 0 or pos >= s.byte_length():
        raise Error("JSON integer cursor outside byte span")
    var p = pos
    var negative = False
    var n = s.byte_length()

    if p < n and ord(s[byte=p]) == 45:  # '-'
        negative = True
        p += 1

    if p >= n or ord(s[byte=p]) < 48 or ord(s[byte=p]) > 57:
        raise Error("Expected digit at position " + String(p))

    if ord(s[byte=p]) == 48 and p + 1 < n and ord(s[byte=p+1]) >= 48 and ord(s[byte=p+1]) <= 57:
        raise Error("Leading zero in JSON integer")
    var value = 0
    while p < n:
        var c = ord(s[byte=p])
        if c >= 48 and c <= 57:
            var digit = c - 48
            if value < -922337203685477580 or (value == -922337203685477580 and digit > (8 if negative else 7)):
                raise Error("JSON integer overflow")
            value = value * 10 - digit
            p += 1
        else:
            break

    if not negative:
        value = -value

    return IntParseResult(value, p)


def json_parse_int_array(s: String, pos: Int) raises -> IntArrayParseResult:
    """Parse a JSON integer array: [1, 2, 3]

    Args:
        s: JSON string.
        pos: Position of opening bracket.

    Returns:
        IntArrayParseResult with values and position after closing bracket.
    """
    if pos < 0 or pos >= s.byte_length() or ord(s[byte=pos]) != 91:  # '['
        raise Error("Expected '[' at position " + String(pos))

    var p = pos + 1
    var values = List[Int]()
    var n = s.byte_length()

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

def _skip_json_value(s: String, pos: Int, depth: Int = 0) raises -> Int:
    """Skip a JSON value (string, number, object, array, bool, null).

    Args:
        s: JSON string.
        pos: Start of value.

    Returns:
        Position after the value.
    """
    if depth < 0 or depth >= 128:
        raise Error("JSON nesting exceeds supported depth")
    var p = json_skip_whitespace(s, pos)
    if p >= s.byte_length():
        raise Error("Unexpected end of JSON")

    var c = ord(s[byte=p])

    if c == 34:  # string
        var r = json_parse_string(s, p)
        return r.pos
    elif c == 123:  # '{'
        return _skip_json_object(s, p, depth + 1)
    elif c == 91:  # '['
        return _skip_json_array(s, p, depth + 1)
    elif c == 116 or c == 102:  # 'true' or 'false'
        if c == 116 and p + 4 <= s.byte_length() and s[byte=p:p+4] == "true":
            return p + 4
        if c == 102 and p + 5 <= s.byte_length() and s[byte=p:p+5] == "false":
            return p + 5
        raise Error("Invalid JSON boolean")
    elif c == 110:  # 'null'
        if p + 4 <= s.byte_length() and s[byte=p:p+4] == "null":
            return p + 4
        raise Error("Invalid JSON null")
    elif c == 45 or (c >= 48 and c <= 57):  # number
        return _skip_json_number(s, p)
    else:
        raise Error("Unexpected character in JSON at " + String(p))


def _skip_json_object(s: String, pos: Int, depth: Int = 0) raises -> Int:
    """Skip a JSON object {...}."""
    if pos < 0 or pos >= s.byte_length() or ord(s[byte=pos]) != 123 or depth < 0 or depth > 128:
        raise Error("Invalid JSON object cursor/depth")
    var seen = Dict[String,Bool]()
    var p = pos + 1
    var n = s.byte_length()
    p = json_skip_whitespace(s, p)
    if p < n and ord(s[byte=p]) == 125:  # '}'
        return p + 1
    while p < n:
        p = json_skip_whitespace(s, p)
        var kr = json_parse_string(s, p)
        if kr.value in seen:
            raise Error("Duplicate JSON object key")
        seen[kr.value] = True
        p = kr.pos
        p = json_skip_whitespace(s, p)
        if p >= n or ord(s[byte=p]) != 58:
            raise Error("Expected JSON colon")
        p += 1
        p = _skip_json_value(s, p, depth)
        p = json_skip_whitespace(s, p)
        if p < n and ord(s[byte=p]) == 44:
            p += 1
        elif p < n and ord(s[byte=p]) == 125:
            return p + 1
        else:
            raise Error("Expected ',' or '}' in object")
    raise Error("Unterminated object")


def _skip_json_array(s: String, pos: Int, depth: Int = 0) raises -> Int:
    """Skip a JSON array [...]."""
    if pos < 0 or pos >= s.byte_length() or ord(s[byte=pos]) != 91 or depth < 0 or depth > 128:
        raise Error("Invalid JSON array cursor/depth")
    var p = pos + 1
    var n = s.byte_length()
    p = json_skip_whitespace(s, p)
    if p < n and ord(s[byte=p]) == 93:  # ']'
        return p + 1
    while p < n:
        p = _skip_json_value(s, p, depth)
        p = json_skip_whitespace(s, p)
        if p < n and ord(s[byte=p]) == 44:
            p += 1
        elif p < n and ord(s[byte=p]) == 93:
            return p + 1
        else:
            raise Error("Expected ',' or ']' in array")
    raise Error("Unterminated array")


def _skip_json_number(s: String, pos: Int) raises -> Int:
    """Lex one complete strict JSON number, including fraction/exponent."""
    if pos < 0 or pos >= s.byte_length():
        raise Error("JSON number cursor outside byte span")
    var p = pos
    var n = s.byte_length()
    if p < n and ord(s[byte=p]) == 45:
        p += 1
    if p >= n or ord(s[byte=p]) < 48 or ord(s[byte=p]) > 57:
        raise Error("Expected JSON number digit")
    if ord(s[byte=p]) == 48:
        p += 1
    else:
        while p < n and ord(s[byte=p]) >= 48 and ord(s[byte=p]) <= 57:
            p += 1
    if p < n and ord(s[byte=p]) == 46:
        p += 1
        var start = p
        while p < n and ord(s[byte=p]) >= 48 and ord(s[byte=p]) <= 57:
            p += 1
        if p == start:
            raise Error("Missing JSON fraction digits")
    if p < n and (ord(s[byte=p]) == 101 or ord(s[byte=p]) == 69):
        p += 1
        if p < n and (ord(s[byte=p]) == 43 or ord(s[byte=p]) == 45):
            p += 1
        var start = p
        while p < n and ord(s[byte=p]) >= 48 and ord(s[byte=p]) <= 57:
            p += 1
        if p == start:
            raise Error("Missing JSON exponent digits")
    if p < n:
        var c = ord(s[byte=p])
        if c != 32 and c != 9 and c != 10 and c != 13 and c != 44 and c != 93 and c != 125:
            raise Error("Invalid JSON number suffix")
    return p


def parse_safetensors_header(json: String) raises -> Dict[String, TensorInfo]:
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
    var document_end = _skip_json_value(json, 0)
    if json_skip_whitespace(json, document_end) != json.byte_length():
        raise Error("Trailing JSON document bytes")

    var result = Dict[String, TensorInfo]()
    var p = json_skip_whitespace(json, 0)
    var n = json.byte_length()

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

    def __init__(out self, var info: TensorInfo, end_pos: Int):
        self.info = info^
        self.end_pos = end_pos

    def __init__(out self, *, deinit move: Self):
        self.info = move.info^
        self.end_pos = move.end_pos^


def _parse_tensor_info_object(
    json: String, pos: Int
) raises -> _TensorInfoParseResult:
    """Parse a tensor info JSON object: {"dtype":"F32","shape":[...],"data_offsets":[...]}"""
    var has_dtype = False
    var has_shape = False
    var has_offsets = False
    var info = TensorInfo()
    var p = pos

    if ord(json[byte=p]) != 123:  # '{'
        raise Error("Expected '{' for tensor info")
    p += 1

    var n = json.byte_length()
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
            has_dtype = True
            var vr = json_parse_string(json, p)
            info.dtype = vr.value
            p = vr.pos
        elif field == "shape":
            has_shape = True
            var ar = json_parse_int_array(json, p)
            info.shape = ar.values.copy()
            p = ar.pos
        elif field == "data_offsets":
            has_offsets = True
            var ar = json_parse_int_array(json, p)
            if len(ar.values) != 2:
                raise Error("SafeTensors requires exactly two data offsets")
            info.data_offset_start = ar.values[0]
            info.data_offset_end = ar.values[1]
            p = ar.pos
        else:
            p = _skip_json_value(json, p)

        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 44:
            p += 1

    if not has_dtype or not has_shape or not has_offsets:
        raise Error("Missing required SafeTensors metadata field")
    return _TensorInfoParseResult(info^, p)


# ===----------------------------------------------------------------------=== #
# Weight Map Parser (for model.safetensors.index.json)
# ===----------------------------------------------------------------------=== #

def parse_weight_map(json: String) raises -> Dict[String, String]:
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
    var document_end = _skip_json_value(json, 0)
    if json_skip_whitespace(json, document_end) != json.byte_length():
        raise Error("Trailing JSON document bytes")

    var result = Dict[String, String]()
    var bare = Dict[String, String]()
    var wrapped = False
    var bare_only = True
    var p = json_skip_whitespace(json, 0)
    var n = json.byte_length()

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
            wrapped = True
            # Parse the inner string->string dict
            result = _parse_string_dict(json, p)
            # Skip past the object to continue
            p = _skip_json_object(json, p)
        else:
            if p < n and ord(json[byte=p]) == 34:
                var value = json_parse_string(json, p)
                bare[key] = value.value
                p = value.pos
            else:
                bare_only = False
                p = _skip_json_value(json, p)

        # Skip comma
        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 44:
            p += 1

    if wrapped:
        return result^
    if not bare_only:
        raise Error("Index lacks a weight_map string dictionary")
    return bare^


def _parse_string_dict(json: String, pos: Int) raises -> Dict[String, String]:
    """Parse a JSON object of string->string pairs.

    Args:
        json: JSON string.
        pos: Position of opening '{'.

    Returns:
        Dict[String, String] with all key-value pairs.
    """
    var result = Dict[String, String]()
    var p = pos
    var n = json.byte_length()

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


def parse_config_json(json: String) raises -> Dict[String, Int]:
    """Read integer configuration fields; validate and skip other JSON values."""
    var document_end = _skip_json_value(json, 0)
    if json_skip_whitespace(json, document_end) != json.byte_length():
        raise Error("Trailing JSON document bytes")

    var result = Dict[String, Int]()
    var p = json_skip_whitespace(json, 0)
    var n = json.byte_length()
    if p >= n or ord(json[byte=p]) != 123:
        raise Error("Expected config object")
    p = json_skip_whitespace(json, p + 1)
    if p < n and ord(json[byte=p]) == 125:
        if json_skip_whitespace(json, p + 1) != n:
            raise Error("Trailing config JSON")
        return result^
    while p < n:
        var kr = json_parse_string(json, p)
        p = json_skip_whitespace(json, kr.pos)
        if p >= n or ord(json[byte=p]) != 58:
            raise Error("Expected config colon")
        p = json_skip_whitespace(json, p + 1)
        if p >= n:
            raise Error("Missing config value")
        var c = ord(json[byte=p])
        if c == 45 or (c >= 48 and c <= 57):
            var end = _skip_json_number(json, p)
            var integer = True
            for i in range(p, end):
                var digit = ord(json[byte=i])
                if digit == 46 or digit == 101 or digit == 69:
                    integer = False
            if integer:
                var ir = json_parse_int(json, p)
                result[kr.value] = ir.value
            p = end
        else:
            p = _skip_json_value(json, p)
        p = json_skip_whitespace(json, p)
        if p < n and ord(json[byte=p]) == 125:
            if json_skip_whitespace(json, p + 1) != n:
                raise Error("Trailing config JSON")
            return result^
        if p >= n or ord(json[byte=p]) != 44:
            raise Error("Expected config comma or closing brace")
        p = json_skip_whitespace(json, p + 1)
    raise Error("Unterminated config JSON")
