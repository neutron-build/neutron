# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Grammar-Constrained Sampling
# ===----------------------------------------------------------------------=== #

"""FSM-based grammar-constrained sampling for structured output.

Enforces output structure (JSON, function calls) by masking logits to only
allow tokens that produce valid next characters according to a grammar FSM.

Key components:
- CharClass: character classification for FSM transitions
- GrammarState: FSM states for JSON grammar
- JsonFSM: finite state machine tracking valid JSON structure
- apply_grammar_mask(): mask logits to enforce grammar constraints
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# Character Classification
# ===----------------------------------------------------------------------=== #

def is_digit(c: Int) -> Bool:
    """Check if byte is ASCII digit 0-9."""
    return c >= 48 and c <= 57

def is_whitespace(c: Int) -> Bool:
    """Check if byte is JSON whitespace (space, tab, newline, CR)."""
    return c == 32 or c == 9 or c == 10 or c == 13

def is_hex(c: Int) -> Bool:
    """Check if byte is hex digit."""
    return is_digit(c) or (c >= 65 and c <= 70) or (c >= 97 and c <= 102)


# ===----------------------------------------------------------------------=== #
# Grammar State
# ===----------------------------------------------------------------------=== #

struct GrammarState(Copyable, Movable, ImplicitlyCopyable):
    """FSM state for JSON grammar tracking."""
    var _value: Int

    # States
    comptime START = 0           # Expecting value start
    comptime IN_OBJECT = 1       # Inside {}, expecting key or }
    comptime IN_ARRAY = 2        # Inside [], expecting value or ]
    comptime IN_STRING = 3       # Inside "", reading string content
    comptime IN_STRING_ESCAPE = 4 # After \ in string
    comptime IN_NUMBER = 5       # Reading number digits
    comptime IN_NUMBER_FRAC = 6  # After decimal point
    comptime IN_NUMBER_EXP = 7   # After e/E in number
    comptime IN_TRUE = 8         # Reading "true" literal
    comptime IN_FALSE = 9        # Reading "false" literal
    comptime IN_NULL = 10        # Reading "null" literal
    comptime AFTER_KEY = 11      # After key string, expecting :
    comptime AFTER_COLON = 12    # After :, expecting value
    comptime AFTER_VALUE = 13    # After value, expecting , or closing bracket
    comptime IN_KEY = 14         # Inside key string
    comptime IN_KEY_ESCAPE = 15  # After \ in key string
    comptime DONE = 16           # Complete valid JSON
    comptime NUMBER_SIGN = 18
    comptime NUMBER_ZERO = 19
    comptime FRAC_START = 20
    comptime EXP_START = 21
    comptime EXP_SIGN = 22
    comptime STRING_UNICODE = 23
    comptime KEY_UNICODE = 24
    comptime OBJECT_KEY = 25
    comptime ERROR = 17          # Invalid state

    @implicit
    def __init__(out self, value: Int):
        self._value = value

    def __init__(out self, *, copy: Self):
        self._value = copy._value

    def __init__(out self, *, deinit move: Self):
        self._value = move._value^

    def __eq__(self, other: GrammarState) -> Bool:
        return self._value == other._value

    def __ne__(self, other: GrammarState) -> Bool:
        return self._value != other._value

    def value(self) -> Int:
        return self._value


# ===----------------------------------------------------------------------=== #
# JSON FSM
# ===----------------------------------------------------------------------=== #

struct JsonFSM(Copyable, Movable, ImplicitlyCopyable):
    """Finite state machine for JSON grammar validation.

    Tracks parsing state through JSON structure. Supports nested objects/arrays
    up to a configurable depth. Used to determine which characters are valid
    at each position during generation.
    """
    var state: GrammarState
    var depth: Int                # Current nesting depth
    var max_depth: Int            # Maximum allowed depth
    var unicode_remaining: Int
    var literal_pos: Int          # Position within literal (true/false/null)
    var num_stack: List[Int]      # Stack of container types (0=object, 1=array)
    var after_stack: List[Int]    # Stack of states to return to after value

    def __init__(out self, max_depth: Int = 32):
        self.state = GrammarState(GrammarState.START)
        self.depth = 0
        self.max_depth = max_depth
        self.unicode_remaining = 0
        self.literal_pos = 0
        self.num_stack = List[Int]()
        self.after_stack = List[Int]()

    def __init__(out self, *, copy: Self):
        self.state = GrammarState(copy.state.value())
        self.depth = copy.depth
        self.max_depth = copy.max_depth
        self.unicode_remaining = copy.unicode_remaining
        self.literal_pos = copy.literal_pos
        self.num_stack = List[Int]()
        for i in range(len(copy.num_stack)):
            self.num_stack.append(copy.num_stack[i])
        self.after_stack = List[Int]()
        for i in range(len(copy.after_stack)):
            self.after_stack.append(copy.after_stack[i])

    def __init__(out self, *, deinit move: Self):
        self.state = GrammarState(move.state.value())
        self.depth = move.depth^
        self.max_depth = move.max_depth^
        self.unicode_remaining = move.unicode_remaining^
        self.literal_pos = move.literal_pos^
        self.num_stack = move.num_stack^
        self.after_stack = move.after_stack^

    def copy(self) -> JsonFSM:
        """Return a copy of this FSM."""
        var fsm = JsonFSM(self.max_depth)
        fsm.state = GrammarState(self.state.value())
        fsm.depth = self.depth
        fsm.unicode_remaining = self.unicode_remaining
        fsm.literal_pos = self.literal_pos
        for i in range(len(self.num_stack)):
            fsm.num_stack.append(self.num_stack[i])
        for i in range(len(self.after_stack)):
            fsm.after_stack.append(self.after_stack[i])
        return fsm^

    def is_done(self) -> Bool:
        """Check if FSM has accepted complete valid JSON."""
        return self.state == GrammarState(GrammarState.DONE)

    def is_error(self) -> Bool:
        """Check if FSM is in error state."""
        return self.state == GrammarState(GrammarState.ERROR)

    def can_end(self) -> Bool:
        """Check if current state allows generation to end (valid JSON so far)."""
        var state = self.state.value()
        return self.depth == 0 and (state == GrammarState.DONE or state == GrammarState.IN_NUMBER or state == GrammarState.NUMBER_ZERO or state == GrammarState.IN_NUMBER_FRAC or state == GrammarState.IN_NUMBER_EXP)

    def _push_container(mut self, container_type: Int):
        """Push a container (0=object, 1=array) onto the stack."""
        self.num_stack.append(container_type)
        self.depth += 1

    def _pop_container(mut self) -> Int:
        """Pop a container from the stack. Returns container type."""
        if len(self.num_stack) > 0:
            var ct = self.num_stack[len(self.num_stack) - 1]
            # Remove last element
            var new_stack = List[Int]()
            for i in range(len(self.num_stack) - 1):
                new_stack.append(self.num_stack[i])
            self.num_stack = new_stack^
            self.depth -= 1
            return ct
        return -1

    def _current_container(self) -> Int:
        """Get current container type (-1 if none)."""
        if len(self.num_stack) > 0:
            return self.num_stack[len(self.num_stack) - 1]
        return -1

    def feed_char(mut self, c: Int):
        """Feed a single character (byte value) to the FSM.

        Transitions the state based on the character and current state.

        Args:
            c: ASCII byte value of the character.
        """
        var s = self.state.value()

        if s == GrammarState.ERROR or s == GrammarState.DONE:
            if s == GrammarState.DONE and not is_whitespace(c):
                self.state = GrammarState(GrammarState.ERROR)
            return

        if s == GrammarState.START or s == GrammarState.AFTER_COLON:
            self._feed_value_start(c)
        elif s == GrammarState.IN_OBJECT or s == GrammarState.OBJECT_KEY:
            if s == GrammarState.OBJECT_KEY and c == 125:
                self.state = GrammarState(GrammarState.ERROR)
            else:
                self._feed_in_object(c)
        elif s == GrammarState.IN_ARRAY:
            self._feed_in_array(c)
        elif s == GrammarState.IN_STRING:
            self._feed_in_string(c)
        elif s == GrammarState.IN_STRING_ESCAPE:
            self._feed_string_escape(c)
        elif s == GrammarState.IN_KEY:
            self._feed_in_key(c)
        elif s == GrammarState.IN_KEY_ESCAPE:
            self._feed_key_escape(c)
        elif s == GrammarState.NUMBER_SIGN:
            if not is_digit(c):
                self.state = GrammarState(GrammarState.ERROR)
            else:
                self.state = GrammarState(GrammarState.NUMBER_ZERO if c == 48 else GrammarState.IN_NUMBER)
        elif s == GrammarState.NUMBER_ZERO:
            if is_digit(c):
                self.state = GrammarState(GrammarState.ERROR)
            else:
                self._feed_in_number(c)
        elif s == GrammarState.FRAC_START or s == GrammarState.EXP_SIGN:
            if not is_digit(c):
                self.state = GrammarState(GrammarState.ERROR)
            else:
                self.state = GrammarState(GrammarState.IN_NUMBER_FRAC if s == GrammarState.FRAC_START else GrammarState.IN_NUMBER_EXP)
        elif s == GrammarState.EXP_START:
            if c == 43 or c == 45:
                self.state = GrammarState(GrammarState.EXP_SIGN)
            elif is_digit(c):
                self.state = GrammarState(GrammarState.IN_NUMBER_EXP)
            else:
                self.state = GrammarState(GrammarState.ERROR)
        elif s == GrammarState.STRING_UNICODE or s == GrammarState.KEY_UNICODE:
            if not is_hex(c):
                self.state = GrammarState(GrammarState.ERROR)
            else:
                self.unicode_remaining -= 1
                if self.unicode_remaining == 0:
                    self.state = GrammarState(GrammarState.IN_STRING if s == GrammarState.STRING_UNICODE else GrammarState.IN_KEY)
        elif s == GrammarState.IN_NUMBER:
            self._feed_in_number(c)
        elif s == GrammarState.IN_NUMBER_FRAC:
            self._feed_in_number_frac(c)
        elif s == GrammarState.IN_NUMBER_EXP:
            self._feed_in_number_exp(c)
        elif s == GrammarState.IN_TRUE:
            self._feed_literal(c, "true")
        elif s == GrammarState.IN_FALSE:
            self._feed_literal(c, "false")
        elif s == GrammarState.IN_NULL:
            self._feed_literal(c, "null")
        elif s == GrammarState.AFTER_KEY:
            self._feed_after_key(c)
        elif s == GrammarState.AFTER_VALUE:
            self._feed_after_value(c)
        else:
            self.state = GrammarState(GrammarState.ERROR)

    def _feed_value_start(mut self, c: Int):
        """Handle character when expecting a value."""
        if is_whitespace(c):
            return  # skip whitespace
        elif c == 123:  # '{'
            if self.depth >= self.max_depth:
                self.state = GrammarState(GrammarState.ERROR)
                return
            self._push_container(0)  # object
            self.state = GrammarState(GrammarState.IN_OBJECT)
        elif c == 91:  # '['
            if self.depth >= self.max_depth:
                self.state = GrammarState(GrammarState.ERROR)
                return
            self._push_container(1)  # array
            self.state = GrammarState(GrammarState.IN_ARRAY)
        elif c == 34:  # '"'
            self.state = GrammarState(GrammarState.IN_STRING)
        elif c == 45 or is_digit(c):  # '-' or digit
            self.state = GrammarState(GrammarState.NUMBER_SIGN if c == 45 else (GrammarState.NUMBER_ZERO if c == 48 else GrammarState.IN_NUMBER))
        elif c == 116:  # 't'
            self.literal_pos = 1
            self.state = GrammarState(GrammarState.IN_TRUE)
        elif c == 102:  # 'f'
            self.literal_pos = 1
            self.state = GrammarState(GrammarState.IN_FALSE)
        elif c == 110:  # 'n'
            self.literal_pos = 1
            self.state = GrammarState(GrammarState.IN_NULL)
        else:
            self.state = GrammarState(GrammarState.ERROR)

    def _feed_in_object(mut self, c: Int):
        """Handle character inside object (expecting key or })."""
        if is_whitespace(c):
            return
        elif c == 34:  # '"' — start of key
            self.state = GrammarState(GrammarState.IN_KEY)
        elif c == 125:  # '}' — empty object
            _ = self._pop_container()
            self._finish_value()
        else:
            self.state = GrammarState(GrammarState.ERROR)

    def _feed_in_array(mut self, c: Int):
        """Handle character inside array (expecting value or ])."""
        if is_whitespace(c):
            return
        elif c == 93:  # ']' — empty array
            _ = self._pop_container()
            self._finish_value()
        else:
            # Any value start
            self._feed_value_start(c)

    def _feed_in_string(mut self, c: Int):
        """Handle character inside string value."""
        if c < 32:
            self.state = GrammarState(GrammarState.ERROR)
        elif c == 92:  # '\\' — escape
            self.state = GrammarState(GrammarState.IN_STRING_ESCAPE)
        elif c == 34:  # '"' — end of string
            self._finish_value()
        # else: any other character stays in string

    def _feed_string_escape(mut self, c: Int):
        """Handle character after backslash in string."""
        # Valid escapes: " \ / b f n r t u
        if c == 117:
            self.unicode_remaining = 4
            self.state = GrammarState(GrammarState.STRING_UNICODE)
        elif c == 34 or c == 92 or c == 47 or c == 98 or c == 102 or c == 110 or c == 114 or c == 116:
            self.state = GrammarState(GrammarState.IN_STRING)
        else:
            self.state = GrammarState(GrammarState.ERROR)

    def _feed_in_key(mut self, c: Int):
        """Handle character inside key string."""
        if c < 32:
            self.state = GrammarState(GrammarState.ERROR)
        elif c == 92:  # '\\' — escape
            self.state = GrammarState(GrammarState.IN_KEY_ESCAPE)
        elif c == 34:  # '"' — end of key
            self.state = GrammarState(GrammarState.AFTER_KEY)
        # else: any other character stays in key

    def _feed_key_escape(mut self, c: Int):
        """Handle character after backslash in key."""
        if c == 117:
            self.unicode_remaining = 4
            self.state = GrammarState(GrammarState.KEY_UNICODE)
        elif c == 34 or c == 92 or c == 47 or c == 98 or c == 102 or c == 110 or c == 114 or c == 116:
            self.state = GrammarState(GrammarState.IN_KEY)
        else:
            self.state = GrammarState(GrammarState.ERROR)

    def _feed_after_key(mut self, c: Int):
        """Handle character after key (expecting colon)."""
        if is_whitespace(c):
            return
        elif c == 58:  # ':'
            self.state = GrammarState(GrammarState.AFTER_COLON)
        else:
            self.state = GrammarState(GrammarState.ERROR)

    def _feed_in_number(mut self, c: Int):
        """Handle character in number."""
        if is_digit(c):
            return  # continue reading digits
        elif c == 46:  # '.'
            self.state = GrammarState(GrammarState.FRAC_START)
        elif c == 101 or c == 69:  # 'e' or 'E'
            self.state = GrammarState(GrammarState.EXP_START)
        else:
            # Number ended — process this character as after-value
            self._finish_value()
            self.feed_char(c)

    def _feed_in_number_frac(mut self, c: Int):
        """Handle character after decimal point."""
        if is_digit(c):
            return
        elif c == 101 or c == 69:  # 'e' or 'E'
            self.state = GrammarState(GrammarState.EXP_START)
        else:
            self._finish_value()
            self.feed_char(c)

    def _feed_in_number_exp(mut self, c: Int):
        """Handle character in exponent."""
        if is_digit(c):
            return
        else:
            self._finish_value()
            self.feed_char(c)

    def _feed_literal(mut self, c: Int, expected: String):
        """Handle character in literal (true/false/null)."""
        if self.literal_pos >= expected.byte_length():
            self._finish_value()
            self.feed_char(c)
            return

        var expected_byte = ord(expected[byte=self.literal_pos])
        if c == expected_byte:
            self.literal_pos += 1
            if self.literal_pos >= expected.byte_length():
                self._finish_value()
        else:
            self.state = GrammarState(GrammarState.ERROR)

    def _feed_after_value(mut self, c: Int):
        """Handle character after a complete value."""
        if is_whitespace(c):
            return
        var ct = self._current_container()
        if ct == 0:  # in object
            if c == 44:  # ',' — next key-value pair
                self.state = GrammarState(GrammarState.OBJECT_KEY)
            elif c == 125:  # '}' — close object
                _ = self._pop_container()
                self._finish_value()
            else:
                self.state = GrammarState(GrammarState.ERROR)
        elif ct == 1:  # in array
            if c == 44:  # ',' — next element
                self.state = GrammarState(GrammarState.AFTER_COLON)  # reuse: expect value
            elif c == 93:  # ']' — close array
                _ = self._pop_container()
                self._finish_value()
            else:
                self.state = GrammarState(GrammarState.ERROR)
        else:
            # Top-level: nothing should follow except whitespace
            if is_whitespace(c):
                return
            self.state = GrammarState(GrammarState.ERROR)

    def _finish_value(mut self):
        """Called when a complete value has been parsed."""
        if self.depth == 0:
            self.state = GrammarState(GrammarState.DONE)
        else:
            self.state = GrammarState(GrammarState.AFTER_VALUE)

    def get_valid_chars(self) -> List[Int]:
        """First-byte prefilter derived from the same transition contract."""
        var valid = List[Int]()
        for c in range(256):
            var candidate = self.copy()
            candidate.feed_char(c)
            if not candidate.is_error():
                valid.append(c)
        return valid^

    def _add_after_value_chars(self, mut valid: List[Int]):
        """Add characters valid after a complete value."""
        var ct = self._current_container()
        if ct == 0:  # in object
            valid.append(44)   # ,
            valid.append(125)  # }
        elif ct == 1:  # in array
            valid.append(44)   # ,
            valid.append(93)   # ]
        # Always allow whitespace
        valid.append(32)
        valid.append(10)
        valid.append(9)
        valid.append(13)


# ===----------------------------------------------------------------------=== #
# Grammar Mask Application
# ===----------------------------------------------------------------------=== #

def apply_grammar_mask(mut logits: Tensor[DType.float32], vocab_size: Int,
                       fsm: JsonFSM, tokenizer_vocab: List[String], eos_id: Int) raises:
    """Enforce the complete token grammar; first-byte filtering is insufficient."""
    apply_grammar_mask_full(logits, vocab_size, fsm, tokenizer_vocab, eos_id)


def apply_grammar_mask_full(mut logits: Tensor[DType.float32], vocab_size: Int,
                            fsm: JsonFSM, tokenizer_vocab: List[String], eos_id: Int) raises:
    """Full-token JSON admission with a true forbidden mask and dead-end refusal."""
    from neutron_mojo.io.binary_reader import _u32_to_f32
    from std.math import isfinite
    if vocab_size <= 0 or logits.numel() != vocab_size or fsm.is_error():
        raise Error("Invalid JSON grammar sampling input")
    var allowed = List[Bool]()
    var any_allowed = False
    for id in range(vocab_size):
        var valid = False
        if id == eos_id:
            valid = fsm.can_end()
        elif id < len(tokenizer_vocab) and tokenizer_vocab[id].byte_length() > 0:
            var candidate = fsm.copy()
            var token = tokenizer_vocab[id]
            for i in range(token.byte_length()):
                candidate.feed_char(ord(token[byte=i]))
                if candidate.is_error():
                    break
            valid = not candidate.is_error()
        allowed.append(valid)
        if valid and isfinite(logits.data_ptr()[id]):
            any_allowed = True
    if not any_allowed:
        raise Error("JSON grammar has no selectable token")
    var forbidden = _u32_to_f32(0xFF800000)
    for id in range(vocab_size):
        if not allowed[id]:
            logits.set(id, forbidden)


def advance_fsm(mut fsm: JsonFSM, token_str: String):
    """Advance FSM state by feeding all bytes of a token string.

    Call this after sampling a token to update the FSM state.

    Args:
        fsm: FSM to advance (modified in-place).
        token_str: The token string that was sampled.
    """
    for i in range(token_str.byte_length()):
        fsm.feed_char(ord(token_str[byte=i]))
