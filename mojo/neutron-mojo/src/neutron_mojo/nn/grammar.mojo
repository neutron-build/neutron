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

fn is_digit(c: Int) -> Bool:
    """Check if byte is ASCII digit 0-9."""
    return c >= 48 and c <= 57

fn is_whitespace(c: Int) -> Bool:
    """Check if byte is JSON whitespace (space, tab, newline, CR)."""
    return c == 32 or c == 9 or c == 10 or c == 13

fn is_hex(c: Int) -> Bool:
    """Check if byte is hex digit."""
    return is_digit(c) or (c >= 65 and c <= 70) or (c >= 97 and c <= 102)


# ===----------------------------------------------------------------------=== #
# Grammar State
# ===----------------------------------------------------------------------=== #

struct GrammarState(Copyable, Movable):
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
    comptime ERROR = 17          # Invalid state

    @implicit
    fn __init__(out self, value: Int):
        self._value = value

    fn __copyinit__(out self, existing: Self):
        self._value = existing._value

    fn __moveinit__(out self, deinit other: Self):
        self._value = other._value

    fn __eq__(self, other: GrammarState) -> Bool:
        return self._value == other._value

    fn __ne__(self, other: GrammarState) -> Bool:
        return self._value != other._value

    fn value(self) -> Int:
        return self._value


# ===----------------------------------------------------------------------=== #
# JSON FSM
# ===----------------------------------------------------------------------=== #

struct JsonFSM(Copyable, Movable):
    """Finite state machine for JSON grammar validation.

    Tracks parsing state through JSON structure. Supports nested objects/arrays
    up to a configurable depth. Used to determine which characters are valid
    at each position during generation.
    """
    var state: GrammarState
    var depth: Int                # Current nesting depth
    var max_depth: Int            # Maximum allowed depth
    var literal_pos: Int          # Position within literal (true/false/null)
    var num_stack: List[Int]      # Stack of container types (0=object, 1=array)
    var after_stack: List[Int]    # Stack of states to return to after value

    fn __init__(out self, max_depth: Int = 32):
        self.state = GrammarState(GrammarState.START)
        self.depth = 0
        self.max_depth = max_depth
        self.literal_pos = 0
        self.num_stack = List[Int]()
        self.after_stack = List[Int]()

    fn __copyinit__(out self, existing: Self):
        self.state = GrammarState(existing.state.value())
        self.depth = existing.depth
        self.max_depth = existing.max_depth
        self.literal_pos = existing.literal_pos
        self.num_stack = List[Int]()
        for i in range(len(existing.num_stack)):
            self.num_stack.append(existing.num_stack[i])
        self.after_stack = List[Int]()
        for i in range(len(existing.after_stack)):
            self.after_stack.append(existing.after_stack[i])

    fn __moveinit__(out self, deinit other: Self):
        self.state = GrammarState(other.state.value())
        self.depth = other.depth
        self.max_depth = other.max_depth
        self.literal_pos = other.literal_pos
        self.num_stack = other.num_stack^
        self.after_stack = other.after_stack^

    fn copy(self) -> JsonFSM:
        """Return a copy of this FSM."""
        var fsm = JsonFSM(self.max_depth)
        fsm.state = GrammarState(self.state.value())
        fsm.depth = self.depth
        fsm.literal_pos = self.literal_pos
        for i in range(len(self.num_stack)):
            fsm.num_stack.append(self.num_stack[i])
        for i in range(len(self.after_stack)):
            fsm.after_stack.append(self.after_stack[i])
        return fsm^

    fn is_done(self) -> Bool:
        """Check if FSM has accepted complete valid JSON."""
        return self.state == GrammarState(GrammarState.DONE)

    fn is_error(self) -> Bool:
        """Check if FSM is in error state."""
        return self.state == GrammarState(GrammarState.ERROR)

    fn can_end(self) -> Bool:
        """Check if current state allows generation to end (valid JSON so far)."""
        return self.state == GrammarState(GrammarState.DONE) or self.state == GrammarState(GrammarState.AFTER_VALUE)

    fn _push_container(mut self, container_type: Int):
        """Push a container (0=object, 1=array) onto the stack."""
        self.num_stack.append(container_type)
        self.depth += 1

    fn _pop_container(mut self) -> Int:
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

    fn _current_container(self) -> Int:
        """Get current container type (-1 if none)."""
        if len(self.num_stack) > 0:
            return self.num_stack[len(self.num_stack) - 1]
        return -1

    fn feed_char(mut self, c: Int):
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
        elif s == GrammarState.IN_OBJECT:
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

    fn _feed_value_start(mut self, c: Int):
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
            self.state = GrammarState(GrammarState.IN_NUMBER)
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

    fn _feed_in_object(mut self, c: Int):
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

    fn _feed_in_array(mut self, c: Int):
        """Handle character inside array (expecting value or ])."""
        if is_whitespace(c):
            return
        elif c == 93:  # ']' — empty array
            _ = self._pop_container()
            self._finish_value()
        else:
            # Any value start
            self._feed_value_start(c)

    fn _feed_in_string(mut self, c: Int):
        """Handle character inside string value."""
        if c == 92:  # '\\' — escape
            self.state = GrammarState(GrammarState.IN_STRING_ESCAPE)
        elif c == 34:  # '"' — end of string
            self._finish_value()
        # else: any other character stays in string

    fn _feed_string_escape(mut self, c: Int):
        """Handle character after backslash in string."""
        # Valid escapes: " \ / b f n r t u
        if c == 34 or c == 92 or c == 47 or c == 98 or c == 102 or c == 110 or c == 114 or c == 116 or c == 117:
            self.state = GrammarState(GrammarState.IN_STRING)
        else:
            self.state = GrammarState(GrammarState.ERROR)

    fn _feed_in_key(mut self, c: Int):
        """Handle character inside key string."""
        if c == 92:  # '\\' — escape
            self.state = GrammarState(GrammarState.IN_KEY_ESCAPE)
        elif c == 34:  # '"' — end of key
            self.state = GrammarState(GrammarState.AFTER_KEY)
        # else: any other character stays in key

    fn _feed_key_escape(mut self, c: Int):
        """Handle character after backslash in key."""
        if c == 34 or c == 92 or c == 47 or c == 98 or c == 102 or c == 110 or c == 114 or c == 116 or c == 117:
            self.state = GrammarState(GrammarState.IN_KEY)
        else:
            self.state = GrammarState(GrammarState.ERROR)

    fn _feed_after_key(mut self, c: Int):
        """Handle character after key (expecting colon)."""
        if is_whitespace(c):
            return
        elif c == 58:  # ':'
            self.state = GrammarState(GrammarState.AFTER_COLON)
        else:
            self.state = GrammarState(GrammarState.ERROR)

    fn _feed_in_number(mut self, c: Int):
        """Handle character in number."""
        if is_digit(c):
            return  # continue reading digits
        elif c == 46:  # '.'
            self.state = GrammarState(GrammarState.IN_NUMBER_FRAC)
        elif c == 101 or c == 69:  # 'e' or 'E'
            self.state = GrammarState(GrammarState.IN_NUMBER_EXP)
        else:
            # Number ended — process this character as after-value
            self._finish_value()
            self._feed_after_value(c)

    fn _feed_in_number_frac(mut self, c: Int):
        """Handle character after decimal point."""
        if is_digit(c):
            return
        elif c == 101 or c == 69:  # 'e' or 'E'
            self.state = GrammarState(GrammarState.IN_NUMBER_EXP)
        else:
            self._finish_value()
            self._feed_after_value(c)

    fn _feed_in_number_exp(mut self, c: Int):
        """Handle character in exponent."""
        if is_digit(c) or c == 43 or c == 45:  # digit, +, -
            return
        else:
            self._finish_value()
            self._feed_after_value(c)

    fn _feed_literal(mut self, c: Int, expected: String):
        """Handle character in literal (true/false/null)."""
        if self.literal_pos >= len(expected):
            self._finish_value()
            self._feed_after_value(c)
            return

        var expected_byte = ord(expected[byte=self.literal_pos])
        if c == expected_byte:
            self.literal_pos += 1
            if self.literal_pos >= len(expected):
                self._finish_value()
        else:
            self.state = GrammarState(GrammarState.ERROR)

    fn _feed_after_value(mut self, c: Int):
        """Handle character after a complete value."""
        if is_whitespace(c):
            return
        var ct = self._current_container()
        if ct == 0:  # in object
            if c == 44:  # ',' — next key-value pair
                self.state = GrammarState(GrammarState.IN_OBJECT)
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

    fn _finish_value(mut self):
        """Called when a complete value has been parsed."""
        if self.depth == 0:
            self.state = GrammarState(GrammarState.DONE)
        else:
            self.state = GrammarState(GrammarState.AFTER_VALUE)

    fn get_valid_chars(self) -> List[Int]:
        """Get list of valid next character byte values.

        Returns:
            List of ASCII byte values that are valid next characters.
        """
        var valid = List[Int]()
        var s = self.state.value()

        if s == GrammarState.ERROR:
            return valid^  # nothing valid

        if s == GrammarState.DONE:
            # Only EOS / whitespace
            valid.append(32)   # space
            valid.append(10)   # newline
            return valid^

        if s == GrammarState.START or s == GrammarState.AFTER_COLON:
            # Value start: { [ " - 0-9 t f n whitespace
            valid.append(123)  # {
            valid.append(91)   # [
            valid.append(34)   # "
            valid.append(45)   # -
            for d in range(10):
                valid.append(48 + d)  # 0-9
            valid.append(116)  # t
            valid.append(102)  # f
            valid.append(110)  # n
            valid.append(32)   # space
            valid.append(10)   # newline
            valid.append(9)    # tab
            valid.append(13)   # CR

        elif s == GrammarState.IN_OBJECT:
            valid.append(34)   # " (key start)
            valid.append(125)  # }
            valid.append(32)
            valid.append(10)
            valid.append(9)
            valid.append(13)

        elif s == GrammarState.IN_ARRAY:
            # Value start + ]
            valid.append(93)   # ]
            valid.append(123)  # {
            valid.append(91)   # [
            valid.append(34)   # "
            valid.append(45)   # -
            for d in range(10):
                valid.append(48 + d)
            valid.append(116)  # t
            valid.append(102)  # f
            valid.append(110)  # n
            valid.append(32)
            valid.append(10)
            valid.append(9)
            valid.append(13)

        elif s == GrammarState.IN_STRING or s == GrammarState.IN_KEY:
            # Any printable ASCII except unescaped control chars
            valid.append(92)   # \ (escape start)
            valid.append(34)   # " (end string)
            for c in range(32, 127):
                if c != 34 and c != 92:
                    valid.append(c)

        elif s == GrammarState.IN_STRING_ESCAPE or s == GrammarState.IN_KEY_ESCAPE:
            valid.append(34)   # "
            valid.append(92)   # backslash
            valid.append(47)   # /
            valid.append(98)   # b
            valid.append(102)  # f
            valid.append(110)  # n
            valid.append(114)  # r
            valid.append(116)  # t
            valid.append(117)  # u

        elif s == GrammarState.AFTER_KEY:
            valid.append(58)   # :
            valid.append(32)
            valid.append(10)
            valid.append(9)
            valid.append(13)

        elif s == GrammarState.IN_NUMBER:
            for d in range(10):
                valid.append(48 + d)
            valid.append(46)   # .
            valid.append(101)  # e
            valid.append(69)   # E
            # Number can end -> add after-value chars
            self._add_after_value_chars(valid)

        elif s == GrammarState.IN_NUMBER_FRAC:
            for d in range(10):
                valid.append(48 + d)
            valid.append(101)  # e
            valid.append(69)   # E
            self._add_after_value_chars(valid)

        elif s == GrammarState.IN_NUMBER_EXP:
            for d in range(10):
                valid.append(48 + d)
            valid.append(43)   # +
            valid.append(45)   # -
            self._add_after_value_chars(valid)

        elif s == GrammarState.IN_TRUE:
            var expected = "true"
            if self.literal_pos < len(expected):
                valid.append(ord(expected[byte=self.literal_pos]))

        elif s == GrammarState.IN_FALSE:
            var expected = "false"
            if self.literal_pos < len(expected):
                valid.append(ord(expected[byte=self.literal_pos]))

        elif s == GrammarState.IN_NULL:
            var expected = "null"
            if self.literal_pos < len(expected):
                valid.append(ord(expected[byte=self.literal_pos]))

        elif s == GrammarState.AFTER_VALUE:
            self._add_after_value_chars(valid)

        return valid^

    fn _add_after_value_chars(self, mut valid: List[Int]):
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

fn apply_grammar_mask(
    mut logits: Tensor[DType.float32],
    vocab_size: Int,
    fsm: JsonFSM,
    tokenizer_vocab: List[String],
    eos_id: Int,
):
    """Mask logits to only allow tokens producing valid JSON characters.

    For each token in the vocabulary, checks if its first character is valid
    according to the current FSM state. Tokens whose first character is
    invalid get their logit set to -infinity.

    Args:
        logits: Raw logits [vocab_size], modified in-place.
        vocab_size: Vocabulary size.
        fsm: Current JSON FSM state.
        tokenizer_vocab: List mapping token ID -> token string.
        eos_id: EOS token ID (allowed when FSM can end).
    """
    var valid_chars = fsm.get_valid_chars()

    for tok_id in range(vocab_size):
        if tok_id == eos_id:
            # Allow EOS only when JSON is complete
            if not fsm.can_end():
                logits.set(tok_id, Float32(-1e30))
            continue

        if tok_id >= len(tokenizer_vocab):
            logits.set(tok_id, Float32(-1e30))
            continue

        var token_str = tokenizer_vocab[tok_id]
        if len(token_str) == 0:
            logits.set(tok_id, Float32(-1e30))
            continue

        # Check if first byte of this token is valid
        var first_byte = ord(token_str[byte=0])
        var is_valid = False
        for i in range(len(valid_chars)):
            if valid_chars[i] == first_byte:
                is_valid = True
                break

        if not is_valid:
            logits.set(tok_id, Float32(-1e30))


fn apply_grammar_mask_full(
    mut logits: Tensor[DType.float32],
    vocab_size: Int,
    fsm: JsonFSM,
    tokenizer_vocab: List[String],
    eos_id: Int,
):
    """Stricter grammar mask: validates ALL bytes of each token.

    Simulates feeding every byte of each token through the FSM.
    If any byte causes an error, the token is masked out.
    More accurate than first-byte-only but slower.

    Args:
        logits: Raw logits [vocab_size], modified in-place.
        vocab_size: Vocabulary size.
        fsm: Current JSON FSM state.
        tokenizer_vocab: List mapping token ID -> token string.
        eos_id: EOS token ID.
    """
    for tok_id in range(vocab_size):
        if tok_id == eos_id:
            if not fsm.can_end():
                logits.set(tok_id, Float32(-1e30))
            continue

        if tok_id >= len(tokenizer_vocab):
            logits.set(tok_id, Float32(-1e30))
            continue

        var token_str = tokenizer_vocab[tok_id]
        if len(token_str) == 0:
            logits.set(tok_id, Float32(-1e30))
            continue

        # Simulate feeding all bytes through a copy of the FSM
        var test_fsm = fsm.copy()
        var valid = True
        for i in range(len(token_str)):
            test_fsm.feed_char(ord(token_str[byte=i]))
            if test_fsm.is_error():
                valid = False
                break

        if not valid:
            logits.set(tok_id, Float32(-1e30))


fn advance_fsm(mut fsm: JsonFSM, token_str: String):
    """Advance FSM state by feeding all bytes of a token string.

    Call this after sampling a token to update the FSM state.

    Args:
        fsm: FSM to advance (modified in-place).
        token_str: The token string that was sampled.
    """
    for i in range(len(token_str)):
        fsm.feed_char(ord(token_str[byte=i]))
