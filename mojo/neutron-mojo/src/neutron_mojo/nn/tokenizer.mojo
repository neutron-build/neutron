# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Byte-Pair Encoding Tokenizer
# ===----------------------------------------------------------------------=== #

"""BPE tokenizer for LLM inference.

Implements byte-level BPE as used by GPT-2, Llama, Mistral, etc.
Encode: text -> token IDs. Decode: token IDs -> text.

Design: lean core — vocab + ordered merge rules, no regex pre-tokenization.
"""

from std.collections import Dict


# ===----------------------------------------------------------------------=== #
# Merge Rule
# ===----------------------------------------------------------------------=== #

struct MergeRule(Copyable, Movable, ImplicitlyCopyable):
    """A single BPE merge: (left, right) -> merged."""
    var left: String
    var right: String
    var merged: String
    var priority: Int  # Lower = higher priority (applied first)

    def __init__(out self, left: String, right: String, priority: Int):
        self.left = left
        self.right = right
        self.merged = left + right
        self.priority = priority

    def __init__(out self, *, copy: Self):
        self.left = copy.left
        self.right = copy.right
        self.merged = copy.merged
        self.priority = copy.priority

    def __init__(out self, *, deinit move: Self):
        self.left = move.left^
        self.right = move.right^
        self.merged = move.merged^
        self.priority = move.priority^


# ===----------------------------------------------------------------------=== #
# BPE Tokenizer
# ===----------------------------------------------------------------------=== #

def _gpt2_visible_byte(b: Int) -> Bool:
    return (b >= 33 and b <= 126) or (b >= 161 and b <= 172) or (b >= 174 and b <= 255)


def _byte_symbol(b: Int, encoding: Int) -> String:
    if encoding == 0 or _gpt2_visible_byte(b):
        return chr(b)
    var index = 0
    for candidate in range(b):
        if not _gpt2_visible_byte(candidate):
            index += 1
    return chr(256 + index)


def _symbol_byte(codepoint: Int, encoding: Int) raises -> UInt8:
    if encoding == 0:
        if codepoint < 0 or codepoint > 255:
            raise Error("Vocabulary does not use the Latin-1 byte alphabet")
        return UInt8(codepoint)
    if codepoint <= 255 and _gpt2_visible_byte(codepoint):
        return UInt8(codepoint)
    var index = 0
    for b in range(256):
        if not _gpt2_visible_byte(b):
            if codepoint == 256 + index:
                return UInt8(b)
            index += 1
    raise Error("Vocabulary does not use the GPT-2 byte alphabet")


struct BPETokenizer(Movable):
    """Byte-level BPE tokenizer.

    Vocabulary maps token strings to IDs and back.
    Merge rules define the BPE algorithm's merge order.
    """
    # Vocab: id -> token string
    var id_to_token: List[String]
    # Vocab: token string -> id
    var token_to_id: Dict[String, Int]
    # Ordered merge rules
    var merges: List[MergeRule]
    # Merge lookup: "left|right" -> priority
    var merge_index: Dict[String, Int]
    # Special tokens
    var bos_id: Int
    var eos_id: Int
    var unk_id: Int
    var pad_id: Int
    var vocab_size: Int
    var byte_encoding: Int  # 0: Latin-1 local byte alphabet, 1: GPT-2

    def __init__(out self):
        """Create an empty tokenizer."""
        self.id_to_token = List[String]()
        self.token_to_id = Dict[String, Int]()
        self.merges = List[MergeRule]()
        self.merge_index = Dict[String, Int]()
        self.bos_id = -1
        self.eos_id = -1
        self.unk_id = -1
        self.pad_id = -1
        self.vocab_size = 0
        self.byte_encoding = 0

    def __init__(out self, *, deinit move: Self):
        self.id_to_token = move.id_to_token^
        self.token_to_id = move.token_to_id^
        self.merges = move.merges^
        self.merge_index = move.merge_index^
        self.bos_id = move.bos_id^
        self.eos_id = move.eos_id^
        self.unk_id = move.unk_id^
        self.pad_id = move.pad_id^
        self.vocab_size = move.vocab_size^
        self.byte_encoding = move.byte_encoding^

    def add_token(mut self, token: String) -> Int:
        """Add a token to the vocabulary.

        Args:
            token: Token string.

        Returns:
            Assigned token ID.
        """
        var id = self.vocab_size
        self.id_to_token.append(token)
        self.token_to_id[token] = id
        self.vocab_size += 1
        return id

    def add_special_token(mut self, token: String, role: String) -> Int:
        """Add a special token and assign its role.

        Args:
            token: Token string.
            role: One of "bos", "eos", "unk", "pad".

        Returns:
            Assigned token ID.
        """
        var id = self.add_token(token)
        if role == "bos":
            self.bos_id = id
        elif role == "eos":
            self.eos_id = id
        elif role == "unk":
            self.unk_id = id
        elif role == "pad":
            self.pad_id = id
        return id

    def add_merge(mut self, left: String, right: String):
        """Add a merge rule (order of addition = priority).

        Args:
            left: Left token.
            right: Right token.
        """
        var priority = len(self.merges)
        var rule = MergeRule(left, right, priority)
        self.merges.append(rule^)
        var key = left + "|" + right
        self.merge_index[key] = priority

    def get_merge_priority(self, left: String, right: String) raises -> Int:
        """Get merge priority for a pair (-1 if no merge exists).

        Args:
            left: Left token.
            right: Right token.

        Returns:
            Priority (lower = merge first), or -1 if not a merge pair.
        """
        var key = left + "|" + right
        if key in self.merge_index:
            return self.merge_index[key]
        return -1

    # === Encode ===

    def encode(self, text: String) raises -> List[Int]:
        """Encode text to token IDs using BPE.

        Steps:
            1. Split text into individual characters (byte-level).
            2. Iteratively merge the highest-priority adjacent pair.
            3. Look up each resulting token in vocabulary.

        Args:
            text: Input text.

        Returns:
            List of token IDs.
        """
        if text.byte_length() == 0:
            return List[Int]()

        # Step 1: Initialize with individual characters
        var tokens = List[String]()
        var bytes = text.as_bytes()
        for i in range(len(bytes)):
            tokens.append(_byte_symbol(Int(bytes[i]), self.byte_encoding))

        # Step 2: Iteratively apply merges
        var changed = True
        while changed:
            changed = False
            var best_priority = len(self.merges) + 1
            var best_pos = -1

            # Find the highest-priority merge in the current token list
            for i in range(len(tokens) - 1):
                var left = String(tokens[i])
                var right = String(tokens[i + 1])
                var p = self.get_merge_priority(left, right)
                if p >= 0 and p < best_priority:
                    best_priority = p
                    best_pos = i

            if best_pos >= 0:
                # Copy to avoid aliasing
                var left_tok = String(tokens[best_pos])
                var right_tok = String(tokens[best_pos + 1])
                var merged = left_tok + right_tok
                var new_tokens = List[String]()
                for i in range(len(tokens)):
                    if i == best_pos:
                        new_tokens.append(merged)
                    elif i == best_pos + 1:
                        continue
                    else:
                        new_tokens.append(String(tokens[i]))
                tokens = new_tokens^
                changed = True

        # Step 3: Convert to IDs
        var ids = List[Int]()
        for i in range(len(tokens)):
            var tok = String(tokens[i])
            if tok in self.token_to_id:
                ids.append(self.token_to_id[tok])
            elif self.unk_id >= 0:
                ids.append(self.unk_id)
            else:
                raise Error("Unknown token and no UNK: '" + tok + "'")
        return ids^

    def encode_with_special(self, text: String, add_bos: Bool = True) raises -> List[Int]:
        """Encode text with optional BOS/EOS tokens.

        Args:
            text: Input text.
            add_bos: Whether to prepend BOS token.

        Returns:
            List of token IDs.
        """
        var ids = self.encode(text)
        if add_bos and self.bos_id >= 0:
            var with_bos = List[Int]()
            with_bos.append(self.bos_id)
            for i in range(len(ids)):
                with_bos.append(ids[i])
            return with_bos^
        return ids^

    # === Decode ===

    def decode_bytes(self, ids: List[Int]) raises -> List[UInt8]:
        """Recover token bytes; UTF-8 may be incomplete across token boundaries."""
        if self.byte_encoding != 0 and self.byte_encoding != 1:
            raise Error("Unsupported tokenizer byte alphabet")
        var bytes = List[UInt8]()
        for i in range(len(ids)):
            var id = ids[i]
            if id < 0 or id >= self.vocab_size or id >= len(self.id_to_token):
                raise Error("Invalid tokenizer token ID")
            if id == self.bos_id or id == self.eos_id or id == self.pad_id:
                continue
            var symbol = self.id_to_token[id]
            var p = 0
            while p < symbol.byte_length():
                # String[byte=p] yields the codepoint starting at boundary p
                # (and refuses mid-codepoint indices), so iterate codepoints
                # and advance by each one's UTF-8 width. This is equivalent to
                # reading the raw UTF-8 bytes and recombining 2-byte pairs.
                var c = ord(symbol[byte=p])
                var width = 1
                if c < 128:
                    width = 1
                elif c < 2048:
                    width = 2
                elif c < 65536:
                    width = 3
                else:
                    width = 4
                if p + width > symbol.byte_length():
                    raise Error("Invalid tokenizer byte symbol")
                bytes.append(_symbol_byte(c, self.byte_encoding))
                p += width
        return bytes^

    def decode(self, ids: List[Int]) raises -> String:
        """Recover the byte alphabet, then decode complete UTF-8 exactly once."""
        return String(from_utf8=self.decode_bytes(ids))

    def decode_single(self, id: Int) raises -> String:
        """Decode a complete one-token UTF-8 value; streaming uses decode_bytes.

        Out-of-range IDs keep the documented "<unk>" fallback so callers can
        surface unknown tokens; strict ID admission lives in decode_bytes.
        """
        if id < 0 or id >= self.vocab_size or id >= len(self.id_to_token):
            return String("<unk>")
        var ids = List[Int]()
        ids.append(id)
        return self.decode(ids)



# ===----------------------------------------------------------------------=== #
# Tokenizer Builder Helpers
# ===----------------------------------------------------------------------=== #

def _hex_char(v: Int) -> String:
    """Convert 0-15 to hex character."""
    if v < 10:
        return chr(48 + v)  # '0' + v
    return chr(97 + v - 10)  # 'a' + (v - 10)


def build_byte_level_vocab(mut tokenizer: BPETokenizer):
    """Add all 256 single-byte tokens to the vocabulary.

    This is the foundation for byte-level BPE — every possible byte
    is a valid token, so any input can be tokenized.

    Args:
        tokenizer: Tokenizer to populate.
    """
    for i in range(256):
        _ = tokenizer.add_token(_byte_symbol(i, tokenizer.byte_encoding))


def build_test_tokenizer() -> BPETokenizer:
    """Build a small test tokenizer with a handful of tokens and merges.

    Vocab: single ascii chars + common pairs/words.
    Useful for unit testing the BPE algorithm.

    Returns:
        A configured BPETokenizer.
    """
    var tok = BPETokenizer()

    # Special tokens
    _ = tok.add_special_token("<bos>", "bos")
    _ = tok.add_special_token("<eos>", "eos")
    _ = tok.add_special_token("<unk>", "unk")

    # Single character tokens (lowercase + space)
    _ = tok.add_token(" ")   # 3
    _ = tok.add_token("a")   # 4
    _ = tok.add_token("b")   # 5
    _ = tok.add_token("c")   # 6
    _ = tok.add_token("d")   # 7
    _ = tok.add_token("e")   # 8
    _ = tok.add_token("f")   # 9
    _ = tok.add_token("g")   # 10
    _ = tok.add_token("h")   # 11
    _ = tok.add_token("i")   # 12
    _ = tok.add_token("l")   # 13
    _ = tok.add_token("n")   # 14
    _ = tok.add_token("o")   # 15
    _ = tok.add_token("r")   # 16
    _ = tok.add_token("s")   # 17
    _ = tok.add_token("t")   # 18
    _ = tok.add_token("u")   # 19
    _ = tok.add_token("w")   # 20

    # Merged tokens (created by BPE merges)
    _ = tok.add_token("th")  # 21
    _ = tok.add_token("he")  # 22
    _ = tok.add_token("the")  # 23
    _ = tok.add_token("in")  # 24
    _ = tok.add_token("er")  # 25
    _ = tok.add_token("is")  # 26

    # Merge rules (order = priority)
    tok.add_merge("t", "h")      # 0: t+h -> th
    tok.add_merge("h", "e")      # 1: h+e -> he
    tok.add_merge("t", "he")     # 2: t+he -> the
    tok.add_merge("th", "e")     # 3: th+e -> the
    tok.add_merge("i", "n")      # 4: i+n -> in
    tok.add_merge("e", "r")      # 5: e+r -> er
    tok.add_merge("i", "s")      # 6: i+s -> is

    return tok^


# ===----------------------------------------------------------------------=== #
# Merge Rule Parser
# ===----------------------------------------------------------------------=== #

struct MergePair(Movable):
    """Pair of strings from a merge rule."""
    var left: String
    var right: String

    def __init__(out self, left: String, right: String):
        self.left = left
        self.right = right

    def __init__(out self, *, deinit move: Self):
        self.left = move.left^
        self.right = move.right^


def _parse_merge_rule(s: String) -> MergePair:
    """Split a merge rule "tok1 tok2" on space.

    Args:
        s: Merge rule string.

    Returns:
        MergePair with left and right tokens.
    """
    var space_idx = -1
    for i in range(s.byte_length()):
        if ord(s[byte=i]) == 32:  # space
            space_idx = i
            break

    if space_idx < 0:
        return MergePair(String(s), String(""))

    var left = String(s[byte=:space_idx])
    var right = String(s[byte=space_idx + 1:])
    return MergePair(left, right)


# ===----------------------------------------------------------------------=== #
# GGUF Tokenizer Loading
# ===----------------------------------------------------------------------=== #

def load_gguf_tokenizer(
    token_vocab: List[String],
    token_scores: List[Float64],
    token_merges: List[String],
    bos_id: Int,
    eos_id: Int,
    model_type: String = "latin1",
) raises -> BPETokenizer:
    """Create a BPETokenizer from GGUF tokenizer data.

    Args:
        token_vocab: List of token strings from GGUF metadata.
        token_scores: List of token scores (unused for now, reserved for SentencePiece).
        token_merges: List of merge rules as "tok1 tok2" strings.
        bos_id: BOS token ID.
        eos_id: EOS token ID.

    Returns:
        Configured BPETokenizer.
    """
    if model_type != "latin1" and model_type != "gpt2":
        raise Error("This byte-BPE tokenizer requires latin1 or gpt2 metadata; SentencePiece needs a separate adapter")
    var tok = BPETokenizer()
    tok.byte_encoding = 1 if model_type == "gpt2" else 0

    # Add all vocab tokens
    for i in range(len(token_vocab)):
        _ = tok.add_token(String(token_vocab[i]))

    # Set special token IDs
    if bos_id >= 0 and bos_id < tok.vocab_size:
        tok.bos_id = bos_id
    if eos_id >= 0 and eos_id < tok.vocab_size:
        tok.eos_id = eos_id

    # Add merge rules
    for i in range(len(token_merges)):
        var pair = _parse_merge_rule(String(token_merges[i]))
        if pair.right.byte_length() > 0:
            tok.add_merge(pair.left, pair.right)

    return tok^


def load_vocab_file(path: String) raises -> BPETokenizer:
    """Load a tokenizer from a simple vocab file.

    Supports two formats:
        - One token per line (ID = line number)
        - Tab-separated: token<TAB>score

    Args:
        path: Path to vocab file.

    Returns:
        BPETokenizer with vocabulary loaded.
    """
    from std.pathlib import Path

    var content = Path(path).read_text()
    var tok = BPETokenizer()

    var line_start = 0
    var n = len(content)

    for i in range(n + 1):
        var at_end = (i == n)
        var is_newline = False
        if not at_end:
            is_newline = ord(content[byte=i]) == 10  # '\n'

        if at_end or is_newline:
            if i > line_start:
                var line = content[line_start:i]
                # Check for tab separator
                var tab_idx = -1
                for j in range(len(line)):
                    if ord(line[byte=j]) == 9:  # '\t'
                        tab_idx = j
                        break

                var token: String
                if tab_idx >= 0:
                    token = line[:tab_idx]
                else:
                    token = line

                # Strip trailing CR
                if len(token) > 0 and ord(token[byte=len(token) - 1]) == 13:
                    token = token[:len(token) - 1]

                if len(token) > 0:
                    _ = tok.add_token(token)
            line_start = i + 1

    return tok^
