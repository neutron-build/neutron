"""MJ-04/MJ-05: reject invalid spans before pointer access or mutation."""
from std.testing import assert_true, assert_equal
from std.os import remove
from neutron_mojo.io.binary_reader import BinaryReader, mmap_reader


def main() raises:
    var buf = List[UInt8]()
    for _ in range(16):
        buf.append(0)
    var reader = BinaryReader(buf^)
    var refused = False
    try:
        reader.skip(-1)
    except:
        refused = True
    assert_true(refused)
    assert_equal(reader.tell(), 0)
    refused = False
    try:
        var bytes = reader.read_bytes(-1)
    except:
        refused = True
    assert_true(refused)
    assert_equal(reader.tell(), 0)
    refused = False
    try:
        var values = reader.read_f32_array(0x4000000000000000)
    except:
        refused = True
    assert_true(refused)
    assert_equal(reader.tell(), 0)
    reader.seek(16)
    reader.skip(0)
    refused = False
    try:
        reader.skip(0x7fffffffffffffff)
    except:
        refused = True
    assert_true(refused)
    assert_equal(reader.tell(), 16)
    var high = List[UInt8]()
    for _ in range(7):
        high.append(0)
    high.append(0x80)
    var high_reader = BinaryReader(high^)
    refused = False
    try:
        var value = high_reader.read_u64_le()
    except:
        refused = True
    assert_true(refused)
    assert_equal(high_reader.tell(), 0)
    # GGUF UTF-8 string is decoded once, not byte-as-codepoint.
    var text = String("é漢🔥")
    var encoded = List[UInt8]()
    encoded.append(UInt8(text.byte_length()))
    for _ in range(7):
        encoded.append(0)
    var utf8 = text.as_bytes()
    for i in range(len(utf8)):
        encoded.append(utf8[i])
    var text_reader = BinaryReader(encoded^)
    assert_equal(text_reader.read_string_gguf(), text)
    # A unique temporary path is supplied by the validation working directory.
    var path = String("reader-boundary-empty.local.bin")
    with open(path, "w") as f:
        f.write_bytes(Span[UInt8](List[UInt8]()))
    var empty = mmap_reader(path)
    assert_equal(empty.remaining(), 0)
    refused = False
    try:
        var byte = empty.read_u8()
    except:
        refused = True
    assert_true(refused)
    remove(path)
    refused = False
    try:
        var absent = mmap_reader("reader-boundary-absent.local.bin")
    except:
        refused = True
    assert_true(refused)
    print("MJ-04/MJ-05: negative/overflow/u64/empty/absent mapping PASS")
    print("MJ-08 GGUF Unicode boundary PASS (tokenizer/HTTP remain separate)")
