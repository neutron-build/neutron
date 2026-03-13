// Layer 0: Variable-length integer encoding — zero allocation
//
// LEB128 (Little Endian Base 128) encoding used by protobuf, DWARF, WebAssembly.
// Also includes protobuf-style ZigZag encoding for signed integers.

const std = @import("std");

pub const Error = error{
    Overflow,
    EndOfBuffer,
};

/// Encode an unsigned integer as LEB128 into buf. Returns bytes written.
pub fn encodeULEB128(value: u64, buf: []u8) Error!usize {
    var v = value;
    var i: usize = 0;
    while (true) {
        if (i >= buf.len) return Error.EndOfBuffer;
        const byte: u8 = @truncate(v & 0x7F);
        v >>= 7;
        if (v != 0) {
            buf[i] = byte | 0x80;
        } else {
            buf[i] = byte;
            return i + 1;
        }
        i += 1;
    }
}

/// Decode an unsigned LEB128 integer from buf. Returns value and bytes consumed.
pub fn decodeULEB128(buf: []const u8) Error!struct { value: u64, consumed: usize } {
    var result: u64 = 0;
    var shift: u6 = 0;
    for (buf, 0..) |byte, i| {
        const payload: u64 = @intCast(byte & 0x7F);
        if (shift >= 64) return Error.Overflow;
        result |= payload << shift;
        if (byte & 0x80 == 0) {
            return .{ .value = result, .consumed = i + 1 };
        }
        shift +|= 7;
    }
    return Error.EndOfBuffer;
}

/// Encode a signed integer as LEB128 into buf. Returns bytes written.
pub fn encodeSLEB128(value: i64, buf: []u8) Error!usize {
    var v = value;
    var i: usize = 0;
    while (true) {
        if (i >= buf.len) return Error.EndOfBuffer;
        const byte: u8 = @truncate(@as(u64, @bitCast(v)) & 0x7F);
        v >>= 7;
        const done = (v == 0 and byte & 0x40 == 0) or (v == -1 and byte & 0x40 != 0);
        if (!done) {
            buf[i] = byte | 0x80;
        } else {
            buf[i] = byte;
            return i + 1;
        }
        i += 1;
    }
}

/// Decode a signed LEB128 integer from buf. Returns value and bytes consumed.
pub fn decodeSLEB128(buf: []const u8) Error!struct { value: i64, consumed: usize } {
    var result: i64 = 0;
    var shift: u6 = 0;
    for (buf, 0..) |byte, i| {
        const payload: u64 = @intCast(byte & 0x7F);
        if (shift >= 64) return Error.Overflow;
        result |= @bitCast(payload << shift);
        shift +|= 7;
        if (byte & 0x80 == 0) {
            // Sign extend
            if (shift < 64 and byte & 0x40 != 0) {
                result |= @bitCast(@as(u64, std.math.maxInt(u64)) << shift);
            }
            return .{ .value = result, .consumed = i + 1 };
        }
    }
    return Error.EndOfBuffer;
}

/// ZigZag encode a signed integer for protobuf-style encoding.
pub inline fn zigzagEncode(value: i64) u64 {
    const v: u64 = @bitCast(value);
    return (v << 1) ^ @as(u64, @bitCast(value >> 63));
}

/// ZigZag decode back to signed integer.
pub inline fn zigzagDecode(value: u64) i64 {
    return @as(i64, @bitCast(value >> 1)) ^ -@as(i64, @bitCast(value & 1));
}

test "ULEB128 round-trip" {
    var buf: [10]u8 = undefined;
    const values = [_]u64{ 0, 1, 127, 128, 255, 256, 16383, 16384, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF };
    for (values) |v| {
        const n = try encodeULEB128(v, &buf);
        const result = try decodeULEB128(buf[0..n]);
        try std.testing.expectEqual(v, result.value);
        try std.testing.expectEqual(n, result.consumed);
    }
}

test "SLEB128 round-trip" {
    var buf: [10]u8 = undefined;
    const values = [_]i64{ 0, 1, -1, 63, -64, 64, -65, 8191, -8192, std.math.minInt(i64), std.math.maxInt(i64) };
    for (values) |v| {
        const n = try encodeSLEB128(v, &buf);
        const result = try decodeSLEB128(buf[0..n]);
        try std.testing.expectEqual(v, result.value);
        try std.testing.expectEqual(n, result.consumed);
    }
}

test "ULEB128 known encodings" {
    // 0 → [0x00]
    var buf: [10]u8 = undefined;
    var n = try encodeULEB128(0, &buf);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(u8, 0x00), buf[0]);

    // 127 → [0x7F]
    n = try encodeULEB128(127, &buf);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(u8, 0x7F), buf[0]);

    // 128 → [0x80, 0x01]
    n = try encodeULEB128(128, &buf);
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(@as(u8, 0x80), buf[0]);
    try std.testing.expectEqual(@as(u8, 0x01), buf[1]);
}

test "ZigZag encoding" {
    try std.testing.expectEqual(@as(u64, 0), zigzagEncode(0));
    try std.testing.expectEqual(@as(u64, 1), zigzagEncode(-1));
    try std.testing.expectEqual(@as(u64, 2), zigzagEncode(1));
    try std.testing.expectEqual(@as(u64, 3), zigzagEncode(-2));
    try std.testing.expectEqual(@as(u64, 4), zigzagEncode(2));

    // Round-trip
    const values = [_]i64{ 0, 1, -1, 42, -42, std.math.minInt(i64), std.math.maxInt(i64) };
    for (values) |v| {
        try std.testing.expectEqual(v, zigzagDecode(zigzagEncode(v)));
    }
}

test "ULEB128 buffer too small" {
    var buf: [1]u8 = undefined;
    try std.testing.expectError(Error.EndOfBuffer, encodeULEB128(128, &buf));
}

test "ULEB128 empty input" {
    try std.testing.expectError(Error.EndOfBuffer, decodeULEB128(&[_]u8{}));
}
