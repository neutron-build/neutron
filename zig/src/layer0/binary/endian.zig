// Layer 0: Network byte order helpers — zero allocation
//
// PostgreSQL wire protocol uses big-endian (network byte order).
// These helpers convert between host and network byte order.

const std = @import("std");
const native_endian = @import("builtin").cpu.arch.endian();

/// Read a big-endian u16 from a byte slice.
pub inline fn readU16(buf: *const [2]u8) u16 {
    return std.mem.readInt(u16, buf, .big);
}

/// Read a big-endian u32 from a byte slice.
pub inline fn readU32(buf: *const [4]u8) u32 {
    return std.mem.readInt(u32, buf, .big);
}

/// Read a big-endian i32 from a byte slice.
pub inline fn readI32(buf: *const [4]u8) i32 {
    return std.mem.readInt(i32, buf, .big);
}

/// Read a big-endian u64 from a byte slice.
pub inline fn readU64(buf: *const [8]u8) u64 {
    return std.mem.readInt(u64, buf, .big);
}

/// Read a big-endian i64 from a byte slice.
pub inline fn readI64(buf: *const [8]u8) i64 {
    return std.mem.readInt(i64, buf, .big);
}

/// Write a u16 in big-endian into a byte slice.
pub inline fn writeU16(buf: *[2]u8, value: u16) void {
    std.mem.writeInt(u16, buf, value, .big);
}

/// Write a u32 in big-endian into a byte slice.
pub inline fn writeU32(buf: *[4]u8, value: u32) void {
    std.mem.writeInt(u32, buf, value, .big);
}

/// Write an i32 in big-endian into a byte slice.
pub inline fn writeI32(buf: *[4]u8, value: i32) void {
    std.mem.writeInt(i32, buf, value, .big);
}

/// Write a u64 in big-endian into a byte slice.
pub inline fn writeU64(buf: *[8]u8, value: u64) void {
    std.mem.writeInt(u64, buf, value, .big);
}

/// Write an i64 in big-endian into a byte slice.
pub inline fn writeI64(buf: *[8]u8, value: i64) void {
    std.mem.writeInt(i64, buf, value, .big);
}

test "u16 round-trip" {
    var buf: [2]u8 = undefined;
    writeU16(&buf, 0x1234);
    try std.testing.expectEqual(@as(u16, 0x1234), readU16(&buf));
    // Verify network byte order (big-endian)
    try std.testing.expectEqual(@as(u8, 0x12), buf[0]);
    try std.testing.expectEqual(@as(u8, 0x34), buf[1]);
}

test "u32 round-trip" {
    var buf: [4]u8 = undefined;
    writeU32(&buf, 0xDEADBEEF);
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), readU32(&buf));
    try std.testing.expectEqual(@as(u8, 0xDE), buf[0]);
    try std.testing.expectEqual(@as(u8, 0xAD), buf[1]);
}

test "i32 round-trip" {
    var buf: [4]u8 = undefined;
    writeI32(&buf, -1);
    try std.testing.expectEqual(@as(i32, -1), readI32(&buf));
    writeI32(&buf, 42);
    try std.testing.expectEqual(@as(i32, 42), readI32(&buf));
}

test "u64 round-trip" {
    var buf: [8]u8 = undefined;
    writeU64(&buf, 0x0102030405060708);
    try std.testing.expectEqual(@as(u64, 0x0102030405060708), readU64(&buf));
}
