// Layer 0: Wire reader helpers — zero allocation
//
// Helpers for reading integers, strings, and structured data from
// PostgreSQL wire format buffers. All operations are zero-copy —
// string slices point into the original buffer.

const std = @import("std");
const endian = @import("../binary/endian.zig");

pub const Error = error{
    BufferTooShort,
    MissingNullTerminator,
};

/// A cursor over a byte buffer for sequential reads.
pub const Reader = struct {
    buf: []const u8,
    pos: usize = 0,

    pub fn init(buf: []const u8) Reader {
        return .{ .buf = buf };
    }

    /// Bytes remaining in the buffer.
    pub inline fn remaining(self: *const Reader) usize {
        return self.buf.len - self.pos;
    }

    /// Read a single byte.
    pub fn readByte(self: *Reader) Error!u8 {
        if (self.pos >= self.buf.len) return Error.BufferTooShort;
        const b = self.buf[self.pos];
        self.pos += 1;
        return b;
    }

    /// Read a big-endian u16.
    pub fn readU16(self: *Reader) Error!u16 {
        if (self.remaining() < 2) return Error.BufferTooShort;
        const val = endian.readU16(@ptrCast(self.buf[self.pos..][0..2]));
        self.pos += 2;
        return val;
    }

    /// Read a big-endian u32.
    pub fn readU32(self: *Reader) Error!u32 {
        if (self.remaining() < 4) return Error.BufferTooShort;
        const val = endian.readU32(@ptrCast(self.buf[self.pos..][0..4]));
        self.pos += 4;
        return val;
    }

    /// Read a big-endian i16.
    pub fn readI16(self: *Reader) Error!i16 {
        if (self.remaining() < 2) return Error.BufferTooShort;
        const val = std.mem.readInt(i16, @ptrCast(self.buf[self.pos..][0..2]), .big);
        self.pos += 2;
        return val;
    }

    /// Read a big-endian i32.
    pub fn readI32(self: *Reader) Error!i32 {
        if (self.remaining() < 4) return Error.BufferTooShort;
        const val = endian.readI32(@ptrCast(self.buf[self.pos..][0..4]));
        self.pos += 4;
        return val;
    }

    /// Read a null-terminated string (C string). Returns slice NOT including the null byte.
    pub fn readCString(self: *Reader) Error![]const u8 {
        const start = self.pos;
        while (self.pos < self.buf.len) {
            if (self.buf[self.pos] == 0) {
                const str = self.buf[start..self.pos];
                self.pos += 1; // skip null terminator
                return str;
            }
            self.pos += 1;
        }
        return Error.MissingNullTerminator;
    }

    /// Read exactly `n` bytes as a slice.
    pub fn readBytes(self: *Reader, n: usize) Error![]const u8 {
        if (self.remaining() < n) return Error.BufferTooShort;
        const slice = self.buf[self.pos .. self.pos + n];
        self.pos += n;
        return slice;
    }

    /// Skip `n` bytes.
    pub fn skip(self: *Reader, n: usize) Error!void {
        if (self.remaining() < n) return Error.BufferTooShort;
        self.pos += n;
    }

    /// Get remaining bytes as a slice without advancing.
    pub fn rest(self: *const Reader) []const u8 {
        return self.buf[self.pos..];
    }
};

/// A cursor for writing into a byte buffer.
pub const Writer = struct {
    buf: []u8,
    pos: usize = 0,

    pub fn init(buf: []u8) Writer {
        return .{ .buf = buf };
    }

    pub inline fn remaining(self: *const Writer) usize {
        return self.buf.len - self.pos;
    }

    pub fn written(self: *const Writer) []const u8 {
        return self.buf[0..self.pos];
    }

    pub fn writeByte(self: *Writer, b: u8) Error!void {
        if (self.pos >= self.buf.len) return Error.BufferTooShort;
        self.buf[self.pos] = b;
        self.pos += 1;
    }

    pub fn writeU16(self: *Writer, value: u16) Error!void {
        if (self.remaining() < 2) return Error.BufferTooShort;
        endian.writeU16(@ptrCast(self.buf[self.pos..][0..2]), value);
        self.pos += 2;
    }

    pub fn writeU32(self: *Writer, value: u32) Error!void {
        if (self.remaining() < 4) return Error.BufferTooShort;
        endian.writeU32(@ptrCast(self.buf[self.pos..][0..4]), value);
        self.pos += 4;
    }

    pub fn writeI32(self: *Writer, value: i32) Error!void {
        if (self.remaining() < 4) return Error.BufferTooShort;
        endian.writeI32(@ptrCast(self.buf[self.pos..][0..4]), value);
        self.pos += 4;
    }

    pub fn writeI16(self: *Writer, value: i16) Error!void {
        if (self.remaining() < 2) return Error.BufferTooShort;
        std.mem.writeInt(i16, @ptrCast(self.buf[self.pos..][0..2]), value, .big);
        self.pos += 2;
    }

    /// Write a null-terminated C string.
    pub fn writeCString(self: *Writer, str: []const u8) Error!void {
        if (self.remaining() < str.len + 1) return Error.BufferTooShort;
        @memcpy(self.buf[self.pos .. self.pos + str.len], str);
        self.pos += str.len;
        self.buf[self.pos] = 0;
        self.pos += 1;
    }

    /// Write raw bytes.
    pub fn writeBytes(self: *Writer, data: []const u8) Error!void {
        if (self.remaining() < data.len) return Error.BufferTooShort;
        @memcpy(self.buf[self.pos .. self.pos + data.len], data);
        self.pos += data.len;
    }

    /// Get current position (for backpatching lengths).
    pub fn getPos(self: *const Writer) usize {
        return self.pos;
    }

    /// Backpatch a u32 at a specific position.
    pub fn patchU32(self: *Writer, pos: usize, value: u32) void {
        endian.writeU32(@ptrCast(self.buf[pos..][0..4]), value);
    }
};

test "Reader: integers and strings" {
    // Build a buffer: u32(42) + u16(7) + "hello\x00" + byte(0xFF)
    var buf: [20]u8 = undefined;
    var w = Writer.init(&buf);
    try w.writeU32(42);
    try w.writeU16(7);
    try w.writeCString("hello");
    try w.writeByte(0xFF);

    var r = Reader.init(w.written());
    try std.testing.expectEqual(@as(u32, 42), try r.readU32());
    try std.testing.expectEqual(@as(u16, 7), try r.readU16());
    try std.testing.expectEqualStrings("hello", try r.readCString());
    try std.testing.expectEqual(@as(u8, 0xFF), try r.readByte());
    try std.testing.expectEqual(@as(usize, 0), r.remaining());
}

test "Reader: buffer too short" {
    var r = Reader.init(&[_]u8{ 0x01, 0x02 });
    try std.testing.expectError(Error.BufferTooShort, r.readU32());
}

test "Reader: missing null terminator" {
    var r = Reader.init(&[_]u8{ 'a', 'b', 'c' });
    try std.testing.expectError(Error.MissingNullTerminator, r.readCString());
}

test "Writer: backpatch" {
    var buf: [8]u8 = undefined;
    var w = Writer.init(&buf);
    const len_pos = w.getPos();
    try w.writeU32(0); // placeholder
    try w.writeU32(99);
    w.patchU32(len_pos, 42); // backpatch

    var r = Reader.init(w.written());
    try std.testing.expectEqual(@as(u32, 42), try r.readU32());
    try std.testing.expectEqual(@as(u32, 99), try r.readU32());
}
