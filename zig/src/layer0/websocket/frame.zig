// Layer 0: WebSocket frame codec — zero allocation
//
// RFC 6455 WebSocket frame encoding/decoding.
// All fields are slices into the input buffer (zero-copy).

const std = @import("std");
const mask_mod = @import("mask.zig");

pub const applyMask = mask_mod.applyMask;

pub const Error = error{
    BufferTooShort,
    InvalidOpcode,
    InvalidFrame,
    PayloadTooLarge,
    ControlFrameTooLarge,
};

pub const Opcode = enum(u4) {
    continuation = 0,
    text = 1,
    binary = 2,
    close = 8,
    ping = 9,
    pong = 10,
    _,

    pub fn isControl(self: Opcode) bool {
        return @intFromEnum(self) >= 8;
    }
};

/// Decoded WebSocket frame — payload is a slice into the input buffer.
pub const Frame = struct {
    fin: bool,
    rsv1: bool,
    rsv2: bool,
    rsv3: bool,
    opcode: Opcode,
    masked: bool,
    payload: []const u8,
};

/// Maximum payload size we'll accept for a single frame (16 MB).
const MAX_PAYLOAD_SIZE: u64 = 16 * 1024 * 1024;

/// Decode a WebSocket frame from buffer.
/// Returns the decoded frame and bytes consumed.
/// If the frame is masked, the payload is unmasked in-place (requires mutable buffer).
pub fn decodeFrame(buf: []u8) Error!struct { frame: Frame, consumed: usize } {
    if (buf.len < 2) return Error.BufferTooShort;

    const byte0 = buf[0];
    const byte1 = buf[1];

    const fin = byte0 & 0x80 != 0;
    const rsv1 = byte0 & 0x40 != 0;
    const rsv2 = byte0 & 0x20 != 0;
    const rsv3 = byte0 & 0x10 != 0;
    const opcode: Opcode = @enumFromInt(@as(u4, @truncate(byte0 & 0x0F)));
    const masked = byte1 & 0x80 != 0;
    var payload_len: u64 = byte1 & 0x7F;

    var header_len: usize = 2;

    if (payload_len == 126) {
        if (buf.len < 4) return Error.BufferTooShort;
        payload_len = std.mem.readInt(u16, @ptrCast(buf[2..4]), .big);
        header_len = 4;
    } else if (payload_len == 127) {
        if (buf.len < 10) return Error.BufferTooShort;
        payload_len = std.mem.readInt(u64, @ptrCast(buf[2..10]), .big);
        header_len = 10;
    }

    if (payload_len > MAX_PAYLOAD_SIZE) return Error.PayloadTooLarge;

    // Control frames must not exceed 125 bytes
    if (opcode.isControl() and payload_len > 125) return Error.ControlFrameTooLarge;

    var mask_key: [4]u8 = undefined;
    if (masked) {
        if (buf.len < header_len + 4) return Error.BufferTooShort;
        @memcpy(&mask_key, buf[header_len .. header_len + 4]);
        header_len += 4;
    }

    const payload_usize: usize = @intCast(payload_len);
    const total_len = header_len + payload_usize;
    if (buf.len < total_len) return Error.BufferTooShort;

    // Unmask payload in-place if masked
    if (masked) {
        applyMask(buf[header_len..total_len], mask_key);
    }

    return .{
        .frame = .{
            .fin = fin,
            .rsv1 = rsv1,
            .rsv2 = rsv2,
            .rsv3 = rsv3,
            .opcode = opcode,
            .masked = masked,
            .payload = buf[header_len..total_len],
        },
        .consumed = total_len,
    };
}

/// Decode a frame from a const buffer (for cases where masking is not needed).
pub fn decodeFrameConst(buf: []const u8) Error!struct { frame: Frame, consumed: usize } {
    if (buf.len < 2) return Error.BufferTooShort;

    const byte0 = buf[0];
    const byte1 = buf[1];

    const fin = byte0 & 0x80 != 0;
    const rsv1 = byte0 & 0x40 != 0;
    const rsv2 = byte0 & 0x20 != 0;
    const rsv3 = byte0 & 0x10 != 0;
    const opcode: Opcode = @enumFromInt(@as(u4, @truncate(byte0 & 0x0F)));
    const masked = byte1 & 0x80 != 0;
    var payload_len: u64 = byte1 & 0x7F;

    var header_len: usize = 2;

    if (payload_len == 126) {
        if (buf.len < 4) return Error.BufferTooShort;
        payload_len = std.mem.readInt(u16, @ptrCast(buf[2..4]), .big);
        header_len = 4;
    } else if (payload_len == 127) {
        if (buf.len < 10) return Error.BufferTooShort;
        payload_len = std.mem.readInt(u64, @ptrCast(buf[2..10]), .big);
        header_len = 10;
    }

    if (payload_len > MAX_PAYLOAD_SIZE) return Error.PayloadTooLarge;
    if (opcode.isControl() and payload_len > 125) return Error.ControlFrameTooLarge;

    if (masked) header_len += 4;

    const payload_usize: usize = @intCast(payload_len);
    const total_len = header_len + payload_usize;
    if (buf.len < total_len) return Error.BufferTooShort;

    return .{
        .frame = .{
            .fin = fin,
            .rsv1 = rsv1,
            .rsv2 = rsv2,
            .rsv3 = rsv3,
            .opcode = opcode,
            .masked = masked,
            .payload = buf[header_len..total_len],
        },
        .consumed = total_len,
    };
}

/// Encode a WebSocket frame into buffer. Returns bytes written.
pub fn encodeFrame(buf: []u8, frame: Frame, mask_key: ?[4]u8) Error!usize {
    var pos: usize = 0;

    // Byte 0: FIN + RSV + opcode
    if (buf.len < 2) return Error.BufferTooShort;
    var byte0: u8 = 0;
    if (frame.fin) byte0 |= 0x80;
    if (frame.rsv1) byte0 |= 0x40;
    if (frame.rsv2) byte0 |= 0x20;
    if (frame.rsv3) byte0 |= 0x10;
    byte0 |= @intFromEnum(frame.opcode);
    buf[pos] = byte0;
    pos += 1;

    // Byte 1: MASK + payload length
    var byte1: u8 = 0;
    if (mask_key != null) byte1 |= 0x80;
    if (frame.payload.len < 126) {
        byte1 |= @intCast(frame.payload.len);
        buf[pos] = byte1;
        pos += 1;
    } else if (frame.payload.len <= 0xFFFF) {
        byte1 |= 126;
        buf[pos] = byte1;
        pos += 1;
        if (buf.len < pos + 2) return Error.BufferTooShort;
        std.mem.writeInt(u16, @ptrCast(buf[pos..][0..2]), @intCast(frame.payload.len), .big);
        pos += 2;
    } else {
        byte1 |= 127;
        buf[pos] = byte1;
        pos += 1;
        if (buf.len < pos + 8) return Error.BufferTooShort;
        std.mem.writeInt(u64, @ptrCast(buf[pos..][0..8]), frame.payload.len, .big);
        pos += 8;
    }

    // Masking key
    if (mask_key) |mk| {
        if (buf.len < pos + 4) return Error.BufferTooShort;
        @memcpy(buf[pos .. pos + 4], &mk);
        pos += 4;
    }

    // Payload
    if (buf.len < pos + frame.payload.len) return Error.BufferTooShort;
    @memcpy(buf[pos .. pos + frame.payload.len], frame.payload);

    // Apply mask to payload in buffer if masked
    if (mask_key) |mk| {
        applyMask(buf[pos .. pos + frame.payload.len], mk);
    }

    pos += frame.payload.len;
    return pos;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encode and decode unmasked text frame" {
    var buf: [256]u8 = undefined;
    const payload = "Hello, WebSocket!";
    const n = try encodeFrame(&buf, .{
        .fin = true,
        .rsv1 = false,
        .rsv2 = false,
        .rsv3 = false,
        .opcode = .text,
        .masked = false,
        .payload = payload,
    }, null);

    const result = try decodeFrameConst(buf[0..n]);
    try std.testing.expect(result.frame.fin);
    try std.testing.expectEqual(Opcode.text, result.frame.opcode);
    try std.testing.expect(!result.frame.masked);
    try std.testing.expectEqualStrings(payload, result.frame.payload);
    try std.testing.expectEqual(n, result.consumed);
}

test "encode and decode masked text frame" {
    var buf: [256]u8 = undefined;
    const payload = "Hello, WebSocket!";
    const mask = [4]u8{ 0x37, 0xfa, 0x21, 0x3d };
    const n = try encodeFrame(&buf, .{
        .fin = true,
        .rsv1 = false,
        .rsv2 = false,
        .rsv3 = false,
        .opcode = .text,
        .masked = false,
        .payload = payload,
    }, mask);

    var decode_buf: [256]u8 = undefined;
    @memcpy(decode_buf[0..n], buf[0..n]);
    const result = try decodeFrame(decode_buf[0..n]);
    try std.testing.expect(result.frame.fin);
    try std.testing.expectEqual(Opcode.text, result.frame.opcode);
    try std.testing.expect(result.frame.masked);
    try std.testing.expectEqualStrings(payload, result.frame.payload);
}

test "ping frame" {
    var buf: [128]u8 = undefined;
    const n = try encodeFrame(&buf, .{
        .fin = true,
        .rsv1 = false,
        .rsv2 = false,
        .rsv3 = false,
        .opcode = .ping,
        .masked = false,
        .payload = "",
    }, null);
    const result = try decodeFrameConst(buf[0..n]);
    try std.testing.expect(result.frame.fin);
    try std.testing.expectEqual(Opcode.ping, result.frame.opcode);
    try std.testing.expectEqual(@as(usize, 0), result.frame.payload.len);
}

test "close frame" {
    var buf: [128]u8 = undefined;
    // Close frame with 2-byte status code
    const close_data = [_]u8{ 0x03, 0xE8 }; // 1000 = normal closure
    const n = try encodeFrame(&buf, .{
        .fin = true,
        .rsv1 = false,
        .rsv2 = false,
        .rsv3 = false,
        .opcode = .close,
        .masked = false,
        .payload = &close_data,
    }, null);
    const result = try decodeFrameConst(buf[0..n]);
    try std.testing.expectEqual(Opcode.close, result.frame.opcode);
    try std.testing.expectEqual(@as(usize, 2), result.frame.payload.len);
}

test "binary frame with 126-byte payload" {
    var buf: [256]u8 = undefined;
    var payload: [126]u8 = undefined;
    @memset(&payload, 0xAA);
    const n = try encodeFrame(&buf, .{
        .fin = true,
        .rsv1 = false,
        .rsv2 = false,
        .rsv3 = false,
        .opcode = .binary,
        .masked = false,
        .payload = &payload,
    }, null);
    const result = try decodeFrameConst(buf[0..n]);
    try std.testing.expectEqual(@as(usize, 126), result.frame.payload.len);
    try std.testing.expectEqual(Opcode.binary, result.frame.opcode);
}

test "buffer too short" {
    try std.testing.expectError(Error.BufferTooShort, decodeFrameConst(&[_]u8{0x81}));
}

test "control frame too large" {
    // Forge a ping frame claiming 126-byte payload
    const buf = [_]u8{ 0x89, 0x7E, 0x00, 0x7E }; // ping, len=126
    try std.testing.expectError(Error.ControlFrameTooLarge, decodeFrameConst(&buf));
}

test "opcode is_control" {
    try std.testing.expect(!Opcode.text.isControl());
    try std.testing.expect(!Opcode.binary.isControl());
    try std.testing.expect(!Opcode.continuation.isControl());
    try std.testing.expect(Opcode.close.isControl());
    try std.testing.expect(Opcode.ping.isControl());
    try std.testing.expect(Opcode.pong.isControl());
}
