// Layer 0: WebSocket XOR masking — SIMD-accelerated with scalar fallback
//
// RFC 6455 requires client-to-server frames to be masked with a 4-byte key.
// This module provides fast XOR masking using SIMD when available.

const std = @import("std");
const builtin = @import("builtin");

/// Apply XOR mask to data in-place.
/// Uses SIMD when available (16-byte vectors), falls back to scalar.
pub fn applyMask(data: []u8, mask_key: [4]u8) void {
    if (data.len == 0) return;

    const can_simd = comptime canUseSIMD();
    if (can_simd and data.len >= 16) {
        applyMaskSimd(data, mask_key);
    } else {
        applyMaskScalar(data, mask_key);
    }
}

/// Scalar fallback — works everywhere, including freestanding.
pub fn applyMaskScalar(data: []u8, mask_key: [4]u8) void {
    for (data, 0..) |*b, i| {
        b.* ^= mask_key[i % 4];
    }
}

fn canUseSIMD() bool {
    const arch = builtin.cpu.arch;
    return switch (arch) {
        .x86_64, .aarch64 => true,
        else => false,
    };
}

/// SIMD-accelerated masking — processes 16 bytes at a time.
fn applyMaskSimd(data: []u8, mask_key: [4]u8) void {
    const Vec16 = @Vector(16, u8);

    // Expand 4-byte mask to 16-byte vector
    const mask_vec: Vec16 = .{
        mask_key[0], mask_key[1], mask_key[2], mask_key[3],
        mask_key[0], mask_key[1], mask_key[2], mask_key[3],
        mask_key[0], mask_key[1], mask_key[2], mask_key[3],
        mask_key[0], mask_key[1], mask_key[2], mask_key[3],
    };

    var i: usize = 0;
    const aligned_len = data.len & ~@as(usize, 15); // round down to 16

    while (i < aligned_len) : (i += 16) {
        const chunk: *align(1) Vec16 = @ptrCast(data[i..][0..16]);
        chunk.* ^= mask_vec;
    }

    // Handle remaining bytes with scalar
    while (i < data.len) : (i += 1) {
        data[i] ^= mask_key[i % 4];
    }
}

test "scalar masking round-trip" {
    const original = "Hello, WebSocket!";
    var data: [original.len]u8 = undefined;
    @memcpy(&data, original);
    const mask = [4]u8{ 0x37, 0xfa, 0x21, 0x3d };

    applyMaskScalar(&data, mask);
    // After masking, data should differ from original
    try std.testing.expect(!std.mem.eql(u8, &data, original));

    // Mask again to unmask (XOR is its own inverse)
    applyMaskScalar(&data, mask);
    try std.testing.expectEqualStrings(original, &data);
}

test "applyMask round-trip (uses SIMD if available)" {
    var data: [64]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @intCast(i % 256);

    var original: [64]u8 = undefined;
    @memcpy(&original, &data);

    const mask = [4]u8{ 0xAB, 0xCD, 0xEF, 0x01 };
    applyMask(&data, mask);
    try std.testing.expect(!std.mem.eql(u8, &data, &original));
    applyMask(&data, mask);
    try std.testing.expectEqualSlices(u8, &original, &data);
}

test "zero-length data" {
    var data: [0]u8 = undefined;
    const mask = [4]u8{ 1, 2, 3, 4 };
    applyMask(&data, mask); // should not crash
}

test "small data (< 16 bytes, scalar path)" {
    var data = [_]u8{ 0x00, 0x00, 0x00, 0x00, 0x00 };
    const mask = [4]u8{ 0xFF, 0x00, 0xFF, 0x00 };
    applyMask(&data, mask);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xFF, 0x00, 0xFF, 0x00, 0xFF }, &data);
}

test "mask key of all zeros is identity" {
    var data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const original = data;
    const mask = [4]u8{ 0, 0, 0, 0 };
    applyMask(&data, mask);
    try std.testing.expectEqualSlices(u8, &original, &data);
}

test "large data SIMD path" {
    var data: [256]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @intCast(i % 256);
    var original: [256]u8 = undefined;
    @memcpy(&original, &data);

    const mask = [4]u8{ 0x12, 0x34, 0x56, 0x78 };
    applyMask(&data, mask);
    applyMask(&data, mask);
    try std.testing.expectEqualSlices(u8, &original, &data);
}
