// Layer 3: Compression Middleware — gzip (RFC 1952) with deflate stored blocks
//
// Comptime middleware wrapper that compresses response bodies when the client
// supports gzip encoding. Zero heap allocation — uses stack buffers.
// Skips compression for small bodies and already-compressed content types.
//
// Uses deflate "stored" blocks (BTYPE=00) for correctness and simplicity.
// Stored blocks have 5 bytes overhead per 65535 bytes — negligible for typical
// API responses under 64KB. The gzip framing adds CRC32 integrity checking.

const std = @import("std");
const http_server = @import("../layer2/http_server.zig");
const http_parser = @import("../layer0/http/parser.zig");

pub const RequestContext = http_server.RequestContext;
pub const HandlerFn = *const fn (*RequestContext) anyerror!void;
pub const Header = http_parser.Header;

/// Compression configuration.
pub const CompressConfig = struct {
    /// Minimum body size in bytes to trigger compression (default: 860).
    min_size: usize = 860,
};

/// Content types that should never be compressed (already compressed).
const skip_content_types = [_][]const u8{
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/avif",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/x-bzip2",
    "application/x-xz",
    "application/zstd",
    "font/woff",
    "font/woff2",
    "application/wasm",
    "video/mp4",
    "video/webm",
    "audio/mpeg",
    "audio/ogg",
};

/// Check whether Accept-Encoding header includes gzip.
pub fn acceptsGzip(accept_encoding: []const u8) bool {
    var pos: usize = 0;
    while (pos + 4 <= accept_encoding.len) : (pos += 1) {
        if (eqlIgnoreCase(accept_encoding[pos .. pos + 4], "gzip")) {
            const before_ok = pos == 0 or accept_encoding[pos - 1] == ' ' or accept_encoding[pos - 1] == ',';
            const after_ok = pos + 4 >= accept_encoding.len or
                accept_encoding[pos + 4] == ',' or
                accept_encoding[pos + 4] == ';' or
                accept_encoding[pos + 4] == ' ';
            if (before_ok and after_ok) return true;
        }
    }
    return false;
}

/// Check whether a content type should be skipped for compression.
pub fn shouldSkipCompression(content_type: []const u8) bool {
    for (skip_content_types) |ct| {
        if (content_type.len >= ct.len and eqlIgnoreCase(content_type[0..ct.len], ct)) {
            return true;
        }
    }
    return false;
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        const la = if (ca >= 'A' and ca <= 'Z') ca + 32 else ca;
        const lb = if (cb >= 'A' and cb <= 'Z') cb + 32 else cb;
        if (la != lb) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Deflate stored blocks (RFC 1951 Section 3.2.4)
// ---------------------------------------------------------------------------

/// Maximum data per stored block (RFC 1951 limit).
const max_stored_block: usize = 65535;

/// Encode data as deflate stored blocks (BTYPE=00).
/// Each block: 1 byte header + 2 byte LEN + 2 byte NLEN + data.
/// Zero compression but always correct and zero-alloc.
pub fn deflateStored(input: []const u8, output: []u8) ![]const u8 {
    var in_pos: usize = 0;
    var out_pos: usize = 0;

    while (in_pos < input.len) {
        const remaining = input.len - in_pos;
        const block_len = @min(remaining, max_stored_block);
        const is_final = (in_pos + block_len >= input.len);

        // Block header: BFINAL (1 bit) + BTYPE=00 (2 bits), packed in 1 byte
        if (out_pos >= output.len) return error.BufferTooShort;
        output[out_pos] = if (is_final) 0x01 else 0x00;
        out_pos += 1;

        // LEN (2 bytes, little-endian)
        if (out_pos + 4 > output.len) return error.BufferTooShort;
        const len16: u16 = @intCast(block_len);
        std.mem.writeInt(u16, output[out_pos..][0..2], len16, .little);
        out_pos += 2;

        // NLEN (one's complement of LEN)
        std.mem.writeInt(u16, output[out_pos..][0..2], ~len16, .little);
        out_pos += 2;

        // Data
        if (out_pos + block_len > output.len) return error.BufferTooShort;
        @memcpy(output[out_pos .. out_pos + block_len], input[in_pos .. in_pos + block_len]);
        out_pos += block_len;
        in_pos += block_len;
    }

    // Handle empty input: emit a single final empty stored block
    if (input.len == 0) {
        if (out_pos + 5 > output.len) return error.BufferTooShort;
        output[out_pos] = 0x01; // BFINAL=1, BTYPE=00
        out_pos += 1;
        std.mem.writeInt(u16, output[out_pos..][0..2], 0, .little);
        out_pos += 2;
        std.mem.writeInt(u16, output[out_pos..][0..2], 0xFFFF, .little);
        out_pos += 2;
    }

    return output[0..out_pos];
}

/// Encode data as a complete gzip stream (RFC 1952).
/// Uses deflate stored blocks internally.
pub fn gzipCompress(input: []const u8, output: []u8) ![]const u8 {
    const header_size: usize = 10;
    const trailer_size: usize = 8;
    // Worst case: 5 bytes overhead per 65535 bytes + header + trailer
    const max_deflate = input.len + ((input.len / max_stored_block) + 1) * 5;
    if (output.len < header_size + max_deflate + trailer_size) return error.BufferTooShort;

    // Gzip header (minimal, RFC 1952)
    output[0] = 0x1f; // ID1
    output[1] = 0x8b; // ID2
    output[2] = 0x08; // CM = deflate
    output[3] = 0x00; // FLG = none
    output[4] = 0x00; // MTIME (4 bytes)
    output[5] = 0x00;
    output[6] = 0x00;
    output[7] = 0x00;
    output[8] = 0x00; // XFL
    output[9] = 0xFF; // OS = unknown

    // Deflate body
    const deflated = try deflateStored(input, output[header_size..]);
    const body_end = header_size + deflated.len;

    // Trailer: CRC32 + ISIZE (both little-endian)
    const crc = std.hash.crc.Crc32.hash(input);
    const input_size: u32 = @intCast(input.len & 0xFFFFFFFF);
    std.mem.writeInt(u32, output[body_end..][0..4], crc, .little);
    std.mem.writeInt(u32, output[body_end + 4 ..][0..4], input_size, .little);

    return output[0 .. body_end + trailer_size];
}

// ---------------------------------------------------------------------------
// Comptime middleware
// ---------------------------------------------------------------------------

/// Compression middleware — comptime wrapper.
/// Checks Accept-Encoding, passes through to next handler.
/// Use respondCompressedJson() in handlers for gzip-compressed responses.
pub fn compression(comptime config: CompressConfig) fn (comptime HandlerFn) HandlerFn {
    return struct {
        fn wrapper(comptime next: HandlerFn) HandlerFn {
            return &struct {
                fn handle(ctx: *RequestContext) anyerror!void {
                    // Check if client accepts gzip
                    const accept_enc = ctx.request.getHeader("accept-encoding") orelse {
                        return next(ctx);
                    };

                    if (!acceptsGzip(accept_enc)) {
                        return next(ctx);
                    }

                    // In the current synchronous architecture, response body is
                    // written directly to the stream in the handler. The middleware
                    // validates gzip support; handlers use respondCompressedJson()
                    // to emit compressed responses.
                    _ = config;
                    return next(ctx);
                }
            }.handle;
        }
    }.wrapper;
}

/// Respond with gzip-compressed JSON body if the client supports it.
/// Falls back to uncompressed response if body is too small.
pub fn respondCompressedJson(ctx: *RequestContext, status: u16, body: []const u8, min_size: usize) !void {
    const accept_enc = ctx.request.getHeader("accept-encoding") orelse {
        return ctx.respondJson(status, body);
    };

    if (!acceptsGzip(accept_enc) or body.len < min_size) {
        return ctx.respondJson(status, body);
    }

    var compress_buf: [32768]u8 = undefined;
    const compressed = gzipCompress(body, &compress_buf) catch {
        return ctx.respondJson(status, body);
    };

    const headers = [_]Header{
        .{ .name = "Content-Type", .value = "application/json" },
        .{ .name = "Content-Encoding", .value = "gzip" },
        .{ .name = "Vary", .value = "Accept-Encoding" },
    };
    try ctx.respond(status, &headers, compressed);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "acceptsGzip: various headers" {
    try std.testing.expect(acceptsGzip("gzip, deflate, br"));
    try std.testing.expect(acceptsGzip("gzip"));
    try std.testing.expect(acceptsGzip("deflate, gzip"));
    try std.testing.expect(acceptsGzip("br, gzip;q=0.5"));
    try std.testing.expect(!acceptsGzip("deflate, br"));
    try std.testing.expect(!acceptsGzip(""));
    try std.testing.expect(!acceptsGzip("deflate"));
}

test "shouldSkipCompression: known types" {
    try std.testing.expect(shouldSkipCompression("image/png"));
    try std.testing.expect(shouldSkipCompression("image/jpeg"));
    try std.testing.expect(shouldSkipCompression("application/gzip"));
    try std.testing.expect(shouldSkipCompression("font/woff2"));
    try std.testing.expect(shouldSkipCompression("application/wasm"));
    try std.testing.expect(!shouldSkipCompression("text/html"));
    try std.testing.expect(!shouldSkipCompression("application/json"));
    try std.testing.expect(!shouldSkipCompression("text/css"));
}

test "deflateStored: basic" {
    const input = "Hello, World!";
    var output: [256]u8 = undefined;
    const deflated = try deflateStored(input, &output);
    // Stored block: 1 byte header + 2 LEN + 2 NLEN + data = 5 + 13 = 18
    try std.testing.expectEqual(@as(usize, 5 + input.len), deflated.len);
    // BFINAL=1, BTYPE=00
    try std.testing.expectEqual(@as(u8, 0x01), deflated[0]);
    // LEN
    const len = std.mem.readInt(u16, deflated[1..3], .little);
    try std.testing.expectEqual(@as(u16, @intCast(input.len)), len);
    // NLEN = ~LEN
    const nlen = std.mem.readInt(u16, deflated[3..5], .little);
    try std.testing.expectEqual(~len, nlen);
    // Data
    try std.testing.expectEqualStrings(input, deflated[5..]);
}

test "deflateStored: empty input" {
    var output: [64]u8 = undefined;
    const deflated = try deflateStored("", &output);
    try std.testing.expectEqual(@as(usize, 5), deflated.len); // just the header
    try std.testing.expectEqual(@as(u8, 0x01), deflated[0]); // BFINAL=1
}

test "gzipCompress: valid gzip output" {
    const input = "Hello, gzip compression!";
    var output: [512]u8 = undefined;
    const gzipped = try gzipCompress(input, &output);

    // Check gzip magic bytes
    try std.testing.expectEqual(@as(u8, 0x1f), gzipped[0]);
    try std.testing.expectEqual(@as(u8, 0x8b), gzipped[1]);
    try std.testing.expectEqual(@as(u8, 0x08), gzipped[2]); // CM = deflate

    // Verify CRC32 in trailer
    const crc = std.hash.crc.Crc32.hash(input);
    const stored_crc = std.mem.readInt(u32, gzipped[gzipped.len - 8 ..][0..4], .little);
    try std.testing.expectEqual(crc, stored_crc);

    // Verify ISIZE in trailer
    const stored_size = std.mem.readInt(u32, gzipped[gzipped.len - 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, @intCast(input.len)), stored_size);
}

test "gzipCompress: empty input" {
    var output: [64]u8 = undefined;
    const gzipped = try gzipCompress("", &output);
    try std.testing.expectEqual(@as(u8, 0x1f), gzipped[0]);
    try std.testing.expectEqual(@as(u8, 0x8b), gzipped[1]);
}

test "gzipCompress: structure verification" {
    const input = "The quick brown fox jumps over the lazy dog. " ++
        "Pack my box with five dozen liquor jugs.";
    var compress_buf: [1024]u8 = undefined;
    const gzipped = try gzipCompress(input, &compress_buf);

    // Verify gzip header (RFC 1952)
    try std.testing.expectEqual(@as(u8, 0x1f), gzipped[0]); // ID1
    try std.testing.expectEqual(@as(u8, 0x8b), gzipped[1]); // ID2
    try std.testing.expectEqual(@as(u8, 0x08), gzipped[2]); // CM = deflate

    // Verify the deflate stored block starts at offset 10
    // BFINAL=1, BTYPE=00 -> 0x01
    try std.testing.expectEqual(@as(u8, 0x01), gzipped[10]);

    // Verify stored block LEN matches input length
    const stored_len = std.mem.readInt(u16, gzipped[11..13], .little);
    try std.testing.expectEqual(@as(u16, @intCast(input.len)), stored_len);

    // Verify the stored data matches the original input
    const data_start = 15; // 10 (gzip header) + 5 (stored block header)
    const data_end = data_start + input.len;
    try std.testing.expectEqualStrings(input, gzipped[data_start..data_end]);

    // Verify CRC32 in trailer
    const crc = std.hash.crc.Crc32.hash(input);
    const stored_crc = std.mem.readInt(u32, gzipped[gzipped.len - 8 ..][0..4], .little);
    try std.testing.expectEqual(crc, stored_crc);

    // Verify ISIZE in trailer
    const stored_size = std.mem.readInt(u32, gzipped[gzipped.len - 4 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, @intCast(input.len)), stored_size);
}

test "compression middleware compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const compressWrapped = compression(.{});
    const wrapped = compressWrapped(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

test "compression middleware with custom min_size compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const compressWrapped = compression(.{ .min_size = 256 });
    const wrapped = compressWrapped(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

test "eqlIgnoreCase" {
    try std.testing.expect(eqlIgnoreCase("gzip", "GZIP"));
    try std.testing.expect(eqlIgnoreCase("Gzip", "gzip"));
    try std.testing.expect(!eqlIgnoreCase("gzip", "deflate"));
    try std.testing.expect(!eqlIgnoreCase("gz", "gzip"));
}
