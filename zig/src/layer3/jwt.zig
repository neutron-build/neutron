// Layer 3: JWT Authentication Middleware — HMAC-SHA256, zero-alloc hot path
//
// Comptime middleware wrapper that verifies JWTs on every request.
// Extracts Bearer token, decodes base64url, verifies HMAC-SHA256 signature,
// and checks expiration. Returns RFC 7807 errors on failure.

const std = @import("std");
const http_server = @import("../layer2/http_server.zig");
const http_parser = @import("../layer0/http/parser.zig");
const app_error = @import("error.zig");

pub const RequestContext = http_server.RequestContext;
pub const HandlerFn = *const fn (*RequestContext) anyerror!void;
pub const Header = http_parser.Header;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

/// JWT configuration — all comptime-known values.
pub const JwtConfig = struct {
    secret: []const u8,
    issuer: ?[]const u8 = null,
    audience: ?[]const u8 = null,
};

/// Decoded JWT claims. All fields are slices into the decode buffer.
pub const Claims = struct {
    sub: []const u8,
    iss: []const u8,
    aud: []const u8,
    exp: i64,
    iat: i64,
};

// ---------------------------------------------------------------------------
// Base64url utilities — zero allocation, writes into caller buffer
// ---------------------------------------------------------------------------

const base64url_alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".*;

const base64url_decode_table: [256]u8 = blk: {
    var table: [256]u8 = [_]u8{0xFF} ** 256;
    for (base64url_alphabet, 0..) |c, i| {
        table[c] = @intCast(i);
    }
    break :blk table;
};

/// Decode base64url-encoded data into output buffer. Returns decoded slice.
pub fn base64urlDecode(input: []const u8, output: []u8) ![]const u8 {
    if (input.len == 0) return output[0..0];

    // Calculate padding needed
    const padding = (4 - (input.len % 4)) % 4;
    const total_len = input.len + padding;
    const out_len = (total_len / 4) * 3;

    if (output.len < out_len) return error.BufferTooShort;

    var out_pos: usize = 0;
    var i: usize = 0;

    while (i + 4 <= input.len) : (i += 4) {
        const a = base64url_decode_table[input[i]];
        const b = base64url_decode_table[input[i + 1]];
        const c = base64url_decode_table[input[i + 2]];
        const d = base64url_decode_table[input[i + 3]];
        if (a == 0xFF or b == 0xFF or c == 0xFF or d == 0xFF) return error.InvalidBase64;

        const triple: u24 = (@as(u24, a) << 18) | (@as(u24, b) << 12) | (@as(u24, c) << 6) | @as(u24, d);
        output[out_pos] = @intCast(triple >> 16);
        output[out_pos + 1] = @intCast((triple >> 8) & 0xFF);
        output[out_pos + 2] = @intCast(triple & 0xFF);
        out_pos += 3;
    }

    // Handle remaining bytes (1-3 leftover means 2 or 3 base64 chars)
    const remaining = input.len - i;
    if (remaining == 2) {
        const a = base64url_decode_table[input[i]];
        const b = base64url_decode_table[input[i + 1]];
        if (a == 0xFF or b == 0xFF) return error.InvalidBase64;
        output[out_pos] = @intCast((@as(u16, a) << 2) | (@as(u16, b) >> 4));
        out_pos += 1;
    } else if (remaining == 3) {
        const a = base64url_decode_table[input[i]];
        const b = base64url_decode_table[input[i + 1]];
        const c = base64url_decode_table[input[i + 2]];
        if (a == 0xFF or b == 0xFF or c == 0xFF) return error.InvalidBase64;
        const triple: u24 = (@as(u24, a) << 18) | (@as(u24, b) << 12) | (@as(u24, c) << 6);
        output[out_pos] = @intCast(triple >> 16);
        output[out_pos + 1] = @intCast((triple >> 8) & 0xFF);
        out_pos += 2;
    }

    return output[0..out_pos];
}

/// Encode data as base64url (no padding) into output buffer.
pub fn base64urlEncode(input: []const u8, output: []u8) ![]const u8 {
    const out_len = ((input.len * 4) + 2) / 3;
    if (output.len < out_len) return error.BufferTooShort;

    var out_pos: usize = 0;
    var i: usize = 0;

    while (i + 3 <= input.len) : (i += 3) {
        const triple: u24 = (@as(u24, input[i]) << 16) |
            (@as(u24, input[i + 1]) << 8) |
            @as(u24, input[i + 2]);
        output[out_pos] = base64url_alphabet[@intCast(triple >> 18)];
        output[out_pos + 1] = base64url_alphabet[@intCast((triple >> 12) & 0x3F)];
        output[out_pos + 2] = base64url_alphabet[@intCast((triple >> 6) & 0x3F)];
        output[out_pos + 3] = base64url_alphabet[@intCast(triple & 0x3F)];
        out_pos += 4;
    }

    const remaining = input.len - i;
    if (remaining == 1) {
        output[out_pos] = base64url_alphabet[@intCast(@as(u8, input[i]) >> 2)];
        output[out_pos + 1] = base64url_alphabet[@intCast((@as(u16, input[i]) & 0x03) << 4)];
        out_pos += 2;
    } else if (remaining == 2) {
        output[out_pos] = base64url_alphabet[@intCast(@as(u8, input[i]) >> 2)];
        output[out_pos + 1] = base64url_alphabet[@intCast(((@as(u16, input[i]) & 0x03) << 4) | (@as(u16, input[i + 1]) >> 4))];
        output[out_pos + 2] = base64url_alphabet[@intCast((@as(u16, input[i + 1]) & 0x0F) << 2)];
        out_pos += 3;
    }

    return output[0..out_pos];
}

// ---------------------------------------------------------------------------
// JWT token operations
// ---------------------------------------------------------------------------

/// Sign a JWT payload with HMAC-SHA256. Returns the signature as raw bytes.
pub fn sign(message: []const u8, secret: []const u8) [HmacSha256.mac_length]u8 {
    var mac: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&mac, message, secret);
    return mac;
}

/// Verify a JWT signature against the expected HMAC-SHA256.
pub fn verifySignature(message: []const u8, signature: []const u8, secret: []const u8) bool {
    if (signature.len != HmacSha256.mac_length) return false;
    const expected = sign(message, secret);
    return std.mem.eql(u8, signature, &expected);
}

/// Split a JWT token into its three parts: header, payload, signature.
/// Returns slices into the original token string.
pub fn splitToken(token: []const u8) !struct { header: []const u8, payload: []const u8, signature: []const u8 } {
    const first_dot = std.mem.indexOfScalar(u8, token, '.') orelse return error.InvalidToken;
    const rest = token[first_dot + 1 ..];
    const second_dot = std.mem.indexOfScalar(u8, rest, '.') orelse return error.InvalidToken;

    return .{
        .header = token[0..first_dot],
        .payload = rest[0..second_dot],
        .signature = rest[second_dot + 1 ..],
    };
}

/// Parse a JSON number from a field in the payload.
/// Zero-alloc: scans the buffer directly.
fn parseJsonI64(json: []const u8, key: []const u8) ?i64 {
    // Search for "key": followed by a number
    var pos: usize = 0;
    while (pos + key.len + 3 < json.len) : (pos += 1) {
        // Look for "key":
        if (json[pos] == '"' and pos + 1 + key.len + 1 < json.len) {
            if (std.mem.eql(u8, json[pos + 1 .. pos + 1 + key.len], key)) {
                const after_key = pos + 1 + key.len;
                if (after_key < json.len and json[after_key] == '"') {
                    // Skip ":<whitespace>
                    var p = after_key + 1;
                    while (p < json.len and (json[p] == ':' or json[p] == ' ')) : (p += 1) {}
                    // Parse number (possibly negative)
                    var end = p;
                    if (end < json.len and json[end] == '-') end += 1;
                    while (end < json.len and json[end] >= '0' and json[end] <= '9') : (end += 1) {}
                    if (end > p) {
                        return std.fmt.parseInt(i64, json[p..end], 10) catch null;
                    }
                }
            }
        }
    }
    return null;
}

/// Parse a JSON string value from a field in the payload.
/// Returns a slice into the input json.
fn parseJsonString(json: []const u8, key: []const u8) ?[]const u8 {
    var pos: usize = 0;
    while (pos + key.len + 3 < json.len) : (pos += 1) {
        if (json[pos] == '"' and pos + 1 + key.len + 1 < json.len) {
            if (std.mem.eql(u8, json[pos + 1 .. pos + 1 + key.len], key)) {
                const after_key = pos + 1 + key.len;
                if (after_key < json.len and json[after_key] == '"') {
                    var p = after_key + 1;
                    while (p < json.len and (json[p] == ':' or json[p] == ' ')) : (p += 1) {}
                    if (p < json.len and json[p] == '"') {
                        const str_start = p + 1;
                        var str_end = str_start;
                        while (str_end < json.len and json[str_end] != '"') : (str_end += 1) {}
                        if (str_end < json.len) {
                            return json[str_start..str_end];
                        }
                    }
                }
            }
        }
    }
    return null;
}

/// Decode and parse JWT claims from a token.
/// Writes decoded payload into decode_buf and returns Claims with slices into it.
pub fn decodeClaims(token: []const u8, secret: []const u8, decode_buf: []u8) !Claims {
    const parts = try splitToken(token);

    // Decode signature
    var sig_buf: [64]u8 = undefined;
    const sig = base64urlDecode(parts.signature, &sig_buf) catch return error.InvalidToken;

    // Verify signature over "header.payload"
    const dot_pos = std.mem.indexOfScalar(u8, token, '.') orelse return error.InvalidToken;
    const rest = token[dot_pos + 1 ..];
    const second_dot = std.mem.indexOfScalar(u8, rest, '.') orelse return error.InvalidToken;
    const signed_part = token[0 .. dot_pos + 1 + second_dot];

    if (!verifySignature(signed_part, sig, secret)) return error.InvalidSignature;

    // Decode payload
    const payload = base64urlDecode(parts.payload, decode_buf) catch return error.InvalidToken;

    return .{
        .sub = parseJsonString(payload, "sub") orelse "",
        .iss = parseJsonString(payload, "iss") orelse "",
        .aud = parseJsonString(payload, "aud") orelse "",
        .exp = parseJsonI64(payload, "exp") orelse 0,
        .iat = parseJsonI64(payload, "iat") orelse 0,
    };
}

// ---------------------------------------------------------------------------
// Comptime middleware
// ---------------------------------------------------------------------------

/// JWT authentication middleware — comptime wrapper with configurable options.
/// Extracts Bearer token from Authorization header, verifies HMAC-SHA256,
/// checks expiration, and returns RFC 7807 error on failure.
pub fn jwtMiddleware(comptime config: JwtConfig) fn (comptime HandlerFn) HandlerFn {
    return struct {
        fn wrapper(comptime next: HandlerFn) HandlerFn {
            return &struct {
                fn handle(ctx: *RequestContext) anyerror!void {
                    // Extract Authorization header
                    const auth_header = ctx.request.getHeader("authorization") orelse {
                        return respondUnauthorized(ctx, "Missing Authorization header");
                    };

                    // Must be Bearer scheme
                    const bearer_prefix = "Bearer ";
                    if (auth_header.len <= bearer_prefix.len or
                        !std.mem.eql(u8, auth_header[0..bearer_prefix.len], bearer_prefix))
                    {
                        return respondUnauthorized(ctx, "Invalid Authorization scheme, expected Bearer");
                    }

                    const token = auth_header[bearer_prefix.len..];

                    // Decode and verify token — uses stack buffer, zero heap alloc
                    var decode_buf: [2048]u8 = undefined;
                    const claims = decodeClaims(token, config.secret, &decode_buf) catch {
                        return respondUnauthorized(ctx, "Invalid or malformed JWT");
                    };

                    // Check expiration
                    const now = @divTrunc(std.time.timestamp(), 1);
                    if (claims.exp > 0 and claims.exp < now) {
                        return respondUnauthorized(ctx, "Token has expired");
                    }

                    // Check issuer if configured
                    if (config.issuer) |expected_iss| {
                        if (!std.mem.eql(u8, claims.iss, expected_iss)) {
                            return respondUnauthorized(ctx, "Invalid token issuer");
                        }
                    }

                    // Check audience if configured
                    if (config.audience) |expected_aud| {
                        if (!std.mem.eql(u8, claims.aud, expected_aud)) {
                            return respondUnauthorized(ctx, "Invalid token audience");
                        }
                    }

                    // Token valid — call next handler
                    return next(ctx);
                }
            }.handle;
        }
    }.wrapper;
}

fn respondUnauthorized(ctx: *RequestContext, detail: []const u8) void {
    var buf: [512]u8 = undefined;
    const err = app_error.unauthorized(detail);
    const json = err.toJson(&buf) catch return;
    const headers = [_]Header{
        .{ .name = "Content-Type", .value = "application/problem+json" },
        .{ .name = "WWW-Authenticate", .value = "Bearer" },
    };
    ctx.respond(401, &headers, json) catch {};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "base64url encode/decode round trip" {
    var encode_buf: [256]u8 = undefined;
    var decode_buf: [256]u8 = undefined;

    const input = "hello, world!";
    const encoded = try base64urlEncode(input, &encode_buf);
    const decoded = try base64urlDecode(encoded, &decode_buf);
    try std.testing.expectEqualStrings(input, decoded);
}

test "base64url decode known value" {
    // "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" is base64url for {"alg":"HS256","typ":"JWT"}
    var buf: [256]u8 = undefined;
    const decoded = try base64urlDecode("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", &buf);
    try std.testing.expectEqualStrings("{\"alg\":\"HS256\",\"typ\":\"JWT\"}", decoded);
}

test "base64url encode empty" {
    var buf: [16]u8 = undefined;
    const encoded = try base64urlEncode("", &buf);
    try std.testing.expectEqual(@as(usize, 0), encoded.len);
}

test "base64url various lengths" {
    var encode_buf: [64]u8 = undefined;
    var decode_buf: [64]u8 = undefined;

    // 1 byte
    const e1 = try base64urlEncode("a", &encode_buf);
    const d1 = try base64urlDecode(e1, &decode_buf);
    try std.testing.expectEqualStrings("a", d1);

    // 2 bytes
    const e2 = try base64urlEncode("ab", &encode_buf);
    const d2 = try base64urlDecode(e2, &decode_buf);
    try std.testing.expectEqualStrings("ab", d2);

    // 3 bytes (exact multiple)
    const e3 = try base64urlEncode("abc", &encode_buf);
    const d3 = try base64urlDecode(e3, &decode_buf);
    try std.testing.expectEqualStrings("abc", d3);
}

test "sign and verify HMAC-SHA256" {
    const secret = "my-secret-key";
    const message = "header.payload";
    const mac = sign(message, secret);
    try std.testing.expect(verifySignature(message, &mac, secret));
    try std.testing.expect(!verifySignature("tampered.payload", &mac, secret));
}

test "splitToken valid" {
    const parts = try splitToken("aaa.bbb.ccc");
    try std.testing.expectEqualStrings("aaa", parts.header);
    try std.testing.expectEqualStrings("bbb", parts.payload);
    try std.testing.expectEqualStrings("ccc", parts.signature);
}

test "splitToken invalid — no dots" {
    try std.testing.expectError(error.InvalidToken, splitToken("nodots"));
}

test "splitToken invalid — one dot" {
    try std.testing.expectError(error.InvalidToken, splitToken("one.dot"));
}

test "parseJsonI64 basic" {
    try std.testing.expectEqual(@as(?i64, 1700000000), parseJsonI64("{\"exp\": 1700000000}", "exp"));
    try std.testing.expectEqual(@as(?i64, 42), parseJsonI64("{\"iat\": 42, \"exp\": 99}", "iat"));
    try std.testing.expectEqual(@as(?i64, null), parseJsonI64("{\"foo\": 1}", "exp"));
}

test "parseJsonString basic" {
    const json = "{\"sub\": \"user-123\", \"iss\": \"neutron\"}";
    try std.testing.expectEqualStrings("user-123", parseJsonString(json, "sub").?);
    try std.testing.expectEqualStrings("neutron", parseJsonString(json, "iss").?);
    try std.testing.expectEqual(@as(?[]const u8, null), parseJsonString(json, "aud"));
}

test "jwtMiddleware compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const jwtWrapped = jwtMiddleware(.{ .secret = "test-secret" });
    const wrapped = jwtWrapped(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

test "jwtMiddleware with issuer and audience compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const jwtWrapped = jwtMiddleware(.{
        .secret = "test-secret",
        .issuer = "neutron",
        .audience = "api",
    });
    const wrapped = jwtWrapped(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

test "full JWT sign and decode" {
    // Build a minimal JWT: header.payload.signature
    const secret = "test-secret-key";

    // Header: {"alg":"HS256","typ":"JWT"}
    const header_json = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
    var header_b64: [256]u8 = undefined;
    const header_enc = try base64urlEncode(header_json, &header_b64);

    // Payload: {"sub":"user-1","iss":"test","aud":"app","exp":9999999999,"iat":1000000000}
    const payload_json = "{\"sub\":\"user-1\",\"iss\":\"test\",\"aud\":\"app\",\"exp\":9999999999,\"iat\":1000000000}";
    var payload_b64: [256]u8 = undefined;
    const payload_enc = try base64urlEncode(payload_json, &payload_b64);

    // Build signing input: header.payload
    var signing_buf: [512]u8 = undefined;
    const signing_input_len = header_enc.len + 1 + payload_enc.len;
    @memcpy(signing_buf[0..header_enc.len], header_enc);
    signing_buf[header_enc.len] = '.';
    @memcpy(signing_buf[header_enc.len + 1 .. signing_input_len], payload_enc);
    const signing_input = signing_buf[0..signing_input_len];

    // Sign
    const mac = sign(signing_input, secret);
    var sig_b64: [256]u8 = undefined;
    const sig_enc = try base64urlEncode(&mac, &sig_b64);

    // Build full token: header.payload.signature
    var token_buf: [1024]u8 = undefined;
    const token_len = signing_input_len + 1 + sig_enc.len;
    @memcpy(token_buf[0..signing_input_len], signing_input);
    token_buf[signing_input_len] = '.';
    @memcpy(token_buf[signing_input_len + 1 .. token_len], sig_enc);
    const token = token_buf[0..token_len];

    // Decode and verify
    var decode_buf: [2048]u8 = undefined;
    const claims = try decodeClaims(token, secret, &decode_buf);
    try std.testing.expectEqualStrings("user-1", claims.sub);
    try std.testing.expectEqualStrings("test", claims.iss);
    try std.testing.expectEqualStrings("app", claims.aud);
    try std.testing.expectEqual(@as(i64, 9999999999), claims.exp);
    try std.testing.expectEqual(@as(i64, 1000000000), claims.iat);
}

test "decodeClaims rejects bad signature" {
    const secret = "real-secret";

    const header_json = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
    var header_b64: [256]u8 = undefined;
    const header_enc = try base64urlEncode(header_json, &header_b64);

    const payload_json = "{\"sub\":\"user\",\"exp\":9999999999}";
    var payload_b64: [256]u8 = undefined;
    const payload_enc = try base64urlEncode(payload_json, &payload_b64);

    // Sign with wrong secret
    var signing_buf: [512]u8 = undefined;
    const signing_input_len = header_enc.len + 1 + payload_enc.len;
    @memcpy(signing_buf[0..header_enc.len], header_enc);
    signing_buf[header_enc.len] = '.';
    @memcpy(signing_buf[header_enc.len + 1 .. signing_input_len], payload_enc);

    const mac = sign(signing_buf[0..signing_input_len], "wrong-secret");
    var sig_b64: [256]u8 = undefined;
    const sig_enc = try base64urlEncode(&mac, &sig_b64);

    var token_buf: [1024]u8 = undefined;
    const token_len = signing_input_len + 1 + sig_enc.len;
    @memcpy(token_buf[0..signing_input_len], signing_buf[0..signing_input_len]);
    token_buf[signing_input_len] = '.';
    @memcpy(token_buf[signing_input_len + 1 .. token_len], sig_enc);

    var decode_buf: [2048]u8 = undefined;
    try std.testing.expectError(error.InvalidSignature, decodeClaims(token_buf[0..token_len], secret, &decode_buf));
}
