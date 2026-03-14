// Layer 2: Server-Sent Events (SSE) — zero-allocation event formatting
//
// Formats and writes SSE events per the W3C EventSource specification.
// All formatting writes into caller-provided buffers — no heap allocation.

const std = @import("std");
const http_parser = @import("../layer0/http/parser.zig");

pub const Header = http_parser.Header;

/// A single SSE event with optional fields.
pub const SseEvent = struct {
    data: []const u8,
    event: ?[]const u8 = null,
    id: ?[]const u8 = null,
    retry: ?u32 = null,
};

/// Standard SSE response headers.
/// Returns a comptime-known header array for SSE streams.
pub const sse_headers = [_]Header{
    .{ .name = "Content-Type", .value = "text/event-stream" },
    .{ .name = "Cache-Control", .value = "no-cache" },
    .{ .name = "Connection", .value = "keep-alive" },
};

/// Return SSE headers as a slice (for use with respond()).
pub fn sseHeaders() []const Header {
    return &sse_headers;
}

/// Format an SSE event into the provided buffer.
/// Returns the formatted event as a slice.
///
/// Output format (per W3C spec):
///   event: <event>\n    (if event is set)
///   id: <id>\n          (if id is set)
///   retry: <retry>\n    (if retry is set)
///   data: <line1>\n     (each line of data gets its own "data:" prefix)
///   data: <line2>\n
///   \n                  (blank line terminates the event)
pub fn formatEvent(ev: SseEvent, buf: []u8) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    // Event type
    if (ev.event) |event_type| {
        writer.writeAll("event: ") catch return error.BufferTooShort;
        writer.writeAll(event_type) catch return error.BufferTooShort;
        writer.writeAll("\n") catch return error.BufferTooShort;
    }

    // Event ID
    if (ev.id) |id| {
        writer.writeAll("id: ") catch return error.BufferTooShort;
        writer.writeAll(id) catch return error.BufferTooShort;
        writer.writeAll("\n") catch return error.BufferTooShort;
    }

    // Retry interval
    if (ev.retry) |retry_ms| {
        writer.writeAll("retry: ") catch return error.BufferTooShort;
        writer.print("{d}", .{retry_ms}) catch return error.BufferTooShort;
        writer.writeAll("\n") catch return error.BufferTooShort;
    }

    // Data — each line gets its own "data:" prefix
    var remaining = ev.data;
    while (remaining.len > 0) {
        writer.writeAll("data: ") catch return error.BufferTooShort;
        if (std.mem.indexOfScalar(u8, remaining, '\n')) |newline| {
            writer.writeAll(remaining[0..newline]) catch return error.BufferTooShort;
            writer.writeAll("\n") catch return error.BufferTooShort;
            remaining = remaining[newline + 1 ..];
        } else {
            writer.writeAll(remaining) catch return error.BufferTooShort;
            writer.writeAll("\n") catch return error.BufferTooShort;
            remaining = remaining[remaining.len..];
        }
    }

    // Blank line to terminate the event
    writer.writeAll("\n") catch return error.BufferTooShort;

    return stream.getWritten();
}

/// Format an SSE comment (lines starting with ':').
/// Comments are used for keep-alive pings.
pub fn formatComment(comment: []const u8, buf: []u8) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeAll(": ") catch return error.BufferTooShort;
    writer.writeAll(comment) catch return error.BufferTooShort;
    writer.writeAll("\n\n") catch return error.BufferTooShort;

    return stream.getWritten();
}

/// Build SSE response headers and initial response line into a buffer.
/// Suitable for writing directly to a TCP stream to initiate an SSE connection.
pub fn buildSseResponse(buf: []u8) !usize {
    return http_parser.encodeResponse(buf, 200, sseHeaders(), null);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "formatEvent: data only" {
    var buf: [256]u8 = undefined;
    const result = try formatEvent(.{ .data = "hello" }, &buf);
    try std.testing.expectEqualStrings("data: hello\n\n", result);
}

test "formatEvent: with event type" {
    var buf: [256]u8 = undefined;
    const result = try formatEvent(.{
        .data = "world",
        .event = "greeting",
    }, &buf);
    try std.testing.expectEqualStrings("event: greeting\ndata: world\n\n", result);
}

test "formatEvent: with id" {
    var buf: [256]u8 = undefined;
    const result = try formatEvent(.{
        .data = "msg",
        .id = "42",
    }, &buf);
    try std.testing.expectEqualStrings("id: 42\ndata: msg\n\n", result);
}

test "formatEvent: with retry" {
    var buf: [256]u8 = undefined;
    const result = try formatEvent(.{
        .data = "reconnect test",
        .retry = 5000,
    }, &buf);
    try std.testing.expectEqualStrings("retry: 5000\ndata: reconnect test\n\n", result);
}

test "formatEvent: all fields" {
    var buf: [512]u8 = undefined;
    const result = try formatEvent(.{
        .data = "{\"count\":1}",
        .event = "update",
        .id = "evt-001",
        .retry = 3000,
    }, &buf);
    try std.testing.expectEqualStrings(
        "event: update\nid: evt-001\nretry: 3000\ndata: {\"count\":1}\n\n",
        result,
    );
}

test "formatEvent: multiline data" {
    var buf: [512]u8 = undefined;
    const result = try formatEvent(.{
        .data = "line1\nline2\nline3",
    }, &buf);
    try std.testing.expectEqualStrings("data: line1\ndata: line2\ndata: line3\n\n", result);
}

test "formatEvent: empty data" {
    var buf: [64]u8 = undefined;
    const result = try formatEvent(.{ .data = "" }, &buf);
    try std.testing.expectEqualStrings("\n", result);
}

test "formatComment: keep-alive" {
    var buf: [128]u8 = undefined;
    const result = try formatComment("keepalive", &buf);
    try std.testing.expectEqualStrings(": keepalive\n\n", result);
}

test "sseHeaders: correct content type" {
    const hdrs = sseHeaders();
    try std.testing.expectEqual(@as(usize, 3), hdrs.len);
    try std.testing.expectEqualStrings("Content-Type", hdrs[0].name);
    try std.testing.expectEqualStrings("text/event-stream", hdrs[0].value);
    try std.testing.expectEqualStrings("Cache-Control", hdrs[1].name);
    try std.testing.expectEqualStrings("no-cache", hdrs[1].value);
    try std.testing.expectEqualStrings("Connection", hdrs[2].name);
    try std.testing.expectEqualStrings("keep-alive", hdrs[2].value);
}

test "buildSseResponse: valid HTTP response" {
    var buf: [512]u8 = undefined;
    const n = try buildSseResponse(&buf);
    const resp = buf[0..n];
    try std.testing.expect(std.mem.startsWith(u8, resp, "HTTP/1.1 200 OK\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "Content-Type: text/event-stream") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "Cache-Control: no-cache") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "Connection: keep-alive") != null);
}

test "formatEvent: buffer too short" {
    var buf: [5]u8 = undefined;
    try std.testing.expectError(error.BufferTooShort, formatEvent(.{
        .data = "this is a long message that won't fit",
    }, &buf));
}

test "formatEvent: JSON payload" {
    var buf: [512]u8 = undefined;
    const result = try formatEvent(.{
        .data = "{\"user\":\"alice\",\"action\":\"login\"}",
        .event = "user.login",
        .id = "1001",
    }, &buf);
    try std.testing.expect(std.mem.indexOf(u8, result, "event: user.login\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "id: 1001\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "data: {\"user\":\"alice\",\"action\":\"login\"}\n") != null);
}
