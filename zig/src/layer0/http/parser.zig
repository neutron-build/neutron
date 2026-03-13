// Layer 0: HTTP/1.1 request/response parser — zero allocation
//
// Zero-copy parser: all string fields (method, path, headers, body) are
// slices into the input buffer. No heap allocation whatsoever.

const std = @import("std");

pub const Error = error{
    BufferTooShort,
    InvalidMethod,
    InvalidRequest,
    InvalidResponse,
    InvalidHeader,
    InvalidChunkSize,
    HeadersTooLarge,
    RequestTooLarge,
    IncompleteMessage,
};

pub const Method = enum {
    GET,
    POST,
    PUT,
    PATCH,
    DELETE,
    HEAD,
    OPTIONS,
    TRACE,
    CONNECT,

    pub fn fromString(s: []const u8) Error!Method {
        if (std.mem.eql(u8, s, "GET")) return .GET;
        if (std.mem.eql(u8, s, "POST")) return .POST;
        if (std.mem.eql(u8, s, "PUT")) return .PUT;
        if (std.mem.eql(u8, s, "PATCH")) return .PATCH;
        if (std.mem.eql(u8, s, "DELETE")) return .DELETE;
        if (std.mem.eql(u8, s, "HEAD")) return .HEAD;
        if (std.mem.eql(u8, s, "OPTIONS")) return .OPTIONS;
        if (std.mem.eql(u8, s, "TRACE")) return .TRACE;
        if (std.mem.eql(u8, s, "CONNECT")) return .CONNECT;
        return Error.InvalidMethod;
    }

    pub fn toString(self: Method) []const u8 {
        return switch (self) {
            .GET => "GET",
            .POST => "POST",
            .PUT => "PUT",
            .PATCH => "PATCH",
            .DELETE => "DELETE",
            .HEAD => "HEAD",
            .OPTIONS => "OPTIONS",
            .TRACE => "TRACE",
            .CONNECT => "CONNECT",
        };
    }
};

pub const Version = enum {
    http_1_0,
    http_1_1,

    pub fn toString(self: Version) []const u8 {
        return switch (self) {
            .http_1_0 => "HTTP/1.0",
            .http_1_1 => "HTTP/1.1",
        };
    }
};

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

/// Parsed HTTP request — all fields are slices into the input buffer.
pub const Request = struct {
    method: Method,
    path: []const u8,
    version: Version,
    header_data: []const u8,
    body: ?[]const u8,
    total_len: usize,

    /// Iterate over headers.
    pub fn headers(self: *const Request) HeaderIterator {
        return HeaderIterator.init(self.header_data);
    }

    /// Find a header by name (case-insensitive).
    pub fn getHeader(self: *const Request, name: []const u8) ?[]const u8 {
        var iter = self.headers();
        while (iter.next()) |h| {
            if (asciiEqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }

    /// Get Content-Length header value.
    pub fn contentLength(self: *const Request) ?usize {
        const val = self.getHeader("content-length") orelse return null;
        return std.fmt.parseInt(usize, val, 10) catch null;
    }
};

/// Parsed HTTP response — all fields are slices into the input buffer.
pub const Response = struct {
    version: Version,
    status: u16,
    reason: []const u8,
    header_data: []const u8,
    body: ?[]const u8,
    total_len: usize,

    pub fn headers(self: *const Response) HeaderIterator {
        return HeaderIterator.init(self.header_data);
    }

    pub fn getHeader(self: *const Response, name: []const u8) ?[]const u8 {
        var iter = self.headers();
        while (iter.next()) |h| {
            if (asciiEqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }
};

/// Zero-alloc header iterator — parses header lines lazily.
pub const HeaderIterator = struct {
    data: []const u8,
    pos: usize,

    pub fn init(data: []const u8) HeaderIterator {
        return .{ .data = data, .pos = 0 };
    }

    pub fn next(self: *HeaderIterator) ?Header {
        if (self.pos >= self.data.len) return null;
        const remaining = self.data[self.pos..];

        // Find end of line
        const line_end = findCRLF(remaining) orelse return null;
        if (line_end == 0) return null; // empty line = end of headers

        const line = remaining[0..line_end];

        // Find colon separator
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse return null;
        const name = line[0..colon];
        var value = line[colon + 1 ..];
        // Trim leading whitespace from value
        while (value.len > 0 and (value[0] == ' ' or value[0] == '\t')) {
            value = value[1..];
        }
        // Trim trailing whitespace
        while (value.len > 0 and (value[value.len - 1] == ' ' or value[value.len - 1] == '\t')) {
            value = value[0 .. value.len - 1];
        }

        self.pos += line_end + 2; // skip CRLF
        return .{ .name = name, .value = value };
    }
};

/// Parse an HTTP request from a buffer.
/// Returns the parsed request (zero-copy, fields are slices into buf).
pub fn parseRequest(buf: []const u8) Error!Request {
    // Find end of request line
    const req_line_end = findCRLF(buf) orelse return Error.IncompleteMessage;
    const req_line = buf[0..req_line_end];

    // Parse: METHOD SP PATH SP VERSION
    const method_end = std.mem.indexOfScalar(u8, req_line, ' ') orelse return Error.InvalidRequest;
    const method = try Method.fromString(req_line[0..method_end]);
    const rest = req_line[method_end + 1 ..];
    const path_end = std.mem.indexOfScalar(u8, rest, ' ') orelse return Error.InvalidRequest;
    const path = rest[0..path_end];
    const version_str = rest[path_end + 1 ..];
    const version: Version = if (std.mem.eql(u8, version_str, "HTTP/1.1"))
        .http_1_1
    else if (std.mem.eql(u8, version_str, "HTTP/1.0"))
        .http_1_0
    else
        return Error.InvalidRequest;

    // Headers start after request line
    const headers_start = req_line_end + 2;
    // Find end of headers (empty line = CRLFCRLF)
    const header_end = findHeaderEnd(buf[headers_start..]) orelse return Error.IncompleteMessage;
    const header_data = buf[headers_start .. headers_start + header_end];
    const body_start = headers_start + header_end + 2; // skip final CRLF

    // Determine body
    var body: ?[]const u8 = null;
    var total = body_start;

    // Check Content-Length
    var hdr_iter = HeaderIterator.init(header_data);
    while (hdr_iter.next()) |h| {
        if (asciiEqlIgnoreCase(h.name, "content-length")) {
            const cl = std.fmt.parseInt(usize, h.value, 10) catch return Error.InvalidHeader;
            if (buf.len < body_start + cl) return Error.IncompleteMessage;
            body = buf[body_start .. body_start + cl];
            total = body_start + cl;
            break;
        }
    }

    return .{
        .method = method,
        .path = path,
        .version = version,
        .header_data = header_data,
        .body = body,
        .total_len = total,
    };
}

/// Parse an HTTP response from a buffer.
pub fn parseResponse(buf: []const u8) Error!Response {
    const status_line_end = findCRLF(buf) orelse return Error.IncompleteMessage;
    const status_line = buf[0..status_line_end];

    // Parse: VERSION SP STATUS SP REASON
    const ver_end = std.mem.indexOfScalar(u8, status_line, ' ') orelse return Error.InvalidResponse;
    const version_str = status_line[0..ver_end];
    const version: Version = if (std.mem.eql(u8, version_str, "HTTP/1.1"))
        .http_1_1
    else if (std.mem.eql(u8, version_str, "HTTP/1.0"))
        .http_1_0
    else
        return Error.InvalidResponse;

    const rest = status_line[ver_end + 1 ..];
    const status_end = std.mem.indexOfScalar(u8, rest, ' ') orelse return Error.InvalidResponse;
    const status = std.fmt.parseInt(u16, rest[0..status_end], 10) catch return Error.InvalidResponse;
    const reason = rest[status_end + 1 ..];

    const headers_start = status_line_end + 2;
    const header_end = findHeaderEnd(buf[headers_start..]) orelse return Error.IncompleteMessage;
    const header_data = buf[headers_start .. headers_start + header_end];
    const body_start = headers_start + header_end + 2;

    var body: ?[]const u8 = null;
    var total = body_start;

    var hdr_iter = HeaderIterator.init(header_data);
    while (hdr_iter.next()) |h| {
        if (asciiEqlIgnoreCase(h.name, "content-length")) {
            const cl = std.fmt.parseInt(usize, h.value, 10) catch return Error.InvalidHeader;
            if (buf.len < body_start + cl) return Error.IncompleteMessage;
            body = buf[body_start .. body_start + cl];
            total = body_start + cl;
            break;
        }
    }

    return .{
        .version = version,
        .status = status,
        .reason = reason,
        .header_data = header_data,
        .body = body,
        .total_len = total,
    };
}

/// Encode an HTTP response into a buffer. Returns bytes written.
pub fn encodeResponse(buf: []u8, status: u16, headers_list: []const Header, body: ?[]const u8) Error!usize {
    var pos: usize = 0;

    // Status line
    const version = "HTTP/1.1 ";
    if (buf.len < version.len + 3 + 1) return Error.BufferTooShort;
    @memcpy(buf[pos .. pos + version.len], version);
    pos += version.len;
    pos += formatU16(buf[pos..], status);
    buf[pos] = ' ';
    pos += 1;
    const reason = statusReason(status);
    if (buf.len - pos < reason.len + 2) return Error.BufferTooShort;
    @memcpy(buf[pos .. pos + reason.len], reason);
    pos += reason.len;
    buf[pos] = '\r';
    buf[pos + 1] = '\n';
    pos += 2;

    // Headers
    for (headers_list) |h| {
        if (buf.len - pos < h.name.len + 2 + h.value.len + 2) return Error.BufferTooShort;
        @memcpy(buf[pos .. pos + h.name.len], h.name);
        pos += h.name.len;
        buf[pos] = ':';
        buf[pos + 1] = ' ';
        pos += 2;
        @memcpy(buf[pos .. pos + h.value.len], h.value);
        pos += h.value.len;
        buf[pos] = '\r';
        buf[pos + 1] = '\n';
        pos += 2;
    }

    // End of headers
    if (buf.len - pos < 2) return Error.BufferTooShort;
    buf[pos] = '\r';
    buf[pos + 1] = '\n';
    pos += 2;

    // Body
    if (body) |b| {
        if (buf.len - pos < b.len) return Error.BufferTooShort;
        @memcpy(buf[pos .. pos + b.len], b);
        pos += b.len;
    }

    return pos;
}

/// Encode an HTTP request into a buffer. Returns bytes written.
pub fn encodeRequest(buf: []u8, method: Method, path: []const u8, headers_list: []const Header, body: ?[]const u8) Error!usize {
    var pos: usize = 0;
    const method_str = method.toString();

    if (buf.len < method_str.len + 1 + path.len + 1 + 8 + 2) return Error.BufferTooShort;
    @memcpy(buf[pos .. pos + method_str.len], method_str);
    pos += method_str.len;
    buf[pos] = ' ';
    pos += 1;
    @memcpy(buf[pos .. pos + path.len], path);
    pos += path.len;
    const suffix = " HTTP/1.1\r\n";
    @memcpy(buf[pos .. pos + suffix.len], suffix);
    pos += suffix.len;

    for (headers_list) |h| {
        if (buf.len - pos < h.name.len + 2 + h.value.len + 2) return Error.BufferTooShort;
        @memcpy(buf[pos .. pos + h.name.len], h.name);
        pos += h.name.len;
        buf[pos] = ':';
        buf[pos + 1] = ' ';
        pos += 2;
        @memcpy(buf[pos .. pos + h.value.len], h.value);
        pos += h.value.len;
        buf[pos] = '\r';
        buf[pos + 1] = '\n';
        pos += 2;
    }

    if (buf.len - pos < 2) return Error.BufferTooShort;
    buf[pos] = '\r';
    buf[pos + 1] = '\n';
    pos += 2;

    if (body) |b| {
        if (buf.len - pos < b.len) return Error.BufferTooShort;
        @memcpy(buf[pos .. pos + b.len], b);
        pos += b.len;
    }

    return pos;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn findCRLF(data: []const u8) ?usize {
    if (data.len < 2) return null;
    for (0..data.len - 1) |i| {
        if (data[i] == '\r' and data[i + 1] == '\n') return i;
    }
    return null;
}

fn findHeaderEnd(data: []const u8) ?usize {
    // Find \r\n\r\n — returns the position of the empty line's \r\n
    // (i.e., all header data is data[0..result])
    if (data.len < 2) return null;
    if (data[0] == '\r' and data[1] == '\n') return 0; // no headers
    if (data.len < 4) return null;
    for (0..data.len - 3) |i| {
        if (data[i] == '\r' and data[i + 1] == '\n' and data[i + 2] == '\r' and data[i + 3] == '\n') {
            return i + 2; // position of the blank line's \r\n; header data is data[0..i+2]
        }
    }
    return null;
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (toLower(ca) != toLower(cb)) return false;
    }
    return true;
}

fn toLower(c: u8) u8 {
    return if (c >= 'A' and c <= 'Z') c + 32 else c;
}

fn formatU16(buf: []u8, value: u16) usize {
    var tmp: [5]u8 = undefined;
    var pos: usize = tmp.len;
    var v = value;
    if (v == 0) {
        buf[0] = '0';
        return 1;
    }
    while (v > 0) {
        pos -= 1;
        tmp[pos] = @intCast('0' + v % 10);
        v /= 10;
    }
    const len = tmp.len - pos;
    @memcpy(buf[0..len], tmp[pos..]);
    return len;
}

pub fn statusReason(code: u16) []const u8 {
    return switch (code) {
        100 => "Continue",
        101 => "Switching Protocols",
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Content Too Large",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Content",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        else => "Unknown",
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "parse GET request" {
    const raw = "GET /api/users HTTP/1.1\r\nHost: example.com\r\nAccept: application/json\r\n\r\n";
    const req = try parseRequest(raw);
    try std.testing.expectEqual(Method.GET, req.method);
    try std.testing.expectEqualStrings("/api/users", req.path);
    try std.testing.expectEqual(Version.http_1_1, req.version);
    try std.testing.expectEqualStrings("example.com", req.getHeader("Host").?);
    try std.testing.expectEqualStrings("application/json", req.getHeader("accept").?);
    try std.testing.expectEqual(@as(?[]const u8, null), req.body);
}

test "parse POST request with body" {
    const raw = "POST /api/users HTTP/1.1\r\nContent-Length: 14\r\n\r\n{\"name\":\"bob\"}";
    const req = try parseRequest(raw);
    try std.testing.expectEqual(Method.POST, req.method);
    try std.testing.expectEqualStrings("/api/users", req.path);
    try std.testing.expectEqualStrings("{\"name\":\"bob\"}", req.body.?);
}

test "parse HTTP response" {
    const raw = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
    const resp = try parseResponse(raw);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("OK", resp.reason);
    try std.testing.expectEqual(Version.http_1_1, resp.version);
    try std.testing.expectEqualStrings("OK", resp.body.?);
}

test "parse HTTP/1.0 response" {
    const raw = "HTTP/1.0 404 Not Found\r\n\r\n";
    const resp = try parseResponse(raw);
    try std.testing.expectEqual(@as(u16, 404), resp.status);
    try std.testing.expectEqualStrings("Not Found", resp.reason);
    try std.testing.expectEqual(Version.http_1_0, resp.version);
}

test "encode response" {
    var buf: [512]u8 = undefined;
    const headers_list = [_]Header{
        .{ .name = "Content-Type", .value = "application/json" },
        .{ .name = "Content-Length", .value = "2" },
    };
    const n = try encodeResponse(&buf, 200, &headers_list, "OK");
    const encoded = buf[0..n];
    try std.testing.expect(std.mem.startsWith(u8, encoded, "HTTP/1.1 200 OK\r\n"));
    try std.testing.expect(std.mem.endsWith(u8, encoded, "OK"));
}

test "encode request" {
    var buf: [512]u8 = undefined;
    const headers_list = [_]Header{
        .{ .name = "Host", .value = "example.com" },
    };
    const n = try encodeRequest(&buf, .GET, "/api/test", &headers_list, null);
    const encoded = buf[0..n];
    try std.testing.expect(std.mem.startsWith(u8, encoded, "GET /api/test HTTP/1.1\r\n"));
}

test "header iteration" {
    const raw = "GET / HTTP/1.1\r\nHost: a.com\r\nX-Custom: val\r\n\r\n";
    const req = try parseRequest(raw);
    var iter = req.headers();
    const h1 = iter.next().?;
    try std.testing.expectEqualStrings("Host", h1.name);
    try std.testing.expectEqualStrings("a.com", h1.value);
    const h2 = iter.next().?;
    try std.testing.expectEqualStrings("X-Custom", h2.name);
    try std.testing.expectEqualStrings("val", h2.value);
}

test "case-insensitive header lookup" {
    const raw = "GET / HTTP/1.1\r\nContent-Type: text/html\r\n\r\n";
    const req = try parseRequest(raw);
    try std.testing.expectEqualStrings("text/html", req.getHeader("content-type").?);
    try std.testing.expectEqualStrings("text/html", req.getHeader("CONTENT-TYPE").?);
    try std.testing.expectEqualStrings("text/html", req.getHeader("Content-Type").?);
}

test "all HTTP methods" {
    const methods = [_]Method{ .GET, .POST, .PUT, .PATCH, .DELETE, .HEAD, .OPTIONS, .TRACE, .CONNECT };
    for (methods) |m| {
        const parsed = try Method.fromString(m.toString());
        try std.testing.expectEqual(m, parsed);
    }
}

test "invalid method" {
    try std.testing.expectError(Error.InvalidMethod, Method.fromString("INVALID"));
}

test "incomplete request" {
    try std.testing.expectError(Error.IncompleteMessage, parseRequest("GET / HTTP/1.1\r\n"));
}

test "encode-decode round trip" {
    var encode_buf: [1024]u8 = undefined;
    const headers_list = [_]Header{
        .{ .name = "Content-Type", .value = "text/plain" },
        .{ .name = "Content-Length", .value = "5" },
    };
    const n = try encodeResponse(&encode_buf, 201, &headers_list, "hello");
    const resp = try parseResponse(encode_buf[0..n]);
    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expectEqualStrings("hello", resp.body.?);
}

test "status reasons" {
    try std.testing.expectEqualStrings("OK", statusReason(200));
    try std.testing.expectEqualStrings("Not Found", statusReason(404));
    try std.testing.expectEqualStrings("Internal Server Error", statusReason(500));
}

// ---------------------------------------------------------------------------
// Chunked Transfer Encoding (RFC 9112 Section 7.1)
// ---------------------------------------------------------------------------

/// Decode a chunked transfer-encoded body into the output buffer.
/// Input is the raw chunked data (after headers). Returns the decoded body slice.
/// Zero-alloc: writes directly into the caller-provided output buffer.
///
/// Chunked format:
///   chunk-size (hex) CRLF
///   chunk-data CRLF
///   ...
///   0 CRLF
///   CRLF
pub fn decodeChunked(input: []const u8, output: []u8) Error![]const u8 {
    var in_pos: usize = 0;
    var out_pos: usize = 0;

    while (in_pos < input.len) {
        // Parse chunk size (hex digits terminated by CRLF)
        const size_end = findCRLF(input[in_pos..]) orelse return Error.IncompleteMessage;
        const size_str = input[in_pos .. in_pos + size_end];
        if (size_str.len == 0) return Error.InvalidChunkSize;

        // Parse hex size, ignoring any chunk extensions after semicolon
        const hex_end = std.mem.indexOfScalar(u8, size_str, ';') orelse size_str.len;
        var hex_str = size_str[0..hex_end];
        // Trim trailing whitespace
        while (hex_str.len > 0 and (hex_str[hex_str.len - 1] == ' ' or hex_str[hex_str.len - 1] == '\t')) {
            hex_str = hex_str[0 .. hex_str.len - 1];
        }
        if (hex_str.len == 0) return Error.InvalidChunkSize;

        const chunk_size = std.fmt.parseUnsigned(usize, hex_str, 16) catch return Error.InvalidChunkSize;

        in_pos += size_end + 2; // skip size line + CRLF

        // Chunk size 0 = end of chunked body
        if (chunk_size == 0) {
            // Skip optional trailers + final CRLF
            break;
        }

        // Verify we have enough input data
        if (in_pos + chunk_size > input.len) return Error.IncompleteMessage;
        // Verify we have enough output space
        if (out_pos + chunk_size > output.len) return Error.BufferTooShort;

        // Copy chunk data
        @memcpy(output[out_pos .. out_pos + chunk_size], input[in_pos .. in_pos + chunk_size]);
        out_pos += chunk_size;
        in_pos += chunk_size;

        // Skip trailing CRLF after chunk data
        if (in_pos + 1 < input.len and input[in_pos] == '\r' and input[in_pos + 1] == '\n') {
            in_pos += 2;
        } else {
            return Error.InvalidChunkSize;
        }
    }

    return output[0..out_pos];
}

/// Encode a body using chunked transfer encoding into the output buffer.
/// Writes a single chunk containing the entire body, followed by the
/// terminating zero-length chunk.
///
/// Output format:
///   <hex-size> CRLF
///   <body> CRLF
///   0 CRLF
///   CRLF
pub fn encodeChunked(body: []const u8, output: []u8) Error![]const u8 {
    var pos: usize = 0;

    if (body.len > 0) {
        // Write chunk size in hex
        const size_str = std.fmt.bufPrint(output[pos..], "{x}", .{body.len}) catch return Error.BufferTooShort;
        pos += size_str.len;

        // CRLF after size
        if (output.len - pos < 2) return Error.BufferTooShort;
        output[pos] = '\r';
        output[pos + 1] = '\n';
        pos += 2;

        // Chunk data
        if (output.len - pos < body.len) return Error.BufferTooShort;
        @memcpy(output[pos .. pos + body.len], body);
        pos += body.len;

        // CRLF after chunk data
        if (output.len - pos < 2) return Error.BufferTooShort;
        output[pos] = '\r';
        output[pos + 1] = '\n';
        pos += 2;
    }

    // Terminating zero-length chunk: "0\r\n\r\n"
    if (output.len - pos < 5) return Error.BufferTooShort;
    output[pos] = '0';
    output[pos + 1] = '\r';
    output[pos + 2] = '\n';
    output[pos + 3] = '\r';
    output[pos + 4] = '\n';
    pos += 5;

    return output[0..pos];
}

/// Encode a body into multiple chunks of the specified maximum size.
/// Useful for streaming large responses.
pub fn encodeChunkedMulti(body: []const u8, chunk_size: usize, output: []u8) Error![]const u8 {
    var pos: usize = 0;
    var remaining = body;

    while (remaining.len > 0) {
        const this_chunk = @min(remaining.len, chunk_size);

        // Write chunk size in hex
        const size_str = std.fmt.bufPrint(output[pos..], "{x}", .{this_chunk}) catch return Error.BufferTooShort;
        pos += size_str.len;

        // CRLF
        if (output.len - pos < 2) return Error.BufferTooShort;
        output[pos] = '\r';
        output[pos + 1] = '\n';
        pos += 2;

        // Chunk data
        if (output.len - pos < this_chunk) return Error.BufferTooShort;
        @memcpy(output[pos .. pos + this_chunk], remaining[0..this_chunk]);
        pos += this_chunk;

        // CRLF
        if (output.len - pos < 2) return Error.BufferTooShort;
        output[pos] = '\r';
        output[pos + 1] = '\n';
        pos += 2;

        remaining = remaining[this_chunk..];
    }

    // Terminating chunk
    if (output.len - pos < 5) return Error.BufferTooShort;
    output[pos] = '0';
    output[pos + 1] = '\r';
    output[pos + 2] = '\n';
    output[pos + 3] = '\r';
    output[pos + 4] = '\n';
    pos += 5;

    return output[0..pos];
}

test "decodeChunked: single chunk" {
    const input = "5\r\nhello\r\n0\r\n\r\n";
    var output: [64]u8 = undefined;
    const decoded = try decodeChunked(input, &output);
    try std.testing.expectEqualStrings("hello", decoded);
}

test "decodeChunked: multiple chunks" {
    const input = "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
    var output: [64]u8 = undefined;
    const decoded = try decodeChunked(input, &output);
    try std.testing.expectEqualStrings("hello world", decoded);
}

test "decodeChunked: empty body" {
    const input = "0\r\n\r\n";
    var output: [64]u8 = undefined;
    const decoded = try decodeChunked(input, &output);
    try std.testing.expectEqualStrings("", decoded);
}

test "decodeChunked: hex sizes" {
    // a = 10, 14 = 20
    const input = "a\r\n0123456789\r\n0\r\n\r\n";
    var output: [64]u8 = undefined;
    const decoded = try decodeChunked(input, &output);
    try std.testing.expectEqualStrings("0123456789", decoded);
}

test "decodeChunked: chunk extension ignored" {
    const input = "5;ext=val\r\nhello\r\n0\r\n\r\n";
    var output: [64]u8 = undefined;
    const decoded = try decodeChunked(input, &output);
    try std.testing.expectEqualStrings("hello", decoded);
}

test "encodeChunked: basic" {
    var output: [256]u8 = undefined;
    const encoded = try encodeChunked("hello", &output);
    try std.testing.expectEqualStrings("5\r\nhello\r\n0\r\n\r\n", encoded);
}

test "encodeChunked: empty body" {
    var output: [64]u8 = undefined;
    const encoded = try encodeChunked("", &output);
    try std.testing.expectEqualStrings("0\r\n\r\n", encoded);
}

test "encodeChunked: round trip" {
    var encode_buf: [512]u8 = undefined;
    var decode_buf: [256]u8 = undefined;
    const body = "The quick brown fox jumps over the lazy dog";
    const encoded = try encodeChunked(body, &encode_buf);
    const decoded = try decodeChunked(encoded, &decode_buf);
    try std.testing.expectEqualStrings(body, decoded);
}

test "encodeChunkedMulti: splits into chunks" {
    var output: [512]u8 = undefined;
    var decode_buf: [256]u8 = undefined;
    const body = "hello world";
    const encoded = try encodeChunkedMulti(body, 5, &output);
    // Should produce: "5\r\nhello\r\n5\r\n worl\r\n1\r\nd\r\n0\r\n\r\n"
    const decoded = try decodeChunked(encoded, &decode_buf);
    try std.testing.expectEqualStrings(body, decoded);
}
