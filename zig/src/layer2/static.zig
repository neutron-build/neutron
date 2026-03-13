// Layer 2: Static file serving utilities
//
// Content-type detection and response helpers for serving static files.

const std = @import("std");

/// Detect content type from file extension.
pub fn contentType(path: []const u8) []const u8 {
    const ext = std.fs.path.extension(path);
    if (ext.len == 0) return "application/octet-stream";

    if (std.mem.eql(u8, ext, ".html") or std.mem.eql(u8, ext, ".htm")) return "text/html; charset=utf-8";
    if (std.mem.eql(u8, ext, ".css")) return "text/css; charset=utf-8";
    if (std.mem.eql(u8, ext, ".js") or std.mem.eql(u8, ext, ".mjs")) return "application/javascript; charset=utf-8";
    if (std.mem.eql(u8, ext, ".json")) return "application/json; charset=utf-8";
    if (std.mem.eql(u8, ext, ".png")) return "image/png";
    if (std.mem.eql(u8, ext, ".jpg") or std.mem.eql(u8, ext, ".jpeg")) return "image/jpeg";
    if (std.mem.eql(u8, ext, ".gif")) return "image/gif";
    if (std.mem.eql(u8, ext, ".svg")) return "image/svg+xml";
    if (std.mem.eql(u8, ext, ".ico")) return "image/x-icon";
    if (std.mem.eql(u8, ext, ".wasm")) return "application/wasm";
    if (std.mem.eql(u8, ext, ".xml")) return "application/xml";
    if (std.mem.eql(u8, ext, ".txt")) return "text/plain; charset=utf-8";
    if (std.mem.eql(u8, ext, ".woff2")) return "font/woff2";
    if (std.mem.eql(u8, ext, ".woff")) return "font/woff";
    if (std.mem.eql(u8, ext, ".pdf")) return "application/pdf";
    if (std.mem.eql(u8, ext, ".zip")) return "application/zip";
    return "application/octet-stream";
}

test "contentType detection" {
    try std.testing.expectEqualStrings("text/html; charset=utf-8", contentType("index.html"));
    try std.testing.expectEqualStrings("application/json; charset=utf-8", contentType("data.json"));
    try std.testing.expectEqualStrings("image/png", contentType("logo.png"));
    try std.testing.expectEqualStrings("application/javascript; charset=utf-8", contentType("app.js"));
    try std.testing.expectEqualStrings("text/css; charset=utf-8", contentType("style.css"));
    try std.testing.expectEqualStrings("application/octet-stream", contentType("unknown"));
    try std.testing.expectEqualStrings("application/wasm", contentType("module.wasm"));
}
