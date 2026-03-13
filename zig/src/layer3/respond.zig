// Layer 3: Response helpers — JSON serialization utilities
//
// Provides comptime JSON serialization for simple structs.
// Zero allocation — writes into caller-provided buffers.

const std = @import("std");

/// Serialize a struct to JSON into a buffer.
/// Supports: bool, integers, floats, []const u8, optional.
pub fn toJson(comptime T: type, value: T, buf: []u8) ![]const u8 {
    var pos: usize = 0;
    pos = try serializeValue(T, value, buf, pos);
    return buf[0..pos];
}

fn serializeValue(comptime T: type, value: T, buf: []u8, start: usize) !usize {
    var pos = start;
    const info = @typeInfo(T);

    switch (info) {
        .@"struct" => |s| {
            if (pos >= buf.len) return error.BufferTooShort;
            buf[pos] = '{';
            pos += 1;
            var first = true;
            inline for (s.fields) |field| {
                if (!first) {
                    if (pos >= buf.len) return error.BufferTooShort;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;
                // Key
                if (pos >= buf.len) return error.BufferTooShort;
                buf[pos] = '"';
                pos += 1;
                if (buf.len - pos < field.name.len) return error.BufferTooShort;
                @memcpy(buf[pos .. pos + field.name.len], field.name);
                pos += field.name.len;
                if (pos + 1 >= buf.len) return error.BufferTooShort;
                buf[pos] = '"';
                pos += 1;
                buf[pos] = ':';
                pos += 1;
                // Value
                pos = try serializeValue(field.type, @field(value, field.name), buf, pos);
            }
            if (pos >= buf.len) return error.BufferTooShort;
            buf[pos] = '}';
            pos += 1;
        },
        .bool => {
            const s = if (value) "true" else "false";
            if (buf.len - pos < s.len) return error.BufferTooShort;
            @memcpy(buf[pos .. pos + s.len], s);
            pos += s.len;
        },
        .int, .comptime_int => {
            const formatted = std.fmt.bufPrint(buf[pos..], "{d}", .{value}) catch return error.BufferTooShort;
            pos += formatted.len;
        },
        .float, .comptime_float => {
            const formatted = std.fmt.bufPrint(buf[pos..], "{d}", .{value}) catch return error.BufferTooShort;
            pos += formatted.len;
        },
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) {
                if (pos >= buf.len) return error.BufferTooShort;
                buf[pos] = '"';
                pos += 1;
                if (buf.len - pos < value.len) return error.BufferTooShort;
                @memcpy(buf[pos .. pos + value.len], value);
                pos += value.len;
                if (pos >= buf.len) return error.BufferTooShort;
                buf[pos] = '"';
                pos += 1;
            } else if (ptr.size == .one and @typeInfo(ptr.child) == .array) {
                // String literal (*const [N:0]u8)
                const s: []const u8 = value;
                if (pos >= buf.len) return error.BufferTooShort;
                buf[pos] = '"';
                pos += 1;
                if (buf.len - pos < s.len) return error.BufferTooShort;
                @memcpy(buf[pos .. pos + s.len], s);
                pos += s.len;
                if (pos >= buf.len) return error.BufferTooShort;
                buf[pos] = '"';
                pos += 1;
            } else {
                return error.BufferTooShort;
            }
        },
        .optional => {
            if (value) |v| {
                pos = try serializeValue(@TypeOf(v), v, buf, pos);
            } else {
                const s = "null";
                if (buf.len - pos < s.len) return error.BufferTooShort;
                @memcpy(buf[pos .. pos + s.len], s);
                pos += s.len;
            }
        },
        else => return error.BufferTooShort,
    }
    return pos;
}

test "toJson: simple struct" {
    const User = struct {
        id: i64,
        name: []const u8,
        active: bool,
    };
    var buf: [256]u8 = undefined;
    const json = try toJson(User, .{ .id = 42, .name = "Alice", .active = true }, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"id\":42") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Alice\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"active\":true") != null);
}

test "toJson: with optional null" {
    const Item = struct {
        name: []const u8,
        count: ?i32,
    };
    var buf: [256]u8 = undefined;
    const json = try toJson(Item, .{ .name = "widget", .count = null }, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"count\":null") != null);
}

test "toJson: with optional value" {
    const Item = struct {
        name: []const u8,
        count: ?i32,
    };
    var buf: [256]u8 = undefined;
    const json = try toJson(Item, .{ .name = "widget", .count = 5 }, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"count\":5") != null);
}

test "toJson: bool only" {
    const Flags = struct { enabled: bool };
    var buf: [64]u8 = undefined;
    const json = try toJson(Flags, .{ .enabled = false }, &buf);
    try std.testing.expectEqualStrings("{\"enabled\":false}", json);
}
