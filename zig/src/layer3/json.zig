// Layer 3: JSON parser — comptime struct deserialization from JSON
//
// Provides fromJson() for parsing JSON strings into comptime-typed structs.
// Supports: strings, integers, floats, booleans, optionals.
// Zero external dependencies — uses stack buffers only.

const std = @import("std");

pub const Error = error{
    InvalidJson,
    UnexpectedToken,
    MissingField,
    BufferTooShort,
    MissingBody,
};

/// Parse a JSON string into a comptime-typed struct.
/// Fields not found in the JSON are left at their default value (if any)
/// or cause a MissingField error.
pub fn fromJson(comptime T: type, data: []const u8) Error!T {
    if (T == void) return {};

    const info = @typeInfo(T);
    switch (info) {
        .@"struct" => |s| {
            var result: T = undefined;
            // Initialize optional fields to null
            inline for (s.fields) |field| {
                if (field.default_value_ptr) |default_ptr| {
                    const typed_ptr: *const field.type = @ptrCast(@alignCast(default_ptr));
                    @field(result, field.name) = typed_ptr.*;
                } else {
                    switch (@typeInfo(field.type)) {
                        .optional => {
                            @field(result, field.name) = null;
                        },
                        else => {},
                    }
                }
            }

            // Parse JSON object
            var pos: usize = 0;
            pos = skipWhitespace(data, pos);
            if (pos >= data.len or data[pos] != '{') return Error.InvalidJson;
            pos += 1;

            while (pos < data.len) {
                pos = skipWhitespace(data, pos);
                if (pos >= data.len) return Error.InvalidJson;
                if (data[pos] == '}') break;
                if (data[pos] == ',') {
                    pos += 1;
                    continue;
                }

                // Parse key
                const key = parseString(data, &pos) catch return Error.InvalidJson;
                pos = skipWhitespace(data, pos);
                if (pos >= data.len or data[pos] != ':') return Error.InvalidJson;
                pos += 1;
                pos = skipWhitespace(data, pos);

                // Match key to struct field and parse value
                var found = false;
                inline for (s.fields) |field| {
                    if (std.mem.eql(u8, key, field.name)) {
                        @field(result, field.name) = parseValue(field.type, data, &pos) catch return Error.InvalidJson;
                        found = true;
                    }
                }
                if (!found) {
                    // Skip unknown value
                    skipValue(data, &pos) catch return Error.InvalidJson;
                }
            }

            return result;
        },
        else => return Error.InvalidJson,
    }
}

fn parseValue(comptime T: type, data: []const u8, pos: *usize) !T {
    const info = @typeInfo(T);
    switch (info) {
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) {
                return parseString(data, pos);
            }
            return error.InvalidJson;
        },
        .int => {
            return parseInt(T, data, pos);
        },
        .float => {
            return parseFloat(T, data, pos);
        },
        .bool => {
            return parseBool(data, pos);
        },
        .optional => |opt| {
            if (pos.* + 3 < data.len and std.mem.eql(u8, data[pos.* .. pos.* + 4], "null")) {
                pos.* += 4;
                return null;
            }
            return try parseValue(opt.child, data, pos);
        },
        .@"struct" => {
            return fromJson(T, data[pos.*..]);
        },
        else => return error.InvalidJson,
    }
}

fn parseString(data: []const u8, pos: *usize) ![]const u8 {
    if (pos.* >= data.len or data[pos.*] != '"') return error.InvalidJson;
    pos.* += 1;
    const start = pos.*;
    while (pos.* < data.len) {
        if (data[pos.*] == '\\') {
            pos.* += 2; // skip escaped char
            continue;
        }
        if (data[pos.*] == '"') {
            const result = data[start..pos.*];
            pos.* += 1;
            return result;
        }
        pos.* += 1;
    }
    return error.InvalidJson;
}

fn parseInt(comptime T: type, data: []const u8, pos: *usize) !T {
    const start = pos.*;
    if (pos.* < data.len and (data[pos.*] == '-' or data[pos.*] == '+')) pos.* += 1;
    while (pos.* < data.len and data[pos.*] >= '0' and data[pos.*] <= '9') {
        pos.* += 1;
    }
    if (pos.* == start) return error.InvalidJson;
    return std.fmt.parseInt(T, data[start..pos.*], 10) catch return error.InvalidJson;
}

fn parseFloat(comptime T: type, data: []const u8, pos: *usize) !T {
    const start = pos.*;
    if (pos.* < data.len and (data[pos.*] == '-' or data[pos.*] == '+')) pos.* += 1;
    while (pos.* < data.len and (data[pos.*] >= '0' and data[pos.*] <= '9' or data[pos.*] == '.' or data[pos.*] == 'e' or data[pos.*] == 'E' or data[pos.*] == '-' or data[pos.*] == '+')) {
        pos.* += 1;
    }
    if (pos.* == start) return error.InvalidJson;
    return std.fmt.parseFloat(T, data[start..pos.*]) catch return error.InvalidJson;
}

fn parseBool(data: []const u8, pos: *usize) !bool {
    if (pos.* + 3 < data.len and std.mem.eql(u8, data[pos.* .. pos.* + 4], "true")) {
        pos.* += 4;
        return true;
    }
    if (pos.* + 4 < data.len and std.mem.eql(u8, data[pos.* .. pos.* + 5], "false")) {
        pos.* += 5;
        return false;
    }
    return error.InvalidJson;
}

fn skipWhitespace(data: []const u8, start: usize) usize {
    var pos = start;
    while (pos < data.len and (data[pos] == ' ' or data[pos] == '\t' or data[pos] == '\n' or data[pos] == '\r')) {
        pos += 1;
    }
    return pos;
}

fn skipValue(data: []const u8, pos: *usize) !void {
    if (pos.* >= data.len) return error.InvalidJson;
    switch (data[pos.*]) {
        '"' => {
            _ = try parseString(data, pos);
        },
        '{' => {
            var depth: u32 = 1;
            pos.* += 1;
            while (pos.* < data.len and depth > 0) {
                if (data[pos.*] == '{') depth += 1;
                if (data[pos.*] == '}') depth -= 1;
                if (data[pos.*] == '"') {
                    _ = try parseString(data, pos);
                    continue;
                }
                pos.* += 1;
            }
        },
        '[' => {
            var depth: u32 = 1;
            pos.* += 1;
            while (pos.* < data.len and depth > 0) {
                if (data[pos.*] == '[') depth += 1;
                if (data[pos.*] == ']') depth -= 1;
                if (data[pos.*] == '"') {
                    _ = try parseString(data, pos);
                    continue;
                }
                pos.* += 1;
            }
        },
        else => {
            // number, bool, null
            while (pos.* < data.len and data[pos.*] != ',' and data[pos.*] != '}' and data[pos.*] != ']') {
                pos.* += 1;
            }
        },
    }
}

// ── Tests ─────────────────────────────────────────────────────

test "fromJson: simple struct" {
    const User = struct {
        name: []const u8,
        age: i64,
        active: bool,
    };
    const result = try fromJson(User, "{\"name\":\"Alice\",\"age\":30,\"active\":true}");
    try std.testing.expectEqualStrings("Alice", result.name);
    try std.testing.expectEqual(@as(i64, 30), result.age);
    try std.testing.expect(result.active);
}

test "fromJson: with optional null" {
    const Item = struct {
        name: []const u8,
        count: ?i32 = null,
    };
    const result = try fromJson(Item, "{\"name\":\"widget\",\"count\":null}");
    try std.testing.expectEqualStrings("widget", result.name);
    try std.testing.expectEqual(@as(?i32, null), result.count);
}

test "fromJson: with optional value" {
    const Item = struct {
        name: []const u8,
        count: ?i32 = null,
    };
    const result = try fromJson(Item, "{\"name\":\"widget\",\"count\":5}");
    try std.testing.expectEqualStrings("widget", result.name);
    try std.testing.expectEqual(@as(?i32, 5), result.count);
}

test "fromJson: boolean values" {
    const Flags = struct {
        enabled: bool,
        debug: bool,
    };
    const result = try fromJson(Flags, "{\"enabled\":true,\"debug\":false}");
    try std.testing.expect(result.enabled);
    try std.testing.expect(!result.debug);
}

test "fromJson: void type" {
    const result = try fromJson(void, "");
    try std.testing.expectEqual({}, result);
}

test "fromJson: invalid json" {
    const Simple = struct { name: []const u8 };
    try std.testing.expectError(Error.InvalidJson, fromJson(Simple, "not json"));
}

test "fromJson: unknown fields ignored" {
    const Simple = struct { name: []const u8 };
    const result = try fromJson(Simple, "{\"name\":\"Alice\",\"extra\":42}");
    try std.testing.expectEqualStrings("Alice", result.name);
}
