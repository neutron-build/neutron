// Nucleus SQL Model — raw query, scalar and command-tag access.
// Comptime helpers below expose struct field names; they do not scan rows.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;
const QueryResult = @import("client.zig").QueryResult;

pub const SqlModel = struct {
    client: *NucleusClient,

    /// Execute a query and return the raw QueryResult.
    pub fn queryRaw(self: SqlModel, sql: []const u8) !QueryResult {
        return try self.client.query(sql);
    }

    /// Execute a query and return the first column of the first row as a string.
    pub fn queryScalar(self: SqlModel, sql: []const u8) !?[]const u8 {
        return try self.client.execute(sql);
    }

    /// Execute a statement (INSERT/UPDATE/DELETE) and return the command tag.
    pub fn execute(self: SqlModel, sql: []const u8) ![]const u8 {
        return try self.client.exec(sql);
    }

    /// Format a SQL string with arguments into a buffer, then execute it.
    /// This interpolates trusted SQL syntax; it does not escape or bind values.
    /// Never pass untrusted input, NULL or binary values through formatting.
    pub fn executeFormatted(self: SqlModel, comptime fmt: []const u8, args: anytype) ![]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = std.fmt.bufPrint(&buf, fmt, args) catch return error.BufferTooShort;
        return try self.client.exec(sql);
    }

    /// Format trusted SQL syntax and return a scalar result.
    /// This is interpolation, not parameter binding. Never format untrusted
    /// input, NULL or binary values into SQL.
    pub fn queryFormatted(self: SqlModel, comptime fmt: []const u8, args: anytype) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = std.fmt.bufPrint(&buf, fmt, args) catch return error.BufferTooShort;
        return try self.client.execute(sql);
    }
};

/// Parse a comptime struct type's fields for column mapping.
/// Returns the field names as a comptime slice, useful for SELECT column lists.
pub fn structFields(comptime T: type) []const []const u8 {
    const info = @typeInfo(T);
    switch (info) {
        .@"struct" => |s| {
            // The array is built in a comptime block so the returned pointer
            // refers to promoted static memory.
            //
            // This was a plain `var` with `return &names`, which Zig 0.14
            // accepted and 0.16 rejects as "returning address of expired local
            // variable" — correctly, since the array lived on a stack frame
            // that was already gone. When CI was pinned to 0.14 the SDK
            // silently stopped compiling on current Zig; the comptime block
            // is the portable shape.
            const names = comptime blk: {
                var acc: [s.fields.len][]const u8 = undefined;
                for (s.fields, 0..) |field, i| {
                    acc[i] = field.name;
                }
                break :blk acc;
            };
            return &names;
        },
        else => @compileError("structFields requires a struct type"),
    }
}

/// Generate a SELECT column list from a struct type at compile time.
pub fn selectColumns(comptime T: type) []const u8 {
    const info = @typeInfo(T);
    switch (info) {
        .@"struct" => |s| {
            var result: []const u8 = "";
            for (s.fields, 0..) |field, i| {
                if (i > 0) result = result ++ ", ";
                result = result ++ field.name;
            }
            return result;
        },
        else => @compileError("selectColumns requires a struct type"),
    }
}

// ── Tests ─────────────────────────────────────────────────────

test "selectColumns: simple struct" {
    const User = struct {
        id: i64,
        name: []const u8,
        email: []const u8,
    };
    const cols = comptime selectColumns(User);
    try std.testing.expectEqualStrings("id, name, email", cols);
}

test "selectColumns: single field" {
    const Count = struct {
        count: i64,
    };
    const cols = comptime selectColumns(Count);
    try std.testing.expectEqualStrings("count", cols);
}

test "structFields: returns field names" {
    const Item = struct {
        id: i64,
        title: []const u8,
    };
    const fields = comptime structFields(Item);
    try std.testing.expectEqual(@as(usize, 2), fields.len);
    try std.testing.expectEqualStrings("id", fields[0]);
    try std.testing.expectEqualStrings("title", fields[1]);
}
