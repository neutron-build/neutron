// Nucleus CDC (Change Data Capture) Model — SQL generation + execution
//
// SQL functions: CDC_READ, CDC_COUNT, CDC_TABLE_READ.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const CdcModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    /// SELECT CDC_READ(offset)
    pub fn readSql(offset: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT CDC_READ({d})", .{offset}) catch return error.BufferTooShort;
    }

    /// SELECT CDC_COUNT()
    pub fn countSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT CDC_COUNT()", .{}) catch return error.BufferTooShort;
    }

    /// SELECT CDC_TABLE_READ('table', offset)
    pub fn tableReadSql(table: []const u8, offset: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT CDC_TABLE_READ('{s}', {d})", .{ table, offset }) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn cdcRead(self: CdcModel, offset: i64) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try readSql(offset, &buf);
        return try self.client.execute(sql);
    }

    pub fn count(self: CdcModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try countSql(&buf);
        return try self.client.execute(sql);
    }

    pub fn tableRead(self: CdcModel, table: []const u8, offset: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try tableReadSql(table, offset, &buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "CDC_READ sql" {
    var buf: [256]u8 = undefined;
    const sql = try CdcModel.readSql(42, &buf);
    try std.testing.expectEqualStrings("SELECT CDC_READ(42)", sql);
}

test "CDC_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try CdcModel.countSql(&buf);
    try std.testing.expectEqualStrings("SELECT CDC_COUNT()", sql);
}

test "CDC_TABLE_READ sql" {
    var buf: [256]u8 = undefined;
    const sql = try CdcModel.tableReadSql("users", 0, &buf);
    try std.testing.expectEqualStrings("SELECT CDC_TABLE_READ('users', 0)", sql);
}
