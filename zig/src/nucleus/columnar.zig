// Nucleus Columnar Model — SQL generation + execution for COLUMNAR_* functions
//
// SQL functions: COLUMNAR_INSERT, COLUMNAR_COUNT, COLUMNAR_SUM,
// COLUMNAR_AVG, COLUMNAR_MIN, COLUMNAR_MAX.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const ColumnarModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    /// SELECT COLUMNAR_INSERT('table', 'values_json')
    pub fn insertSql(table: []const u8, values_json: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT COLUMNAR_INSERT('{s}', '{s}')", .{ table, values_json }) catch return error.BufferTooShort;
    }

    /// SELECT COLUMNAR_COUNT('table')
    pub fn countSql(table: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT COLUMNAR_COUNT('{s}')", .{table}) catch return error.BufferTooShort;
    }

    /// SELECT COLUMNAR_SUM('table', 'column')
    pub fn sumSql(table: []const u8, column: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT COLUMNAR_SUM('{s}', '{s}')", .{ table, column }) catch return error.BufferTooShort;
    }

    /// SELECT COLUMNAR_AVG('table', 'column')
    pub fn avgSql(table: []const u8, column: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT COLUMNAR_AVG('{s}', '{s}')", .{ table, column }) catch return error.BufferTooShort;
    }

    /// SELECT COLUMNAR_MIN('table', 'column')
    pub fn minSql(table: []const u8, column: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT COLUMNAR_MIN('{s}', '{s}')", .{ table, column }) catch return error.BufferTooShort;
    }

    /// SELECT COLUMNAR_MAX('table', 'column')
    pub fn maxSql(table: []const u8, column: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT COLUMNAR_MAX('{s}', '{s}')", .{ table, column }) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn colInsert(self: ColumnarModel, table: []const u8, values_json: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try insertSql(table, values_json, &buf);
        return try self.client.execute(sql);
    }

    pub fn count(self: ColumnarModel, table: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try countSql(table, &buf);
        return try self.client.execute(sql);
    }

    pub fn sum(self: ColumnarModel, table: []const u8, column: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try sumSql(table, column, &buf);
        return try self.client.execute(sql);
    }

    pub fn avg(self: ColumnarModel, table: []const u8, column: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try avgSql(table, column, &buf);
        return try self.client.execute(sql);
    }

    pub fn min(self: ColumnarModel, table: []const u8, column: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try minSql(table, column, &buf);
        return try self.client.execute(sql);
    }

    pub fn max(self: ColumnarModel, table: []const u8, column: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try maxSql(table, column, &buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "COLUMNAR_INSERT sql" {
    var buf: [512]u8 = undefined;
    const sql = try ColumnarModel.insertSql("events", "{\"ts\":1000,\"val\":42}", &buf);
    try std.testing.expectEqualStrings("SELECT COLUMNAR_INSERT('events', '{\"ts\":1000,\"val\":42}')", sql);
}

test "COLUMNAR_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try ColumnarModel.countSql("events", &buf);
    try std.testing.expectEqualStrings("SELECT COLUMNAR_COUNT('events')", sql);
}

test "COLUMNAR_SUM sql" {
    var buf: [256]u8 = undefined;
    const sql = try ColumnarModel.sumSql("sales", "amount", &buf);
    try std.testing.expectEqualStrings("SELECT COLUMNAR_SUM('sales', 'amount')", sql);
}

test "COLUMNAR_AVG sql" {
    var buf: [256]u8 = undefined;
    const sql = try ColumnarModel.avgSql("sensors", "temperature", &buf);
    try std.testing.expectEqualStrings("SELECT COLUMNAR_AVG('sensors', 'temperature')", sql);
}

test "COLUMNAR_MIN sql" {
    var buf: [256]u8 = undefined;
    const sql = try ColumnarModel.minSql("prices", "usd", &buf);
    try std.testing.expectEqualStrings("SELECT COLUMNAR_MIN('prices', 'usd')", sql);
}

test "COLUMNAR_MAX sql" {
    var buf: [256]u8 = undefined;
    const sql = try ColumnarModel.maxSql("prices", "usd", &buf);
    try std.testing.expectEqualStrings("SELECT COLUMNAR_MAX('prices', 'usd')", sql);
}
