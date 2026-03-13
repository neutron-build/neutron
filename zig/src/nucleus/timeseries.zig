// Nucleus Time-Series Model — SQL generation + execution for TS_* functions
//
// SQL functions: TS_INSERT, TS_LAST, TS_COUNT, TS_RANGE_COUNT,
// TS_RANGE_AVG, TS_RETENTION, TS_MATCH, TIME_BUCKET.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const TimeSeriesModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    pub fn insertSql(series: []const u8, timestamp_ms: i64, value: f64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT TS_INSERT('{s}', {d}, {d})", .{ series, timestamp_ms, value }) catch return error.BufferTooShort;
    }

    pub fn lastSql(series: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT TS_LAST('{s}')", .{series}) catch return error.BufferTooShort;
    }

    pub fn countSql(series: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT TS_COUNT('{s}')", .{series}) catch return error.BufferTooShort;
    }

    pub fn rangeCountSql(series: []const u8, start_ms: i64, end_ms: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT TS_RANGE_COUNT('{s}', {d}, {d})", .{ series, start_ms, end_ms }) catch return error.BufferTooShort;
    }

    pub fn rangeAvgSql(series: []const u8, start_ms: i64, end_ms: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT TS_RANGE_AVG('{s}', {d}, {d})", .{ series, start_ms, end_ms }) catch return error.BufferTooShort;
    }

    pub fn retentionSql(series: []const u8, days: u32, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT TS_RETENTION('{s}', {d})", .{ series, days }) catch return error.BufferTooShort;
    }

    /// SELECT TS_MATCH('series', 'pattern')
    pub fn matchSql(series: []const u8, pattern: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT TS_MATCH('{s}', '{s}')", .{ series, pattern }) catch return error.BufferTooShort;
    }

    pub fn timeBucketSql(interval: []const u8, timestamp_ms: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT TIME_BUCKET('{s}', {d})", .{ interval, timestamp_ms }) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn tsInsert(self: TimeSeriesModel, series: []const u8, timestamp_ms: i64, value: f64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try insertSql(series, timestamp_ms, value, &buf);
        return try self.client.execute(sql);
    }

    pub fn last(self: TimeSeriesModel, series: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try lastSql(series, &buf);
        return try self.client.execute(sql);
    }

    pub fn count(self: TimeSeriesModel, series: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try countSql(series, &buf);
        return try self.client.execute(sql);
    }

    pub fn rangeCount(self: TimeSeriesModel, series: []const u8, start_ms: i64, end_ms: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try rangeCountSql(series, start_ms, end_ms, &buf);
        return try self.client.execute(sql);
    }

    pub fn rangeAvg(self: TimeSeriesModel, series: []const u8, start_ms: i64, end_ms: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try rangeAvgSql(series, start_ms, end_ms, &buf);
        return try self.client.execute(sql);
    }

    pub fn retention(self: TimeSeriesModel, series: []const u8, days: u32) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try retentionSql(series, days, &buf);
        return try self.client.execute(sql);
    }

    pub fn match(self: TimeSeriesModel, series: []const u8, pattern: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try matchSql(series, pattern, &buf);
        return try self.client.execute(sql);
    }

    pub fn timeBucket(self: TimeSeriesModel, interval: []const u8, timestamp_ms: i64) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try timeBucketSql(interval, timestamp_ms, &buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "TS_INSERT sql" {
    var buf: [256]u8 = undefined;
    const sql = try TimeSeriesModel.insertSql("cpu_usage", 1700000000000, 72.5, &buf);
    try std.testing.expectEqualStrings("SELECT TS_INSERT('cpu_usage', 1700000000000, 72.5)", sql);
}

test "TS_LAST sql" {
    var buf: [256]u8 = undefined;
    const sql = try TimeSeriesModel.lastSql("temperature", &buf);
    try std.testing.expectEqualStrings("SELECT TS_LAST('temperature')", sql);
}

test "TS_RANGE_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try TimeSeriesModel.rangeCountSql("metrics", 1000, 2000, &buf);
    try std.testing.expectEqualStrings("SELECT TS_RANGE_COUNT('metrics', 1000, 2000)", sql);
}

test "TS_RANGE_AVG sql" {
    var buf: [256]u8 = undefined;
    const sql = try TimeSeriesModel.rangeAvgSql("metrics", 1000, 2000, &buf);
    try std.testing.expectEqualStrings("SELECT TS_RANGE_AVG('metrics', 1000, 2000)", sql);
}

test "TS_RETENTION sql" {
    var buf: [256]u8 = undefined;
    const sql = try TimeSeriesModel.retentionSql("logs", 90, &buf);
    try std.testing.expectEqualStrings("SELECT TS_RETENTION('logs', 90)", sql);
}

test "TS_MATCH sql" {
    var buf: [256]u8 = undefined;
    const sql = try TimeSeriesModel.matchSql("cpu_usage", "spike*", &buf);
    try std.testing.expectEqualStrings("SELECT TS_MATCH('cpu_usage', 'spike*')", sql);
}

test "TIME_BUCKET sql" {
    var buf: [256]u8 = undefined;
    const sql = try TimeSeriesModel.timeBucketSql("5 minutes", 1700000000000, &buf);
    try std.testing.expectEqualStrings("SELECT TIME_BUCKET('5 minutes', 1700000000000)", sql);
}
