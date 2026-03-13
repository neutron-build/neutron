// Nucleus Streams Model — SQL generation + execution for STREAM_* functions
//
// SQL functions: STREAM_XADD, STREAM_XLEN, STREAM_XRANGE, STREAM_XREAD,
// STREAM_XGROUP_CREATE, STREAM_XREADGROUP, STREAM_XACK.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const StreamsModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    /// SELECT STREAM_XADD('stream', 'field1', 'val1')
    pub fn xaddSql(stream: []const u8, field: []const u8, value: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT STREAM_XADD('{s}', '{s}', '{s}')", .{ stream, field, value }) catch return error.BufferTooShort;
    }

    /// SELECT STREAM_XLEN('stream')
    pub fn xlenSql(stream: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT STREAM_XLEN('{s}')", .{stream}) catch return error.BufferTooShort;
    }

    /// SELECT STREAM_XRANGE('stream', start_ms, end_ms, count)
    pub fn xrangeSql(stream: []const u8, start_ms: i64, end_ms: i64, count: u32, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT STREAM_XRANGE('{s}', {d}, {d}, {d})", .{ stream, start_ms, end_ms, count }) catch return error.BufferTooShort;
    }

    /// SELECT STREAM_XREAD('stream', last_id_ms, count)
    pub fn xreadSql(stream: []const u8, last_id_ms: i64, count: u32, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT STREAM_XREAD('{s}', {d}, {d})", .{ stream, last_id_ms, count }) catch return error.BufferTooShort;
    }

    /// SELECT STREAM_XGROUP_CREATE('stream', 'group', start_id)
    pub fn xgroupCreateSql(stream: []const u8, group: []const u8, start_id: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT STREAM_XGROUP_CREATE('{s}', '{s}', {d})", .{ stream, group, start_id }) catch return error.BufferTooShort;
    }

    /// SELECT STREAM_XREADGROUP('stream', 'group', 'consumer', count)
    pub fn xreadgroupSql(stream: []const u8, group: []const u8, consumer: []const u8, count: u32, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT STREAM_XREADGROUP('{s}', '{s}', '{s}', {d})", .{ stream, group, consumer, count }) catch return error.BufferTooShort;
    }

    /// SELECT STREAM_XACK('stream', 'group', id_ms, id_seq)
    pub fn xackSql(stream: []const u8, group: []const u8, id_ms: i64, id_seq: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT STREAM_XACK('{s}', '{s}', {d}, {d})", .{ stream, group, id_ms, id_seq }) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn xadd(self: StreamsModel, stream: []const u8, field: []const u8, value: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try xaddSql(stream, field, value, &buf);
        return try self.client.execute(sql);
    }

    pub fn xlen(self: StreamsModel, stream: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try xlenSql(stream, &buf);
        return try self.client.execute(sql);
    }

    pub fn xrange(self: StreamsModel, stream: []const u8, start_ms: i64, end_ms: i64, count: u32) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try xrangeSql(stream, start_ms, end_ms, count, &buf);
        return try self.client.execute(sql);
    }

    pub fn xread(self: StreamsModel, stream: []const u8, last_id_ms: i64, count: u32) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try xreadSql(stream, last_id_ms, count, &buf);
        return try self.client.execute(sql);
    }

    pub fn xgroupCreate(self: StreamsModel, stream: []const u8, group: []const u8, start_id: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try xgroupCreateSql(stream, group, start_id, &buf);
        return try self.client.execute(sql);
    }

    pub fn xreadgroup(self: StreamsModel, stream: []const u8, group: []const u8, consumer: []const u8, count: u32) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try xreadgroupSql(stream, group, consumer, count, &buf);
        return try self.client.execute(sql);
    }

    pub fn xack(self: StreamsModel, stream: []const u8, group: []const u8, id_ms: i64, id_seq: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try xackSql(stream, group, id_ms, id_seq, &buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "STREAM_XADD sql" {
    var buf: [512]u8 = undefined;
    const sql = try StreamsModel.xaddSql("events", "type", "click", &buf);
    try std.testing.expectEqualStrings("SELECT STREAM_XADD('events', 'type', 'click')", sql);
}

test "STREAM_XLEN sql" {
    var buf: [256]u8 = undefined;
    const sql = try StreamsModel.xlenSql("events", &buf);
    try std.testing.expectEqualStrings("SELECT STREAM_XLEN('events')", sql);
}

test "STREAM_XRANGE sql" {
    var buf: [512]u8 = undefined;
    const sql = try StreamsModel.xrangeSql("events", 1000, 2000, 100, &buf);
    try std.testing.expectEqualStrings("SELECT STREAM_XRANGE('events', 1000, 2000, 100)", sql);
}

test "STREAM_XREAD sql" {
    var buf: [256]u8 = undefined;
    const sql = try StreamsModel.xreadSql("events", 0, 10, &buf);
    try std.testing.expectEqualStrings("SELECT STREAM_XREAD('events', 0, 10)", sql);
}

test "STREAM_XGROUP_CREATE sql" {
    var buf: [256]u8 = undefined;
    const sql = try StreamsModel.xgroupCreateSql("events", "workers", 0, &buf);
    try std.testing.expectEqualStrings("SELECT STREAM_XGROUP_CREATE('events', 'workers', 0)", sql);
}

test "STREAM_XREADGROUP sql" {
    var buf: [512]u8 = undefined;
    const sql = try StreamsModel.xreadgroupSql("events", "workers", "worker-1", 5, &buf);
    try std.testing.expectEqualStrings("SELECT STREAM_XREADGROUP('events', 'workers', 'worker-1', 5)", sql);
}

test "STREAM_XACK sql" {
    var buf: [256]u8 = undefined;
    const sql = try StreamsModel.xackSql("events", "workers", 1000, 0, &buf);
    try std.testing.expectEqualStrings("SELECT STREAM_XACK('events', 'workers', 1000, 0)", sql);
}
