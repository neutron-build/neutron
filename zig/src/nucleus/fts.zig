// Nucleus Full-Text Search Model — SQL generation + execution for FTS_* functions
//
// SQL functions: FTS_INDEX, FTS_SEARCH, FTS_FUZZY_SEARCH, FTS_REMOVE,
// FTS_DOC_COUNT, FTS_TERM_COUNT.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const FTSModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    pub fn indexSql(doc_id: []const u8, text: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT FTS_INDEX('{s}', '{s}')", .{ doc_id, text }) catch return error.BufferTooShort;
    }

    pub fn searchSql(query_str: []const u8, limit: u32, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT FTS_SEARCH('{s}', {d})", .{ query_str, limit }) catch return error.BufferTooShort;
    }

    pub fn fuzzySearchSql(query_str: []const u8, max_distance: u32, limit: u32, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT FTS_FUZZY_SEARCH('{s}', {d}, {d})", .{ query_str, max_distance, limit }) catch return error.BufferTooShort;
    }

    pub fn removeSql(doc_id: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT FTS_REMOVE('{s}')", .{doc_id}) catch return error.BufferTooShort;
    }

    pub fn docCountSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT FTS_DOC_COUNT()", .{}) catch return error.BufferTooShort;
    }

    pub fn termCountSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT FTS_TERM_COUNT()", .{}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn ftsIndex(self: FTSModel, doc_id: []const u8, text: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try indexSql(doc_id, text, &buf);
        return try self.client.execute(sql);
    }

    pub fn search(self: FTSModel, query_str: []const u8, limit: u32) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try searchSql(query_str, limit, &buf);
        return try self.client.execute(sql);
    }

    pub fn fuzzySearch(self: FTSModel, query_str: []const u8, max_distance: u32, limit: u32) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try fuzzySearchSql(query_str, max_distance, limit, &buf);
        return try self.client.execute(sql);
    }

    pub fn remove(self: FTSModel, doc_id: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try removeSql(doc_id, &buf);
        return try self.client.execute(sql);
    }

    pub fn docCount(self: FTSModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try docCountSql(&buf);
        return try self.client.execute(sql);
    }

    pub fn termCount(self: FTSModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try termCountSql(&buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "FTS_INDEX sql" {
    var buf: [512]u8 = undefined;
    const sql = try FTSModel.indexSql("doc-42", "The quick brown fox jumps over the lazy dog", &buf);
    try std.testing.expectEqualStrings("SELECT FTS_INDEX('doc-42', 'The quick brown fox jumps over the lazy dog')", sql);
}

test "FTS_SEARCH sql" {
    var buf: [256]u8 = undefined;
    const sql = try FTSModel.searchSql("brown fox", 20, &buf);
    try std.testing.expectEqualStrings("SELECT FTS_SEARCH('brown fox', 20)", sql);
}

test "FTS_FUZZY_SEARCH sql" {
    var buf: [256]u8 = undefined;
    const sql = try FTSModel.fuzzySearchSql("quik", 2, 10, &buf);
    try std.testing.expectEqualStrings("SELECT FTS_FUZZY_SEARCH('quik', 2, 10)", sql);
}

test "FTS_REMOVE sql" {
    var buf: [256]u8 = undefined;
    const sql = try FTSModel.removeSql("doc-42", &buf);
    try std.testing.expectEqualStrings("SELECT FTS_REMOVE('doc-42')", sql);
}

test "FTS_DOC_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try FTSModel.docCountSql(&buf);
    try std.testing.expectEqualStrings("SELECT FTS_DOC_COUNT()", sql);
}

test "FTS_TERM_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try FTSModel.termCountSql(&buf);
    try std.testing.expectEqualStrings("SELECT FTS_TERM_COUNT()", sql);
}
