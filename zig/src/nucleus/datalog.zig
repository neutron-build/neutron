// Nucleus Datalog Model — SQL generation + execution for DATALOG_* functions
//
// SQL functions: DATALOG_ASSERT, DATALOG_RETRACT, DATALOG_RULE,
// DATALOG_QUERY, DATALOG_CLEAR, DATALOG_IMPORT_GRAPH.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const DatalogModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    /// SELECT DATALOG_ASSERT('fact')
    pub fn assertSql(fact: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DATALOG_ASSERT('{s}')", .{fact}) catch return error.BufferTooShort;
    }

    /// SELECT DATALOG_RETRACT('fact')
    pub fn retractSql(fact: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DATALOG_RETRACT('{s}')", .{fact}) catch return error.BufferTooShort;
    }

    /// SELECT DATALOG_RULE('head', 'body')
    pub fn ruleSql(head: []const u8, body: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DATALOG_RULE('{s}', '{s}')", .{ head, body }) catch return error.BufferTooShort;
    }

    /// SELECT DATALOG_QUERY('query')
    pub fn querySql(query_str: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DATALOG_QUERY('{s}')", .{query_str}) catch return error.BufferTooShort;
    }

    /// SELECT DATALOG_CLEAR('predicate'). The argument is required — the
    /// engine answers DATALOG_CLEAR() with "DATALOG_CLEAR requires 1
    /// argument(s), got 0" (measured live; the Go SDK binds the predicate).
    pub fn clearSql(predicate: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DATALOG_CLEAR('{s}')", .{predicate}) catch return error.BufferTooShort;
    }

    /// SELECT DATALOG_IMPORT_GRAPH('predicate') — same arity, verified the
    /// same way: DATALOG_IMPORT_GRAPH() is rejected with "requires 1
    /// argument(s)".
    pub fn importGraphSql(predicate: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DATALOG_IMPORT_GRAPH('{s}')", .{predicate}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn assertFact(self: DatalogModel, fact: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try assertSql(fact, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn retract(self: DatalogModel, fact: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try retractSql(fact, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn rule(self: DatalogModel, head: []const u8, body: []const u8) !?[]const u8 {
        var buf: [2048]u8 = undefined;
        const sql = try ruleSql(head, body, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn datalogQuery(self: DatalogModel, query_str: []const u8) !?[]const u8 {
        var buf: [2048]u8 = undefined;
        const sql = try querySql(query_str, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn clear(self: DatalogModel, predicate: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try clearSql(predicate, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn importGraph(self: DatalogModel, predicate: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try importGraphSql(predicate, &buf);
        return try self.client.executeModel(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "DATALOG_ASSERT sql" {
    var buf: [256]u8 = undefined;
    const sql = try DatalogModel.assertSql("parent(alice, bob)", &buf);
    try std.testing.expectEqualStrings("SELECT DATALOG_ASSERT('parent(alice, bob)')", sql);
}

test "DATALOG_RETRACT sql" {
    var buf: [256]u8 = undefined;
    const sql = try DatalogModel.retractSql("parent(alice, bob)", &buf);
    try std.testing.expectEqualStrings("SELECT DATALOG_RETRACT('parent(alice, bob)')", sql);
}

test "DATALOG_RULE sql" {
    var buf: [512]u8 = undefined;
    const sql = try DatalogModel.ruleSql("ancestor(X,Y)", "parent(X,Y)", &buf);
    try std.testing.expectEqualStrings("SELECT DATALOG_RULE('ancestor(X,Y)', 'parent(X,Y)')", sql);
}

test "DATALOG_QUERY sql" {
    var buf: [256]u8 = undefined;
    const sql = try DatalogModel.querySql("ancestor(alice, X)?", &buf);
    try std.testing.expectEqualStrings("SELECT DATALOG_QUERY('ancestor(alice, X)?')", sql);
}

test "DATALOG_CLEAR sql" {
    var buf: [256]u8 = undefined;
    const sql = try DatalogModel.clearSql("parent", &buf);
    try std.testing.expectEqualStrings("SELECT DATALOG_CLEAR('parent')", sql);
}

test "DATALOG_IMPORT_GRAPH sql" {
    var buf: [256]u8 = undefined;
    const sql = try DatalogModel.importGraphSql("parent", &buf);
    try std.testing.expectEqualStrings("SELECT DATALOG_IMPORT_GRAPH('parent')", sql);
}
