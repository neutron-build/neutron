// Nucleus Document Model — SQL generation + execution for DOC_* functions
//
// SQL functions: DOC_INSERT, DOC_GET, DOC_QUERY, DOC_PATH, DOC_COUNT.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const DocumentModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    pub fn insertSql(json: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_INSERT('{s}')", .{json}) catch return error.BufferTooShort;
    }

    pub fn getSql(id: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_GET('{s}')", .{id}) catch return error.BufferTooShort;
    }

    pub fn querySql(json_query: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_QUERY('{s}')", .{json_query}) catch return error.BufferTooShort;
    }

    pub fn pathSql(id: []const u8, keys: []const []const u8, buf: []u8) ![]const u8 {
        var stream = std.io.fixedBufferStream(buf);
        const writer = stream.writer();
        writer.print("SELECT DOC_PATH('{s}'", .{id}) catch return error.BufferTooShort;
        for (keys) |key| {
            writer.print(", '{s}'", .{key}) catch return error.BufferTooShort;
        }
        writer.writeAll(")") catch return error.BufferTooShort;
        return stream.getWritten();
    }

    pub fn countSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_COUNT()", .{}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn docInsert(self: DocumentModel, json: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try insertSql(json, &buf);
        return try self.client.execute(sql);
    }

    pub fn get(self: DocumentModel, id: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try getSql(id, &buf);
        return try self.client.execute(sql);
    }

    pub fn docQuery(self: DocumentModel, json_query: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try querySql(json_query, &buf);
        return try self.client.execute(sql);
    }

    pub fn path(self: DocumentModel, id: []const u8, keys: []const []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try pathSql(id, keys, &buf);
        return try self.client.execute(sql);
    }

    pub fn count(self: DocumentModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try countSql(&buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "DOC_INSERT sql" {
    var buf: [512]u8 = undefined;
    const sql = try DocumentModel.insertSql("{\"name\":\"Alice\",\"age\":30}", &buf);
    try std.testing.expectEqualStrings("SELECT DOC_INSERT('{\"name\":\"Alice\",\"age\":30}')", sql);
}

test "DOC_GET sql" {
    var buf: [256]u8 = undefined;
    const sql = try DocumentModel.getSql("doc-abc-123", &buf);
    try std.testing.expectEqualStrings("SELECT DOC_GET('doc-abc-123')", sql);
}

test "DOC_QUERY sql" {
    var buf: [512]u8 = undefined;
    const sql = try DocumentModel.querySql("{\"age\":{\"$gt\":25}}", &buf);
    try std.testing.expectEqualStrings("SELECT DOC_QUERY('{\"age\":{\"$gt\":25}}')", sql);
}

test "DOC_PATH sql with keys" {
    var buf: [256]u8 = undefined;
    const keys = [_][]const u8{ "address", "city" };
    const sql = try DocumentModel.pathSql("doc-1", &keys, &buf);
    try std.testing.expectEqualStrings("SELECT DOC_PATH('doc-1', 'address', 'city')", sql);
}

test "DOC_PATH sql single key" {
    var buf: [256]u8 = undefined;
    const keys = [_][]const u8{"name"};
    const sql = try DocumentModel.pathSql("doc-1", &keys, &buf);
    try std.testing.expectEqualStrings("SELECT DOC_PATH('doc-1', 'name')", sql);
}

test "DOC_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try DocumentModel.countSql(&buf);
    try std.testing.expectEqualStrings("SELECT DOC_COUNT()", sql);
}
