// Nucleus Document Model — SQL generation + execution for DOC_* functions
//
// SQL functions: DOC_INSERT, DOC_GET, DOC_UPDATE, DOC_DELETE, DOC_QUERY,
// DOC_PATH, DOC_COUNT.
//
// Document ids are integers (DOC_INSERT returns the new id as an integer;
// DOC_GET/DOC_UPDATE/DOC_DELETE/DOC_PATH take it back as one).

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const DocumentModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    pub fn insertSql(json: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_INSERT('{s}')", .{json}) catch return error.BufferTooShort;
    }

    pub fn getSql(id: u64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_GET({d})", .{id}) catch return error.BufferTooShort;
    }

    /// SELECT DOC_UPDATE(id, 'json') — replaces the document in place; returns bool.
    pub fn updateSql(id: u64, json: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_UPDATE({d}, '{s}')", .{ id, json }) catch return error.BufferTooShort;
    }

    /// SELECT DOC_DELETE(id) — returns bool (true if the document existed).
    pub fn deleteSql(id: u64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_DELETE({d})", .{id}) catch return error.BufferTooShort;
    }

    pub fn querySql(json_query: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_QUERY('{s}')", .{json_query}) catch return error.BufferTooShort;
    }

    pub fn pathSql(id: u64, keys: []const []const u8, buf: []u8) ![]const u8 {
        var stream = std.io.fixedBufferStream(buf);
        const writer = stream.writer();
        writer.print("SELECT DOC_PATH({d}", .{id}) catch return error.BufferTooShort;
        for (keys) |key| {
            writer.print(", '{s}'", .{key}) catch return error.BufferTooShort;
        }
        writer.writeAll(")") catch return error.BufferTooShort;
        return stream.getWritten();
    }

    pub fn countSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT DOC_COUNT()", .{}) catch return error.BufferTooShort;
    }

    // ── Collections ──────────────────────────────────────────────
    //
    // A document belongs to exactly one collection, and an operation naming a
    // collection sees only that one — a document elsewhere reads as absent
    // rather than erroring, so an id cannot probe across the boundary. The
    // collection-less builders above address the default (unnamed) collection,
    // which is where every document written before collections existed lives.
    //
    // These builders interpolate rather than bind, like the rest of this file,
    // so a collection name carrying a quote would change the statement's
    // meaning. Such a name is refused instead of escaped: Nucleus collection
    // names are identifiers, and refusing is the behaviour that cannot be
    // subtly wrong.

    pub fn validateCollection(collection: []const u8) !void {
        for (collection) |c| {
            if (c == '\'' or c == '\\' or c == 0) return error.InvalidCollectionName;
        }
    }

    pub fn insertInSql(collection: []const u8, json: []const u8, buf: []u8) ![]const u8 {
        try validateCollection(collection);
        return std.fmt.bufPrint(buf, "SELECT DOC_INSERT('{s}', '{s}')", .{ collection, json }) catch return error.BufferTooShort;
    }

    pub fn getInSql(collection: []const u8, id: u64, buf: []u8) ![]const u8 {
        try validateCollection(collection);
        return std.fmt.bufPrint(buf, "SELECT DOC_GET('{s}', {d})", .{ collection, id }) catch return error.BufferTooShort;
    }

    pub fn updateInSql(collection: []const u8, id: u64, json: []const u8, buf: []u8) ![]const u8 {
        try validateCollection(collection);
        return std.fmt.bufPrint(buf, "SELECT DOC_UPDATE('{s}', {d}, '{s}')", .{ collection, id, json }) catch return error.BufferTooShort;
    }

    pub fn deleteInSql(collection: []const u8, id: u64, buf: []u8) ![]const u8 {
        try validateCollection(collection);
        return std.fmt.bufPrint(buf, "SELECT DOC_DELETE('{s}', {d})", .{ collection, id }) catch return error.BufferTooShort;
    }

    pub fn queryInSql(collection: []const u8, json_query: []const u8, buf: []u8) ![]const u8 {
        try validateCollection(collection);
        return std.fmt.bufPrint(buf, "SELECT DOC_QUERY('{s}', '{s}')", .{ collection, json_query }) catch return error.BufferTooShort;
    }

    /// A distinct FUNCTION rather than an extra argument: the key tail is
    /// variadic, so a leading collection could not be told apart from an id.
    pub fn pathInSql(collection: []const u8, id: u64, keys: []const []const u8, buf: []u8) ![]const u8 {
        try validateCollection(collection);
        var stream = std.io.fixedBufferStream(buf);
        const writer = stream.writer();
        writer.print("SELECT DOC_PATH_IN('{s}', {d}", .{ collection, id }) catch return error.BufferTooShort;
        for (keys) |key| {
            writer.print(", '{s}'", .{key}) catch return error.BufferTooShort;
        }
        writer.writeAll(")") catch return error.BufferTooShort;
        return stream.getWritten();
    }

    pub fn countInSql(collection: []const u8, buf: []u8) ![]const u8 {
        try validateCollection(collection);
        return std.fmt.bufPrint(buf, "SELECT DOC_COUNT('{s}')", .{collection}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn docInsert(self: DocumentModel, json: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try insertSql(json, &buf);
        return try self.client.execute(sql);
    }

    pub fn get(self: DocumentModel, id: u64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try getSql(id, &buf);
        return try self.client.execute(sql);
    }

    pub fn update(self: DocumentModel, id: u64, json: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try updateSql(id, json, &buf);
        return try self.client.execute(sql);
    }

    pub fn delete(self: DocumentModel, id: u64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try deleteSql(id, &buf);
        return try self.client.execute(sql);
    }

    pub fn docQuery(self: DocumentModel, json_query: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try querySql(json_query, &buf);
        return try self.client.execute(sql);
    }

    // ── Collection-scoped execution ──────────────────────────────

    pub fn docInsertIn(self: DocumentModel, collection: []const u8, json: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try insertInSql(collection, json, &buf);
        return try self.client.execute(sql);
    }

    pub fn getIn(self: DocumentModel, collection: []const u8, id: u64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try getInSql(collection, id, &buf);
        return try self.client.execute(sql);
    }

    pub fn updateIn(self: DocumentModel, collection: []const u8, id: u64, json: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try updateInSql(collection, id, json, &buf);
        return try self.client.execute(sql);
    }

    pub fn deleteIn(self: DocumentModel, collection: []const u8, id: u64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try deleteInSql(collection, id, &buf);
        return try self.client.execute(sql);
    }

    pub fn docQueryIn(self: DocumentModel, collection: []const u8, json_query: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try queryInSql(collection, json_query, &buf);
        return try self.client.execute(sql);
    }

    pub fn path(self: DocumentModel, id: u64, keys: []const []const u8) !?[]const u8 {
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
    const sql = try DocumentModel.getSql(123, &buf);
    try std.testing.expectEqualStrings("SELECT DOC_GET(123)", sql);
}

test "DOC_UPDATE sql" {
    var buf: [512]u8 = undefined;
    const sql = try DocumentModel.updateSql(7, "{\"name\":\"Bob\"}", &buf);
    try std.testing.expectEqualStrings("SELECT DOC_UPDATE(7, '{\"name\":\"Bob\"}')", sql);
}

test "DOC_DELETE sql" {
    var buf: [256]u8 = undefined;
    const sql = try DocumentModel.deleteSql(42, &buf);
    try std.testing.expectEqualStrings("SELECT DOC_DELETE(42)", sql);
}

test "DOC_QUERY sql" {
    var buf: [512]u8 = undefined;
    const sql = try DocumentModel.querySql("{\"age\":{\"$gt\":25}}", &buf);
    try std.testing.expectEqualStrings("SELECT DOC_QUERY('{\"age\":{\"$gt\":25}}')", sql);
}

test "DOC_PATH sql with keys" {
    var buf: [256]u8 = undefined;
    const keys = [_][]const u8{ "address", "city" };
    const sql = try DocumentModel.pathSql(1, &keys, &buf);
    try std.testing.expectEqualStrings("SELECT DOC_PATH(1, 'address', 'city')", sql);
}

test "DOC_PATH sql single key" {
    var buf: [256]u8 = undefined;
    const keys = [_][]const u8{"name"};
    const sql = try DocumentModel.pathSql(1, &keys, &buf);
    try std.testing.expectEqualStrings("SELECT DOC_PATH(1, 'name')", sql);
}

test "DOC_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try DocumentModel.countSql(&buf);
    try std.testing.expectEqualStrings("SELECT DOC_COUNT()", sql);
}
