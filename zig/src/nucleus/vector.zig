// Nucleus Vector Model — SQL generation + execution for vector similarity search
//
// SQL functions: VECTOR(), VECTOR_DIMS(), VECTOR_DISTANCE(),
// COSINE_DISTANCE(), INNER_PRODUCT().

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const VectorModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    pub fn searchSql(collection: []const u8, query_vec: []const u8, k: u32, metric: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT VECTOR_DISTANCE('{s}', VECTOR('{s}'), {d}, '{s}')", .{ collection, query_vec, k, metric }) catch return error.BufferTooShort;
    }

    pub fn insertSql(collection: []const u8, id: []const u8, vector_data: []const u8, metadata: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "INSERT INTO {s} (id, vector, metadata) VALUES ('{s}', VECTOR('{s}'), '{s}')", .{ collection, id, vector_data, metadata }) catch return error.BufferTooShort;
    }

    pub fn deleteSql(collection: []const u8, id: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "DELETE FROM {s} WHERE id = '{s}'", .{ collection, id }) catch return error.BufferTooShort;
    }

    pub fn distanceSql(v1: []const u8, v2: []const u8, metric: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT VECTOR_DISTANCE(VECTOR('{s}'), VECTOR('{s}'), '{s}')", .{ v1, v2, metric }) catch return error.BufferTooShort;
    }

    pub fn cosineDistanceSql(v1: []const u8, v2: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT COSINE_DISTANCE(VECTOR('{s}'), VECTOR('{s}'))", .{ v1, v2 }) catch return error.BufferTooShort;
    }

    pub fn innerProductSql(v1: []const u8, v2: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT INNER_PRODUCT(VECTOR('{s}'), VECTOR('{s}'))", .{ v1, v2 }) catch return error.BufferTooShort;
    }

    pub fn dimsSql(v: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT VECTOR_DIMS(VECTOR('{s}'))", .{v}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn search(self: VectorModel, collection: []const u8, query_vec: []const u8, k: u32, metric: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try searchSql(collection, query_vec, k, metric, &buf);
        return try self.client.execute(sql);
    }

    pub fn insert(self: VectorModel, collection: []const u8, id: []const u8, vector_data: []const u8, metadata: []const u8) ![]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try insertSql(collection, id, vector_data, metadata, &buf);
        return try self.client.exec(sql);
    }

    pub fn delete(self: VectorModel, collection: []const u8, id: []const u8) ![]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try deleteSql(collection, id, &buf);
        return try self.client.exec(sql);
    }

    pub fn distance(self: VectorModel, v1: []const u8, v2: []const u8, metric: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try distanceSql(v1, v2, metric, &buf);
        return try self.client.execute(sql);
    }

    pub fn cosineDistance(self: VectorModel, v1: []const u8, v2: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try cosineDistanceSql(v1, v2, &buf);
        return try self.client.execute(sql);
    }

    pub fn innerProduct(self: VectorModel, v1: []const u8, v2: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try innerProductSql(v1, v2, &buf);
        return try self.client.execute(sql);
    }

    pub fn dims(self: VectorModel, v: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try dimsSql(v, &buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "VECTOR search sql" {
    var buf: [512]u8 = undefined;
    const sql = try VectorModel.searchSql("embeddings", "[0.1,0.2,0.3]", 10, "cosine", &buf);
    try std.testing.expectEqualStrings("SELECT VECTOR_DISTANCE('embeddings', VECTOR('[0.1,0.2,0.3]'), 10, 'cosine')", sql);
}

test "VECTOR insert sql" {
    var buf: [512]u8 = undefined;
    const sql = try VectorModel.insertSql("embeddings", "doc-1", "[0.5,0.6]", "{\"source\":\"test\"}", &buf);
    try std.testing.expectEqualStrings("INSERT INTO embeddings (id, vector, metadata) VALUES ('doc-1', VECTOR('[0.5,0.6]'), '{\"source\":\"test\"}')", sql);
}

test "VECTOR_DIMS sql" {
    var buf: [256]u8 = undefined;
    const sql = try VectorModel.dimsSql("[1.0,2.0,3.0]", &buf);
    try std.testing.expectEqualStrings("SELECT VECTOR_DIMS(VECTOR('[1.0,2.0,3.0]'))", sql);
}

test "COSINE_DISTANCE sql" {
    var buf: [512]u8 = undefined;
    const sql = try VectorModel.cosineDistanceSql("[1,0]", "[0,1]", &buf);
    try std.testing.expectEqualStrings("SELECT COSINE_DISTANCE(VECTOR('[1,0]'), VECTOR('[0,1]'))", sql);
}

test "INNER_PRODUCT sql" {
    var buf: [512]u8 = undefined;
    const sql = try VectorModel.innerProductSql("[1,2,3]", "[4,5,6]", &buf);
    try std.testing.expectEqualStrings("SELECT INNER_PRODUCT(VECTOR('[1,2,3]'), VECTOR('[4,5,6]'))", sql);
}

test "VECTOR delete sql" {
    var buf: [256]u8 = undefined;
    const sql = try VectorModel.deleteSql("embeddings", "doc-99", &buf);
    try std.testing.expectEqualStrings("DELETE FROM embeddings WHERE id = 'doc-99'", sql);
}
