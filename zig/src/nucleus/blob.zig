// Nucleus Blob Model — SQL generation + execution for BLOB_* functions
//
// SQL functions: BLOB_STORE, BLOB_GET, BLOB_DELETE, BLOB_META,
// BLOB_TAG, BLOB_LIST, BLOB_COUNT, BLOB_DEDUP_RATIO.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const BlobModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    pub fn storeSql(key: []const u8, data_hex: []const u8, content_type: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT BLOB_STORE('{s}', '{s}', '{s}')", .{ key, data_hex, content_type }) catch return error.BufferTooShort;
    }

    pub fn getSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT BLOB_GET('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn deleteSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT BLOB_DELETE('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn metaSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT BLOB_META('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn tagSql(key: []const u8, tag_key: []const u8, tag_value: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT BLOB_TAG('{s}', '{s}', '{s}')", .{ key, tag_key, tag_value }) catch return error.BufferTooShort;
    }

    pub fn listSql(prefix: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT BLOB_LIST('{s}')", .{prefix}) catch return error.BufferTooShort;
    }

    pub fn countSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT BLOB_COUNT()", .{}) catch return error.BufferTooShort;
    }

    pub fn dedupRatioSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT BLOB_DEDUP_RATIO()", .{}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn store(self: BlobModel, key: []const u8, data_hex: []const u8, content_type: []const u8) !?[]const u8 {
        var buf: [8192]u8 = undefined;
        const sql = try storeSql(key, data_hex, content_type, &buf);
        return try self.client.execute(sql);
    }

    pub fn get(self: BlobModel, key: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try getSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn delete(self: BlobModel, key: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try deleteSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn meta(self: BlobModel, key: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try metaSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn tag(self: BlobModel, key: []const u8, tag_key: []const u8, tag_value: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try tagSql(key, tag_key, tag_value, &buf);
        return try self.client.execute(sql);
    }

    pub fn list(self: BlobModel, prefix: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try listSql(prefix, &buf);
        return try self.client.execute(sql);
    }

    pub fn count(self: BlobModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try countSql(&buf);
        return try self.client.execute(sql);
    }

    pub fn dedupRatio(self: BlobModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try dedupRatioSql(&buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "BLOB_STORE sql" {
    var buf: [512]u8 = undefined;
    const sql = try BlobModel.storeSql("images/logo.png", "89504e47", "image/png", &buf);
    try std.testing.expectEqualStrings("SELECT BLOB_STORE('images/logo.png', '89504e47', 'image/png')", sql);
}

test "BLOB_GET sql" {
    var buf: [256]u8 = undefined;
    const sql = try BlobModel.getSql("images/logo.png", &buf);
    try std.testing.expectEqualStrings("SELECT BLOB_GET('images/logo.png')", sql);
}

test "BLOB_DELETE sql" {
    var buf: [256]u8 = undefined;
    const sql = try BlobModel.deleteSql("tmp/upload.bin", &buf);
    try std.testing.expectEqualStrings("SELECT BLOB_DELETE('tmp/upload.bin')", sql);
}

test "BLOB_META sql" {
    var buf: [256]u8 = undefined;
    const sql = try BlobModel.metaSql("docs/report.pdf", &buf);
    try std.testing.expectEqualStrings("SELECT BLOB_META('docs/report.pdf')", sql);
}

test "BLOB_TAG sql" {
    var buf: [256]u8 = undefined;
    const sql = try BlobModel.tagSql("images/logo.png", "category", "branding", &buf);
    try std.testing.expectEqualStrings("SELECT BLOB_TAG('images/logo.png', 'category', 'branding')", sql);
}

test "BLOB_LIST sql" {
    var buf: [256]u8 = undefined;
    const sql = try BlobModel.listSql("images/", &buf);
    try std.testing.expectEqualStrings("SELECT BLOB_LIST('images/')", sql);
}

test "BLOB_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try BlobModel.countSql(&buf);
    try std.testing.expectEqualStrings("SELECT BLOB_COUNT()", sql);
}

test "BLOB_DEDUP_RATIO sql" {
    var buf: [256]u8 = undefined;
    const sql = try BlobModel.dedupRatioSql(&buf);
    try std.testing.expectEqualStrings("SELECT BLOB_DEDUP_RATIO()", sql);
}
