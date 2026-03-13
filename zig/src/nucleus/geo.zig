// Nucleus Geo Model — SQL generation + execution for GEO_* / ST_* functions
//
// SQL functions: GEO_DISTANCE, GEO_DISTANCE_EUCLIDEAN, GEO_WITHIN, GEO_AREA,
// ST_MAKEPOINT, ST_X, ST_Y.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const GeoModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    pub fn distanceSql(lat1: f64, lon1: f64, lat2: f64, lon2: f64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GEO_DISTANCE({d}, {d}, {d}, {d})", .{ lat1, lon1, lat2, lon2 }) catch return error.BufferTooShort;
    }

    pub fn distanceEuclideanSql(x1: f64, y1: f64, x2: f64, y2: f64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GEO_DISTANCE_EUCLIDEAN({d}, {d}, {d}, {d})", .{ x1, y1, x2, y2 }) catch return error.BufferTooShort;
    }

    pub fn withinSql(lat1: f64, lon1: f64, lat2: f64, lon2: f64, radius: f64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GEO_WITHIN({d}, {d}, {d}, {d}, {d})", .{ lat1, lon1, lat2, lon2, radius }) catch return error.BufferTooShort;
    }

    pub fn areaSql(coords: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GEO_AREA('{s}')", .{coords}) catch return error.BufferTooShort;
    }

    /// SELECT ST_MAKEPOINT(lon, lat)
    pub fn makePointSql(lon: f64, lat: f64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT ST_MAKEPOINT({d}, {d})", .{ lon, lat }) catch return error.BufferTooShort;
    }

    /// SELECT ST_X(point_expr)
    pub fn stXSql(point_expr: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT ST_X({s})", .{point_expr}) catch return error.BufferTooShort;
    }

    /// SELECT ST_Y(point_expr)
    pub fn stYSql(point_expr: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT ST_Y({s})", .{point_expr}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn geoDistance(self: GeoModel, lat1: f64, lon1: f64, lat2: f64, lon2: f64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try distanceSql(lat1, lon1, lat2, lon2, &buf);
        return try self.client.execute(sql);
    }

    pub fn geoDistanceEuclidean(self: GeoModel, x1: f64, y1: f64, x2: f64, y2: f64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try distanceEuclideanSql(x1, y1, x2, y2, &buf);
        return try self.client.execute(sql);
    }

    pub fn geoWithin(self: GeoModel, lat1: f64, lon1: f64, lat2: f64, lon2: f64, radius: f64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try withinSql(lat1, lon1, lat2, lon2, radius, &buf);
        return try self.client.execute(sql);
    }

    pub fn area(self: GeoModel, coords: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try areaSql(coords, &buf);
        return try self.client.execute(sql);
    }

    pub fn makePoint(self: GeoModel, lon: f64, lat: f64) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try makePointSql(lon, lat, &buf);
        return try self.client.execute(sql);
    }

    pub fn stX(self: GeoModel, point_expr: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try stXSql(point_expr, &buf);
        return try self.client.execute(sql);
    }

    pub fn stY(self: GeoModel, point_expr: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try stYSql(point_expr, &buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "GEO_DISTANCE sql" {
    var buf: [256]u8 = undefined;
    const sql = try GeoModel.distanceSql(4.0689e1, -7.39941e1, 3.47749e1, -1.18243e2, &buf);
    try std.testing.expect(std.mem.startsWith(u8, sql, "SELECT GEO_DISTANCE("));
    try std.testing.expect(std.mem.endsWith(u8, sql, ")"));
}

test "GEO_DISTANCE_EUCLIDEAN sql" {
    var buf: [256]u8 = undefined;
    const sql = try GeoModel.distanceEuclideanSql(0, 0, 3, 4, &buf);
    try std.testing.expectEqualStrings("SELECT GEO_DISTANCE_EUCLIDEAN(0, 0, 3, 4)", sql);
}

test "GEO_WITHIN sql" {
    var buf: [256]u8 = undefined;
    const sql = try GeoModel.withinSql(4.07e1, -7.4e1, 4.07e1, -7.4e1, 1.0e3, &buf);
    try std.testing.expect(std.mem.startsWith(u8, sql, "SELECT GEO_WITHIN("));
    try std.testing.expect(std.mem.endsWith(u8, sql, ")"));
}

test "GEO_AREA sql" {
    var buf: [512]u8 = undefined;
    const sql = try GeoModel.areaSql("[[0,0],[1,0],[1,1],[0,1]]", &buf);
    try std.testing.expectEqualStrings("SELECT GEO_AREA('[[0,0],[1,0],[1,1],[0,1]]')", sql);
}

test "ST_MAKEPOINT sql" {
    var buf: [256]u8 = undefined;
    const sql = try GeoModel.makePointSql(-73.9857, 40.7484, &buf);
    try std.testing.expect(std.mem.startsWith(u8, sql, "SELECT ST_MAKEPOINT("));
}

test "ST_X sql" {
    var buf: [256]u8 = undefined;
    const sql = try GeoModel.stXSql("ST_MAKEPOINT(-73.9857, 40.7484)", &buf);
    try std.testing.expectEqualStrings("SELECT ST_X(ST_MAKEPOINT(-73.9857, 40.7484))", sql);
}

test "ST_Y sql" {
    var buf: [256]u8 = undefined;
    const sql = try GeoModel.stYSql("ST_MAKEPOINT(-73.9857, 40.7484)", &buf);
    try std.testing.expectEqualStrings("SELECT ST_Y(ST_MAKEPOINT(-73.9857, 40.7484))", sql);
}
