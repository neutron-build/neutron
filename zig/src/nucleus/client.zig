// Nucleus multi-model client — connection pool + auto-detection
//
// Wraps Layer 2 PgClient with connection pooling and Nucleus feature detection.
// Provides typed accessors for each data model (KV, Vector, etc.).
// Models execute queries via the client's execute/query methods.

const std = @import("std");
const pg = @import("../layer2/pg_client.zig");
const pool_mod = @import("../layer1/pool.zig");

pub const PgClient = pg.PgClient;
pub const QueryResult = pg.QueryResult;

pub const Features = struct {
    is_nucleus: bool = false,
    version: [64]u8 = undefined,
    version_len: usize = 0,

    pub fn versionStr(self: *const Features) []const u8 {
        return self.version[0..self.version_len];
    }
};

pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 5432,
    user: []const u8 = "postgres",
    password: []const u8 = "",
    database: []const u8 = "postgres",
    pool_size: u16 = 25,
};

/// Parse a PostgreSQL connection URL.
/// Format: postgres://user:password@host:port/database
pub fn parseUrl(url: []const u8) !Config {
    var cfg = Config{};

    // Strip scheme
    var rest = url;
    if (std.mem.startsWith(u8, rest, "postgres://") or std.mem.startsWith(u8, rest, "postgresql://")) {
        const scheme_end = std.mem.indexOf(u8, rest, "://").? + 3;
        rest = rest[scheme_end..];
    }

    // user:password@host:port/database
    if (std.mem.indexOfScalar(u8, rest, '@')) |at_pos| {
        const userinfo = rest[0..at_pos];
        if (std.mem.indexOfScalar(u8, userinfo, ':')) |colon| {
            cfg.user = userinfo[0..colon];
            cfg.password = userinfo[colon + 1 ..];
        } else {
            cfg.user = userinfo;
        }
        rest = rest[at_pos + 1 ..];
    }

    // host:port/database
    if (std.mem.indexOfScalar(u8, rest, '/')) |slash| {
        cfg.database = rest[slash + 1 ..];
        rest = rest[0..slash];
    }

    if (std.mem.indexOfScalar(u8, rest, ':')) |colon| {
        cfg.host = rest[0..colon];
        cfg.port = std.fmt.parseInt(u16, rest[colon + 1 ..], 10) catch 5432;
    } else {
        cfg.host = rest;
    }

    return cfg;
}

/// Nucleus client — multi-model database access over pgwire.
/// Provides connection pooling, auto-detection of Nucleus vs plain PostgreSQL,
/// and typed accessors for each data model.
pub const NucleusClient = struct {
    config: Config,
    features: Features,
    allocator: std.mem.Allocator,
    connected: bool,
    // Active PgClient connection (single connection for now; pool in future)
    pg_client: ?PgClient = null,

    pub fn init(allocator: std.mem.Allocator, config: Config) NucleusClient {
        return .{
            .config = config,
            .features = .{},
            .allocator = allocator,
            .connected = false,
        };
    }

    /// Initialize from connection URL.
    pub fn fromUrl(allocator: std.mem.Allocator, url: []const u8) !NucleusClient {
        const config = try parseUrl(url);
        return init(allocator, config);
    }

    /// Connect to the database, authenticate, and detect Nucleus features.
    pub fn connect(self: *NucleusClient) !void {
        var client = try PgClient.connect(self.allocator, .{
            .host = self.config.host,
            .port = self.config.port,
            .user = self.config.user,
            .password = self.config.password,
            .database = self.config.database,
        });

        // Auto-detect Nucleus
        if (client.is_nucleus) {
            self.features.is_nucleus = true;
            const ver_len = @min(client.server_version_len, self.features.version.len);
            @memcpy(self.features.version[0..ver_len], client.server_version[0..ver_len]);
            self.features.version_len = ver_len;
        }

        self.pg_client = client;
        self.connected = true;
    }

    /// Execute a SQL query and return the raw result text (first column, first row).
    pub fn execute(self: *NucleusClient, sql_str: []const u8) !?[]const u8 {
        if (self.pg_client) |*client| {
            const result = try client.query(sql_str);
            return result.scalar();
        }
        return error.NotConnected;
    }

    /// Execute a SQL query and return the full QueryResult.
    pub fn query(self: *NucleusClient, sql_str: []const u8) !QueryResult {
        if (self.pg_client) |*client| {
            return try client.query(sql_str);
        }
        return error.NotConnected;
    }

    /// Execute a statement (INSERT/UPDATE/DELETE) and return the command tag.
    pub fn exec(self: *NucleusClient, sql_str: []const u8) ![]const u8 {
        if (self.pg_client) |*client| {
            return try client.execute(sql_str);
        }
        return error.NotConnected;
    }

    pub fn isNucleus(self: *const NucleusClient) bool {
        return self.features.is_nucleus;
    }

    pub fn close(self: *NucleusClient) void {
        if (self.pg_client) |*client| {
            client.close(self.allocator);
            self.pg_client = null;
        }
        self.connected = false;
    }

    // ── Model accessors ─────────────────────────────────────────

    pub fn sql(self: *NucleusClient) @import("sql.zig").SqlModel {
        return .{ .client = self };
    }

    pub fn kv(self: *NucleusClient) @import("kv.zig").KVModel {
        return .{ .client = self };
    }

    pub fn vector(self: *NucleusClient) @import("vector.zig").VectorModel {
        return .{ .client = self };
    }

    pub fn timeseries(self: *NucleusClient) @import("timeseries.zig").TimeSeriesModel {
        return .{ .client = self };
    }

    pub fn document(self: *NucleusClient) @import("document.zig").DocumentModel {
        return .{ .client = self };
    }

    pub fn fts(self: *NucleusClient) @import("fts.zig").FTSModel {
        return .{ .client = self };
    }

    pub fn graph(self: *NucleusClient) @import("graph.zig").GraphModel {
        return .{ .client = self };
    }

    pub fn geo(self: *NucleusClient) @import("geo.zig").GeoModel {
        return .{ .client = self };
    }

    pub fn blob(self: *NucleusClient) @import("blob.zig").BlobModel {
        return .{ .client = self };
    }

    pub fn streams(self: *NucleusClient) @import("streams.zig").StreamsModel {
        return .{ .client = self };
    }

    pub fn pubsub(self: *NucleusClient) @import("pubsub.zig").PubSubModel {
        return .{ .client = self };
    }

    pub fn columnar(self: *NucleusClient) @import("columnar.zig").ColumnarModel {
        return .{ .client = self };
    }

    pub fn datalog(self: *NucleusClient) @import("datalog.zig").DatalogModel {
        return .{ .client = self };
    }

    pub fn cdc(self: *NucleusClient) @import("cdc.zig").CdcModel {
        return .{ .client = self };
    }

    /// Begin a transaction.
    pub fn begin(self: *NucleusClient) !@import("tx.zig").Transaction {
        _ = try self.exec("BEGIN");
        return @import("tx.zig").Transaction{ .client = self };
    }
};

test "parseUrl: full url" {
    const cfg = try parseUrl("postgres://admin:secret@db.example.com:5433/mydb");
    try std.testing.expectEqualStrings("admin", cfg.user);
    try std.testing.expectEqualStrings("secret", cfg.password);
    try std.testing.expectEqualStrings("db.example.com", cfg.host);
    try std.testing.expectEqual(@as(u16, 5433), cfg.port);
    try std.testing.expectEqualStrings("mydb", cfg.database);
}

test "parseUrl: minimal" {
    const cfg = try parseUrl("postgres://localhost/testdb");
    try std.testing.expectEqualStrings("localhost", cfg.host);
    try std.testing.expectEqualStrings("testdb", cfg.database);
}

test "parseUrl: with user no password" {
    const cfg = try parseUrl("postgres://user@host:5432/db");
    try std.testing.expectEqualStrings("user", cfg.user);
    try std.testing.expectEqualStrings("host", cfg.host);
    try std.testing.expectEqual(@as(u16, 5432), cfg.port);
}

test "NucleusClient: init" {
    const client = NucleusClient.init(std.testing.allocator, .{});
    try std.testing.expect(!client.isNucleus());
    try std.testing.expect(!client.connected);
}
