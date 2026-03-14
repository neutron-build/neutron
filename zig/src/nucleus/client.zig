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

/// Maximum number of connections the pool can hold (comptime fixed).
/// The actual number of connections created is controlled by Config.pool_size at runtime.
const MAX_POOL_SIZE = 32;

/// Pool type for PgClient connections.
const PgPool = pool_mod.ConnectionPool(PgClient, MAX_POOL_SIZE);

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
    /// Connection pool — fixed array of MAX_POOL_SIZE slots, with active_pool_size
    /// slots actually initialized with live connections.
    pool: PgPool,
    /// Number of slots in the pool that hold live connections.
    active_pool_size: u16,

    pub fn init(allocator: std.mem.Allocator, config: Config) NucleusClient {
        return .{
            .config = config,
            .features = .{},
            .allocator = allocator,
            .connected = false,
            .pool = PgPool.init(),
            .active_pool_size = 0,
        };
    }

    /// Initialize from connection URL.
    pub fn fromUrl(allocator: std.mem.Allocator, url: []const u8) !NucleusClient {
        const config = try parseUrl(url);
        return init(allocator, config);
    }

    /// Connect to the database, authenticate, and detect Nucleus features.
    /// Creates pool_size connections (clamped to MAX_POOL_SIZE).
    pub fn connect(self: *NucleusClient) !void {
        const target_size: u16 = if (self.config.pool_size > MAX_POOL_SIZE)
            MAX_POOL_SIZE
        else
            self.config.pool_size;

        // Ensure at least 1 connection
        const effective_size: u16 = if (target_size == 0) 1 else target_size;

        var established: u16 = 0;
        errdefer {
            // On failure, close any connections we already opened
            self.closePoolConnections(established);
        }

        for (0..effective_size) |i| {
            var client = try PgClient.connect(self.allocator, .{
                .host = self.config.host,
                .port = self.config.port,
                .user = self.config.user,
                .password = self.config.password,
                .database = self.config.database,
            });

            // Detect Nucleus features from the first connection
            if (i == 0 and client.is_nucleus) {
                self.features.is_nucleus = true;
                const ver_len = @min(client.server_version_len, self.features.version.len);
                @memcpy(self.features.version[0..ver_len], client.server_version[0..ver_len]);
                self.features.version_len = ver_len;
            }

            // Place connection directly into the pool slot
            self.pool.slots[i].conn = client;
            self.pool.slots[i].healthy = true;
            self.pool.slots[i].in_use = false;
            established += 1;
        }

        // Mark remaining slots as unhealthy so they are never acquired
        for (effective_size..MAX_POOL_SIZE) |i| {
            self.pool.slots[i].healthy = false;
            self.pool.slots[i].in_use = false;
        }

        // Update available count to reflect only the live slots
        self.pool.available_count = effective_size;
        self.active_pool_size = established;
        self.connected = true;
    }

    /// Acquire a healthy connection from the pool.
    /// If the acquired connection fails a health-check ping, it is marked unhealthy
    /// and a reconnection is attempted lazily. The next healthy connection is tried.
    fn acquireHealthy(self: *NucleusClient) !*PgClient {
        // Try up to active_pool_size times to find or create a healthy connection
        var attempts: u16 = 0;
        while (attempts < self.active_pool_size) : (attempts += 1) {
            const conn = self.pool.acquire() orelse return error.NoAvailableConnections;

            // Health check: try a lightweight query
            if (self.healthCheck(conn)) {
                return conn;
            }

            // Connection is unhealthy — attempt lazy reconnect
            if (self.reconnect(conn)) {
                return conn;
            } else {
                // Reconnect failed — mark unhealthy and release so we try next slot
                self.pool.markUnhealthy(conn);
                self.pool.release(conn);
            }
        }
        return error.NoAvailableConnections;
    }

    /// Quick health check — attempts a trivial query on the connection.
    fn healthCheck(_: *NucleusClient, conn: *PgClient) bool {
        if (!conn.ready) return false;
        // Attempt a simple no-op query
        _ = conn.execute("SELECT 1") catch return false;
        return true;
    }

    /// Try to reconnect an unhealthy connection in-place.
    /// Closes the old connection and opens a fresh one with the same config.
    fn reconnect(self: *NucleusClient, conn: *PgClient) bool {
        // Close the dead connection (ignore errors on close)
        conn.close(self.allocator);

        // Attempt to establish a new connection
        const new_conn = PgClient.connect(self.allocator, .{
            .host = self.config.host,
            .port = self.config.port,
            .user = self.config.user,
            .password = self.config.password,
            .database = self.config.database,
        }) catch return false;

        conn.* = new_conn;
        return true;
    }

    /// Execute a SQL query and return the raw result text (first column, first row).
    pub fn execute(self: *NucleusClient, sql_str: []const u8) !?[]const u8 {
        if (!self.connected) return error.NotConnected;
        const conn = try self.acquireHealthy();
        defer self.pool.release(conn);

        const result = try conn.query(sql_str);
        return result.scalar();
    }

    /// Execute a SQL query and return the full QueryResult.
    pub fn query(self: *NucleusClient, sql_str: []const u8) !QueryResult {
        if (!self.connected) return error.NotConnected;
        const conn = try self.acquireHealthy();
        defer self.pool.release(conn);

        return try conn.query(sql_str);
    }

    /// Execute a statement (INSERT/UPDATE/DELETE) and return the command tag.
    pub fn exec(self: *NucleusClient, sql_str: []const u8) ![]const u8 {
        if (!self.connected) return error.NotConnected;
        const conn = try self.acquireHealthy();
        defer self.pool.release(conn);

        return try conn.execute(sql_str);
    }

    pub fn isNucleus(self: *const NucleusClient) bool {
        return self.features.is_nucleus;
    }

    /// Close all connections in the pool and release resources.
    pub fn close(self: *NucleusClient) void {
        self.closePoolConnections(self.active_pool_size);
        self.active_pool_size = 0;
        self.pool = PgPool.init();
        self.connected = false;
    }

    /// Internal: close the first `count` pool connections.
    fn closePoolConnections(self: *NucleusClient, count: u16) void {
        for (0..count) |i| {
            // Only close if the slot was healthy (has a real connection)
            // or if it's in use (still has a connection even if marked unhealthy).
            // Use a simple approach: close all first `count` slots unconditionally.
            self.pool.slots[i].conn.close(self.allocator);
        }
    }

    /// Return the number of idle (available) connections in the pool.
    pub fn availableConnections(self: *NucleusClient) usize {
        return self.pool.availableCount();
    }

    /// Return the number of in-use connections.
    pub fn inUseConnections(self: *NucleusClient) usize {
        return self.pool.inUseCount();
    }

    /// Return the total number of live connections in the pool.
    pub fn poolSize(self: *const NucleusClient) u16 {
        return self.active_pool_size;
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

// ── Tests ─────────────────────────────────────────────────────

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
    try std.testing.expectEqual(@as(u16, 0), client.active_pool_size);
}

test "NucleusClient: init with custom pool size" {
    const client = NucleusClient.init(std.testing.allocator, .{ .pool_size = 10 });
    try std.testing.expectEqual(@as(u16, 10), client.config.pool_size);
    try std.testing.expect(!client.connected);
    try std.testing.expectEqual(@as(u16, 0), client.active_pool_size);
}

test "NucleusClient: pool starts empty" {
    var client = NucleusClient.init(std.testing.allocator, .{});
    try std.testing.expectEqual(@as(u16, 0), client.poolSize());
    // Before connect, pool has MAX_POOL_SIZE available (all zeroed slots)
    // but that's just the init state — no real connections
    try std.testing.expect(!client.connected);
}

test "NucleusClient: pool size clamping" {
    // Pool size > MAX_POOL_SIZE should be clamped
    const client = NucleusClient.init(std.testing.allocator, .{ .pool_size = 100 });
    // The clamping happens in connect(), verify config is stored as-is
    try std.testing.expectEqual(@as(u16, 100), client.config.pool_size);
}

test "NucleusClient: execute without connect returns NotConnected" {
    var client = NucleusClient.init(std.testing.allocator, .{});
    const result = client.execute("SELECT 1");
    try std.testing.expectError(error.NotConnected, result);
}

test "NucleusClient: query without connect returns NotConnected" {
    var client = NucleusClient.init(std.testing.allocator, .{});
    const result = client.query("SELECT 1");
    try std.testing.expectError(error.NotConnected, result);
}

test "NucleusClient: exec without connect returns NotConnected" {
    var client = NucleusClient.init(std.testing.allocator, .{});
    const result = client.exec("INSERT INTO t VALUES (1)");
    try std.testing.expectError(error.NotConnected, result);
}

test "NucleusClient: pool type is correct" {
    // Verify PgPool is a ConnectionPool of PgClient with MAX_POOL_SIZE slots
    try std.testing.expectEqual(@as(usize, MAX_POOL_SIZE), PgPool.init().capacity());
}

test "NucleusClient: close on unconnected client is safe" {
    var client = NucleusClient.init(std.testing.allocator, .{});
    // close() on an unconnected client should not crash
    client.close();
    try std.testing.expect(!client.connected);
    try std.testing.expectEqual(@as(u16, 0), client.active_pool_size);
}

test "PgPool: acquire release cycle" {
    // Test the pool type directly with zeroed PgClient slots
    var pool = PgPool.init();

    // Mark all slots except first 2 as unhealthy (simulating active_pool_size = 2)
    for (2..MAX_POOL_SIZE) |i| {
        pool.slots[i].healthy = false;
    }
    pool.available_count = 2;

    // Acquire first connection
    const c1 = pool.acquire();
    try std.testing.expect(c1 != null);
    try std.testing.expectEqual(@as(usize, 1), pool.availableCount());

    // Acquire second connection
    const c2 = pool.acquire();
    try std.testing.expect(c2 != null);
    try std.testing.expectEqual(@as(usize, 0), pool.availableCount());

    // Pool exhausted
    const c3 = pool.acquire();
    try std.testing.expect(c3 == null);

    // Release first connection
    pool.release(c1.?);
    try std.testing.expectEqual(@as(usize, 1), pool.availableCount());

    // Release second connection
    pool.release(c2.?);
    try std.testing.expectEqual(@as(usize, 2), pool.availableCount());
}

test "PgPool: unhealthy connections skipped" {
    var pool = PgPool.init();

    // Only 3 slots are live
    for (3..MAX_POOL_SIZE) |i| {
        pool.slots[i].healthy = false;
    }
    pool.available_count = 3;

    // Acquire and release first slot, then mark it unhealthy
    const c1 = pool.acquire().?;
    pool.release(c1);
    pool.markUnhealthy(c1);

    // Next acquire should skip the unhealthy slot
    const c2 = pool.acquire().?;
    try std.testing.expect(c1 != c2);

    pool.release(c2);
}

test "PgPool: capacity matches MAX_POOL_SIZE" {
    const pool = PgPool.init();
    try std.testing.expectEqual(@as(usize, MAX_POOL_SIZE), pool.capacity());
}
