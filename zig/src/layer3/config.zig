// Layer 3: Configuration loading from environment variables
//
// Supports the NEUTRON_ prefix convention from FRAMEWORK_CONTRACT.md.
// Uses comptime for default values, runtime for env var parsing.

const std = @import("std");

pub const LogLevel = enum {
    debug,
    info,
    warn,
    err,

    pub fn fromString(s: []const u8) LogLevel {
        if (std.mem.eql(u8, s, "debug")) return .debug;
        if (std.mem.eql(u8, s, "info")) return .info;
        if (std.mem.eql(u8, s, "warn") or std.mem.eql(u8, s, "warning")) return .warn;
        if (std.mem.eql(u8, s, "error") or std.mem.eql(u8, s, "err")) return .err;
        return .info;
    }
};

pub const Config = struct {
    host: []const u8 = "0.0.0.0",
    port: u16 = 8080,
    max_connections: u16 = 1024,
    read_timeout_ms: u64 = 5000,
    write_timeout_ms: u64 = 10000,
    shutdown_timeout_ms: u64 = 30000,

    database_url: []const u8 = "",
    db_pool_size: u16 = 25,

    log_level: LogLevel = .info,

    /// Load configuration from environment variables with given prefix.
    /// e.g., prefix = "NEUTRON" reads NEUTRON_HOST, NEUTRON_PORT, etc.
    pub fn fromEnv(comptime prefix: []const u8) Config {
        var cfg = Config{};
        const env = std.posix.environ;

        for (env) |entry| {
            const kv = std.mem.span(entry);
            if (getEnvValue(kv, prefix ++ "_HOST")) |v| cfg.host = v;
            if (getEnvValue(kv, prefix ++ "_PORT")) |v| cfg.port = std.fmt.parseInt(u16, v, 10) catch 8080;
            if (getEnvValue(kv, prefix ++ "_DATABASE_URL")) |v| cfg.database_url = v;
            if (getEnvValue(kv, prefix ++ "_LOG_LEVEL")) |v| cfg.log_level = LogLevel.fromString(v);
            if (getEnvValue(kv, prefix ++ "_MAX_CONNECTIONS")) |v| cfg.max_connections = std.fmt.parseInt(u16, v, 10) catch 1024;
            if (getEnvValue(kv, prefix ++ "_SHUTDOWN_TIMEOUT")) |v| cfg.shutdown_timeout_ms = std.fmt.parseInt(u64, v, 10) catch 30000;
            if (getEnvValue(kv, prefix ++ "_DB_POOL_SIZE")) |v| cfg.db_pool_size = std.fmt.parseInt(u16, v, 10) catch 25;
        }

        return cfg;
    }
};

fn getEnvValue(entry: []const u8, key: []const u8) ?[]const u8 {
    if (entry.len <= key.len + 1) return null;
    if (!std.mem.startsWith(u8, entry, key)) return null;
    if (entry[key.len] != '=') return null;
    return entry[key.len + 1 ..];
}

test "Config defaults" {
    const cfg = Config{};
    try std.testing.expectEqualStrings("0.0.0.0", cfg.host);
    try std.testing.expectEqual(@as(u16, 8080), cfg.port);
    try std.testing.expectEqual(@as(u16, 1024), cfg.max_connections);
    try std.testing.expectEqual(@as(u64, 30000), cfg.shutdown_timeout_ms);
    try std.testing.expectEqual(LogLevel.info, cfg.log_level);
}

test "LogLevel fromString" {
    try std.testing.expectEqual(LogLevel.debug, LogLevel.fromString("debug"));
    try std.testing.expectEqual(LogLevel.info, LogLevel.fromString("info"));
    try std.testing.expectEqual(LogLevel.warn, LogLevel.fromString("warn"));
    try std.testing.expectEqual(LogLevel.warn, LogLevel.fromString("warning"));
    try std.testing.expectEqual(LogLevel.err, LogLevel.fromString("error"));
    try std.testing.expectEqual(LogLevel.info, LogLevel.fromString("unknown"));
}

test "getEnvValue" {
    try std.testing.expectEqualStrings("8080", getEnvValue("NEUTRON_PORT=8080", "NEUTRON_PORT").?);
    try std.testing.expectEqual(@as(?[]const u8, null), getEnvValue("NEUTRON_PORT=8080", "OTHER_PORT"));
    try std.testing.expectEqual(@as(?[]const u8, null), getEnvValue("SHORT=1", "LONGKEY"));
}
