// Nucleus KV Model — SQL generation + execution for KV_* functions
//
// Covers basic key-value ops, list ops (L/R push/pop, LRANGE, LLEN, LINDEX),
// hash ops (HSET/HGET/HDEL/HEXISTS/HGETALL/HLEN), set ops (SADD/SREM/
// SMEMBERS/SISMEMBER/SCARD), sorted-set ops (ZADD/ZRANGE/ZRANGEBYSCORE/
// ZREM/ZCARD), and HyperLogLog ops (PFADD/PFCOUNT).
//
// Each operation has a *Sql() generator (public, for comptime validation)
// and an execution method that sends the query via NucleusClient.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const KVModel = struct {
    client: *NucleusClient,

    // ── Basic KV — SQL generators ────────────────────────────────

    pub fn getSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_GET('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn setSql(key: []const u8, value: []const u8, ttl_seconds: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_SET('{s}', '{s}', {d})", .{ key, value, ttl_seconds }) catch return error.BufferTooShort;
    }

    pub fn setnxSql(key: []const u8, value: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_SETNX('{s}', '{s}')", .{ key, value }) catch return error.BufferTooShort;
    }

    pub fn delSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_DEL('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn existsSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_EXISTS('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn incrSql(key: []const u8, amount: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_INCR('{s}', {d})", .{ key, amount }) catch return error.BufferTooShort;
    }

    pub fn ttlSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_TTL('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn expireSql(key: []const u8, ttl_seconds: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_EXPIRE('{s}', {d})", .{ key, ttl_seconds }) catch return error.BufferTooShort;
    }

    pub fn dbsizeSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_DBSIZE()", .{}) catch return error.BufferTooShort;
    }

    pub fn flushdbSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_FLUSHDB()", .{}) catch return error.BufferTooShort;
    }

    // ── List ops — SQL generators ────────────────────────────────

    pub fn lpushSql(key: []const u8, value: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_LPUSH('{s}', '{s}')", .{ key, value }) catch return error.BufferTooShort;
    }

    pub fn rpushSql(key: []const u8, value: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_RPUSH('{s}', '{s}')", .{ key, value }) catch return error.BufferTooShort;
    }

    pub fn lpopSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_LPOP('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn rpopSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_RPOP('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn lrangeSql(key: []const u8, start: i64, stop: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_LRANGE('{s}', {d}, {d})", .{ key, start, stop }) catch return error.BufferTooShort;
    }

    pub fn llenSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_LLEN('{s}')", .{key}) catch return error.BufferTooShort;
    }

    /// SELECT KV_LINDEX('key', index)
    pub fn lindexSql(key: []const u8, index: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_LINDEX('{s}', {d})", .{ key, index }) catch return error.BufferTooShort;
    }

    // ── Hash ops — SQL generators ────────────────────────────────

    pub fn hsetSql(key: []const u8, field: []const u8, value: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_HSET('{s}', '{s}', '{s}')", .{ key, field, value }) catch return error.BufferTooShort;
    }

    pub fn hgetSql(key: []const u8, field: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_HGET('{s}', '{s}')", .{ key, field }) catch return error.BufferTooShort;
    }

    pub fn hdelSql(key: []const u8, field: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_HDEL('{s}', '{s}')", .{ key, field }) catch return error.BufferTooShort;
    }

    pub fn hexistsSql(key: []const u8, field: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_HEXISTS('{s}', '{s}')", .{ key, field }) catch return error.BufferTooShort;
    }

    pub fn hgetallSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_HGETALL('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn hlenSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_HLEN('{s}')", .{key}) catch return error.BufferTooShort;
    }

    // ── Set ops — SQL generators ─────────────────────────────────

    pub fn saddSql(key: []const u8, member: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_SADD('{s}', '{s}')", .{ key, member }) catch return error.BufferTooShort;
    }

    pub fn sremSql(key: []const u8, member: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_SREM('{s}', '{s}')", .{ key, member }) catch return error.BufferTooShort;
    }

    pub fn smembersSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_SMEMBERS('{s}')", .{key}) catch return error.BufferTooShort;
    }

    pub fn sismemberSql(key: []const u8, member: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_SISMEMBER('{s}', '{s}')", .{ key, member }) catch return error.BufferTooShort;
    }

    pub fn scardSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_SCARD('{s}')", .{key}) catch return error.BufferTooShort;
    }

    // ── Sorted-set ops — SQL generators ──────────────────────────

    pub fn zaddSql(key: []const u8, score: f64, member: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_ZADD('{s}', {d}, '{s}')", .{ key, score, member }) catch return error.BufferTooShort;
    }

    pub fn zrangeSql(key: []const u8, start: i64, stop: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_ZRANGE('{s}', {d}, {d})", .{ key, start, stop }) catch return error.BufferTooShort;
    }

    pub fn zrangebyscoreSql(key: []const u8, min_score: f64, max_score: f64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_ZRANGEBYSCORE('{s}', {d}, {d})", .{ key, min_score, max_score }) catch return error.BufferTooShort;
    }

    pub fn zremSql(key: []const u8, member: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_ZREM('{s}', '{s}')", .{ key, member }) catch return error.BufferTooShort;
    }

    pub fn zcardSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_ZCARD('{s}')", .{key}) catch return error.BufferTooShort;
    }

    // ── HyperLogLog ops — SQL generators ─────────────────────────

    pub fn pfaddSql(key: []const u8, element: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_PFADD('{s}', '{s}')", .{ key, element }) catch return error.BufferTooShort;
    }

    pub fn pfcountSql(key: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT KV_PFCOUNT('{s}')", .{key}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn get(self: KVModel, key: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try getSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn set(self: KVModel, key: []const u8, value: []const u8, ttl_seconds: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try setSql(key, value, ttl_seconds, &buf);
        return try self.client.execute(sql);
    }

    pub fn setnx(self: KVModel, key: []const u8, value: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try setnxSql(key, value, &buf);
        return try self.client.execute(sql);
    }

    pub fn del(self: KVModel, key: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try delSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn exists(self: KVModel, key: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try existsSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn incr(self: KVModel, key: []const u8, amount: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try incrSql(key, amount, &buf);
        return try self.client.execute(sql);
    }

    pub fn ttl(self: KVModel, key: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try ttlSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn expire(self: KVModel, key: []const u8, ttl_seconds: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try expireSql(key, ttl_seconds, &buf);
        return try self.client.execute(sql);
    }

    pub fn dbsize(self: KVModel) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try dbsizeSql(&buf);
        return try self.client.execute(sql);
    }

    pub fn flushdb(self: KVModel) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try flushdbSql(&buf);
        return try self.client.execute(sql);
    }

    pub fn lpush(self: KVModel, key: []const u8, value: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try lpushSql(key, value, &buf);
        return try self.client.execute(sql);
    }

    pub fn rpush(self: KVModel, key: []const u8, value: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try rpushSql(key, value, &buf);
        return try self.client.execute(sql);
    }

    pub fn lpop(self: KVModel, key: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try lpopSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn rpop(self: KVModel, key: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try rpopSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn lrange(self: KVModel, key: []const u8, start: i64, stop: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try lrangeSql(key, start, stop, &buf);
        return try self.client.execute(sql);
    }

    pub fn llen(self: KVModel, key: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try llenSql(key, &buf);
        return try self.client.execute(sql);
    }

    pub fn lindex(self: KVModel, key: []const u8, index: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try lindexSql(key, index, &buf);
        return try self.client.execute(sql);
    }

    pub fn hset(self: KVModel, key: []const u8, field: []const u8, value: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try hsetSql(key, field, value, &buf);
        return try self.client.execute(sql);
    }

    pub fn hget(self: KVModel, key: []const u8, field: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try hgetSql(key, field, &buf);
        return try self.client.execute(sql);
    }

    pub fn pfadd(self: KVModel, key: []const u8, element: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try pfaddSql(key, element, &buf);
        return try self.client.execute(sql);
    }

    pub fn pfcount(self: KVModel, key: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try pfcountSql(key, &buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "KV_GET sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.getSql("mykey", &buf);
    try std.testing.expectEqualStrings("SELECT KV_GET('mykey')", sql);
}

test "KV_SET sql with ttl" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.setSql("session:abc", "token123", 3600, &buf);
    try std.testing.expectEqualStrings("SELECT KV_SET('session:abc', 'token123', 3600)", sql);
}

test "KV_DEL sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.delSql("old_key", &buf);
    try std.testing.expectEqualStrings("SELECT KV_DEL('old_key')", sql);
}

test "KV_SETNX sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.setnxSql("lock:job1", "acquired", &buf);
    try std.testing.expectEqualStrings("SELECT KV_SETNX('lock:job1', 'acquired')", sql);
}

test "KV_INCR sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.incrSql("counter", 5, &buf);
    try std.testing.expectEqualStrings("SELECT KV_INCR('counter', 5)", sql);
}

test "KV_EXPIRE sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.expireSql("temp", 120, &buf);
    try std.testing.expectEqualStrings("SELECT KV_EXPIRE('temp', 120)", sql);
}

test "KV_DBSIZE sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.dbsizeSql(&buf);
    try std.testing.expectEqualStrings("SELECT KV_DBSIZE()", sql);
}

test "KV_LPUSH sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.lpushSql("queue", "task1", &buf);
    try std.testing.expectEqualStrings("SELECT KV_LPUSH('queue', 'task1')", sql);
}

test "KV_LRANGE sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.lrangeSql("list", 0, 10, &buf);
    try std.testing.expectEqualStrings("SELECT KV_LRANGE('list', 0, 10)", sql);
}

test "KV_LINDEX sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.lindexSql("mylist", 3, &buf);
    try std.testing.expectEqualStrings("SELECT KV_LINDEX('mylist', 3)", sql);
}

test "KV_HSET sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.hsetSql("user:1", "name", "Alice", &buf);
    try std.testing.expectEqualStrings("SELECT KV_HSET('user:1', 'name', 'Alice')", sql);
}

test "KV_HGET sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.hgetSql("user:1", "name", &buf);
    try std.testing.expectEqualStrings("SELECT KV_HGET('user:1', 'name')", sql);
}

test "KV_SADD sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.saddSql("tags", "zig", &buf);
    try std.testing.expectEqualStrings("SELECT KV_SADD('tags', 'zig')", sql);
}

test "KV_SISMEMBER sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.sismemberSql("tags", "zig", &buf);
    try std.testing.expectEqualStrings("SELECT KV_SISMEMBER('tags', 'zig')", sql);
}

test "KV_ZADD sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.zaddSql("leaderboard", 99.5, "player1", &buf);
    try std.testing.expectEqualStrings("SELECT KV_ZADD('leaderboard', 99.5, 'player1')", sql);
}

test "KV_PFADD sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.pfaddSql("visitors", "user42", &buf);
    try std.testing.expectEqualStrings("SELECT KV_PFADD('visitors', 'user42')", sql);
}

test "KV_PFCOUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try KVModel.pfcountSql("visitors", &buf);
    try std.testing.expectEqualStrings("SELECT KV_PFCOUNT('visitors')", sql);
}

test "buffer too short returns error" {
    var buf: [5]u8 = undefined;
    const result = KVModel.getSql("mykey", &buf);
    try std.testing.expectError(error.BufferTooShort, result);
}
