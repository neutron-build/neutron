// Nucleus PubSub Model — SQL generation + execution for PUBSUB_* functions
//
// SQL functions: PUBSUB_PUBLISH, PUBSUB_CHANNELS, PUBSUB_SUBSCRIBERS.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const PubSubModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    /// SELECT PUBSUB_PUBLISH('channel', 'message')
    pub fn publishSql(channel: []const u8, message: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT PUBSUB_PUBLISH('{s}', '{s}')", .{ channel, message }) catch return error.BufferTooShort;
    }

    /// SELECT PUBSUB_CHANNELS('pattern')
    pub fn channelsSql(pattern: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT PUBSUB_CHANNELS('{s}')", .{pattern}) catch return error.BufferTooShort;
    }

    /// SELECT PUBSUB_SUBSCRIBERS('channel')
    pub fn subscribersSql(channel: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT PUBSUB_SUBSCRIBERS('{s}')", .{channel}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn publish(self: PubSubModel, channel: []const u8, message: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try publishSql(channel, message, &buf);
        return try self.client.execute(sql);
    }

    pub fn channels(self: PubSubModel, pattern: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try channelsSql(pattern, &buf);
        return try self.client.execute(sql);
    }

    pub fn subscribers(self: PubSubModel, channel: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try subscribersSql(channel, &buf);
        return try self.client.execute(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "PUBSUB_PUBLISH sql" {
    var buf: [512]u8 = undefined;
    const sql = try PubSubModel.publishSql("notifications", "hello world", &buf);
    try std.testing.expectEqualStrings("SELECT PUBSUB_PUBLISH('notifications', 'hello world')", sql);
}

test "PUBSUB_CHANNELS sql" {
    var buf: [256]u8 = undefined;
    const sql = try PubSubModel.channelsSql("notify*", &buf);
    try std.testing.expectEqualStrings("SELECT PUBSUB_CHANNELS('notify*')", sql);
}

test "PUBSUB_SUBSCRIBERS sql" {
    var buf: [256]u8 = undefined;
    const sql = try PubSubModel.subscribersSql("notifications", &buf);
    try std.testing.expectEqualStrings("SELECT PUBSUB_SUBSCRIBERS('notifications')", sql);
}
