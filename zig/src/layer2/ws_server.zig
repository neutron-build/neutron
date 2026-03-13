// Layer 2: WebSocket server — upgrade from HTTP, then frame I/O
//
// Composes Layer 0 WebSocket frame codec + Layer 0 HTTP parser + Layer 1 TCP.

const std = @import("std");
const tcp = @import("../layer1/tcp.zig");
const frame_mod = @import("../layer0/websocket/frame.zig");
const http_parser = @import("../layer0/http/parser.zig");

pub const Frame = frame_mod.Frame;
pub const Opcode = frame_mod.Opcode;

pub const Error = error{
    UpgradeFailed,
    InvalidHandshake,
    BufferTooShort,
    ConnectionClosed,
    ProtocolError,
};

pub const WsConnection = struct {
    stream: *tcp.TcpStream,
    recv_buf: []u8,
    send_buf: []u8,

    /// Read the next WebSocket frame.
    pub fn readFrame(self: *WsConnection) !Frame {
        const n = try self.stream.read(self.recv_buf);
        if (n == 0) return Error.ConnectionClosed;

        const result = frame_mod.decodeFrame(self.recv_buf[0..n]) catch return Error.ProtocolError;
        return result.frame;
    }

    /// Send a text frame.
    pub fn writeText(self: *WsConnection, text: []const u8) !void {
        const n = try frame_mod.encodeFrame(self.send_buf, .{
            .fin = true,
            .rsv1 = false,
            .rsv2 = false,
            .rsv3 = false,
            .opcode = .text,
            .masked = false,
            .payload = text,
        }, null);
        try self.stream.writeAll(self.send_buf[0..n]);
    }

    /// Send a binary frame.
    pub fn writeBinary(self: *WsConnection, data: []const u8) !void {
        const n = try frame_mod.encodeFrame(self.send_buf, .{
            .fin = true,
            .rsv1 = false,
            .rsv2 = false,
            .rsv3 = false,
            .opcode = .binary,
            .masked = false,
            .payload = data,
        }, null);
        try self.stream.writeAll(self.send_buf[0..n]);
    }

    /// Send a ping frame.
    pub fn ping(self: *WsConnection) !void {
        const n = try frame_mod.encodeFrame(self.send_buf, .{
            .fin = true,
            .rsv1 = false,
            .rsv2 = false,
            .rsv3 = false,
            .opcode = .ping,
            .masked = false,
            .payload = "",
        }, null);
        try self.stream.writeAll(self.send_buf[0..n]);
    }

    /// Send a close frame.
    pub fn close(self: *WsConnection, code: u16, reason: []const u8) !void {
        var payload: [127]u8 = undefined;
        std.mem.writeInt(u16, payload[0..2], code, .big);
        const reason_len = @min(reason.len, 125);
        @memcpy(payload[2 .. 2 + reason_len], reason[0..reason_len]);

        const n = try frame_mod.encodeFrame(self.send_buf, .{
            .fin = true,
            .rsv1 = false,
            .rsv2 = false,
            .rsv3 = false,
            .opcode = .close,
            .masked = false,
            .payload = payload[0 .. 2 + reason_len],
        }, null);
        try self.stream.writeAll(self.send_buf[0..n]);
    }
};

/// Compute WebSocket accept key from client key.
/// SHA-1 of (client_key ++ magic) then base64-encode.
pub fn computeAcceptKey(client_key: []const u8, out: *[28]u8) void {
    const magic = "258EAFA5-E914-47DA-95CA-5AB5DC11AD85";
    var hasher = std.crypto.hash.Sha1.init(.{});
    hasher.update(client_key);
    hasher.update(magic);
    var digest: [20]u8 = undefined;
    hasher.final(&digest);
    _ = std.base64.standard.Encoder.encode(out, &digest);
}

/// Build a WebSocket upgrade response.
pub fn buildUpgradeResponse(accept_key: []const u8, buf: []u8) !usize {
    const headers = [_]http_parser.Header{
        .{ .name = "Upgrade", .value = "websocket" },
        .{ .name = "Connection", .value = "Upgrade" },
        .{ .name = "Sec-WebSocket-Accept", .value = accept_key },
    };
    return http_parser.encodeResponse(buf, 101, &headers, null);
}

test "computeAcceptKey known value" {
    // SHA-1(client_key + magic_guid) → base64
    var out: [28]u8 = undefined;
    computeAcceptKey("dGhlIHNhbXBsZSBub25jZQ==", &out);
    try std.testing.expectEqualStrings("Zp3WnPgY3ScSQoOTqqt1ZpctLUs=", &out);
}

test "buildUpgradeResponse" {
    var buf: [512]u8 = undefined;
    const n = try buildUpgradeResponse("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", &buf);
    const resp = buf[0..n];
    try std.testing.expect(std.mem.startsWith(u8, resp, "HTTP/1.1 101"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "Upgrade: websocket") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") != null);
}
