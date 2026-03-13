// Layer 1: I/O abstraction — wraps std.net and std.posix
//
// Provides a uniform interface for TCP networking.
// Uses std.net.Server/Stream (blocking I/O) with thread-per-connection.
// Future: swap to io_uring/kqueue backend without changing Layer 2+ code.

const std = @import("std");
const net = std.net;
const posix = std.posix;

pub const Address = net.Address;
pub const Stream = net.Stream;

/// Resolve an address from host string and port.
pub fn resolveAddress(host: []const u8, port: u16) !Address {
    return Address.parseIp(host, port) catch {
        // Try as hostname (IPv4 loopback fallback)
        if (std.mem.eql(u8, host, "localhost")) {
            return Address.initIp4(.{ 127, 0, 0, 1 }, port);
        }
        return Address.initIp4(.{ 0, 0, 0, 0 }, port);
    };
}

/// Set socket read timeout via SO_RCVTIMEO.
pub fn setReadTimeout(stream: Stream, timeout_ms: u64) void {
    if (timeout_ms == 0) return;
    const secs: @TypeOf(@as(posix.timeval, undefined).sec) = @intCast(timeout_ms / 1000);
    const usecs: @TypeOf(@as(posix.timeval, undefined).usec) = @intCast((timeout_ms % 1000) * 1000);
    const tv: posix.timeval = .{ .sec = secs, .usec = usecs };
    posix.setsockopt(stream.handle, posix.SOL.SOCKET, posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
}

/// Set socket write timeout via SO_SNDTIMEO.
pub fn setWriteTimeout(stream: Stream, timeout_ms: u64) void {
    if (timeout_ms == 0) return;
    const secs: @TypeOf(@as(posix.timeval, undefined).sec) = @intCast(timeout_ms / 1000);
    const usecs: @TypeOf(@as(posix.timeval, undefined).usec) = @intCast((timeout_ms % 1000) * 1000);
    const tv: posix.timeval = .{ .sec = secs, .usec = usecs };
    posix.setsockopt(stream.handle, posix.SOL.SOCKET, posix.SO.SNDTIMEO, std.mem.asBytes(&tv)) catch {};
}

/// Set TCP_NODELAY (disable Nagle's algorithm).
pub fn setNoDelay(stream: Stream, enable: bool) void {
    const val: u32 = if (enable) 1 else 0;
    posix.setsockopt(stream.handle, posix.IPPROTO.TCP, posix.TCP.NODELAY, std.mem.asBytes(&val)) catch {};
}

/// Set SO_REUSEADDR on a socket.
pub fn setReuseAddr(stream: Stream) void {
    const val: u32 = 1;
    posix.setsockopt(stream.handle, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&val)) catch {};
}

test "resolveAddress localhost" {
    const addr = try resolveAddress("127.0.0.1", 8080);
    try std.testing.expectEqual(@as(u16, 8080), addr.getPort());
}

test "resolveAddress 0.0.0.0" {
    const addr = try resolveAddress("0.0.0.0", 3000);
    try std.testing.expectEqual(@as(u16, 3000), addr.getPort());
}
