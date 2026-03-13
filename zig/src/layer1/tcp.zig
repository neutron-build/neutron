// Layer 1: TCP listener and connections
//
// Wraps std.net.Server for accepting connections.
// Provides connection management with configurable timeouts.

const std = @import("std");
const net = std.net;
const io = @import("io.zig");

pub const TcpListener = struct {
    server: net.Server,
    config: Config,

    pub const Config = struct {
        read_timeout_ms: u64 = 5000,
        write_timeout_ms: u64 = 10000,
        nodelay: bool = true,
    };

    /// Bind and listen on the given address.
    pub fn bind(address: net.Address, config: Config) !TcpListener {
        const server = try address.listen(.{
            .reuse_address = true,
        });
        return .{
            .server = server,
            .config = config,
        };
    }

    /// Accept a connection. Blocks until a client connects.
    /// Applies configured timeouts and TCP options.
    pub fn accept(self: *TcpListener) !TcpStream {
        const conn = try self.server.accept();
        if (self.config.nodelay) io.setNoDelay(conn.stream, true);
        if (self.config.read_timeout_ms > 0) io.setReadTimeout(conn.stream, self.config.read_timeout_ms);
        if (self.config.write_timeout_ms > 0) io.setWriteTimeout(conn.stream, self.config.write_timeout_ms);
        return .{
            .stream = conn.stream,
            .address = conn.address,
        };
    }

    pub fn deinit(self: *TcpListener) void {
        self.server.deinit();
    }

    pub fn getPort(self: *const TcpListener) u16 {
        return self.server.listen_address.getPort();
    }
};

pub const TcpStream = struct {
    stream: net.Stream,
    address: net.Address,

    pub fn read(self: *TcpStream, buf: []u8) !usize {
        return self.stream.read(buf);
    }

    pub fn write(self: *TcpStream, data: []const u8) !usize {
        return self.stream.write(data);
    }

    pub fn writeAll(self: *TcpStream, data: []const u8) !void {
        return self.stream.writeAll(data);
    }

    pub fn close(self: *TcpStream) void {
        self.stream.close();
    }

    /// Set read deadline from now.
    pub fn setReadTimeout(self: *TcpStream, timeout_ms: u64) void {
        io.setReadTimeout(self.stream, timeout_ms);
    }

    /// Set write deadline from now.
    pub fn setWriteTimeout(self: *TcpStream, timeout_ms: u64) void {
        io.setWriteTimeout(self.stream, timeout_ms);
    }
};

test "TcpListener bind and accept" {
    // Bind to a random port
    const addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);
    var listener = try TcpListener.bind(addr, .{});
    defer listener.deinit();

    const port = listener.getPort();
    try std.testing.expect(port > 0);

    // Connect from a client thread
    const thread = try std.Thread.spawn(.{}, struct {
        fn run(p: u16) void {
            const client_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, p);
            const conn = net.tcpConnectToAddress(client_addr) catch return;
            conn.close();
        }
    }.run, .{port});

    var conn = try listener.accept();
    defer conn.close();
    try std.testing.expect(conn.address.getPort() > 0);
    thread.join();
}
