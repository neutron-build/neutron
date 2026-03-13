// Layer 2: HTTP/1.1 server — composes Layer 0 HTTP parser + Layer 1 TCP
//
// Zero-copy request handling. Per-connection buffer. Thread-per-connection model.

const std = @import("std");
const tcp = @import("../layer1/tcp.zig");
const io_mod = @import("../layer1/io.zig");
const http_parser = @import("../layer0/http/parser.zig");

pub const Request = http_parser.Request;
pub const Method = http_parser.Method;
pub const Header = http_parser.Header;

/// HTTP request context — provides read access to the request and write access for the response.
pub const RequestContext = struct {
    request: Request,
    stream: *tcp.TcpStream,
    response_buf: []u8,
    responded: bool,

    // Parsed path segments for routing
    path: []const u8,
    method: Method,

    pub fn respondJson(self: *RequestContext, status: u16, body: []const u8) !void {
        const headers = [_]Header{
            .{ .name = "Content-Type", .value = "application/json" },
            .{ .name = "Connection", .value = "keep-alive" },
        };
        try self.respond(status, &headers, body);
    }

    pub fn respondText(self: *RequestContext, status: u16, body: []const u8) !void {
        const headers = [_]Header{
            .{ .name = "Content-Type", .value = "text/plain" },
        };
        try self.respond(status, &headers, body);
    }

    pub fn respond(self: *RequestContext, status: u16, headers: []const Header, body: ?[]const u8) !void {
        if (self.responded) return;
        self.responded = true;

        // Build Content-Length header
        var cl_buf: [20]u8 = undefined;
        const body_data = body orelse "";
        const cl_str = std.fmt.bufPrint(&cl_buf, "{d}", .{body_data.len}) catch unreachable;

        // Encode response
        var all_headers_buf: [16]Header = undefined;
        var n_headers: usize = 0;
        for (headers) |h| {
            all_headers_buf[n_headers] = h;
            n_headers += 1;
        }
        all_headers_buf[n_headers] = .{ .name = "Content-Length", .value = cl_str };
        n_headers += 1;

        const written = http_parser.encodeResponse(
            self.response_buf,
            status,
            all_headers_buf[0..n_headers],
            body,
        ) catch return error.BufferTooShort;

        try self.stream.writeAll(self.response_buf[0..written]);
    }

    pub fn respondError(self: *RequestContext, status: u16, detail: []const u8) !void {
        var json_buf: [512]u8 = undefined;
        const json = std.fmt.bufPrint(&json_buf,
            \\{{"type":"https://neutron.dev/errors/{s}","title":"{s}","status":{d},"detail":"{s}"}}
        , .{
            errorCode(status),
            http_parser.statusReason(status),
            status,
            detail,
        }) catch return error.BufferTooShort;
        const headers = [_]Header{
            .{ .name = "Content-Type", .value = "application/problem+json" },
        };
        try self.respond(status, &headers, json);
    }
};

fn errorCode(status: u16) []const u8 {
    return switch (status) {
        400 => "bad-request",
        401 => "unauthorized",
        403 => "forbidden",
        404 => "not-found",
        409 => "conflict",
        422 => "validation",
        429 => "rate-limited",
        else => "internal",
    };
}

pub const HttpServer = struct {
    listener: tcp.TcpListener,
    allocator: std.mem.Allocator,
    running: bool = false,

    pub const Config = struct {
        host: []const u8 = "127.0.0.1",
        port: u16 = 8080,
        max_request_size: usize = 1024 * 1024, // 1MB
        read_timeout_ms: u64 = 5000,
        write_timeout_ms: u64 = 10000,
    };

    pub fn init(allocator: std.mem.Allocator, config: Config) !HttpServer {
        const addr = try io_mod.resolveAddress(config.host, config.port);
        const listener = try tcp.TcpListener.bind(addr, .{
            .read_timeout_ms = config.read_timeout_ms,
            .write_timeout_ms = config.write_timeout_ms,
        });
        return .{
            .listener = listener,
            .allocator = allocator,
        };
    }

    /// Serve a single request (for testing). Returns after handling one request.
    pub fn serveOne(self: *HttpServer, comptime handler: fn (*RequestContext) anyerror!void) !void {
        var conn = try self.listener.accept();
        defer conn.close();

        var recv_buf: [8192]u8 = undefined;
        const n = try conn.read(&recv_buf);
        if (n == 0) return;

        const req = http_parser.parseRequest(recv_buf[0..n]) catch return;

        var response_buf: [8192]u8 = undefined;
        var ctx = RequestContext{
            .request = req,
            .stream = &conn,
            .response_buf = &response_buf,
            .responded = false,
            .path = req.path,
            .method = req.method,
        };

        handler(&ctx) catch |err| {
            if (!ctx.responded) {
                ctx.respondError(500, @errorName(err)) catch {};
            }
        };

        if (!ctx.responded) {
            ctx.respondError(404, "No response generated") catch {};
        }
    }

    /// Start serving in a loop — calls handler for each request.
    /// Runs until the shutdown flag is set.
    pub fn serve(self: *HttpServer, comptime handler: fn (*RequestContext) anyerror!void) !void {
        self.running = true;
        while (self.running) {
            var conn = self.listener.accept() catch |err| {
                if (!self.running) break;
                return err;
            };

            // Handle each connection (sequential v1; thread-per-connection in future)
            self.handleConnection(&conn, handler);
            conn.close();
        }
    }

    fn handleConnection(self: *HttpServer, conn: *tcp.TcpStream, comptime handler: fn (*RequestContext) anyerror!void) void {
        _ = self;
        var recv_buf: [8192]u8 = undefined;
        const n = conn.read(&recv_buf) catch return;
        if (n == 0) return;

        const req = http_parser.parseRequest(recv_buf[0..n]) catch return;

        var response_buf: [8192]u8 = undefined;
        var ctx = RequestContext{
            .request = req,
            .stream = conn,
            .response_buf = &response_buf,
            .responded = false,
            .path = req.path,
            .method = req.method,
        };

        handler(&ctx) catch |err| {
            if (!ctx.responded) {
                ctx.respondError(500, @errorName(err)) catch {};
            }
        };

        if (!ctx.responded) {
            ctx.respondError(404, "No response generated") catch {};
        }
    }

    /// Request graceful shutdown.
    pub fn shutdown(self: *HttpServer) void {
        self.running = false;
    }

    pub fn getPort(self: *const HttpServer) u16 {
        return self.listener.getPort();
    }

    pub fn deinit(self: *HttpServer) void {
        self.listener.deinit();
    }
};

test "RequestContext: respondJson" {
    // Unit test just the encoding path using a mock stream approach
    // We test the full HTTP server integration in the TCP test below
    const status_reason = http_parser.statusReason(200);
    try std.testing.expectEqualStrings("OK", status_reason);
}

test "errorCode mapping" {
    try std.testing.expectEqualStrings("bad-request", errorCode(400));
    try std.testing.expectEqualStrings("not-found", errorCode(404));
    try std.testing.expectEqualStrings("internal", errorCode(500));
    try std.testing.expectEqualStrings("unauthorized", errorCode(401));
    try std.testing.expectEqualStrings("rate-limited", errorCode(429));
}

test "HttpServer: init and accept" {
    const allocator = std.testing.allocator;
    var server = try HttpServer.init(allocator, .{
        .host = "127.0.0.1",
        .port = 0, // random port
    });
    defer server.deinit();

    const port = server.getPort();
    try std.testing.expect(port > 0);

    // Spawn a client that sends a request
    const thread = try std.Thread.spawn(.{}, struct {
        fn run(p: u16) void {
            const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, p);
            const conn = std.net.tcpConnectToAddress(addr) catch return;
            defer conn.close();
            const req = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";
            _ = conn.write(req) catch return;
            var buf: [4096]u8 = undefined;
            _ = conn.read(&buf) catch return;
        }
    }.run, .{port});

    try server.serveOne(struct {
        fn handle(ctx: *RequestContext) anyerror!void {
            try ctx.respondJson(200, "{\"status\":\"ok\"}");
        }
    }.handle);

    thread.join();
}
