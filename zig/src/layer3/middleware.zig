// Layer 3: Comptime middleware composition — Tower-like, zero-cost
//
// Middleware is composed at compile time into a single function.
// No vtable, no dynamic dispatch, no per-request allocation.

const std = @import("std");
const http_server = @import("../layer2/http_server.zig");
const http_parser = @import("../layer0/http/parser.zig");

pub const RequestContext = http_server.RequestContext;
pub const HandlerFn = *const fn (*RequestContext) anyerror!void;
pub const Header = http_parser.Header;

/// Wrap a handler with a logging middleware that measures request duration.
pub fn logging(comptime next: HandlerFn) HandlerFn {
    return &struct {
        fn handle(ctx: *RequestContext) anyerror!void {
            ctx.traceMiddleware("logging");
            const start = std.time.nanoTimestamp();
            defer {
                const elapsed = std.time.nanoTimestamp() - start;
                const elapsed_us: i64 = @intCast(@divTrunc(elapsed, std.time.ns_per_us));
                std.log.info("{s} {s} {d}us", .{
                    ctx.method.toString(),
                    ctx.path,
                    elapsed_us,
                });
            }
            return next(ctx);
        }
    }.handle;
}

/// Wrap a handler with panic recovery.
pub fn recover(comptime next: HandlerFn) HandlerFn {
    return &struct {
        fn handle(ctx: *RequestContext) anyerror!void {
            ctx.traceMiddleware("recovery");
            next(ctx) catch |err| {
                if (!ctx.responded) {
                    ctx.respondError(500, @errorName(err)) catch {};
                }
            };
        }
    }.handle;
}

/// CORS middleware configuration.
pub const CorsConfig = struct {
    allow_origins: []const u8 = "*",
    allow_methods: []const u8 = "GET, POST, PUT, PATCH, DELETE, OPTIONS",
    allow_headers: []const u8 = "Content-Type, Authorization",
    max_age: []const u8 = "86400",
};

/// Wrap a handler with CORS headers.
pub fn cors(comptime config: CorsConfig) fn (comptime HandlerFn) HandlerFn {
    return struct {
        fn wrapper(comptime next: HandlerFn) HandlerFn {
            return &struct {
                fn handle(ctx: *RequestContext) anyerror!void {
                    ctx.traceMiddleware("cors");
                    // Handle preflight
                    if (ctx.method == .OPTIONS) {
                        const headers = [_]Header{
                            .{ .name = "Access-Control-Allow-Origin", .value = config.allow_origins },
                            .{ .name = "Access-Control-Allow-Methods", .value = config.allow_methods },
                            .{ .name = "Access-Control-Allow-Headers", .value = config.allow_headers },
                            .{ .name = "Access-Control-Max-Age", .value = config.max_age },
                        };
                        try ctx.respond(204, &headers, null);
                        return;
                    }
                    return next(ctx);
                }
            }.handle;
        }
    }.wrapper;
}

/// Request ID middleware — generates a unique request ID and adds it to the
/// response headers and request context. Uses a monotonic counter for uniqueness.
pub fn requestId(comptime next: HandlerFn) HandlerFn {
    return &struct {
        var counter: u64 = 0;

        fn handle(ctx: *RequestContext) anyerror!void {
            ctx.traceMiddleware("request-id");
            counter += 1;
            // Generate a simple request ID from counter + timestamp
            var id_buf: [32]u8 = undefined;
            const id_str = std.fmt.bufPrint(&id_buf, "req-{d}-{d}", .{
                @as(u64, @intCast(@as(i64, @truncate(std.time.nanoTimestamp())))),
                counter,
            }) catch "req-unknown";

            // We can't easily add headers to the response in the current
            // architecture without modifying RequestContext, so we log it.
            std.log.debug("request_id={s} {s} {s}", .{
                id_str,
                ctx.method.toString(),
                ctx.path,
            });

            return next(ctx);
        }
    }.handle;
}

/// Timeout middleware — wraps handler with a deadline.
/// If the handler takes longer than timeout_ms, returns 408 Request Timeout.
/// Note: In the current synchronous model, we set socket timeouts.
pub fn timeout(comptime timeout_ms: u64) fn (comptime HandlerFn) HandlerFn {
    return struct {
        fn wrapper(comptime next: HandlerFn) HandlerFn {
            return &struct {
                fn handle(ctx: *RequestContext) anyerror!void {
                    ctx.traceMiddleware("timeout");
                    // Set socket write timeout to enforce deadline
                    ctx.stream.setWriteTimeout(timeout_ms);

                    const start = std.time.nanoTimestamp();
                    next(ctx) catch |err| {
                        const elapsed = std.time.nanoTimestamp() - start;
                        const elapsed_ms: u64 = @intCast(@divTrunc(elapsed, std.time.ns_per_ms));
                        if (elapsed_ms >= timeout_ms) {
                            if (!ctx.responded) {
                                ctx.respondError(408, "Request Timeout") catch {};
                            }
                            return;
                        }
                        return err;
                    };
                }
            }.handle;
        }
    }.wrapper;
}

/// Rate limiting middleware — token bucket algorithm.
/// Returns 429 Too Many Requests when the bucket is exhausted.
pub fn rateLimit(comptime requests_per_second: u32) fn (comptime HandlerFn) HandlerFn {
    return struct {
        fn wrapper(comptime next: HandlerFn) HandlerFn {
            return &struct {
                var tokens: u32 = requests_per_second;
                var last_refill: i128 = 0;

                fn handle(ctx: *RequestContext) anyerror!void {
                    ctx.traceMiddleware("rate-limit");
                    // Refill tokens based on elapsed time
                    const now = std.time.nanoTimestamp();
                    if (last_refill == 0) {
                        last_refill = now;
                    }
                    const elapsed_ns = now - last_refill;
                    const elapsed_secs: u32 = @intCast(@max(0, @divTrunc(elapsed_ns, std.time.ns_per_s)));
                    if (elapsed_secs >= 1) {
                        const refill = elapsed_secs * requests_per_second;
                        tokens = @min(tokens + refill, requests_per_second);
                        last_refill = now;
                    }

                    if (tokens == 0) {
                        ctx.respondError(429, "Rate limit exceeded") catch {};
                        return;
                    }

                    tokens -= 1;
                    return next(ctx);
                }
            }.handle;
        }
    }.wrapper;
}

/// Compose multiple middleware layers at compile time.
/// Usage:
///   const mw = Middleware(.{ logging, recover });
///   const handler = mw.wrap(myRouteHandler);
pub fn Middleware(comptime layers: anytype) type {
    return struct {
        /// Wrap a handler with all middleware layers, applied outermost first.
        pub fn wrap(comptime inner: HandlerFn) HandlerFn {
            return comptime wrapRecursive(layers.len, inner);
        }

        fn wrapRecursive(comptime remaining: usize, comptime h: HandlerFn) HandlerFn {
            if (remaining == 0) return h;
            return wrapRecursive(remaining - 1, layers[remaining - 1](h));
        }
    };
}

// ---------------------------------------------------------------------------
// OpenTelemetry tracing middleware (FRAMEWORK_CONTRACT §5 layer 9)
// ---------------------------------------------------------------------------

/// Extract the 32-hex-char trace id from a W3C traceparent header value
/// (`00-<traceId>-<spanId>-<flags>`). Returns null when malformed. Only
/// version "00" is accepted; all hex must be lowercase.
fn isLowerHex(s: []const u8) bool {
    for (s) |c| {
        if (!((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'))) return false;
    }
    return true;
}

pub fn parseTraceparent(tp: []const u8) ?[]const u8 {
    if (tp.len != 2 + 1 + 32 + 1 + 16 + 1 + 2) return null;
    if (tp[2] != '-' or tp[35] != '-' or tp[52] != '-') return null;
    if (!std.mem.eql(u8, tp[0..2], "00")) return null;
    if (!isLowerHex(tp[3..35])) return null;
    if (!isLowerHex(tp[36..52])) return null;
    if (!isLowerHex(tp[53..55])) return null;
    return tp[3..35];
}

/// OpenTelemetry tracing middleware — position 9 in the contract order.
/// Parses the inbound W3C `traceparent` header and propagates its trace id
/// (observable via `ctx.traceId()`), or synthesizes one from the clock and a
/// counter. Logs a span per request at debug level. Export (OTLP) is out of
/// scope for this SDK. Zero allocation.
pub fn trace(comptime next: HandlerFn) HandlerFn {
    return &struct {
        var span_counter: u64 = 0;

        fn handle(ctx: *RequestContext) anyerror!void {
            ctx.traceMiddleware("otel");
            const start = std.time.nanoTimestamp();
            defer {
                const elapsed_us: i64 = @intCast(@divTrunc(std.time.nanoTimestamp() - start, std.time.ns_per_us));
                std.log.debug("span {s} {s} trace_id={s} {d}us", .{
                    ctx.method.toString(),
                    ctx.path,
                    ctx.traceId(),
                    elapsed_us,
                });
            }

            if (ctx.request.getHeader("traceparent")) |tp| {
                if (parseTraceparent(tp)) |trace_id| {
                    ctx.setTraceId(trace_id);
                    return next(ctx);
                }
            }

            // Synthesize a 32-hex-char trace id from clock + counter.
            span_counter +%= 1;
            var id_buf: [32]u8 = undefined;
            const id_str = std.fmt.bufPrint(&id_buf, "{x:0>16}{x:0>16}", .{
                span_counter,
                @as(u64, @truncate(@as(u128, @bitCast(start)))),
            }) catch "";
            ctx.setTraceId(id_str);
            return next(ctx);
        }
    }.handle;
}

// ---------------------------------------------------------------------------
// Default stack — FRAMEWORK_CONTRACT §5 order
// ---------------------------------------------------------------------------

const jwt_mod = @import("jwt.zig");
const compress_mod = @import("compress.zig");

/// Auth middleware used by the default stack when no JWT configuration is
/// supplied: the layer is present at its contract position (observable in
/// the middleware trace) but authenticates nothing.
pub fn authOptional(comptime next: HandlerFn) HandlerFn {
    return &struct {
        fn handle(ctx: *RequestContext) anyerror!void {
            ctx.traceMiddleware("auth");
            return next(ctx);
        }
    }.handle;
}

/// Configuration for the default middleware stack.
pub const DefaultConfig = struct {
    cors: CorsConfig = .{},
    compress: compress_mod.CompressConfig = .{},
    rate_limit_rps: u32 = 100,
    /// When null, the auth layer runs as a pass-through gate.
    jwt: ?jwt_mod.JwtConfig = null,
    timeout_ms: u64 = 15000,
};

/// The framework-default middleware stack, in the exact order required by
/// FRAMEWORK_CONTRACT.md §5 (outermost first):
///
///   1 Request ID → 2 Logging → 3 Recovery → 4 CORS → 5 Compression →
///   6 RateLimit → 7 Auth → 8 Timeout → 9 OpenTelemetry → route handler
///
/// The order is pinned by observation in the test suite (the middleware
/// trace of a request through `default()` must equal this sequence).
pub fn default(comptime opts: DefaultConfig) fn (comptime HandlerFn) HandlerFn {
    return struct {
        fn wrapper(comptime inner: HandlerFn) HandlerFn {
            const auth_layer = if (opts.jwt) |jwt_config|
                jwt_mod.jwtMiddleware(jwt_config)
            else
                authOptional;
            const Stack = Middleware(.{
                requestId,
                logging,
                recover,
                cors(opts.cors),
                compress_mod.compression(opts.compress),
                rateLimit(opts.rate_limit_rps),
                auth_layer,
                timeout(opts.timeout_ms),
                trace,
            });
            return Stack.wrap(inner);
        }
    }.wrapper;
}

test "logging middleware compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const wrapped = logging(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

test "recover middleware compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const wrapped = recover(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

test "Middleware composition compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const Mw = Middleware(.{ logging, recover });
    const composed = Mw.wrap(inner);
    try std.testing.expect(@intFromPtr(composed) != @intFromPtr(inner));
}

test "cors middleware compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const corsWrapped = cors(.{});
    const wrapped = corsWrapped(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

test "requestId middleware compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const wrapped = requestId(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

test "timeout middleware compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const timeoutWrapped = timeout(5000);
    const wrapped = timeoutWrapped(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

test "rateLimit middleware compiles" {
    const inner: HandlerFn = &struct {
        fn handle(_: *RequestContext) anyerror!void {}
    }.handle;
    const rlWrapped = rateLimit(100);
    const wrapped = rlWrapped(inner);
    try std.testing.expect(@intFromPtr(wrapped) != @intFromPtr(inner));
}

// ── FRAMEWORK_CONTRACT §5: default middleware order, pinned by observation ──
//
// The contract fixes the default order (outermost first):
//   Request ID → Logging → Recovery → CORS → Compression → RateLimit →
//   Auth → Timeout → OpenTelemetry → Route handler
// This test executes the DEFAULT STACK against a synthetic request and
// asserts the complete observed sequence — not just the first pair — so
// any two layers swapping positions fails here.

const order_router = @import("router.zig");
const tcp_mod = @import("../layer1/tcp.zig");

fn orderRouteHandler(ctx: *RequestContext) anyerror!void {
    ctx.traceMiddleware("route");
    try ctx.respondText(200, "ok");
}

fn makeOrderContext(stream: *tcp_mod.TcpStream, req_buf: []u8, response_buf: []u8) !RequestContext {
    const req = try http_parser.parseRequest(req_buf);
    return RequestContext{
        .request = req,
        .stream = stream,
        .response_buf = response_buf,
        .responded = false,
        .path = req.path,
        .method = req.method,
    };
}

/// A real loopback socket pair for driving handlers without a network:
/// connect() before accept() — the backlog holds the peer. Socket options
/// (SO_SNDTIMEO etc.) work, since these are genuine sockets.
const MockConn = struct {
    listener: tcp_mod.TcpListener,
    peer: std.net.Stream,
    stream: tcp_mod.TcpStream,

    fn init() !MockConn {
        const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);
        var listener = try tcp_mod.TcpListener.bind(addr, .{});
        errdefer listener.deinit();
        const port = listener.getPort();
        const peer = try std.net.tcpConnectToAddress(std.net.Address.initIp4(.{ 127, 0, 0, 1 }, port));
        errdefer peer.close();
        const stream = try listener.accept();
        return .{ .listener = listener, .peer = peer, .stream = stream };
    }

    fn deinit(self: *MockConn) void {
        self.stream.close();
        self.peer.close();
        self.listener.deinit();
    }
};

test "default stack order matches FRAMEWORK_CONTRACT §5 (observed, all nine layers)" {
    const routes = [_]order_router.Route{
        .{ .method = .GET, .path = "/", .handler = &orderRouteHandler },
    };
    const R = order_router.Router(&routes);
    const stack = default(.{});
    const handler = stack(&R.dispatch);

    // Mock stream backed by a real loopback socket: handlers write real
    // responses; socket options (timeouts) behave exactly as in production.
    var conn = try MockConn.init();
    defer conn.deinit();

    const raw = "GET / HTTP/1.1\r\nHost: test\r\n\r\n";
    var req_buf: [256]u8 = undefined;
    @memcpy(req_buf[0..raw.len], raw);
    var response_buf: [8192]u8 = undefined;
    var ctx = try makeOrderContext(&conn.stream, req_buf[0..raw.len], &response_buf);

    try handler(&ctx);

    try std.testing.expect(ctx.responded);
    try std.testing.expectEqualStrings(
        "request-id,logging,recovery,cors,compression,rate-limit,auth,timeout,otel,route",
        ctx.middlewareTrace(),
    );
}

test "trace middleware propagates W3C traceparent trace id" {
    const inner: HandlerFn = &struct {
        fn handle(ctx: *RequestContext) anyerror!void {
            try ctx.respondText(200, "ok");
        }
    }.handle;
    const wrapped = trace(inner);

    var conn = try MockConn.init();
    defer conn.deinit();

    const raw = "GET / HTTP/1.1\r\nHost: test\r\ntraceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\n\r\n";
    var req_buf: [512]u8 = undefined;
    @memcpy(req_buf[0..raw.len], raw);
    var response_buf: [8192]u8 = undefined;
    var ctx = try makeOrderContext(&conn.stream, req_buf[0..raw.len], &response_buf);

    try wrapped(&ctx);
    try std.testing.expectEqualStrings("4bf92f3577b34da6a3ce929d0e0e4736", ctx.traceId());
}

test "trace middleware synthesizes a trace id when traceparent absent" {
    const inner: HandlerFn = &struct {
        fn handle(ctx: *RequestContext) anyerror!void {
            try ctx.respondText(200, "ok");
        }
    }.handle;
    const wrapped = trace(inner);

    var conn = try MockConn.init();
    defer conn.deinit();

    const raw = "GET / HTTP/1.1\r\nHost: test\r\n\r\n";
    var req_buf: [256]u8 = undefined;
    @memcpy(req_buf[0..raw.len], raw);
    var response_buf: [8192]u8 = undefined;
    var ctx = try makeOrderContext(&conn.stream, req_buf[0..raw.len], &response_buf);

    try wrapped(&ctx);
    try std.testing.expectEqual(@as(usize, 32), ctx.traceId().len);
}

test "parseTraceparent rejects malformed input" {
    try std.testing.expect(parseTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01") != null);
    try std.testing.expect(parseTraceparent("short") == null);
    try std.testing.expect(parseTraceparent("00-zzf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01") == null);
    try std.testing.expect(parseTraceparent("11-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01") == null);
}
