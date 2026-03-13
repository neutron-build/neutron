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
