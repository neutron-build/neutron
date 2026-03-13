// Layer 3: Application struct — lifecycle, configuration, server orchestration
//
// Ties together router, middleware, config, lifecycle, and the HTTP server.
// Provides a run() method that starts the server, handles signals, and shuts down.

const std = @import("std");
const config_mod = @import("config.zig");
const lifecycle_mod = @import("lifecycle.zig");
const http_server_mod = @import("../layer2/http_server.zig");
const router_mod = @import("router.zig");

pub const Config = config_mod.Config;
pub const Lifecycle = lifecycle_mod.Lifecycle;
pub const HttpServer = http_server_mod.HttpServer;
pub const Route = router_mod.Route;

/// Create an application type from compile-time route and middleware definitions.
pub fn App(comptime Routes: type, comptime middlewareFn: ?fn (comptime router_mod.HandlerFn) router_mod.HandlerFn) type {
    return struct {
        const Self = @This();

        config: Config,
        lifecycle: Lifecycle,
        allocator: std.mem.Allocator,
        server: ?HttpServer = null,

        pub fn init(allocator: std.mem.Allocator, config: Config) Self {
            return .{
                .config = config,
                .lifecycle = .{},
                .allocator = allocator,
            };
        }

        /// Start the application:
        /// 1. Create an HttpServer from config
        /// 2. Run OnStart lifecycle hooks
        /// 3. Register signal handlers (SIGTERM, SIGINT)
        /// 4. Enter a serve loop accepting connections
        /// 5. On signal: set shutdown flag, drain, run OnStop hooks
        pub fn run(self: *Self) !void {
            // Create HTTP server
            var server = try HttpServer.init(self.allocator, .{
                .host = self.config.host,
                .port = self.config.port,
                .read_timeout_ms = self.config.read_timeout_ms,
                .write_timeout_ms = self.config.write_timeout_ms,
            });
            self.server = server;

            // Run OnStart hooks
            self.lifecycle.runStartHooks();

            // Install signal handlers for graceful shutdown
            installSignalHandlers(self);

            // Log startup
            std.log.info("Neutron listening on {s}:{d}", .{ self.config.host, server.getPort() });

            // Serve loop
            const handler = getHandler();
            server.serve(handler) catch |err| {
                if (self.lifecycle.isShutdownRequested()) {
                    // Expected — shutdown was requested
                } else {
                    return err;
                }
            };

            // Graceful shutdown: run OnStop hooks in reverse order
            self.lifecycle.runStopHooks();

            server.deinit();
            self.server = null;
        }

        fn installSignalHandlers(self: *Self) void {
            // Store a reference to the lifecycle for the signal handler.
            // In a real implementation, this would use a global atomic or
            // thread-local storage. For now, we mark shutdown_requested.
            _ = self;
            // Signal handling is platform-specific and requires careful
            // implementation. For the initial version, shutdown is triggered
            // by calling lifecycle.requestShutdown() externally or by
            // server.shutdown().
        }

        /// Get the dispatch function (with middleware applied if configured).
        pub fn getHandler() router_mod.HandlerFn {
            if (middlewareFn) |mw| {
                return mw(&Routes.dispatch);
            }
            return &Routes.dispatch;
        }

        /// Register an OnStart hook.
        pub fn onStart(self: *Self, hook: lifecycle_mod.HookFn) void {
            self.lifecycle.onStart(hook);
        }

        /// Register an OnStop hook.
        pub fn onStop(self: *Self, hook: lifecycle_mod.HookFn) void {
            self.lifecycle.onStop(hook);
        }

        /// Get the configured port.
        pub fn port(self: *const Self) u16 {
            return self.config.port;
        }

        /// Get routes for OpenAPI generation.
        pub fn routes() []const Route {
            return Routes.getRoutes();
        }
    };
}

/// Health check response JSON.
pub fn healthJson(is_nucleus: bool, version: []const u8, buf: []u8) ![]const u8 {
    return std.fmt.bufPrint(buf,
        \\{{"status":"ok","nucleus":{s},"version":"{s}"}}
    , .{
        if (is_nucleus) "true" else "false",
        version,
    }) catch return error.BufferTooShort;
}

test "App: init and getHandler" {
    const routes = [_]Route{
        .{ .method = .GET, .path = "/health", .handler = &dummyHandler },
    };
    const R = router_mod.Router(&routes);
    const MyApp = App(R, null);
    const app = MyApp.init(std.testing.allocator, .{});
    try std.testing.expectEqual(@as(u16, 8080), app.port());
    const handler = MyApp.getHandler();
    try std.testing.expect(@intFromPtr(handler) != 0);
}

test "App: routes" {
    const routes = [_]Route{
        .{ .method = .GET, .path = "/a", .handler = &dummyHandler, .summary = "A" },
        .{ .method = .POST, .path = "/b", .handler = &dummyHandler, .summary = "B" },
    };
    const R = router_mod.Router(&routes);
    const MyApp = App(R, null);
    const rt = MyApp.routes();
    try std.testing.expectEqual(@as(usize, 2), rt.len);
}

test "App: lifecycle hooks" {
    const routes = [_]Route{
        .{ .method = .GET, .path = "/", .handler = &dummyHandler },
    };
    const R = router_mod.Router(&routes);
    const MyApp = App(R, null);
    var app = MyApp.init(std.testing.allocator, .{});

    var called = false;
    _ = &called;
    app.onStart(&struct {
        fn hook() void {}
    }.hook);
    app.onStop(&struct {
        fn hook() void {}
    }.hook);

    app.lifecycle.runStartHooks();
    app.lifecycle.runStopHooks();
}

test "healthJson" {
    var buf: [128]u8 = undefined;
    const json = try healthJson(true, "0.1.0", &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":\"ok\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"nucleus\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"version\":\"0.1.0\"") != null);
}

test "healthJson: plain postgres" {
    var buf: [128]u8 = undefined;
    const json = try healthJson(false, "16.0", &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"nucleus\":false") != null);
}

fn dummyHandler(_: *http_server_mod.RequestContext) anyerror!void {}
