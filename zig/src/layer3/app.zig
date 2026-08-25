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

            // Install signal handlers for graceful shutdown (§8)
            installSignalHandlers(self);

            // Log startup
            std.log.info("Neutron listening on {s}:{d}", .{ self.config.host, server.getPort() });

            // Serve on the app-owned instance. The SIGTERM/SIGINT handler
            // shuts down `self.server` — serving on the local copy meant
            // the signal flipped a flag the loop never polled, and the
            // process ignored SIGTERM until SIGKILL.
            const handler = getHandler();
            self.server.?.serve(handler) catch |err| {
                if (self.lifecycle.isShutdownRequested()) {
                    // Expected — shutdown was requested
                } else {
                    return err;
                }
            };

            // Graceful shutdown: run OnStop hooks in reverse order
            self.lifecycle.runStopHooks();

            self.server.?.deinit();
            self.server = null;
        }

        /// The app instance the signal handler acts on. One per App type —
        /// the SDK serves one app per process.
        var active_instance: ?*Self = null;

        /// FRAMEWORK_CONTRACT §8: catch SIGTERM/SIGINT. Async-signal-safe:
        /// only plain writes — the serve loop observes the flags within its
        /// poll interval and drains.
        fn handleSignal(_: c_int) callconv(.c) void {
            if (active_instance) |app| {
                app.lifecycle.requestShutdown();
                if (app.server) |*srv| srv.shutdown();
            }
        }

        /// Install SIGTERM/SIGINT handlers that request graceful shutdown:
        /// stop accepting new connections, finish in-flight requests, run
        /// OnStop hooks in reverse, close the listener.
        pub fn installSignalHandlers(self: *Self) void {
            active_instance = self;
            const act = std.posix.Sigaction{
                .handler = .{ .handler = &handleSignal },
                .mask = std.mem.zeroes(std.posix.sigset_t),
                .flags = 0,
            };
            std.posix.sigaction(std.posix.SIG.TERM, &act, null);
            std.posix.sigaction(std.posix.SIG.INT, &act, null);
        }

        /// Restore default SIGTERM/SIGINT dispositions (used by tests).
        pub fn restoreDefaultSignalHandlers() void {
            const act = std.posix.Sigaction{
                .handler = .{ .handler = std.posix.SIG.DFL },
                .mask = std.mem.zeroes(std.posix.sigset_t),
                .flags = 0,
            };
            std.posix.sigaction(std.posix.SIG.TERM, &act, null);
            std.posix.sigaction(std.posix.SIG.INT, &act, null);
            active_instance = null;
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

/// Health of the nucleus dependency — FRAMEWORK_CONTRACT.md §7.
/// `nucleus` in the /health payload is this tri-state, serialized as a
/// string. It is NOT a boolean: "unconfigured" (no DB configured) is a
/// different state from "disconnected" (configured but unreachable).
pub const NucleusHealth = enum {
    connected,
    disconnected,
    unconfigured,

    pub fn toString(self: NucleusHealth) []const u8 {
        return @tagName(self);
    }
};

/// Health check response JSON — FRAMEWORK_CONTRACT.md §7 line 432:
/// GET /health → { "status": "ok", "nucleus": "connected"|"disconnected"|
/// "unconfigured", "version": "X.Y.Z" }
/// `status` is "degraded" when nucleus is configured but unreachable.
pub fn healthJson(health: NucleusHealth, version: []const u8, buf: []u8) ![]const u8 {
    const status: []const u8 = switch (health) {
        .connected => "ok",
        .disconnected => "degraded",
        .unconfigured => "ok",
    };
    return std.fmt.bufPrint(buf,
        \\{{"status":"{s}","nucleus":"{s}","version":"{s}"}}
    , .{
        status,
        health.toString(),
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

// FRAMEWORK_CONTRACT §8: the framework MUST catch SIGTERM/SIGINT and begin
// graceful shutdown (stop accepting, drain, OnStop hooks in reverse, close).
test "SIGTERM/SIGINT request graceful shutdown (FRAMEWORK_CONTRACT §8)" {
    const routes = [_]Route{
        .{ .method = .GET, .path = "/", .handler = &dummyHandler },
    };
    const R = router_mod.Router(&routes);
    const MyApp = App(R, null);
    var app = MyApp.init(std.testing.allocator, .{});

    const server = try HttpServer.init(std.testing.allocator, .{ .host = "127.0.0.1", .port = 0 });
    app.server = server;
    app.server.?.running = true;
    app.installSignalHandlers();
    defer MyApp.restoreDefaultSignalHandlers();
    defer if (app.server) |*s| s.deinit();

    try std.testing.expect(!app.lifecycle.isShutdownRequested());
    try std.posix.raise(std.posix.SIG.TERM);
    try std.testing.expect(app.lifecycle.isShutdownRequested());
    try std.testing.expect(!app.server.?.running);

    // SIGINT takes the same path.
    app.lifecycle.requestShutdown();
    app.lifecycle.shutdown_requested = false;
    app.server.?.running = true;
    try std.posix.raise(std.posix.SIG.INT);
    try std.testing.expect(app.lifecycle.isShutdownRequested());
    try std.testing.expect(!app.server.?.running);
}

// FRAMEWORK_CONTRACT.md §7 (line 432): GET /health returns
// { "status": "ok", "nucleus": "connected"|"disconnected"|"unconfigured",
//   "version": "X.Y.Z" } — `nucleus` is a tri-state STRING, and
// `status` becomes "degraded" when the dependency is configured but
// unreachable.
test "healthJson: contract tri-state nucleus field (FRAMEWORK_CONTRACT §7)" {
    var buf: [128]u8 = undefined;

    const conn = try healthJson(.connected, "0.1.0", &buf);
    try std.testing.expect(std.mem.indexOf(u8, conn, "\"status\":\"ok\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, conn, "\"nucleus\":\"connected\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, conn, "\"version\":\"0.1.0\"") != null);

    var dis_buf: [128]u8 = undefined;
    const dis = try healthJson(.disconnected, "0.1.0", &dis_buf);
    try std.testing.expect(std.mem.indexOf(u8, dis, "\"status\":\"degraded\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, dis, "\"nucleus\":\"disconnected\"") != null);

    var unc_buf: [128]u8 = undefined;
    const unc = try healthJson(.unconfigured, "0.1.0", &unc_buf);
    try std.testing.expect(std.mem.indexOf(u8, unc, "\"status\":\"ok\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unc, "\"nucleus\":\"unconfigured\"") != null);
    // A boolean would serialize as bare true/false — assert it is a string.
    try std.testing.expect(std.mem.indexOf(u8, unc, "\"nucleus\":true") == null);
    try std.testing.expect(std.mem.indexOf(u8, unc, "\"nucleus\":false") == null);
}

test "healthJson" {
    var buf: [128]u8 = undefined;
    const json = try healthJson(.connected, "0.1.0", &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":\"ok\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"nucleus\":\"connected\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"version\":\"0.1.0\"") != null);
}

test "healthJson: plain postgres is still connected-health" {
    // Feature detection (§1: is the server Nucleus vs plain PG) is separate
    // from dependency health (§7). A plain PostgreSQL server that answers is
    // a *connected* dependency.
    var buf: [128]u8 = undefined;
    const json = try healthJson(.connected, "16.0", &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"nucleus\":\"connected\"") != null);
}

// FRAMEWORK_CONTRACT §8, driven through the REAL run() path. run() used to
// serve on a local HttpServer while the signal handler shut down the copy
// stored in self.server, so SIGTERM never stopped the serving loop — the
// process ignored graceful shutdown until SIGKILL. The in-place test above
// could not catch this: it installs the handlers directly, without run().
test "App.run serves real requests and exits on SIGTERM (FRAMEWORK_CONTRACT §8)" {
    const routes = [_]Route{
        .{ .method = .GET, .path = "/", .handler = &dummyHandler },
    };
    const R = router_mod.Router(&routes);
    const MyApp = App(R, null);
    var app = MyApp.init(std.testing.allocator, .{ .host = "127.0.0.1", .port = 0 });
    defer MyApp.restoreDefaultSignalHandlers();

    const serve_thread = try std.Thread.spawn(.{}, struct {
        fn run(a: *MyApp) void {
            a.run() catch {};
        }
    }.run, .{&app});

    // Wait for the listener to come up.
    var spins: usize = 0;
    while (app.server == null and spins < 1000) : (spins += 1) {
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
    try std.testing.expect(app.server != null);
    const port = app.server.?.getPort();

    // Prove it serves: one real request over loopback. dummyHandler does
    // not respond, so the server's fallback answers — any HTTP status line
    // means the serve loop dispatched a connection.
    {
        const conn = try std.net.tcpConnectToAddress(std.net.Address.initIp4(.{ 127, 0, 0, 1 }, port));
        defer conn.close();
        try conn.writeAll("GET / HTTP/1.1\r\nHost: test\r\n\r\n");
        var buf: [256]u8 = undefined;
        const n = try conn.read(&buf);
        try std.testing.expect(std.mem.indexOf(u8, buf[0..n], "HTTP/1.1") != null);
    }

    // SIGTERM must end run() (which clears self.server). Without the fix
    // the flag lands on a struct the loop never reads and this times out.
    try std.posix.raise(std.posix.SIG.TERM);
    var exited = false;
    spins = 0;
    while (spins < 3000) : (spins += 1) {
        if (app.server == null) {
            exited = true;
            break;
        }
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
    try std.testing.expect(exited);
    // Detach, not join: on the broken path join() would hang the suite
    // forever; the process tears down test threads at exit anyway.
    serve_thread.detach();
    try std.testing.expect(app.lifecycle.isShutdownRequested());
}

fn dummyHandler(_: *http_server_mod.RequestContext) anyerror!void {}
