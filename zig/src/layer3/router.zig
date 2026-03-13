// Layer 3: Comptime route dispatch — zero runtime allocation
//
// Routes are defined at compile time. Matching is done with inline for
// which the compiler optimizes into a jump table.

const std = @import("std");
const http_parser = @import("../layer0/http/parser.zig");
const http_server = @import("../layer2/http_server.zig");

pub const Method = http_parser.Method;
pub const RequestContext = http_server.RequestContext;
pub const HandlerFn = *const fn (*RequestContext) anyerror!void;

pub const Route = struct {
    method: Method,
    path: []const u8,
    handler: HandlerFn,
    summary: ?[]const u8 = null,
    tags: ?[]const []const u8 = null,
};

/// Comptime router — matches request path and method against a compile-time route table.
pub fn Router(comptime routes: []const Route) type {
    return struct {
        /// Match a request to a route handler. Compiled to inline comparisons.
        pub fn dispatch(ctx: *RequestContext) anyerror!void {
            inline for (routes) |route| {
                if (ctx.method == route.method and matchPath(route.path, ctx.path)) {
                    return route.handler(ctx);
                }
            }
            try ctx.respondError(404, "Not Found");
        }

        /// Get the route table (for OpenAPI generation).
        pub fn getRoutes() []const Route {
            return routes;
        }

        /// Number of routes.
        pub fn routeCount() usize {
            return routes.len;
        }
    };
}

/// Match a route pattern against a request path.
/// Supports exact matches and `{param}` placeholders.
/// e.g., "/api/users/{id}" matches "/api/users/42"
pub fn matchPath(comptime pattern: []const u8, path: []const u8) bool {
    comptime var pi: usize = 0;
    var ri: usize = 0;

    inline while (pi < pattern.len) {
        if (pattern[pi] == '{') {
            // Skip to end of param name
            comptime var end = pi;
            inline while (end < pattern.len and pattern[end] != '}') {
                end += 1;
            }
            pi = end + 1; // skip '}'

            // Consume path chars until next '/' or end
            while (ri < path.len and path[ri] != '/') {
                ri += 1;
            }
        } else {
            if (ri >= path.len) return false;
            if (pattern[pi] != path[ri]) return false;
            pi += 1;
            ri += 1;
        }
    }

    return ri == path.len;
}

/// Extract a path parameter value by name from a pattern and path.
/// e.g., extractParam("/users/{id}", "/users/42", "id") → "42"
pub fn extractParam(comptime pattern: []const u8, path: []const u8, comptime param_name: []const u8) ?[]const u8 {
    comptime var pi: usize = 0;
    var ri: usize = 0;

    inline while (pi < pattern.len) {
        if (pattern[pi] == '{') {
            const name_start = pi + 1;
            comptime var name_end = name_start;
            inline while (name_end < pattern.len and pattern[name_end] != '}') {
                name_end += 1;
            }
            const this_name = pattern[name_start..name_end];
            pi = name_end + 1;

            const value_start = ri;
            while (ri < path.len and path[ri] != '/') {
                ri += 1;
            }

            if (comptime std.mem.eql(u8, this_name, param_name)) {
                return path[value_start..ri];
            }
        } else {
            if (ri >= path.len) return null;
            if (pattern[pi] != path[ri]) return null;
            pi += 1;
            ri += 1;
        }
    }
    return null;
}

test "matchPath: exact" {
    try std.testing.expect(matchPath("/api/users", "/api/users"));
    try std.testing.expect(!matchPath("/api/users", "/api/posts"));
    try std.testing.expect(!matchPath("/api/users", "/api/users/42"));
    try std.testing.expect(!matchPath("/api/users/42", "/api/users"));
}

test "matchPath: with params" {
    try std.testing.expect(matchPath("/api/users/{id}", "/api/users/42"));
    try std.testing.expect(matchPath("/api/users/{id}", "/api/users/abc"));
    try std.testing.expect(matchPath("/api/users/{id}/posts/{post_id}", "/api/users/1/posts/99"));
    try std.testing.expect(!matchPath("/api/users/{id}", "/api/users/42/extra"));
}

test "matchPath: root" {
    try std.testing.expect(matchPath("/", "/"));
    try std.testing.expect(!matchPath("/", "/api"));
}

test "extractParam" {
    try std.testing.expectEqualStrings("42", extractParam("/api/users/{id}", "/api/users/42", "id").?);
    try std.testing.expectEqualStrings("99", extractParam("/api/users/{uid}/posts/{pid}", "/api/users/1/posts/99", "pid").?);
    try std.testing.expectEqualStrings("1", extractParam("/api/users/{uid}/posts/{pid}", "/api/users/1/posts/99", "uid").?);
    try std.testing.expectEqual(@as(?[]const u8, null), extractParam("/api/users/{id}", "/api/users/42", "name"));
}

test "Router: route count" {
    const routes = [_]Route{
        .{ .method = .GET, .path = "/health", .handler = &dummyHandler },
        .{ .method = .POST, .path = "/api/users", .handler = &dummyHandler },
    };
    const R = Router(&routes);
    try std.testing.expectEqual(@as(usize, 2), R.routeCount());
}

test "Router: getRoutes" {
    const routes = [_]Route{
        .{ .method = .GET, .path = "/", .handler = &dummyHandler, .summary = "Root" },
    };
    const R = Router(&routes);
    const rt = R.getRoutes();
    try std.testing.expectEqual(@as(usize, 1), rt.len);
    try std.testing.expectEqualStrings("/", rt[0].path);
    try std.testing.expectEqualStrings("Root", rt[0].summary.?);
}

fn dummyHandler(_: *RequestContext) anyerror!void {}
