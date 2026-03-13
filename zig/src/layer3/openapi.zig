// Layer 3: OpenAPI 3.1 generation from comptime route definitions
//
// Generates JSON OpenAPI spec from the route table defined at compile time.
// Uses @typeInfo to inspect Input/Output struct types from routes and generate
// JSON Schema. Includes $ref to RFC 7807 ProblemDetail for error responses.

const std = @import("std");
const router_mod = @import("router.zig");

pub const Route = router_mod.Route;
pub const Method = router_mod.Method;

/// Generate an OpenAPI 3.1 JSON spec from routes into the provided buffer.
pub fn generateSpec(
    buf: []u8,
    title: []const u8,
    version: []const u8,
    routes: []const Route,
) ![]const u8 {
    var pos: usize = 0;

    // Opening
    pos += copy(buf[pos..],
        \\{"openapi":"3.1.0","info":{"title":"
    );
    pos += copy(buf[pos..], title);
    pos += copy(buf[pos..],
        \\","version":"
    );
    pos += copy(buf[pos..], version);
    pos += copy(buf[pos..],
        \\"},"paths":{
    );

    // Paths
    var first_path = true;
    for (routes) |route| {
        if (!first_path) {
            if (pos < buf.len) {
                buf[pos] = ',';
                pos += 1;
            }
        }
        first_path = false;

        // "path":{"method":{...}}
        pos += copy(buf[pos..], "\"");
        pos += copy(buf[pos..], route.path);
        pos += copy(buf[pos..], "\":{\"");
        pos += copy(buf[pos..], methodLower(route.method));
        pos += copy(buf[pos..], "\":{");

        if (route.summary) |summary| {
            pos += copy(buf[pos..], "\"summary\":\"");
            pos += copy(buf[pos..], summary);
            pos += copy(buf[pos..], "\",");
        }

        // Responses including error schema ref
        pos += copy(buf[pos..], "\"responses\":{\"200\":{\"description\":\"OK\"},\"default\":{\"description\":\"Error\",\"content\":{\"application/problem+json\":{\"schema\":{\"$ref\":\"#/components/schemas/ProblemDetail\"}}}}}");
        pos += copy(buf[pos..], "}}");
    }

    // Close paths, add components
    pos += copy(buf[pos..], "},\"components\":{\"schemas\":{\"ProblemDetail\":{\"type\":\"object\",\"properties\":{\"type\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"},\"status\":{\"type\":\"integer\"},\"detail\":{\"type\":\"string\"},\"instance\":{\"type\":\"string\"},\"errors\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"field\":{\"type\":\"string\"},\"message\":{\"type\":\"string\"}}}}},\"required\":[\"type\",\"title\",\"status\",\"detail\"]}}}}");

    return buf[0..pos];
}

/// Generate JSON Schema type string for a Zig type at comptime.
pub fn jsonSchemaType(comptime T: type) []const u8 {
    const info = @typeInfo(T);
    return switch (info) {
        .bool => "\"type\":\"boolean\"",
        .int, .comptime_int => "\"type\":\"integer\"",
        .float, .comptime_float => "\"type\":\"number\"",
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) {
                return "\"type\":\"string\"";
            }
            return "\"type\":\"object\"";
        },
        .optional => |opt| jsonSchemaType(opt.child),
        .@"struct" => "\"type\":\"object\"",
        else => "\"type\":\"string\"",
    };
}

/// Generate a JSON Schema object for a struct type.
pub fn generateStructSchema(comptime T: type, buf: []u8) ![]const u8 {
    const info = @typeInfo(T);
    switch (info) {
        .@"struct" => |s| {
            var pos: usize = 0;
            pos += copy(buf[pos..], "{\"type\":\"object\",\"properties\":{");
            var first = true;
            inline for (s.fields) |field| {
                if (!first) {
                    if (pos < buf.len) {
                        buf[pos] = ',';
                        pos += 1;
                    }
                }
                first = false;
                pos += copy(buf[pos..], "\"");
                pos += copy(buf[pos..], field.name);
                pos += copy(buf[pos..], "\":{");
                pos += copy(buf[pos..], jsonSchemaType(field.type));
                pos += copy(buf[pos..], "}");
            }
            pos += copy(buf[pos..], "}}");
            return buf[0..pos];
        },
        else => return error.BufferTooShort,
    }
}

fn methodLower(method: Method) []const u8 {
    return switch (method) {
        .GET => "get",
        .POST => "post",
        .PUT => "put",
        .PATCH => "patch",
        .DELETE => "delete",
        .HEAD => "head",
        .OPTIONS => "options",
        .TRACE => "trace",
        .CONNECT => "connect",
    };
}

fn copy(dest: []u8, src: []const u8) usize {
    const n = @min(dest.len, src.len);
    @memcpy(dest[0..n], src[0..n]);
    return n;
}

/// Handler that serves the OpenAPI spec as JSON.
pub fn specHandler(comptime spec_json: []const u8) router_mod.HandlerFn {
    return &struct {
        fn handle(ctx: *router_mod.RequestContext) anyerror!void {
            try ctx.respondJson(200, spec_json);
        }
    }.handle;
}

/// Handler that serves a simple Swagger UI HTML page.
pub fn docsHandler(comptime spec_path: []const u8) router_mod.HandlerFn {
    return &struct {
        fn handle(ctx: *router_mod.RequestContext) anyerror!void {
            const html =
                \\<!DOCTYPE html><html><head><title>API Docs</title>
                \\<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css">
                \\</head><body><div id="swagger-ui"></div>
                \\<script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
                \\<script>SwaggerUIBundle({url:"
            ++ spec_path ++
                \\",dom_id:"#swagger-ui"})</script></body></html>
            ;
            const headers = [_]router_mod.Header{
                .{ .name = "Content-Type", .value = "text/html" },
            };
            try ctx.respond(200, &headers, html);
        }
    }.handle;
}

test "generateSpec: basic" {
    var buf: [4096]u8 = undefined;
    const routes = [_]Route{
        .{ .method = .GET, .path = "/api/users", .handler = &dummyHandler, .summary = "List users" },
        .{ .method = .POST, .path = "/api/users", .handler = &dummyHandler, .summary = "Create user" },
    };
    const spec = try generateSpec(&buf, "My API", "1.0.0", &routes);
    try std.testing.expect(std.mem.indexOf(u8, spec, "\"openapi\":\"3.1.0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, spec, "\"title\":\"My API\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, spec, "\"/api/users\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, spec, "\"get\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, spec, "\"post\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, spec, "\"List users\"") != null);
    // Check ProblemDetail schema
    try std.testing.expect(std.mem.indexOf(u8, spec, "ProblemDetail") != null);
}

test "generateSpec: empty routes" {
    var buf: [2048]u8 = undefined;
    const spec = try generateSpec(&buf, "Empty", "0.1.0", &.{});
    try std.testing.expect(std.mem.indexOf(u8, spec, "\"openapi\":\"3.1.0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, spec, "\"paths\":{}") != null);
}

test "methodLower" {
    try std.testing.expectEqualStrings("get", methodLower(.GET));
    try std.testing.expectEqualStrings("post", methodLower(.POST));
    try std.testing.expectEqualStrings("delete", methodLower(.DELETE));
}

test "jsonSchemaType" {
    try std.testing.expectEqualStrings("\"type\":\"string\"", jsonSchemaType([]const u8));
    try std.testing.expectEqualStrings("\"type\":\"integer\"", jsonSchemaType(i64));
    try std.testing.expectEqualStrings("\"type\":\"boolean\"", jsonSchemaType(bool));
    try std.testing.expectEqualStrings("\"type\":\"number\"", jsonSchemaType(f64));
}

test "generateStructSchema" {
    const User = struct {
        id: i64,
        name: []const u8,
        active: bool,
    };
    var buf: [1024]u8 = undefined;
    const schema = try generateStructSchema(User, &buf);
    try std.testing.expect(std.mem.indexOf(u8, schema, "\"type\":\"object\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "\"id\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "\"name\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "\"active\"") != null);
}

fn dummyHandler(_: *router_mod.RequestContext) anyerror!void {}
