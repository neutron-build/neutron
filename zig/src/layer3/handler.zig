// Layer 3: Typed handler wrapper — comptime extraction + serialization
//
// Wraps a user function `fn(*RequestContext, Input) anyerror!Output` into a
// standard `fn(*RequestContext) anyerror!void` handler. Parses request body
// JSON into Input type, calls the user function, serializes Output to JSON,
// and handles errors with RFC 7807.

const std = @import("std");
const http_server = @import("../layer2/http_server.zig");
const respond = @import("respond.zig");
const json_mod = @import("json.zig");
const error_mod = @import("error.zig");

pub const RequestContext = http_server.RequestContext;
pub const HandlerFn = *const fn (*RequestContext) anyerror!void;

/// Typed handler wrapper. Converts a typed function into a standard handler.
///
/// Usage:
///   const CreateUserInput = struct { name: []const u8, email: []const u8 };
///   const UserResponse = struct { id: i64, name: []const u8 };
///   fn createUser(ctx: *RequestContext, input: CreateUserInput) !UserResponse { ... }
///   const routes = .{ .handler = handler(CreateUserInput, UserResponse, createUser) };
pub fn handler(
    comptime Input: type,
    comptime Output: type,
    comptime func: fn (*RequestContext, Input) anyerror!Output,
) HandlerFn {
    return &struct {
        fn handle(ctx: *RequestContext) anyerror!void {
            // 1. Parse request body JSON into Input type
            const input = parseInput(Input, ctx) catch {
                var err_buf: [512]u8 = undefined;
                const err_json = error_mod.badRequest("Invalid request body").toJson(&err_buf) catch return;
                const headers = [_]http_server.Header{
                    .{ .name = "Content-Type", .value = "application/problem+json" },
                };
                ctx.respond(400, &headers, err_json) catch {};
                return;
            };

            // 2. Call the user function
            const output = func(ctx, input) catch |err| {
                if (!ctx.responded) {
                    var err_buf: [512]u8 = undefined;
                    const err_json = error_mod.internalError(@errorName(err)).toJson(&err_buf) catch return;
                    const headers = [_]http_server.Header{
                        .{ .name = "Content-Type", .value = "application/problem+json" },
                    };
                    ctx.respond(500, &headers, err_json) catch {};
                }
                return;
            };

            // 3. Serialize Output to JSON and write response
            if (!ctx.responded) {
                var json_buf: [4096]u8 = undefined;
                const json_str = respond.toJson(Output, output, &json_buf) catch {
                    ctx.respondError(500, "Failed to serialize response") catch {};
                    return;
                };
                ctx.respondJson(200, json_str) catch {};
            }
        }
    }.handle;
}

/// Parse the Input type from the request context.
/// For void Input, returns void immediately.
/// For struct Input, parses from request body JSON.
fn parseInput(comptime Input: type, ctx: *RequestContext) !Input {
    if (Input == void) return {};

    const body = ctx.request.body orelse return error.MissingBody;
    return json_mod.fromJson(Input, body);
}

// ── Tests ─────────────────────────────────────────────────────

test "handler: compiles with void input" {
    const Output = struct { status: []const u8 };
    const h = handler(void, Output, struct {
        fn handle(_: *RequestContext, _: void) anyerror!Output {
            return .{ .status = "ok" };
        }
    }.handle);
    try std.testing.expect(@intFromPtr(h) != 0);
}

test "handler: compiles with struct input" {
    const Input = struct { name: []const u8 };
    const Output = struct { id: i64, name: []const u8 };
    const h = handler(Input, Output, struct {
        fn handle(_: *RequestContext, input: Input) anyerror!Output {
            return .{ .id = 1, .name = input.name };
        }
    }.handle);
    try std.testing.expect(@intFromPtr(h) != 0);
}
