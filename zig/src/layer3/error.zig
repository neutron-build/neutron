// Layer 3: RFC 7807 Problem Details error handling
//
// All errors are serialized as JSON per RFC 7807.
// Zero-alloc — writes into caller-provided buffers.
// Supports validation error arrays for field-level errors.

const std = @import("std");

/// Individual field validation error.
pub const FieldError = struct {
    field: []const u8,
    message: []const u8,
};

pub const AppError = struct {
    status: u16,
    code: []const u8,
    title: []const u8,
    detail: []const u8,
    instance: ?[]const u8 = null,
    errors: ?[]const FieldError = null,

    /// Serialize to RFC 7807 JSON into the provided buffer.
    pub fn toJson(self: AppError, buf: []u8) ![]const u8 {
        var stream = std.io.fixedBufferStream(buf);
        const writer = stream.writer();

        writer.writeAll("{\"type\":\"https://neutron.dev/errors/") catch return error.BufferTooShort;
        writer.writeAll(self.code) catch return error.BufferTooShort;
        writer.writeAll("\",\"title\":\"") catch return error.BufferTooShort;
        writer.writeAll(self.title) catch return error.BufferTooShort;
        writer.writeAll("\",\"status\":") catch return error.BufferTooShort;
        writer.print("{d}", .{self.status}) catch return error.BufferTooShort;
        writer.writeAll(",\"detail\":\"") catch return error.BufferTooShort;
        writer.writeAll(self.detail) catch return error.BufferTooShort;
        writer.writeAll("\"") catch return error.BufferTooShort;

        if (self.instance) |inst| {
            writer.writeAll(",\"instance\":\"") catch return error.BufferTooShort;
            writer.writeAll(inst) catch return error.BufferTooShort;
            writer.writeAll("\"") catch return error.BufferTooShort;
        }

        if (self.errors) |field_errors| {
            writer.writeAll(",\"errors\":[") catch return error.BufferTooShort;
            for (field_errors, 0..) |fe, i| {
                if (i > 0) writer.writeAll(",") catch return error.BufferTooShort;
                writer.writeAll("{\"field\":\"") catch return error.BufferTooShort;
                writer.writeAll(fe.field) catch return error.BufferTooShort;
                writer.writeAll("\",\"message\":\"") catch return error.BufferTooShort;
                writer.writeAll(fe.message) catch return error.BufferTooShort;
                writer.writeAll("\"}") catch return error.BufferTooShort;
            }
            writer.writeAll("]") catch return error.BufferTooShort;
        }

        writer.writeAll("}") catch return error.BufferTooShort;

        return stream.getWritten();
    }
};

// Standard error constructors per FRAMEWORK_CONTRACT.md
pub fn badRequest(detail: []const u8) AppError {
    return .{ .status = 400, .code = "bad-request", .title = "Bad Request", .detail = detail };
}

pub fn unauthorized(detail: []const u8) AppError {
    return .{ .status = 401, .code = "unauthorized", .title = "Unauthorized", .detail = detail };
}

pub fn forbidden(detail: []const u8) AppError {
    return .{ .status = 403, .code = "forbidden", .title = "Forbidden", .detail = detail };
}

pub fn notFound(detail: []const u8) AppError {
    return .{ .status = 404, .code = "not-found", .title = "Not Found", .detail = detail };
}

pub fn conflict(detail: []const u8) AppError {
    return .{ .status = 409, .code = "conflict", .title = "Conflict", .detail = detail };
}

pub fn validation(detail: []const u8) AppError {
    return .{ .status = 422, .code = "validation", .title = "Validation Failed", .detail = detail };
}

pub fn validationWithErrors(detail: []const u8, field_errors: []const FieldError) AppError {
    return .{ .status = 422, .code = "validation", .title = "Validation Failed", .detail = detail, .errors = field_errors };
}

pub fn rateLimited(detail: []const u8) AppError {
    return .{ .status = 429, .code = "rate-limited", .title = "Rate Limited", .detail = detail };
}

pub fn internalError(detail: []const u8) AppError {
    return .{ .status = 500, .code = "internal", .title = "Internal Server Error", .detail = detail };
}

test "RFC 7807: badRequest" {
    var buf: [512]u8 = undefined;
    const err = badRequest("missing field 'name'");
    const json = try err.toJson(&buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":400") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "bad-request") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "missing field 'name'") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "https://neutron.dev/errors/") != null);
}

test "RFC 7807: notFound with instance" {
    var buf: [512]u8 = undefined;
    var err = notFound("User not found");
    err.instance = "/api/users/42";
    const json = try err.toJson(&buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":404") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "/api/users/42") != null);
}

test "RFC 7807: validation with errors array" {
    var buf: [1024]u8 = undefined;
    const field_errors = [_]FieldError{
        .{ .field = "email", .message = "must be a valid email address" },
        .{ .field = "name", .message = "is required" },
    };
    const err = validationWithErrors("Request body failed validation", &field_errors);
    const json = try err.toJson(&buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":422") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"errors\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"field\":\"email\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"field\":\"name\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "must be a valid email address") != null);
}

test "all error constructors" {
    const errors_list = [_]AppError{
        badRequest("a"),
        unauthorized("b"),
        forbidden("c"),
        notFound("d"),
        conflict("e"),
        validation("f"),
        rateLimited("g"),
        internalError("h"),
    };
    const statuses = [_]u16{ 400, 401, 403, 404, 409, 422, 429, 500 };
    for (errors_list, statuses) |err, expected_status| {
        try std.testing.expectEqual(expected_status, err.status);
    }
}
