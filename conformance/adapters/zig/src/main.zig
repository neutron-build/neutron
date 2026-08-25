//! Canonical Neutron conformance app (Zig SDK).
//!
//! Boots a Neutron Zig server with NO database so the cross-SDK conformance
//! runner can assert FRAMEWORK_CONTRACT.md against it. Mirrors the
//! Go/Rust/Python/Elixir conformance apps endpoint-for-endpoint:
//!
//!     GET  /health                  §7 health shape {status, nucleus, version}
//!     GET  /openapi.json            §4 OpenAPI 3.1 document
//!     GET  /docs                    §4 interactive docs
//!     GET  /api/items               200 list (compression / request-id probe)
//!     POST /api/items               422 validation error (RFC 7807 + errors[])
//!     GET  /errors/{bad-request,…}  forced standard §2 errors
//!
//! No NEUTRON_DATABASE_URL is set, so /health reports
//! `"nucleus": "unconfigured"` — which §7 calls out as "not an error".
//! Listen address comes from NEUTRON_HOST/NEUTRON_PORT, so the runner can
//! pin an ephemeral port.
//!
//! The middleware is the SDK's `default` stack, which applies the §5 order:
//! Request ID → Logging → Recovery → CORS → Compression → RateLimit → Auth →
//! Timeout → OTel.

const std = @import("std");
const neutron = @import("neutron");

const RequestContext = neutron.http_server.RequestContext;
const app_error = neutron.app_error;

const APP_VERSION = "9.9.9";

// ── Handlers ────────────────────────────────────────────────────────────────

fn healthHandler(ctx: *RequestContext) anyerror!void {
    var buf: [128]u8 = undefined;
    const body = try neutron.app.healthJson(.unconfigured, APP_VERSION, &buf);
    try ctx.respondJson(200, body);
}

fn openapiHandler(ctx: *RequestContext) anyerror!void {
    var buf: [8192]u8 = undefined;
    const spec = try neutron.openapi.generateSpec(&buf, "Neutron Conformance API", APP_VERSION, Routes.getRoutes());
    try ctx.respondJson(200, spec);
}

// A list big enough and compressible enough for the gzip probe, and a plain
// 200 for the request-id probe. respondCompressedJson negotiates
// Content-Encoding + Vary from the request's Accept-Encoding.
fn listItems(ctx: *RequestContext) anyerror!void {
    var body_buf: [8192]u8 = undefined;
    var len: usize = 0;
    const open = std.fmt.bufPrint(body_buf[len..], "[", .{}) catch return error.BufferTooShort;
    len += open.len;
    var i: usize = 1;
    while (i <= 50) : (i += 1) {
        const sep: []const u8 = if (i > 1) "," else "";
        const item = std.fmt.bufPrint(body_buf[len..], "{s}{{\"id\":{d},\"name\":\"conformance-item-{d}\",\"price\":{d}}}", .{ sep, i, i, i }) catch return error.BufferTooShort;
        len += item.len;
    }
    const close = std.fmt.bufPrint(body_buf[len..], "]", .{}) catch return error.BufferTooShort;
    len += close.len;
    try neutron.compress.respondCompressedJson(ctx, 200, body_buf[0..len], 860);
}

// §2 validation: a missing/blank name or a negative price must produce RFC
// 7807 with `errors[]`. A body that is not JSON at all is a 400, not a 422.
const NewItem = struct {
    name: ?[]const u8 = null,
    price: ?f64 = null,
};

fn createItem(ctx: *RequestContext) anyerror!void {
    const body = ctx.request.body orelse {
        try app_error.sendProblem(ctx, app_error.badRequest("body must be JSON"));
        return;
    };
    const input = neutron.json.fromJson(NewItem, body) catch {
        try app_error.sendProblem(ctx, app_error.badRequest("body must be JSON"));
        return;
    };

    var price_val_buf: [32]u8 = undefined;
    var field_errors: [2]app_error.FieldError = undefined;
    var n_errors: usize = 0;

    const name_ok = if (input.name) |nm| std.mem.trim(u8, nm, " ").len > 0 else false;
    if (!name_ok) {
        field_errors[n_errors] = .{ .field = "name", .message = "must not be blank" };
        n_errors += 1;
    }
    const price_ok = if (input.price) |p| p >= 0 else false;
    if (!price_ok) {
        const val = if (input.price) |p|
            (std.fmt.bufPrint(&price_val_buf, "{d}", .{p}) catch "")
        else
            "null";
        field_errors[n_errors] = .{ .field = "price", .message = "must be a number >= 0", .value = val };
        n_errors += 1;
    }

    if (n_errors > 0) {
        try app_error.sendProblem(ctx, app_error.validationWithErrors(
            "Request body failed validation",
            field_errors[0..n_errors],
        ));
        return;
    }

    var out_buf: [512]u8 = undefined;
    const out = try std.fmt.bufPrint(&out_buf, "{{\"id\":1,\"name\":\"{s}\",\"price\":{d}}}", .{ input.name.?, input.price.? });
    try ctx.respondJson(201, out);
}

// One route per §2 standard error code, via the {code} path parameter.
fn forcedError(ctx: *RequestContext) anyerror!void {
    const code = neutron.router.extractParam("/errors/{code}", ctx.path, "code") orelse "";
    const eql = std.mem.eql;
    const err = if (eql(u8, code, "bad-request"))
        app_error.badRequest("forced bad request")
    else if (eql(u8, code, "unauthorized"))
        app_error.unauthorized("forced unauthorized")
    else if (eql(u8, code, "forbidden"))
        app_error.forbidden("forced forbidden")
    else if (eql(u8, code, "not-found"))
        app_error.notFound("forced not found")
    else if (eql(u8, code, "conflict"))
        app_error.conflict("forced conflict")
    else if (eql(u8, code, "rate-limited"))
        app_error.rateLimited("forced rate limited")
    else if (eql(u8, code, "internal"))
        app_error.internalError("forced internal error")
    else
        app_error.notFound("forced not found");
    try app_error.sendProblem(ctx, err);
}

// ── App wiring ──────────────────────────────────────────────────────────────

const routes = [_]neutron.Route{
    .{ .method = .GET, .path = "/health", .handler = &healthHandler, .summary = "Health" },
    .{ .method = .GET, .path = "/openapi.json", .handler = &openapiHandler, .summary = "OpenAPI spec" },
    .{ .method = .GET, .path = "/docs", .handler = neutron.openapi.docsHandler("/openapi.json"), .summary = "Interactive docs" },
    .{ .method = .GET, .path = "/api/items", .handler = &listItems, .summary = "List items" },
    .{ .method = .POST, .path = "/api/items", .handler = &createItem, .summary = "Create item" },
    .{ .method = .GET, .path = "/errors/{code}", .handler = &forcedError, .summary = "Forced standard error" },
};

const Routes = neutron.router.Router(&routes);
const ConformanceApp = neutron.app.App(Routes, neutron.middleware.default(.{}));

pub fn main() !void {
    const cfg = neutron.config.Config.fromEnv("NEUTRON");
    var app = ConformanceApp.init(std.heap.page_allocator, cfg);
    try app.run();
}
