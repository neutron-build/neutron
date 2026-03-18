import assert from "node:assert/strict";
import { describe, it, mock } from "node:test";
import {
  createRequestContextMiddleware,
  createHealthcheckMiddleware,
  createJsonLoggingHooks,
  getRequestIdFromContext,
  getTraceIdFromContext,
  mergeNeutronHooks,
} from "./index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const OK_RESPONSE = () => new Response("ok", { status: 200 });

// ---------------------------------------------------------------------------
// createRequestContextMiddleware
// ---------------------------------------------------------------------------

describe("createRequestContextMiddleware", () => {
  it("generates a request ID when none is provided in headers", async () => {
    const mw = createRequestContextMiddleware();
    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(typeof ctx["requestId"], "string");
    assert.ok((ctx["requestId"] as string).length > 0);
    // Should also set the response header
    assert.equal(response.headers.get("x-request-id"), ctx["requestId"] as string);
  });

  it("uses existing request ID from headers", async () => {
    const mw = createRequestContextMiddleware();
    const ctx: Record<string, unknown> = {};
    await mw(
      new Request("https://example.com", {
        headers: { "x-request-id": "existing-id-123" },
      }),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(ctx["requestId"], "existing-id-123");
  });

  it("supports custom request ID header", async () => {
    const mw = createRequestContextMiddleware({ requestIdHeader: "X-Trace-ID" });
    const ctx: Record<string, unknown> = {};
    await mw(
      new Request("https://example.com", {
        headers: { "X-Trace-ID": "custom-trace-456" },
      }),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(ctx["requestId"], "custom-trace-456");
  });

  it("supports custom response header", async () => {
    const mw = createRequestContextMiddleware({ responseHeader: "X-Req-ID" });
    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.ok(response.headers.get("X-Req-ID"));
    assert.equal(response.headers.get("x-request-id"), null);
  });

  it("supports custom context keys", async () => {
    const mw = createRequestContextMiddleware({
      requestIdContextKey: "myReqId",
      traceIdContextKey: "myTraceId",
    });
    const traceId = "0af7651916cd43dd8448eb211c80319c";
    const ctx: Record<string, unknown> = {};
    await mw(
      new Request("https://example.com", {
        headers: {
          traceparent: `00-${traceId}-b7ad6b7169203331-01`,
        },
      }),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(typeof ctx["myReqId"], "string");
    assert.equal(ctx["myTraceId"], traceId);
    // Default keys should not be set
    assert.equal(ctx["requestId"], undefined);
    assert.equal(ctx["traceId"], undefined);
  });

  it("extracts trace ID from traceparent header", async () => {
    const mw = createRequestContextMiddleware();
    const traceId = "0af7651916cd43dd8448eb211c80319c";
    const ctx: Record<string, unknown> = {};
    await mw(
      new Request("https://example.com", {
        headers: {
          traceparent: `00-${traceId}-b7ad6b7169203331-01`,
        },
      }),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(ctx["traceId"], traceId);
  });

  it("does not set traceId when traceparent is absent", async () => {
    const mw = createRequestContextMiddleware();
    const ctx: Record<string, unknown> = {};
    await mw(
      new Request("https://example.com"),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(ctx["traceId"], undefined);
  });

  it("does not set traceId when traceparent is malformed", async () => {
    const mw = createRequestContextMiddleware();
    const ctx: Record<string, unknown> = {};
    await mw(
      new Request("https://example.com", {
        headers: { traceparent: "invalid" },
      }),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(ctx["traceId"], undefined);
  });

  it("does not overwrite response header if already set by next()", async () => {
    const mw = createRequestContextMiddleware();
    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () => new Response("ok", { headers: { "x-request-id": "already-set" } })
    );

    assert.equal(response.headers.get("x-request-id"), "already-set");
  });
});

// ---------------------------------------------------------------------------
// createHealthcheckMiddleware
// ---------------------------------------------------------------------------

describe("createHealthcheckMiddleware", () => {
  it("responds to /healthz with healthy status", async () => {
    const mw = createHealthcheckMiddleware();
    const response = await mw(
      new Request("https://example.com/healthz"),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.ok, true);
    assert.equal(body.status, "healthy");
    assert.equal(body.service, "neutron-app");
    assert.equal(response.headers.get("content-type"), "application/json; charset=utf-8");
    assert.equal(response.headers.get("cache-control"), "no-store");
  });

  it("responds to /readyz with ready status", async () => {
    const mw = createHealthcheckMiddleware();
    const response = await mw(
      new Request("https://example.com/readyz"),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.ok, true);
    assert.equal(body.status, "ready");
  });

  it("responds with 503 when ready() returns false", async () => {
    const mw = createHealthcheckMiddleware({
      ready: () => false,
    });
    const response = await mw(
      new Request("https://example.com/readyz"),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 503);
    const body = await response.json();
    assert.equal(body.ok, false);
    assert.equal(body.status, "not-ready");
  });

  it("responds with 503 when async ready() returns false", async () => {
    const mw = createHealthcheckMiddleware({
      ready: async () => false,
    });
    const response = await mw(
      new Request("https://example.com/readyz"),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 503);
  });

  it("supports custom health and ready paths", async () => {
    const mw = createHealthcheckMiddleware({
      healthPath: "/health",
      readyPath: "/ready",
    });

    const healthResponse = await mw(
      new Request("https://example.com/health"),
      {},
      async () => OK_RESPONSE()
    );
    assert.equal(healthResponse.status, 200);
    const healthBody = await healthResponse.json();
    assert.equal(healthBody.status, "healthy");

    const readyResponse = await mw(
      new Request("https://example.com/ready"),
      {},
      async () => OK_RESPONSE()
    );
    assert.equal(readyResponse.status, 200);

    // Default paths should pass through
    const passthrough = await mw(
      new Request("https://example.com/healthz"),
      {},
      async () => OK_RESPONSE()
    );
    assert.equal(await passthrough.text(), "ok");
  });

  it("supports custom service name", async () => {
    const mw = createHealthcheckMiddleware({ service: "my-api" });
    const response = await mw(
      new Request("https://example.com/healthz"),
      {},
      async () => OK_RESPONSE()
    );

    const body = await response.json();
    assert.equal(body.service, "my-api");
  });

  it("passes through non-health requests to next()", async () => {
    const mw = createHealthcheckMiddleware();
    const response = await mw(
      new Request("https://example.com/api/users"),
      {},
      async () => new Response("users-data", { status: 200 })
    );

    assert.equal(response.status, 200);
    assert.equal(await response.text(), "users-data");
  });
});

// ---------------------------------------------------------------------------
// createJsonLoggingHooks
// ---------------------------------------------------------------------------

describe("createJsonLoggingHooks", () => {
  it("logs structured JSON on request end", () => {
    const lines: string[] = [];
    const hooks = createJsonLoggingHooks({
      logger: {
        info: (line: string) => lines.push(line),
        error: () => {},
      },
    });

    hooks.onRequestEnd!({
      requestId: "req-1",
      method: "GET",
      url: "https://example.com/page",
      pathname: "/page",
      startedAt: 1000,
      endedAt: 1050,
      durationMs: 50,
      status: 200,
      routeId: "page-route",
      routePath: "/page",
      routeMode: "app",
      cacheState: "miss",
    });

    assert.equal(lines.length, 1);
    const parsed = JSON.parse(lines[0]);
    assert.equal(parsed.level, "info");
    assert.equal(parsed.event, "request.end");
    assert.equal(parsed.requestId, "req-1");
    assert.equal(parsed.method, "GET");
    assert.equal(parsed.pathname, "/page");
    assert.equal(parsed.status, 200);
    assert.equal(parsed.durationMs, 50);
    assert.equal(parsed.routeId, "page-route");
    assert.equal(parsed.cacheState, "miss");
    assert.ok(parsed.timestamp);
  });

  it("logs structured JSON on error", () => {
    const errorLines: string[] = [];
    const hooks = createJsonLoggingHooks({
      logger: {
        info: () => {},
        error: (line: string) => errorLines.push(line),
      },
    });

    const err = new Error("Something broke");
    hooks.onError!({
      requestId: "req-2",
      method: "POST",
      pathname: "/submit",
      source: "action",
      routeId: "submit-route",
      routePath: "/submit",
      error: err,
    });

    assert.equal(errorLines.length, 1);
    const parsed = JSON.parse(errorLines[0]);
    assert.equal(parsed.level, "error");
    assert.equal(parsed.event, "request.error");
    assert.equal(parsed.requestId, "req-2");
    assert.equal(parsed.source, "action");
    assert.equal(parsed.error, "Something broke");
    assert.ok(parsed.timestamp);
  });

  it("includes baseFields in all log entries", () => {
    const lines: string[] = [];
    const errorLines: string[] = [];
    const hooks = createJsonLoggingHooks({
      logger: {
        info: (line: string) => lines.push(line),
        error: (line: string) => errorLines.push(line),
      },
      baseFields: { service: "my-app", env: "test" },
    });

    hooks.onRequestEnd!({
      requestId: "req-3",
      method: "GET",
      url: "https://example.com",
      pathname: "/",
      startedAt: 1000,
      endedAt: 1010,
      durationMs: 10,
      status: 200,
    });

    hooks.onError!({
      requestId: "req-3",
      method: "GET",
      pathname: "/",
      source: "request",
      error: new Error("test"),
    });

    const infoParsed = JSON.parse(lines[0]);
    assert.equal(infoParsed.service, "my-app");
    assert.equal(infoParsed.env, "test");

    const errorParsed = JSON.parse(errorLines[0]);
    assert.equal(errorParsed.service, "my-app");
    assert.equal(errorParsed.env, "test");
  });
});

// ---------------------------------------------------------------------------
// getRequestIdFromContext / getTraceIdFromContext
// ---------------------------------------------------------------------------

describe("getRequestIdFromContext", () => {
  it("returns request ID from default key", () => {
    assert.equal(getRequestIdFromContext({ requestId: "abc-123" }), "abc-123");
  });

  it("returns null when key is missing", () => {
    assert.equal(getRequestIdFromContext({}), null);
  });

  it("returns null when value is not a string", () => {
    assert.equal(getRequestIdFromContext({ requestId: 42 }), null);
  });

  it("supports custom context key", () => {
    assert.equal(getRequestIdFromContext({ myId: "xyz" }, "myId"), "xyz");
  });
});

describe("getTraceIdFromContext", () => {
  it("returns trace ID from default key", () => {
    assert.equal(getTraceIdFromContext({ traceId: "trace-abc" }), "trace-abc");
  });

  it("returns null when key is missing", () => {
    assert.equal(getTraceIdFromContext({}), null);
  });

  it("returns null when value is not a string", () => {
    assert.equal(getTraceIdFromContext({ traceId: 123 }), null);
  });

  it("supports custom context key", () => {
    assert.equal(getTraceIdFromContext({ myTrace: "t1" }, "myTrace"), "t1");
  });
});

// ---------------------------------------------------------------------------
// mergeNeutronHooks
// ---------------------------------------------------------------------------

describe("mergeNeutronHooks", () => {
  it("returns undefined when both are undefined", () => {
    assert.equal(mergeNeutronHooks(undefined, undefined), undefined);
  });

  it("returns extra when base is undefined", () => {
    const extra = { onRequestStart: () => {} };
    assert.equal(mergeNeutronHooks(undefined, extra), extra);
  });

  it("returns base when extra is undefined", () => {
    const base = { onRequestEnd: () => {} };
    assert.equal(mergeNeutronHooks(base, undefined), base);
  });

  it("chains hooks from both base and extra in order", async () => {
    const calls: string[] = [];
    const base = {
      onRequestEnd: () => { calls.push("base-end"); },
      onError: () => { calls.push("base-error"); },
    };
    const extra = {
      onRequestEnd: () => { calls.push("extra-end"); },
    };

    const merged = mergeNeutronHooks(base, extra)!;

    await merged.onRequestEnd!({
      requestId: "r1",
      method: "GET",
      url: "https://example.com",
      pathname: "/",
      startedAt: 0,
      endedAt: 10,
      durationMs: 10,
      status: 200,
    });

    assert.deepEqual(calls, ["base-end", "extra-end"]);

    // onError should still work (only base has it)
    await merged.onError!({
      requestId: "r1",
      method: "GET",
      pathname: "/",
      source: "request",
      error: new Error("test"),
    });

    assert.deepEqual(calls, ["base-end", "extra-end", "base-error"]);
  });
});
