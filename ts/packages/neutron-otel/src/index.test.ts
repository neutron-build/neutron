import assert from "node:assert/strict";
import { describe, it, mock } from "node:test";
import { createOpenTelemetryHooks, mergeNeutronHooks } from "./index.js";

// ---------------------------------------------------------------------------
// Mock OTel API
// ---------------------------------------------------------------------------

interface MockSpan {
  attributes: Record<string, string | number | boolean>;
  exceptions: Error[];
  statusCode: number | null;
  statusMessage: string | undefined;
  ended: boolean;
  endTime: number | undefined;
  setAttribute(key: string, value: string | number | boolean): void;
  recordException(error: Error): void;
  setStatus(status: { code: number; message?: string }): void;
  end(endTime?: number): void;
}

function createMockSpan(): MockSpan {
  const span: MockSpan = {
    attributes: {},
    exceptions: [],
    statusCode: null,
    statusMessage: undefined,
    ended: false,
    endTime: undefined,
    setAttribute(key, value) {
      span.attributes[key] = value;
    },
    recordException(error) {
      span.exceptions.push(error);
    },
    setStatus(status) {
      span.statusCode = status.code;
      span.statusMessage = status.message;
    },
    end(endTime) {
      span.ended = true;
      span.endTime = endTime;
    },
  };
  return span;
}

function createMockOtelApi() {
  const spans: MockSpan[] = [];
  const api = {
    context: {
      active: () => ({ _ctx: "root" }),
    },
    trace: {
      getTracer: mock.fn((_name: string, _version?: string) => ({
        startSpan: mock.fn(
          (_name: string, _options?: Record<string, unknown>, _context?: unknown) => {
            const span = createMockSpan();
            // Apply initial attributes from options, matching real OTel behavior
            if (_options && typeof _options === "object" && _options.attributes) {
              const attrs = _options.attributes as Record<string, string | number | boolean>;
              for (const [k, v] of Object.entries(attrs)) {
                span.attributes[k] = v;
              }
            }
            spans.push(span);
            return span;
          }
        ),
      })),
      setSpan: (_context: unknown, span: MockSpan) => ({ _ctx: "with-span", span }),
    },
    propagation: {
      extract: (context: unknown, _carrier: unknown) => context,
    },
    SpanStatusCode: {
      ERROR: 2,
    },
  };
  return { api, spans };
}

// ---------------------------------------------------------------------------
// createOpenTelemetryHooks
// ---------------------------------------------------------------------------

describe("createOpenTelemetryHooks", () => {
  it("returns empty hooks when api is unavailable and import fails", async () => {
    const warnMessages: string[] = [];
    const hooks = await createOpenTelemetryHooks({
      api: undefined,
      logger: {
        warn: (msg: string) => warnMessages.push(msg),
      },
    });

    // The import of @opentelemetry/api will fail, so hooks should be empty
    // Since api is undefined it will try to dynamically import which will fail
    assert.equal(typeof hooks, "object");
  });

  it("creates a tracer with the provided service name", async () => {
    const { api } = createMockOtelApi();
    await createOpenTelemetryHooks({
      api: api as any,
      serviceName: "my-service",
      serviceVersion: "1.0.0",
    });

    assert.equal(api.trace.getTracer.mock.calls.length, 1);
    const [tracerName, tracerVersion] = api.trace.getTracer.mock.calls[0].arguments;
    assert.equal(tracerName, "my-service");
    assert.equal(tracerVersion, "1.0.0");
  });

  it("uses tracerName over serviceName when both provided", async () => {
    const { api } = createMockOtelApi();
    await createOpenTelemetryHooks({
      api: api as any,
      serviceName: "my-service",
      tracerName: "custom-tracer",
    });

    const [tracerName] = api.trace.getTracer.mock.calls[0].arguments;
    assert.equal(tracerName, "custom-tracer");
  });

  it("defaults tracer name to 'neutron'", async () => {
    const { api } = createMockOtelApi();
    await createOpenTelemetryHooks({ api: api as any });

    const [tracerName] = api.trace.getTracer.mock.calls[0].arguments;
    assert.equal(tracerName, "neutron");
  });

  it("creates and ends a request span via onRequestStart/onRequestEnd", async () => {
    const { api, spans } = createMockOtelApi();
    const hooks = await createOpenTelemetryHooks({
      api: api as any,
      serviceName: "test-svc",
    });

    const requestId = "req-1";
    hooks.onRequestStart!({
      requestId,
      method: "GET",
      url: "https://example.com/api",
      pathname: "/api",
      startedAt: 1000,
    });

    assert.equal(spans.length, 1);
    const span = spans[0];
    assert.equal(span.attributes["http.method"], "GET");
    assert.equal(span.attributes["url.path"], "/api");
    assert.equal(span.attributes["neutron.request_id"], "req-1");
    assert.equal(span.ended, false);

    hooks.onRequestEnd!({
      requestId,
      method: "GET",
      url: "https://example.com/api",
      pathname: "/api",
      startedAt: 1000,
      endedAt: 1050,
      durationMs: 50,
      status: 200,
      routeId: "route-1",
      routeMode: "app",
      cacheState: "miss",
    });

    assert.equal(span.ended, true);
    assert.equal(span.endTime, 1050);
    assert.equal(span.attributes["http.status_code"], 200);
    assert.equal(span.attributes["http.server_duration_ms"], 50);
    assert.equal(span.attributes["neutron.route_id"], "route-1");
    assert.equal(span.attributes["neutron.route_mode"], "app");
    assert.equal(span.attributes["neutron.cache_state"], "miss");
  });

  it("onRequestEnd is a no-op if onRequestStart was not called", async () => {
    const { api, spans } = createMockOtelApi();
    const hooks = await createOpenTelemetryHooks({ api: api as any });

    // Should not throw
    hooks.onRequestEnd!({
      requestId: "unknown",
      method: "GET",
      url: "https://example.com",
      pathname: "/",
      startedAt: 1000,
      endedAt: 1050,
      durationMs: 50,
      status: 200,
    });

    assert.equal(spans.length, 0);
  });

  it("creates loader spans as children of request spans", async () => {
    const { api, spans } = createMockOtelApi();
    const hooks = await createOpenTelemetryHooks({ api: api as any });

    hooks.onRequestStart!({
      requestId: "req-2",
      method: "GET",
      url: "https://example.com/page",
      pathname: "/page",
      startedAt: 1000,
    });

    hooks.onLoaderStart!({
      requestId: "req-2",
      method: "GET",
      pathname: "/page",
      routeId: "page-route",
      routePath: "/page",
      startedAt: 1010,
    });

    assert.equal(spans.length, 2);
    const loaderSpan = spans[1];
    assert.equal(loaderSpan.attributes["neutron.phase"], "loader");
    assert.equal(loaderSpan.attributes["neutron.route_id"], "page-route");

    hooks.onLoaderEnd!({
      requestId: "req-2",
      method: "GET",
      pathname: "/page",
      routeId: "page-route",
      routePath: "/page",
      startedAt: 1010,
      endedAt: 1020,
      durationMs: 10,
      outcome: "success",
      cacheStatus: "hit",
      responseStatus: 200,
    });

    assert.equal(loaderSpan.ended, true);
    assert.equal(loaderSpan.attributes["neutron.loader.outcome"], "success");
    assert.equal(loaderSpan.attributes["neutron.loader.cache_status"], "hit");
    assert.equal(loaderSpan.attributes["http.status_code"], 200);
  });

  it("creates action spans as children of request spans", async () => {
    const { api, spans } = createMockOtelApi();
    const hooks = await createOpenTelemetryHooks({ api: api as any });

    hooks.onRequestStart!({
      requestId: "req-3",
      method: "POST",
      url: "https://example.com/submit",
      pathname: "/submit",
      startedAt: 2000,
    });

    hooks.onActionStart!({
      requestId: "req-3",
      method: "POST",
      pathname: "/submit",
      routeId: "submit-route",
      routePath: "/submit",
      startedAt: 2010,
    });

    assert.equal(spans.length, 2);
    const actionSpan = spans[1];
    assert.equal(actionSpan.attributes["neutron.phase"], "action");

    hooks.onActionEnd!({
      requestId: "req-3",
      method: "POST",
      pathname: "/submit",
      routeId: "submit-route",
      routePath: "/submit",
      startedAt: 2010,
      endedAt: 2030,
      durationMs: 20,
      outcome: "success",
      responseStatus: 201,
    });

    assert.equal(actionSpan.ended, true);
    assert.equal(actionSpan.attributes["neutron.action.outcome"], "success");
    assert.equal(actionSpan.attributes["http.status_code"], 201);
  });

  it("records exceptions and error status on onError", async () => {
    const { api, spans } = createMockOtelApi();
    const hooks = await createOpenTelemetryHooks({ api: api as any });

    hooks.onRequestStart!({
      requestId: "req-4",
      method: "GET",
      url: "https://example.com/fail",
      pathname: "/fail",
      startedAt: 3000,
    });

    const error = new Error("Something went wrong");
    hooks.onError!({
      requestId: "req-4",
      method: "GET",
      pathname: "/fail",
      source: "loader",
      routeId: "fail-route",
      error,
    });

    const span = spans[0];
    assert.equal(span.exceptions.length, 1);
    assert.equal(span.exceptions[0], error);
    assert.equal(span.statusCode, 2); // ERROR
    assert.equal(span.statusMessage, "Something went wrong");
    assert.equal(span.attributes["neutron.error.source"], "loader");
    assert.equal(span.attributes["neutron.route_id"], "fail-route");
  });

  it("onError is a no-op when no matching request span exists", async () => {
    const { api } = createMockOtelApi();
    const hooks = await createOpenTelemetryHooks({ api: api as any });

    // Should not throw
    hooks.onError!({
      requestId: "nonexistent",
      method: "GET",
      pathname: "/",
      source: "request",
      error: new Error("test"),
    });
  });

  it("includes defaultAttributes on all spans", async () => {
    const { api, spans } = createMockOtelApi();
    const hooks = await createOpenTelemetryHooks({
      api: api as any,
      defaultAttributes: { "deployment.environment": "test" },
    });

    hooks.onRequestStart!({
      requestId: "req-5",
      method: "GET",
      url: "https://example.com",
      pathname: "/",
      startedAt: 4000,
    });

    const span = spans[0];
    assert.equal(span.attributes["deployment.environment"], "test");
    assert.equal(span.attributes["neutron.runtime"], "node");
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

  it("chains hooks from both base and extra", async () => {
    const calls: string[] = [];
    const base = {
      onRequestStart: () => { calls.push("base"); },
    };
    const extra = {
      onRequestStart: () => { calls.push("extra"); },
    };

    const merged = mergeNeutronHooks(base, extra)!;
    assert.ok(merged.onRequestStart);

    await merged.onRequestStart!({
      requestId: "r1",
      method: "GET",
      url: "https://example.com",
      pathname: "/",
      startedAt: 0,
    });

    assert.deepEqual(calls, ["base", "extra"]);
  });

  it("preserves hooks that only exist on one side", async () => {
    const calls: string[] = [];
    const base = {
      onRequestStart: () => { calls.push("start"); },
    };
    const extra = {
      onRequestEnd: () => { calls.push("end"); },
    };

    const merged = mergeNeutronHooks(base, extra)!;
    assert.ok(merged.onRequestStart);
    assert.ok(merged.onRequestEnd);

    await merged.onRequestStart!({
      requestId: "r1",
      method: "GET",
      url: "https://example.com",
      pathname: "/",
      startedAt: 0,
    });
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

    assert.deepEqual(calls, ["start", "end"]);
  });
});
