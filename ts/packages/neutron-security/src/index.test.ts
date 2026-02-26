import assert from "node:assert/strict";
import test from "node:test";
import {
  createCsrfMiddleware,
  createRateLimitMiddleware,
  resolveClientIp,
} from "./index.js";

test("resolveClientIp does not trust forwarded headers by default", () => {
  const request = new Request("https://example.com", {
    headers: {
      "x-forwarded-for": "203.0.113.9",
    },
  });

  assert.equal(resolveClientIp(request), null);
});

test("resolveClientIp returns first forwarded IP when trustProxy is enabled", () => {
  const request = new Request("https://example.com", {
    headers: {
      "x-forwarded-for": "203.0.113.9, 198.51.100.7",
    },
  });

  assert.equal(resolveClientIp(request, { trustProxy: true }), "203.0.113.9");
});

test("createRateLimitMiddleware denies once bucket capacity is exceeded", async () => {
  const middleware = createRateLimitMiddleware({
    capacity: 2,
    refillPerSecond: 0.0001,
    key: () => "shared-test-key",
  });

  const request = new Request("https://example.com/api");
  const context = {};

  const first = await middleware(request, context, async () => new Response("ok"));
  const second = await middleware(request, context, async () => new Response("ok"));
  const denied = await middleware(request, context, async () => new Response("ok"));

  assert.equal(first.status, 200);
  assert.equal(second.status, 200);
  assert.equal(denied.status, 429);
  assert.equal(denied.headers.get("RateLimit-Limit"), "2");
  assert.equal(denied.headers.get("RateLimit-Remaining"), "0");
});

test("createCsrfMiddleware sets CSRF cookie on safe method requests", async () => {
  const middleware = createCsrfMiddleware();
  const request = new Request("https://example.com/account", { method: "GET" });
  const context: Record<string, unknown> = {};

  const response = await middleware(request, context, async () => new Response("ok"));

  assert.equal(response.status, 200);
  const setCookie = response.headers.get("set-cookie") || "";
  assert.equal(setCookie.includes("__neutron_csrf="), true);
  assert.equal(typeof context.csrfToken, "string");
  assert.equal((context.csrfToken as string).length > 0, true);
});

test("createCsrfMiddleware rejects unsafe methods without a matching token", async () => {
  const middleware = createCsrfMiddleware();
  const request = new Request("https://example.com/account", {
    method: "POST",
  });

  const response = await middleware(request, {}, async () => new Response("ok"));
  assert.equal(response.status, 403);
  assert.equal(await response.text(), "Invalid CSRF token");
});

test("createCsrfMiddleware accepts unsafe methods with matching cookie and header token", async () => {
  const middleware = createCsrfMiddleware();
  const token = "csrf-token-123";
  const request = new Request("https://example.com/account", {
    method: "POST",
    headers: {
      cookie: `__neutron_csrf=${token}`,
      "x-csrf-token": token,
    },
  });

  const response = await middleware(request, {}, async () => new Response("ok"));
  assert.equal(response.status, 200);
});
