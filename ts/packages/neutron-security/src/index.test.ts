import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  createCspNonceMiddleware,
  getCspNonceFromContext,
  createCsrfMiddleware,
  getCsrfTokenFromContext,
  resolveClientIp,
  createRateLimitMiddleware,
  resolveSecureCookieOptions,
} from "./index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const OK_RESPONSE = () => new Response("ok", { status: 200 });

// ---------------------------------------------------------------------------
// resolveClientIp
// ---------------------------------------------------------------------------

describe("resolveClientIp", () => {
  it("returns null when trustProxy is false (default)", () => {
    const request = new Request("https://example.com", {
      headers: { "x-forwarded-for": "203.0.113.9" },
    });
    assert.equal(resolveClientIp(request), null);
  });

  it("returns first forwarded IP when trustProxy is enabled", () => {
    const request = new Request("https://example.com", {
      headers: { "x-forwarded-for": "203.0.113.9, 198.51.100.7" },
    });
    assert.equal(resolveClientIp(request, { trustProxy: true }), "203.0.113.9");
  });

  it("prefers cf-connecting-ip over other headers", () => {
    const request = new Request("https://example.com", {
      headers: {
        "cf-connecting-ip": "1.2.3.4",
        "x-real-ip": "5.6.7.8",
        "x-forwarded-for": "9.10.11.12",
      },
    });
    assert.equal(resolveClientIp(request, { trustProxy: true }), "1.2.3.4");
  });

  it("falls back to x-real-ip when cf-connecting-ip is absent", () => {
    const request = new Request("https://example.com", {
      headers: {
        "x-real-ip": "5.6.7.8",
        "x-forwarded-for": "9.10.11.12",
      },
    });
    assert.equal(resolveClientIp(request, { trustProxy: true }), "5.6.7.8");
  });

  it("respects maxForwardedIps limit", () => {
    const request = new Request("https://example.com", {
      headers: {
        "x-forwarded-for": "1.1.1.1, 2.2.2.2, 3.3.3.3",
      },
    });
    const ip = resolveClientIp(request, { trustProxy: true, maxForwardedIps: 1 });
    assert.equal(ip, "1.1.1.1");
  });

  it("supports custom forwarded header", () => {
    const request = new Request("https://example.com", {
      headers: { "x-custom-forwarded": "10.0.0.1" },
    });
    const ip = resolveClientIp(request, {
      trustProxy: true,
      forwardedHeader: "x-custom-forwarded",
    });
    assert.equal(ip, "10.0.0.1");
  });

  it("returns null when no forwarded headers are present", () => {
    const request = new Request("https://example.com");
    assert.equal(resolveClientIp(request, { trustProxy: true }), null);
  });
});

// ---------------------------------------------------------------------------
// createCspNonceMiddleware
// ---------------------------------------------------------------------------

describe("createCspNonceMiddleware", () => {
  it("generates a nonce and sets default CSP header", async () => {
    const mw = createCspNonceMiddleware();
    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () => OK_RESPONSE()
    );

    const nonce = ctx["cspNonce"] as string;
    assert.ok(nonce);
    assert.ok(nonce.length > 0);

    const csp = response.headers.get("Content-Security-Policy")!;
    assert.ok(csp.includes(`'nonce-${nonce}'`));
    assert.ok(csp.includes("default-src 'self'"));
    assert.ok(csp.includes("object-src 'none'"));
  });

  it("uses a custom contextKey", async () => {
    const mw = createCspNonceMiddleware({ contextKey: "myNonce" });
    const ctx: Record<string, unknown> = {};
    await mw(new Request("https://example.com"), ctx, async () => OK_RESPONSE());

    assert.ok(ctx["myNonce"]);
    assert.equal(ctx["cspNonce"], undefined);
  });

  it("uses a custom CSP header name", async () => {
    const mw = createCspNonceMiddleware({
      headerName: "Content-Security-Policy-Report-Only",
    });
    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.ok(response.headers.get("Content-Security-Policy-Report-Only"));
    assert.equal(response.headers.get("Content-Security-Policy"), null);
  });

  it("supports a static string policy with {{nonce}} placeholder", async () => {
    const mw = createCspNonceMiddleware({
      policy: "script-src 'nonce-{{nonce}}'",
    });
    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () => OK_RESPONSE()
    );

    const nonce = ctx["cspNonce"] as string;
    assert.equal(
      response.headers.get("Content-Security-Policy"),
      `script-src 'nonce-${nonce}'`
    );
  });

  it("supports a function policy", async () => {
    const mw = createCspNonceMiddleware({
      policy: ({ nonce }) => `script-src 'nonce-${nonce}' 'self'`,
    });
    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () => OK_RESPONSE()
    );

    const nonce = ctx["cspNonce"] as string;
    assert.equal(
      response.headers.get("Content-Security-Policy"),
      `script-src 'nonce-${nonce}' 'self'`
    );
  });

  it("does not overwrite CSP header if already set by next()", async () => {
    const mw = createCspNonceMiddleware();
    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () =>
        new Response("ok", {
          headers: { "Content-Security-Policy": "already-set" },
        })
    );

    assert.equal(response.headers.get("Content-Security-Policy"), "already-set");
  });
});

describe("getCspNonceFromContext", () => {
  it("returns nonce from default key", () => {
    assert.equal(getCspNonceFromContext({ cspNonce: "abc123" }), "abc123");
  });

  it("returns null when key is missing", () => {
    assert.equal(getCspNonceFromContext({}), null);
  });

  it("returns null when value is not a string", () => {
    assert.equal(getCspNonceFromContext({ cspNonce: 42 }), null);
  });

  it("supports custom key", () => {
    assert.equal(getCspNonceFromContext({ myKey: "n1" }, "myKey"), "n1");
  });
});

// ---------------------------------------------------------------------------
// createCsrfMiddleware
// ---------------------------------------------------------------------------

describe("createCsrfMiddleware", () => {
  it("sets CSRF cookie on safe GET request", async () => {
    const mw = createCsrfMiddleware();
    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com/account", { method: "GET" }),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 200);
    const setCookie = response.headers.get("set-cookie") || "";
    assert.ok(setCookie.includes("__neutron_csrf="));
    assert.equal(typeof ctx["csrfToken"], "string");
    assert.ok((ctx["csrfToken"] as string).length > 0);
  });

  it("allows HEAD and OPTIONS as safe methods by default", async () => {
    const mw = createCsrfMiddleware();

    for (const method of ["HEAD", "OPTIONS"]) {
      const ctx: Record<string, unknown> = {};
      const response = await mw(
        new Request("https://example.com", { method }),
        ctx,
        async () => OK_RESPONSE()
      );
      assert.equal(response.status, 200, `Expected 200 for ${method}`);
    }
  });

  it("rejects POST without a matching token", async () => {
    const mw = createCsrfMiddleware();
    const response = await mw(
      new Request("https://example.com/account", { method: "POST" }),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 403);
    assert.equal(await response.text(), "Invalid CSRF token");
  });

  it("rejects POST when cookie token does not match header token", async () => {
    const mw = createCsrfMiddleware();
    const response = await mw(
      new Request("https://example.com/account", {
        method: "POST",
        headers: {
          cookie: "__neutron_csrf=cookie-token",
          "x-csrf-token": "different-token",
        },
      }),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 403);
  });

  it("accepts POST with matching cookie and header token", async () => {
    const mw = createCsrfMiddleware();
    const token = "csrf-token-123";
    const response = await mw(
      new Request("https://example.com/account", {
        method: "POST",
        headers: {
          cookie: `__neutron_csrf=${token}`,
          "x-csrf-token": token,
        },
      }),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 200);
  });

  it("rejects PUT and DELETE as unsafe methods", async () => {
    const mw = createCsrfMiddleware();
    for (const method of ["PUT", "DELETE", "PATCH"]) {
      const response = await mw(
        new Request("https://example.com/account", { method }),
        {},
        async () => OK_RESPONSE()
      );
      assert.equal(response.status, 403, `Expected 403 for ${method}`);
    }
  });

  it("supports custom safe methods", async () => {
    const mw = createCsrfMiddleware({
      safeMethods: ["GET", "HEAD", "OPTIONS", "POST"],
    });

    const response = await mw(
      new Request("https://example.com/account", { method: "POST" }),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 200);
  });

  it("supports custom cookie and header names", async () => {
    const mw = createCsrfMiddleware({
      cookieName: "_my_csrf",
      headerName: "x-my-csrf",
    });

    const token = "my-token";
    const response = await mw(
      new Request("https://example.com", {
        method: "POST",
        headers: {
          cookie: `_my_csrf=${token}`,
          "x-my-csrf": token,
        },
      }),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 200);
  });

  it("does not set cookie if one already exists", async () => {
    const mw = createCsrfMiddleware();
    const token = "existing-token";
    const response = await mw(
      new Request("https://example.com/page", {
        method: "GET",
        headers: { cookie: `__neutron_csrf=${token}` },
      }),
      {},
      async () => OK_RESPONSE()
    );

    // No Set-Cookie should be added when token already exists
    const setCookie = response.headers.get("set-cookie");
    assert.equal(setCookie, null);
  });
});

describe("getCsrfTokenFromContext", () => {
  it("returns token from default key", () => {
    assert.equal(getCsrfTokenFromContext({ csrfToken: "tok" }), "tok");
  });

  it("returns null when key is missing", () => {
    assert.equal(getCsrfTokenFromContext({}), null);
  });

  it("returns null when value is not a string", () => {
    assert.equal(getCsrfTokenFromContext({ csrfToken: 42 }), null);
  });
});

// ---------------------------------------------------------------------------
// createRateLimitMiddleware
// ---------------------------------------------------------------------------

describe("createRateLimitMiddleware", () => {
  it("allows requests within capacity", async () => {
    const mw = createRateLimitMiddleware({
      capacity: 5,
      refillPerSecond: 0.0001,
      key: () => "test-key",
    });

    const request = new Request("https://example.com/api");
    for (let i = 0; i < 5; i++) {
      const response = await mw(request, {}, async () => OK_RESPONSE());
      assert.equal(response.status, 200);
    }
  });

  it("denies once bucket capacity is exceeded", async () => {
    const mw = createRateLimitMiddleware({
      capacity: 2,
      refillPerSecond: 0.0001,
      key: () => "shared-test-key",
    });

    const request = new Request("https://example.com/api");
    const first = await mw(request, {}, async () => OK_RESPONSE());
    const second = await mw(request, {}, async () => OK_RESPONSE());
    const denied = await mw(request, {}, async () => OK_RESPONSE());

    assert.equal(first.status, 200);
    assert.equal(second.status, 200);
    assert.equal(denied.status, 429);
    assert.equal(denied.headers.get("RateLimit-Limit"), "2");
    assert.equal(denied.headers.get("RateLimit-Remaining"), "0");
  });

  it("includes Retry-After header on denied requests", async () => {
    const mw = createRateLimitMiddleware({
      capacity: 1,
      refillPerSecond: 1,
      key: () => "retry-key",
    });

    const request = new Request("https://example.com/api");
    await mw(request, {}, async () => OK_RESPONSE());
    const denied = await mw(request, {}, async () => OK_RESPONSE());

    assert.equal(denied.status, 429);
    const retryAfter = denied.headers.get("Retry-After");
    assert.ok(retryAfter);
    assert.ok(parseInt(retryAfter, 10) >= 1);
  });

  it("sets RateLimit headers on allowed requests", async () => {
    const mw = createRateLimitMiddleware({
      capacity: 10,
      refillPerSecond: 0.0001,
      key: () => "header-key",
    });

    const response = await mw(
      new Request("https://example.com/api"),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.headers.get("RateLimit-Limit"), "10");
    const remaining = parseInt(response.headers.get("RateLimit-Remaining")!, 10);
    assert.ok(remaining >= 8 && remaining <= 9);
    assert.ok(response.headers.get("RateLimit-Reset"));
  });

  it("supports custom deny status", async () => {
    const mw = createRateLimitMiddleware({
      capacity: 1,
      refillPerSecond: 0.0001,
      key: () => "deny-key",
      denyStatus: 503,
    });

    const request = new Request("https://example.com/api");
    await mw(request, {}, async () => OK_RESPONSE());
    const denied = await mw(request, {}, async () => OK_RESPONSE());

    assert.equal(denied.status, 503);
  });

  it("uses separate buckets for different keys", async () => {
    let callCount = 0;
    const mw = createRateLimitMiddleware({
      capacity: 1,
      refillPerSecond: 0.0001,
      key: () => `key-${callCount++}`,
    });

    const request = new Request("https://example.com/api");
    const r1 = await mw(request, {}, async () => OK_RESPONSE());
    const r2 = await mw(request, {}, async () => OK_RESPONSE());

    assert.equal(r1.status, 200);
    assert.equal(r2.status, 200);
  });

  it("supports async key function", async () => {
    const mw = createRateLimitMiddleware({
      capacity: 1,
      refillPerSecond: 0.0001,
      key: async () => "async-key",
    });

    const request = new Request("https://example.com/api");
    const r1 = await mw(request, {}, async () => OK_RESPONSE());
    const r2 = await mw(request, {}, async () => OK_RESPONSE());

    assert.equal(r1.status, 200);
    assert.equal(r2.status, 429);
  });
});

// ---------------------------------------------------------------------------
// resolveSecureCookieOptions
// ---------------------------------------------------------------------------

describe("resolveSecureCookieOptions", () => {
  it("defaults to development settings", () => {
    const opts = resolveSecureCookieOptions({ nodeEnv: "development" });
    assert.equal(opts.path, "/");
    assert.equal(opts.httpOnly, true);
    assert.equal(opts.sameSite, "Lax");
    assert.equal(opts.secure, false);
  });

  it("sets secure=true in production", () => {
    const opts = resolveSecureCookieOptions({ nodeEnv: "production" });
    assert.equal(opts.secure, true);
    assert.equal(opts.httpOnly, true);
    assert.equal(opts.sameSite, "Lax");
  });

  it("allows overriding individual options", () => {
    const opts = resolveSecureCookieOptions({
      nodeEnv: "development",
      httpOnly: false,
      sameSite: "Strict",
      path: "/app",
    });
    assert.equal(opts.httpOnly, false);
    assert.equal(opts.sameSite, "Strict");
    assert.equal(opts.path, "/app");
  });

  it("preserves domain and maxAge when provided", () => {
    const opts = resolveSecureCookieOptions({
      nodeEnv: "production",
      domain: "example.com",
      maxAge: 3600,
    });
    assert.equal(opts.domain, "example.com");
    assert.equal(opts.maxAge, 3600);
  });
});
