import { describe, expect, it } from "vitest";
import { runMiddlewareChain } from "../core/middleware.js";
import type { AppContext } from "../core/types.js";
import {
  createMemorySessionStorage,
  getSessionFromContext,
  sessionMiddleware,
} from "./session.js";

describe("session middleware", () => {
  it("creates and persists a new session", async () => {
    const storage = createMemorySessionStorage();
    const middleware = sessionMiddleware({
      storage,
      cookie: { name: "sid" },
      ttlSeconds: 60,
    });

    const context: AppContext = {};
    const request = new Request("http://localhost/test");

    const response = await runMiddlewareChain([middleware], request, context, async () => {
      const session = getSessionFromContext(context);
      expect(session).toBeTruthy();
      session!.set("userId", "123");
      return new Response("ok");
    });

    const setCookie = response.headers.get("Set-Cookie");
    expect(setCookie).toBeTruthy();
    expect(setCookie).toContain("sid=");
    expect(setCookie).toContain("HttpOnly");

    const id = extractCookieValue(setCookie!, "sid");
    const stored = await storage.getSession(id);
    expect(stored?.data.userId).toBe("123");
  });

  it("loads existing session from cookie", async () => {
    const storage = createMemorySessionStorage();
    await storage.setSession("abc-session", { role: "admin" });

    const middleware = sessionMiddleware({
      storage,
      cookie: { name: "sid" },
    });

    const context: AppContext = {};
    const request = new Request("http://localhost/test", {
      headers: {
        Cookie: "sid=abc-session",
      },
    });

    await runMiddlewareChain([middleware], request, context, async () => {
      const session = getSessionFromContext(context);
      expect(session?.get("role")).toBe("admin");
      return new Response("ok");
    });
  });

  it("destroys session and clears cookie", async () => {
    const storage = createMemorySessionStorage();
    await storage.setSession("delete-me", { role: "admin" });

    const middleware = sessionMiddleware({
      storage,
      cookie: { name: "sid" },
    });

    const context: AppContext = {};
    const request = new Request("http://localhost/test", {
      headers: {
        Cookie: "sid=delete-me",
      },
    });

    const response = await runMiddlewareChain([middleware], request, context, async () => {
      const session = getSessionFromContext(context);
      session?.destroy();
      return new Response("ok");
    });

    const setCookie = response.headers.get("Set-Cookie");
    expect(setCookie).toContain("sid=");
    expect(setCookie).toContain("Max-Age=0");

    const stored = await storage.getSession("delete-me");
    expect(stored).toBeNull();
  });

  it("marks session cookie as Secure for https requests by default", async () => {
    const storage = createMemorySessionStorage();
    const middleware = sessionMiddleware({
      storage,
      cookie: { name: "sid" },
    });

    const context: AppContext = {};
    const request = new Request("https://example.com/test");
    const response = await runMiddlewareChain([middleware], request, context, async () => {
      getSessionFromContext(context)?.set("userId", "1");
      return new Response("ok");
    });

    const setCookie = response.headers.get("Set-Cookie");
    expect(setCookie).toBeTruthy();
    expect(setCookie).toContain("Secure");
  });

  it("ignores forwarded protocol from an untrusted source", async () => {
    // No trustedProxies configured: a spoofed X-Forwarded-Proto must NOT make
    // the cookie Secure on a plain-HTTP request.
    const storage = createMemorySessionStorage();
    const middleware = sessionMiddleware({
      storage,
      cookie: { name: "sid" },
    });

    const context: AppContext = {};
    const request = new Request("http://example.com/test", {
      headers: { "x-forwarded-proto": "https" },
    });
    const response = await runMiddlewareChain([middleware], request, context, async () => {
      getSessionFromContext(context)?.set("userId", "1");
      return new Response("ok");
    });

    const setCookie = response.headers.get("Set-Cookie");
    expect(setCookie).toBeTruthy();
    expect(setCookie).not.toContain("Secure");
  });

  it("honors forwarded protocol only from a trusted proxy", async () => {
    const storage = createMemorySessionStorage();
    const middleware = sessionMiddleware({
      storage,
      cookie: { name: "sid" },
      trustedProxies: ["10.0.0.0/8"],
    });

    const context: AppContext = {};
    const request = new Request("http://example.com/test", {
      headers: {
        "x-forwarded-proto": "https",
        "x-real-ip": "10.0.0.5",
      },
    });
    const response = await runMiddlewareChain([middleware], request, context, async () => {
      getSessionFromContext(context)?.set("userId", "1");
      return new Response("ok");
    });

    const setCookie = response.headers.get("Set-Cookie");
    expect(setCookie).toBeTruthy();
    expect(setCookie).toContain("Secure");
  });

  it("defaults Secure in production over plain HTTP without trusted proxies", async () => {
    const prev = process.env.NODE_ENV;
    process.env.NODE_ENV = "production";
    try {
      const storage = createMemorySessionStorage();
      const middleware = sessionMiddleware({ storage, cookie: { name: "sid" } });
      const context: AppContext = {};
      const request = new Request("http://example.com/test");
      const response = await runMiddlewareChain([middleware], request, context, async () => {
        getSessionFromContext(context)?.set("userId", "1");
        return new Response("ok");
      });
      const setCookie = response.headers.get("Set-Cookie");
      expect(setCookie).toBeTruthy();
      expect(setCookie).toContain("Secure");
    } finally {
      process.env.NODE_ENV = prev;
    }
  });
});

describe("memory session storage eviction", () => {
  it("evicts expired sessions after writes when map exceeds threshold", async () => {
    const storage = createMemorySessionStorage({ ttlSeconds: 1 });

    // Fill storage past 1000 entries with already-expired data
    const pastExpiry = Date.now() - 10000;
    for (let i = 0; i < 1010; i++) {
      await storage.setSession(`expired-${i}`, { n: i }, pastExpiry);
    }

    // Trigger 100 writes to activate lazy sweep
    for (let i = 0; i < 100; i++) {
      await storage.setSession(`fresh-${i}`, { n: i });
    }

    // Expired entries should have been swept
    const expiredResult = await storage.getSession("expired-0");
    expect(expiredResult).toBeNull();

    // Fresh entries should survive
    const freshResult = await storage.getSession("fresh-0");
    expect(freshResult).not.toBeNull();
  });
});

function extractCookieValue(header: string, name: string): string {
  const parts = header.split(";");
  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed.startsWith(`${name}=`)) {
      continue;
    }
    return decodeURIComponent(trimmed.slice(name.length + 1));
  }
  return "";
}
