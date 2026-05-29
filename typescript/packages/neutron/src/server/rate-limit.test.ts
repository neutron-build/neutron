import { describe, it, expect } from "vitest";
import { rateLimitMiddleware } from "./rate-limit.js";
import type { AppContext } from "../core/types.js";

function call(
  mw: ReturnType<typeof rateLimitMiddleware>,
  headers: Record<string, string> = {},
  context: AppContext = {}
) {
  return mw(
    new Request("http://localhost/", { headers }),
    context,
    async () => new Response("ok")
  );
}

describe("rateLimitMiddleware", () => {
  it("does not let X-Forwarded-For spoofing bypass the limit by default", async () => {
    const mw = rateLimitMiddleware({ maxRequests: 2, windowMs: 60_000 });
    // Each request spoofs a different XFF; by default they must all share one
    // bucket, so the third trips the limit.
    const r1 = await call(mw, { "x-forwarded-for": "1.1.1.1" });
    const r2 = await call(mw, { "x-forwarded-for": "2.2.2.2" });
    const r3 = await call(mw, { "x-forwarded-for": "3.3.3.3" });
    expect(r1.status).toBe(200);
    expect(r2.status).toBe(200);
    expect(r3.status).toBe(429);
  });

  it("keys per client from X-Forwarded-For when trustProxy is enabled", async () => {
    const mw = rateLimitMiddleware({
      maxRequests: 1,
      windowMs: 60_000,
      trustProxy: true,
    });
    const a1 = await call(mw, { "x-forwarded-for": "1.1.1.1" });
    const a2 = await call(mw, { "x-forwarded-for": "1.1.1.1" });
    const b1 = await call(mw, { "x-forwarded-for": "9.9.9.9" });
    expect(a1.status).toBe(200);
    expect(a2.status).toBe(429);
    expect(b1.status).toBe(200);
  });

  it("prefers a server-provided clientAddress over forwarded headers", async () => {
    const mw = rateLimitMiddleware({
      maxRequests: 1,
      windowMs: 60_000,
      trustProxy: true,
    });
    const spoof = { "x-forwarded-for": "1.1.1.1" };
    const r1 = await call(mw, spoof, { clientAddress: "10.0.0.1" } as AppContext);
    const r2 = await call(mw, spoof, { clientAddress: "10.0.0.1" } as AppContext);
    const r3 = await call(mw, spoof, { clientAddress: "10.0.0.2" } as AppContext);
    expect(r1.status).toBe(200);
    expect(r2.status).toBe(429);
    expect(r3.status).toBe(200);
  });

  it("reads the trusted hop from the right of the forwarded chain", async () => {
    const mw = rateLimitMiddleware({
      maxRequests: 1,
      windowMs: 60_000,
      trustProxy: true,
    });
    // Attacker prepends a spoofed entry; the real (right-most) hop is constant,
    // so the limit still applies to that client.
    const r1 = await call(mw, { "x-forwarded-for": "spoof, 5.5.5.5" });
    const r2 = await call(mw, { "x-forwarded-for": "other-spoof, 5.5.5.5" });
    expect(r1.status).toBe(200);
    expect(r2.status).toBe(429);
  });
});
