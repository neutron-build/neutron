import { describe, it, expect } from "vitest";
import { csrfMiddleware } from "./csrf.js";
import type { AppContext } from "../core/types.js";

const TOKEN = "a".repeat(64);
const OTHER = "b".repeat(64);

function run(
  mw: ReturnType<typeof csrfMiddleware>,
  request: Request,
  context: AppContext = {}
) {
  return mw(request, context, async () => new Response("ok"));
}

describe("csrfMiddleware", () => {
  it("mints a token and sets an HttpOnly cookie on a safe request", async () => {
    const ctx: AppContext = {};
    const res = await run(csrfMiddleware(), new Request("http://localhost/"), ctx);
    expect(typeof ctx.csrfToken).toBe("string");
    const setCookie = res.headers.get("Set-Cookie") || "";
    expect(setCookie).toContain("_csrf=");
    expect(setCookie).toContain("HttpOnly");
  });

  it("reuses an existing token instead of churning the cookie", async () => {
    const ctx: AppContext = {};
    const req = new Request("http://localhost/", {
      headers: { Cookie: `_csrf=${TOKEN}` },
    });
    const res = await run(csrfMiddleware(), req, ctx);
    expect(ctx.csrfToken).toBe(TOKEN);
    expect(res.headers.get("Set-Cookie")).toBeNull();
  });

  it("accepts a same-origin POST with a matching token", async () => {
    const req = new Request("http://localhost/submit", {
      method: "POST",
      headers: {
        Cookie: `_csrf=${TOKEN}`,
        "x-csrf-token": TOKEN,
        Origin: "http://localhost",
      },
    });
    const res = await run(csrfMiddleware(), req);
    expect(res.status).toBe(200);
  });

  it("rejects a POST with a mismatched token", async () => {
    const req = new Request("http://localhost/submit", {
      method: "POST",
      headers: {
        Cookie: `_csrf=${TOKEN}`,
        "x-csrf-token": OTHER,
        Origin: "http://localhost",
      },
    });
    const res = await run(csrfMiddleware(), req);
    expect(res.status).toBe(403);
  });

  it("rejects a cross-origin POST even with a matching token", async () => {
    const req = new Request("http://localhost/submit", {
      method: "POST",
      headers: {
        Cookie: `_csrf=${TOKEN}`,
        "x-csrf-token": TOKEN,
        Origin: "http://evil.example",
      },
    });
    const res = await run(csrfMiddleware(), req);
    expect(res.status).toBe(403);
  });

  it("rejects a POST with no token", async () => {
    const req = new Request("http://localhost/submit", {
      method: "POST",
      headers: { Origin: "http://localhost" },
    });
    const res = await run(csrfMiddleware(), req);
    expect(res.status).toBe(403);
  });
});
