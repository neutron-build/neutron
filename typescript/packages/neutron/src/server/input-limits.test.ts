import { describe, it, expect } from "vitest";
import { inputLimitsMiddleware } from "./input-limits.js";
import type { AppContext } from "../core/types.js";

const ok = async () => new Response("ok");
const ctx: AppContext = {};

describe("inputLimitsMiddleware", () => {
  it("rejects an over-long URL with 414", async () => {
    const mw = inputLimitsMiddleware({ maxUrlLength: 40 });
    const res = await mw(
      new Request("http://localhost/" + "a".repeat(100)),
      ctx,
      ok
    );
    expect(res.status).toBe(414);
  });

  it("allows a body within the limit", async () => {
    const mw = inputLimitsMiddleware({ maxRequestBodySize: 1000 });
    const res = await mw(
      new Request("http://localhost/", { method: "POST", body: "small" }),
      ctx,
      ok
    );
    expect(res.status).toBe(200);
  });

  it("rejects too many headers with 431", async () => {
    const mw = inputLimitsMiddleware({ maxHeaderCount: 2 });
    const headers: Record<string, string> = {};
    for (let i = 0; i < 10; i++) headers[`x-h-${i}`] = "v";
    const res = await mw(new Request("http://localhost/", { headers }), ctx, ok);
    expect(res.status).toBe(431);
  });
});
