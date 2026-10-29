import { describe, expect, it } from "vitest";
import {
  dispatchResourceHandler,
  isResourceRoute,
  resourceRouteAllowedMethods,
  RESOURCE_ROUTE_METHODS,
} from "./resource-routes.js";
import { notFoundError, unauthorized } from "./problem.js";
import { parseRouteFacts } from "./manifest.js";
import type { Route } from "./types.js";

function request(method: string, url = "https://x.test/api/thing"): Request {
  return new Request(url, { method });
}

function route(facts: Partial<Route>): Route {
  return {
    id: "route:x.ts",
    path: "/api/thing",
    file: "/x/src/routes/api/thing.ts",
    params: [],
    config: { mode: "static" },
    parentId: null,
    ...facts,
  };
}

describe("isResourceRoute", () => {
  it("method handlers + no default component = resource route", () => {
    expect(
      isResourceRoute(route({ hasMethodHandlers: true, hasDefaultExport: false }))
    ).toBe(true);
  });

  it("a default component disqualifies (that is a page — and a discovery error)", () => {
    expect(
      isResourceRoute(route({ hasMethodHandlers: true, hasDefaultExport: true }))
    ).toBe(false);
  });

  it("unknown facts are NOT resource routes (safe direction: keep page behavior)", () => {
    expect(isResourceRoute(route({}))).toBe(false);
    expect(isResourceRoute(route({ hasMethodHandlers: false, hasDefaultExport: false }))).toBe(
      false
    );
  });

  it("layouts and not-found pages are never resource routes", () => {
    expect(
      isResourceRoute(
        route({ isLayout: true, hasMethodHandlers: true, hasDefaultExport: false })
      )
    ).toBe(false);
    expect(
      isResourceRoute(
        route({ isNotFound: true, hasMethodHandlers: true, hasDefaultExport: false })
      )
    ).toBe(false);
  });
});

describe("resourceRouteAllowedMethods", () => {
  it("lists exported methods and adds HEAD when GET exists (RFC 9110 §9.3.2)", () => {
    expect(resourceRouteAllowedMethods({ GET: () => 1, POST: () => 1 })).toEqual([
      "GET",
      "POST",
      "HEAD",
    ]);
    expect(resourceRouteAllowedMethods({ DELETE: () => 1 })).toEqual(["DELETE"]);
  });
});

describe("dispatchResourceHandler", () => {
  it("answers a Response return verbatim", async () => {
    const response = await dispatchResourceHandler(
      { GET: () => new Response("raw body", { headers: { "X-A": "1" } }) },
      request("GET"),
      {},
      {}
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("X-A")).toBe("1");
    expect(await response.text()).toBe("raw body");
  });

  it("serializes a non-Response return as JSON (200)", async () => {
    const response = await dispatchResourceHandler(
      { GET: () => ({ items: [1, 2] }) },
      request("GET"),
      {},
      {}
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("application/json");
    expect(await response.json()).toEqual({ items: [1, 2] });
  });

  it("passes params and the middleware chain's context to the handler", async () => {
    const seen: unknown[] = [];
    await dispatchResourceHandler(
      {
        GET: (args) => {
          seen.push(args.params, args.context);
          return {};
        },
      },
      request("GET"),
      { id: "42" },
      { tenant: "acme" } as Record<string, unknown>
    );
    expect(seen[0]).toEqual({ id: "42" });
    expect(seen[1]).toEqual({ tenant: "acme" });
  });

  it("405 problem+json with Allow for an unsupported method", async () => {
    const response = await dispatchResourceHandler(
      { GET: () => 1, POST: () => 1 },
      request("DELETE"),
      {},
      {}
    );
    expect(response.status).toBe(405);
    expect(response.headers.get("content-type")).toContain("application/problem+json");
    expect(response.headers.get("allow")).toBe("GET, POST, HEAD");
    const body = (await response.json()) as Record<string, unknown>;
    expect(body.type).toBe("https://neutron.dev/errors/method-not-allowed");
    expect(body.title).toBe("Method Not Allowed");
    expect(body.status).toBe(405);
    expect(body.instance).toBe("/api/thing");
  });

  it("405 for a method outside the dispatch set entirely", async () => {
    const response = await dispatchResourceHandler({ GET: () => 1 }, request("PROPFIND"), {}, {});
    expect(response.status).toBe(405);
  });

  it("HEAD falls back to GET: same headers, no body", async () => {
    const response = await dispatchResourceHandler(
      { GET: () => new Response("body!", { headers: { "Content-Length": "5" } }) },
      request("HEAD"),
      {},
      {}
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("content-length")).toBe("5");
    expect(await response.text()).toBe("");
  });

  it("an explicit HEAD export wins over the GET fallback", async () => {
    const response = await dispatchResourceHandler(
      { GET: () => new Response("from-get"), HEAD: () => new Response("from-head") },
      request("HEAD"),
      {},
      {}
    );
    expect(await response.text()).toBe("from-head");
  });

  it("a thrown ProblemError answers RFC 7807", async () => {
    const response = await dispatchResourceHandler(
      { GET: () => Promise.reject(unauthorized("no session")) },
      request("GET"),
      {},
      {}
    );
    expect(response.status).toBe(401);
    expect(response.headers.get("content-type")).toContain("application/problem+json");
    const body = (await response.json()) as Record<string, unknown>;
    expect(body.type).toBe("https://neutron.dev/errors/unauthorized");
    expect(body.instance).toBe("/api/thing");
  });

  it("a notFoundError handler answers the 404 problem shape", async () => {
    const response = await dispatchResourceHandler(
      { GET: () => Promise.reject(notFoundError("no such run")) },
      request("GET"),
      {},
      {}
    );
    expect(response.status).toBe(404);
    const body = (await response.json()) as Record<string, unknown>;
    expect(body.type).toBe("https://neutron.dev/errors/not-found");
    expect(body.detail).toBe("no such run");
  });

  it("a thrown Response is a documented short-circuit", async () => {
    const redirect = new Response(null, { status: 302, headers: { Location: "/login" } });
    const response = await dispatchResourceHandler(
      { GET: () => Promise.reject(redirect) },
      request("GET"),
      {},
      {}
    );
    expect(response.status).toBe(302);
    expect(response.headers.get("Location")).toBe("/login");
  });

  it("any other thrown error becomes a 500 problem (this surface has no HTML boundary)", async () => {
    const response = await dispatchResourceHandler(
      { GET: () => Promise.reject(new Error("boom")) },
      request("GET"),
      {},
      {}
    );
    expect(response.status).toBe(500);
    expect(response.headers.get("content-type")).toContain("application/problem+json");
    const body = (await response.json()) as Record<string, unknown>;
    expect(body.type).toBe("https://neutron.dev/errors/internal");
  });
});

describe("parseRouteFacts: resource-route detection", () => {
  it("detects declared method handlers", () => {
    const facts = parseRouteFacts(`export async function GET() { return 1; }`);
    expect(facts.hasMethodHandlers).toBe(true);
    expect(facts.hasDefaultExport).toBe(false);
  });

  it("detects const arrow handlers and named re-exports", () => {
    expect(parseRouteFacts(`export const POST = async () => 1;`).hasMethodHandlers).toBe(true);
    expect(parseRouteFacts(`export { handler as DELETE };`).hasMethodHandlers).toBe(true);
  });

  it("does not match near-name exports (GETX, GET_DATA, get)", () => {
    expect(parseRouteFacts(`export const GETX = 1;`).hasMethodHandlers).toBe(false);
    expect(parseRouteFacts(`export const GET_DATA = 1;`).hasMethodHandlers).toBe(false);
    expect(parseRouteFacts(`export function get() {}`).hasMethodHandlers).toBe(false);
  });

  it("detects default components in all common forms", () => {
    expect(parseRouteFacts(`export default function Page() {}`).hasDefaultExport).toBe(true);
    expect(parseRouteFacts(`const X = () => 1;\nexport default X;`).hasDefaultExport).toBe(true);
    expect(parseRouteFacts(`export default class Thing {}`).hasDefaultExport).toBe(true);
    expect(parseRouteFacts(`export function GET() {}\n`).hasDefaultExport).toBe(false);
  });

  it("RESOURCE_ROUTE_METHODS matches the dispatch set", () => {
    expect([...RESOURCE_ROUTE_METHODS]).toEqual([
      "GET",
      "POST",
      "PUT",
      "PATCH",
      "DELETE",
      "OPTIONS",
      "HEAD",
    ]);
  });
});
