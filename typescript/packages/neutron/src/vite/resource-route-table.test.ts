import { describe, expect, it } from "vitest";
import { generateRoutesModule } from "./plugin.js";
import type { Route } from "../core/types.js";

function pageRoute(id: string, path: string): Route {
  return {
    id,
    path,
    file: `/app/src/routes/${id.replace("route:", "")}`,
    params: [],
    config: { mode: "static" },
    hasDefaultExport: true,
    parentId: null,
  };
}

function resourceRoute(id: string, path: string): Route {
  return {
    ...pageRoute(id, path),
    hasMethodHandlers: true,
    hasDefaultExport: false,
  };
}

describe("generateRoutesModule excludes resource routes from the client table", () => {
  it("emits pages but no endpoint entries — handler modules never reach a client chunk", () => {
    const code = generateRoutesModule([
      pageRoute("route:index.tsx", "/"),
      pageRoute("route:about.ts", "/about"),
      resourceRoute("route:api/time.ts", "/api/time"),
      resourceRoute("route:api/users/[id].ts", "/api/users/:id"),
    ]);

    expect(code).toContain('"route:index.tsx"');
    expect(code).toContain('"route:about.ts"');
    // No route-table entry, no lazy import, no id in routeIds: an endpoint
    // that is not in the client route table can never be navigated to (and
    // its module never imported) client-side.
    expect(code).not.toContain("api/time");
    expect(code).not.toContain("users");
    expect(code).not.toContain('"route:api/');
    expect(code).not.toMatch(/import\([^)]*api/);
  });
});
