import { describe, expect, it } from "vitest";
import { generateRouteTypesDeclaration } from "./route-typegen.js";
import type { Route } from "./types.js";

function route(path: string): Route {
  return {
    id: `route:${path}`,
    path,
    file: `/tmp/${path.replace(/[/:*]/g, "_")}.tsx`,
    pattern: /^$/,
    params: [],
    config: { mode: "static" },
    parentId: null,
  };
}

describe("route type generation", () => {
  it("generates module augmentation for static and dynamic paths", () => {
    const declaration = generateRouteTypesDeclaration([
      route("/"),
      route("/pricing"),
      route("/users/:id"),
      route("/docs/*"),
    ]);

    expect(declaration).toContain('declare module "neutron"');
    expect(declaration).toContain('| "/"');
    expect(declaration).toContain('| "/pricing"');
    expect(declaration).toContain("| `/users/${string}`");
    expect(declaration).toContain("| `/docs/${string}`");
  });

  it("generates fallback union when no routes exist", () => {
    const declaration = generateRouteTypesDeclaration([]);
    expect(declaration).toContain("paths: never");
  });
});
