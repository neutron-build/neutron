import { describe, it, expect } from "vitest";
import { generateRoutesModule } from "./plugin.js";
import type { Route } from "../core/types.js";

function makeRoute(overrides: Partial<Route> & Pick<Route, "id" | "path" | "file">): Route {
  return {
    pattern: /./,
    params: [],
    config: { mode: "static" as const },
    parentId: null,
    ...overrides,
  } as Route;
}

describe("generateRoutesModule", () => {
  it("produces relative import paths for files inside cwd", () => {
    const cwd = process.cwd().replace(/\\/g, "/");
    const route = makeRoute({
      id: "route:index.tsx",
      path: "/",
      file: `${cwd}/src/routes/index.tsx`,
    });

    const output = generateRoutesModule([route]);
    expect(output).toContain("/src/routes/index.tsx?neutron-client-route");
    expect(output).not.toMatch(/import\("\/\/[A-Za-z]/);
  });

  it("uses /@fs prefix for files outside project root", () => {
    const route = makeRoute({
      id: "route:external",
      path: "/external",
      file: "/some/other/path/component.tsx",
    });

    const output = generateRoutesModule([route]);
    expect(output).toContain("/@fs/some/other/path/component.tsx?neutron-client-route");
  });

  it("never produces double-slash protocol-relative URLs", () => {
    const cwd = process.cwd().replace(/\\/g, "/");
    const routes = [
      makeRoute({ id: "route:index.tsx", path: "/", file: `${cwd}/src/routes/index.tsx` }),
      makeRoute({ id: "route:about.tsx", path: "/about", file: `${cwd}/src/routes/about.tsx` }),
    ];

    const output = generateRoutesModule(routes);
    expect(output).not.toMatch(/import\("\/\//);
  });

  it("includes route metadata (id, path, parentId, isLayout)", () => {
    const cwd = process.cwd().replace(/\\/g, "/");
    const route = makeRoute({
      id: "route:blog/[slug].tsx",
      path: "/blog/:slug",
      file: `${cwd}/src/routes/blog/[slug].tsx`,
      parentId: "route:_layout.tsx",
      isLayout: false,
    });

    const output = generateRoutesModule([route]);
    expect(output).toContain('"route:blog/[slug].tsx"');
    expect(output).toContain('path: "/blog/:slug"');
    expect(output).toContain('parentId: "route:_layout.tsx"');
    expect(output).toContain("isLayout: false");
  });

  it("marks layout routes with isLayout: true", () => {
    const cwd = process.cwd().replace(/\\/g, "/");
    const route = makeRoute({
      id: "route:_layout.tsx",
      path: "/",
      file: `${cwd}/src/routes/_layout.tsx`,
      isLayout: true,
    });

    const output = generateRoutesModule([route]);
    expect(output).toContain("isLayout: true");
  });

  it("exports routeIds array", () => {
    const cwd = process.cwd().replace(/\\/g, "/");
    const routes = [
      makeRoute({ id: "route:index.tsx", path: "/", file: `${cwd}/src/routes/index.tsx` }),
      makeRoute({ id: "route:about.tsx", path: "/about", file: `${cwd}/src/routes/about.tsx` }),
    ];

    const output = generateRoutesModule(routes);
    expect(output).toContain('["route:index.tsx","route:about.tsx"]');
  });
});
