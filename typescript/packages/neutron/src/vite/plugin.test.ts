import { describe, it, expect } from "vitest";
import { generateRoutesModule, neutronPlugin } from "./plugin.js";
import type { Route } from "../core/types.js";

describe("content module client stub", () => {
  const plugin = neutronPlugin();
  // resolveId/load are plain methods on the plugin object.
  const resolveId = plugin.resolveId as unknown as (
    id: string,
    importer: string | undefined,
    ctx: { ssr?: boolean }
  ) => string | null;
  const load = plugin.load as unknown as (id: string) => Promise<string | null>;
  const STUB = "\0neutron:content-client-stub";

  it("stubs the server-only content module in client (non-ssr) builds", () => {
    expect(resolveId("@neutron-build/core/content", undefined, {})).toBe(STUB);
  });

  it("leaves the content module intact for SSR (server needs the real thing)", () => {
    expect(resolveId("@neutron-build/core/content", undefined, { ssr: true })).toBe(null);
  });

  it("the stub satisfies the named content imports without node builtins", async () => {
    const src = await load(STUB);
    expect(src).toContain("export const getCollection");
    expect(src).toContain("export const getEntry");
    expect(src).toContain("export const defineCollection");
    expect(src).not.toContain("node:crypto");
    expect(src).not.toContain("node:fs");
  });
});

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
