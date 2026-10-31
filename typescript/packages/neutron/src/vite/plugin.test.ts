import { describe, it, expect } from "vitest";
import { findImportSpecifier, generateRoutesModule, neutronPlugin } from "./plugin.js";
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
    // renderEntry is re-exported by the package ROOT index (the documented
    // import surface). A hydratable component importing any root API (e.g.
    // useLocation) pulls the root index into the client graph; a stub without
    // this name fails the whole client build with MISSING_EXPORT.
    expect(src).toContain("export const renderEntry");
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

describe("generateRoutesModule: static-tier routes", () => {
  // A page whose whole chain renders at the static tier is reached by a
  // document load and never rendered by the router, so the client route
  // table carries no import() for it — its module and imports stay out of
  // the client build. Layouts shared with an app route keep theirs.
  it("emits no import() for static-tier pages and layouts only they use", () => {
    const cwd = process.cwd().replace(/\\/g, "/");
    const routes = [
      makeRoute({ id: "route:_layout.tsx", path: "/", file: `${cwd}/src/routes/_layout.tsx`, isLayout: true }),
      makeRoute({ id: "route:legal/_layout.tsx", path: "/legal", file: `${cwd}/src/routes/legal/_layout.tsx`, isLayout: true, parentId: "route:_layout.tsx" }),
      makeRoute({ id: "route:legal/terms.tsx", path: "/legal/terms", file: `${cwd}/src/routes/legal/terms.tsx`, parentId: "route:legal/_layout.tsx" }),
      makeRoute({ id: "route:about.tsx", path: "/about", file: `${cwd}/src/routes/about.tsx`, parentId: "route:_layout.tsx" }),
      makeRoute({ id: "route:pinned.tsx", path: "/pinned", file: `${cwd}/src/routes/pinned.tsx`, parentId: "route:_layout.tsx", config: { mode: "static", hydrate: true } }),
      makeRoute({ id: "route:app.tsx", path: "/app", file: `${cwd}/src/routes/app.tsx`, parentId: "route:_layout.tsx", config: { mode: "app" } }),
    ];
    const output = generateRoutesModule(routes);
    for (const kept of ["_layout.tsx", "app.tsx", "pinned.tsx"]) {
      expect(output).toContain(`import("/src/routes/${kept}?neutron-client-route")`);
    }
    for (const stubbed of ["legal/_layout.tsx", "legal/terms.tsx", "about.tsx"]) {
      expect(output).not.toContain(`/src/routes/${stubbed}?`);
    }
    expect(output.match(/clientModule: false/g)).toHaveLength(3);
  });
});

describe("generateRoutesModule", () => {
  it("produces relative import paths for files inside cwd", () => {
    const cwd = process.cwd().replace(/\\/g, "/");
    const route = makeRoute({
      id: "route:index.tsx",
      path: "/",
      file: `${cwd}/src/routes/index.tsx`,
      config: { mode: "app" },
    });

    const output = generateRoutesModule([route]);
    expect(output).toContain("/src/routes/index.tsx?neutron-client-route");
    expect(output).not.toMatch(/import\("\/\/[A-Za-z]/);
  });

  it("configures the router from neutron.config client options, and emits nothing without them", () => {
    const route = makeRoute({ id: "route:index.tsx", path: "/", file: "/x/index.tsx", config: { mode: "app" } });
    expect(generateRoutesModule([route])).not.toContain("configureRouter");
    expect(generateRoutesModule([route], { unloadRouteStylesheets: false })).not.toContain("configureRouter");
    const output = generateRoutesModule([route], { unloadRouteStylesheets: true });
    expect(output.startsWith('import { configureRouter } from "@neutron-build/core/client";\nconfigureRouter({"unloadRouteStylesheets":true});')).toBe(true);
  });

  it("uses /@fs prefix for files outside project root", () => {
    const route = makeRoute({
      id: "route:external",
      path: "/external",
      file: "/some/other/path/component.tsx",
      config: { mode: "app" },
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

describe("dep optimizer virtual-namespace externals", () => {
  type ConfigHook = (this: unknown) => {
    optimizeDeps: {
      exclude: string[];
      rolldownOptions?: { plugins: Array<{ resolveId(id: string): unknown }> };
      esbuildOptions?: { plugins: unknown[] };
    };
  };
  const config = neutronPlugin().config as unknown as ConfigHook;

  it("hands Rolldown its plugin on Vite 8, without the deprecated esbuild option", () => {
    const { optimizeDeps } = config.call({ meta: { rolldownVersion: "1.0.0" } });
    expect(optimizeDeps.esbuildOptions).toBeUndefined();
    const [plugin] = optimizeDeps.rolldownOptions!.plugins;
    expect(plugin.resolveId("virtual:neutron-islands")).toEqual({
      id: "virtual:neutron-islands",
      external: true,
    });
    expect(plugin.resolveId("preact")).toBe(null);
  });

  it("keeps the esbuild plugin for Vite 6, which calls the hook without a context", () => {
    const { optimizeDeps } = config.call(undefined);
    expect(optimizeDeps.rolldownOptions).toBeUndefined();
    expect(optimizeDeps.esbuildOptions!.plugins).toHaveLength(1);
  });
});

describe("findImportSpecifier (Island __src resolution)", () => {
  it.each([
    ['import Counter from "./c.js";', "Counter", "./c.js"],
    ['import { Counter } from "./c.js";', "Counter", "./c.js"],
    ['import { Other as Counter } from "./c.js";', "Counter", "./c.js"],
    ['import Counter, { type Props } from "./c.js";', "Counter", "./c.js"],
    ['import Def, { Counter } from "./c.js";', "Counter", "./c.js"],
    ['import Counter, * as ns from "./c.js";', "Counter", "./c.js"],
    ['import * as Counter from "./c.js";', "Counter", "./c.js"],
    ['import {\n  A,\n  Counter,\n} from "./c.js";', "Counter", "./c.js"],
    ["import Counter from '../c';", "Counter", "../c"],
  ])("%s", (code, name, expected) => {
    expect(findImportSpecifier(code, name)).toBe(expected);
  });

  it("skips type-only bindings and look-alike names", () => {
    expect(findImportSpecifier('import type Counter from "./c.js";', "Counter")).toBeNull();
    expect(findImportSpecifier('import { type Counter } from "./c.js";', "Counter")).toBeNull();
    expect(findImportSpecifier('import { Counter as X } from "./c.js";', "Counter")).toBeNull();
    expect(findImportSpecifier('import CounterBox from "./c.js";', "Counter")).toBeNull();
  });

  it("picks the import that binds the name, not the first one", () => {
    const code = 'import A from "./a.js";\nimport B, { type T } from "./b.js";';
    expect(findImportSpecifier(code, "B")).toBe("./b.js");
  });
});
