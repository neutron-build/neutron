import { describe, expect, it } from "vitest";
import { assertStaticRoutesUngated } from "./render-static.js";
import type { Route, RouteModule } from "./types.js";

/**
 * NA-04: the static prerender pipelines must refuse a `mode: "static"` route
 * whose chain ACTUALLY exports middleware, checked against loaded modules
 * rather than derived facts. The facts regex missed real spellings — a
 * non-first declarator, alias export lists, star re-exports — and each miss
 * published a gated page. These tests feed the gate the loaded module shapes
 * directly, which is what both the production build (neutron-cli build.ts)
 * and the standalone renderer run before writing anything.
 */

function route(path: string, id = path, parentId: string | null = null): Route {
  return {
    id: `route:${id}`,
    path,
    file: `/routes${path === "/" ? "/index" : path}.tsx`,
    params: [],
    config: { mode: "static" },
    hasLoader: false,
    hasMiddleware: false,
    hasAction: false,
    parentId,
    isLayout: false,
  };
}

function layoutFor(child: Route, id: string): { layout: Route; chain: (r: Route) => Route[] } {
  const layout: Route = {
    ...route(child.path, id, null),
    file: `/routes${child.path === "/" ? "" : child.path}/_layout.tsx`,
    isLayout: true,
  };
  return {
    layout,
    chain: (r: Route) => (r.parentId === layout.id ? [layout] : []),
  };
}

// Imported JS can expose invalid middleware values. The static gate must reject
// their presence too, without widening the public route middleware contract.
type MiddlewareExportFixture = Omit<RouteModule, "middleware"> & { middleware: unknown };

function gateWith(
  modules: Map<string, RouteModule | MiddlewareExportFixture>,
  chain: (r: Route) => Route[] = () => []
) {
  return (staticRoutes: Route[]) =>
    assertStaticRoutesUngated(staticRoutes, {
      // Model the dynamic-import boundary; the gate never invokes middleware.
      loadRouteModule: async (r) => (modules.get(r.id) ?? {}) as RouteModule,
      getLayoutChain: chain,
    });
}

describe("assertStaticRoutesUngated", () => {
  it("accepts an ungated route", async () => {
    const r = route("/public");
    const gate = gateWith(new Map([[r.id, { default: () => null }]]));
    await expect(gate([r])).resolves.toBeUndefined();
  });

  it("rejects a direct middleware export", async () => {
    const r = route("/secret");
    const gate = gateWith(new Map([[r.id, {
      default: () => null,
      middleware: async () => new Response("denied", { status: 403 }),
    }]]));
    await expect(gate([r])).rejects.toThrow(/secret.*exports middleware/s);
  });

  it("rejects when only a LAYOUT in the chain exports middleware", async () => {
    const r = route("/admin", "admin-page", "route:admin-layout");
    const { layout, chain } = layoutFor(r, "admin-layout");
    const gate = gateWith(
      new Map([
        [layout.id, { default: () => null, middleware: [() => null] }],
        [r.id, { default: () => null }],
      ]),
      chain
    );
    await expect(gate([r])).rejects.toThrow(/its layout .*_layout.* exports middleware/s);
  });

  // The spellings the regex version missed. Here they arrive as loaded
  // modules, so the spellings are what the module EVALUATES to — but the
  // point of the shared gate is that it never consults facts at all, so a
  // facts defect downstream cannot resurrect the bypass.
  it("rejects an empty middleware array — presence is the gate", async () => {
    const r = route("/empty-gate");
    const gate = gateWith(new Map([[r.id, { default: () => null, middleware: [] }]]));
    await expect(gate([r])).rejects.toThrow(/Cannot prerender/);
  });

  it("rejects a route whose middleware export is explicitly undefined", async () => {
    const r = route("/undef-gate");
    const module = { default: () => null, middleware: undefined };
    // `export const middleware = undefined` still defines the property.
    const gate = gateWith(new Map([[r.id, module]]));
    await expect(gate([r])).rejects.toThrow(/Cannot prerender/);
  });

  it("checks the whole batch and names every gated route", async () => {
    const ok = route("/fine");
    const a = route("/a");
    const b = route("/b");
    const gate = gateWith(
      new Map([
        [ok.id, { default: () => null }],
        [a.id, { default: () => null, middleware: [] }],
        [b.id, { default: () => null, middleware: [] }],
      ])
    );
    await expect(gate([ok, a, b])).rejects.toThrow(/2 route\(s\)[\s\S]*\/a[\s\S]*\/b/);
  });

  it("a module without the property at all passes", async () => {
    const r = route("/clean");
    const gate = gateWith(new Map([[r.id, { default: () => null, loader: async () => null }]]));
    await expect(gate([r])).resolves.toBeUndefined();
  });

  // The end-to-end spelling coverage lives in client-tier.test.ts's
  // parseRouteFacts cases (comma declarator, alias, star, string-literal
  // name); the gate's contract is narrower: whatever the module exports as
  // `middleware` — however it was spelled — stops the prerender.
});
