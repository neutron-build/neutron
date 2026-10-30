/**
 * A-020, production-build side. `mode: "static"` + `middleware` cannot both
 * be honored: a prerendered file answers before `renderAppRoute` — the only
 * place middleware runs — so prerendering a gated page publishes it. Core's
 * `renderStatic` refuses the combination for its own renderer; `neutron build`
 * refuses it over the same rule, before writing anything.
 *
 * Split out of build.ts (which imports vite at load time) so the gate can be
 * tested from source — the same reason static-paths.ts exists.
 */
import { isResourceRoute } from "@neutron-build/core";
import type { Route } from "@neutron-build/core";

export interface GatedStaticRoute {
  route: Route;
  /** The route or ancestor layout whose `middleware` export gates it. */
  gate: Route;
}

function isLayoutRoute(route: Route): boolean {
  return route.file.includes("_layout") || route.isLayout === true;
}

/**
 * Every static-mode page route gated by `middleware` — its own export or an
 * ancestor layout's (renderAppRoute collects middleware from the whole
 * layout chain, so a layout gate exposes the child exactly the same way).
 *
 * Resource routes are exempt: they are never prerendered and are served per
 * request through the middleware chain, so gating one is legal — refusing it
 * would break every authed endpoint. `hasMiddleware === true` (not `!== true`)
 * is deliberate, mirroring render-static: facts here are always populated
 * (discoverRoutes just read every file), and failing a build on a guess is
 * worse than not failing it.
 */
export function findGatedStaticRoutes(routes: Route[]): GatedStaticRoute[] {
  const layouts = new Map<string, Route>();
  for (const route of routes) {
    if (isLayoutRoute(route)) {
      layouts.set(route.id, route);
    }
  }

  const gated: GatedStaticRoute[] = [];
  for (const route of routes) {
    if (isLayoutRoute(route)) continue;
    if (isResourceRoute(route)) continue;
    if (route.config.mode !== "static") continue;

    const chain: Route[] = [];
    let currentId: string | null = route.parentId;
    while (currentId) {
      const parent = layouts.get(currentId);
      if (!parent) break;
      chain.push(parent);
      currentId = parent.parentId;
    }

    const gate = [...chain, route].find((r) => r.hasMiddleware === true);
    if (gate) {
      gated.push({ route, gate });
    }
  }
  return gated;
}

/**
 * The build-failure message, mirroring render-static's wording so both
 * refusals teach the same fix.
 */
export function gatedStaticRoutesError(gated: GatedStaticRoute[]): string {
  const detail = gated
    .map(({ route, gate }) =>
      gate.id === route.id
        ? `  ${route.path} — ${route.file} exports \`middleware\``
        : `  ${route.path} — its layout ${gate.file} exports \`middleware\``
    )
    .join("\n");
  return (
    `Cannot prerender ${gated.length} route(s) that are gated by middleware:\n${detail}\n\n` +
    "A prerendered file is served before any middleware runs, so these pages " +
    "would be public. Either drop `config = { mode: \"static\" }` so the route " +
    "renders per request with its gate, or remove the middleware if the page " +
    "is genuinely public."
  );
}
