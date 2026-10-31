/**
 * Per-route stylesheets.
 *
 * `neutron-ts build` records, for every page route (and not-found route), the
 * stylesheets its route graph needs: the client entry's CSS, then each layout's
 * (outermost first), then the route module's own — each module's CSS being
 * what its static import graph reaches, with CSS shared by several routes
 * coming from the common chunk Vite splits it into, so one file serves them
 * all. Pages link only their own list; a route whose list is unknown (a dist
 * from an older CLI, or a route added since the build) links every stylesheet
 * in `dist/assets/`, as before.
 *
 * Kept free of `node:` imports: the edge runtime bundles embed the same table.
 */

/** File in the dist root that holds the per-route stylesheet table. */
export const ROUTE_CSS_MANIFEST = ".neutron-route-css.json";
export const ROUTE_CSS_MANIFEST_FORMAT = 1;

export interface RouteCssManifest {
  format: typeof ROUTE_CSS_MANIFEST_FORMAT;
  /** Route id → stylesheet hrefs, in link order. */
  routes: Record<string, string[]>;
  /**
   * Route id → the CSS its chain's dynamic imports can bring in (Vite links
   * it when the lazy module loads), minus what `routes` already lists. Not
   * linked by the server; the client router keeps these on navigation
   * (`client.unloadRouteStylesheets`). Absent in tables from older CLIs.
   */
  lazy?: Record<string, string[]>;
}

/** The subset of a Vite manifest chunk this module reads. */
export interface ViteManifestChunk {
  file: string;
  isEntry?: boolean;
  imports?: string[];
  dynamicImports?: string[];
  css?: string[];
}

export type ViteManifest = Record<string, ViteManifestChunk>;

/**
 * The CSS a manifest chunk needs: that of its static imports first (shared
 * chunks before the module that depends on them, the order Vite's own
 * preloading applies), then its own. Dynamic imports are not followed — they
 * load later, with their own CSS.
 */
export function collectChunkCss(manifest: ViteManifest, key: string, seen = new Set<string>()): string[] {
  if (seen.has(key)) return [];
  seen.add(key);
  const chunk = manifest[key];
  if (!chunk) return [];
  const out: string[] = [];
  for (const imported of chunk.imports ?? []) {
    out.push(...collectChunkCss(manifest, imported, seen));
  }
  for (const css of chunk.css ?? []) {
    out.push("/" + css.replace(/^\/+/, ""));
  }
  return out;
}

/**
 * The CSS that dynamic imports reachable from `keys` (and from their static
 * imports, transitively, and from those dynamic modules in turn) bring in.
 * Dynamic imports of `routeKeys` (route modules: the route table imports
 * every route) and an entry chunk's are not followed.
 */
export function collectDynamicChunkCss(
  manifest: ViteManifest,
  keys: string[],
  routeKeys: ReadonlySet<string> = new Set()
): string[] {
  const out: string[] = [];
  const visited = new Set<string>();
  const walkStatic = (key: string, dynamicTargets: string[], seen: Set<string>) => {
    if (seen.has(key)) return;
    seen.add(key);
    const chunk = manifest[key];
    if (!chunk) return;
    for (const imported of chunk.imports ?? []) walkStatic(imported, dynamicTargets, seen);
    // An entry's dynamic imports are the route table — every route, not this one's.
    if (!chunk.isEntry) {
      dynamicTargets.push(...(chunk.dynamicImports ?? []).filter((target) => !routeKeys.has(target)));
    }
  };
  const queue: string[] = [];
  const staticSeen = new Set<string>();
  for (const key of keys) walkStatic(key, queue, staticSeen);
  while (queue.length > 0) {
    const key = queue.shift()!;
    if (visited.has(key)) continue;
    visited.add(key);
    out.push(...collectChunkCss(manifest, key));
    walkStatic(key, queue, new Set());
  }
  return [...new Set(out)];
}

/** The lazy CSS recorded for one route (none when the table predates it). */
export function routeLazyStylesheetHrefs(
  manifest: RouteCssManifest | null | undefined,
  routeId: string
): string[] {
  return manifest?.lazy?.[routeId] ?? [];
}

/**
 * Stylesheets for one route: its recorded list, or `fallback` (every built
 * stylesheet) when the manifest does not know the route.
 */
export function routeStylesheetHrefs(
  manifest: RouteCssManifest | null | undefined,
  routeId: string,
  fallback: string[]
): string[] {
  const hrefs = manifest?.routes[routeId];
  return hrefs ?? fallback;
}

export function parseRouteCssManifest(source: string): RouteCssManifest | null {
  try {
    const parsed = JSON.parse(source) as Partial<RouteCssManifest>;
    if (parsed?.format !== ROUTE_CSS_MANIFEST_FORMAT || !parsed.routes || typeof parsed.routes !== "object") {
      return null;
    }
    return parsed as RouteCssManifest;
  } catch {
    return null;
  }
}
