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
}

/** The subset of a Vite manifest chunk this module reads. */
export interface ViteManifestChunk {
  file: string;
  isEntry?: boolean;
  imports?: string[];
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
