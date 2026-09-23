import type { Route } from "../core/types.js";

/** Version GET /health reports when the app configures none. */
export const DEFAULT_HEALTH_VERSION = "0.1.0";

/**
 * FRAMEWORK_CONTRACT.md §7 body. The SSR server holds no Nucleus pool (loaders
 * connect per-request), so `nucleus` is always "unconfigured" here.
 */
export function healthBody(version: string = DEFAULT_HEALTH_VERSION) {
  return { status: "ok", nucleus: "unconfigured", version } as const;
}

/**
 * True when the app defines its own /health route. The built-in is then
 * suppressed so the user route can report dependency-aware health (e.g. 503
 * when a backing store is down) instead of the built-in returning a false 200.
 */
export function appDefinesHealthRoute(routes: readonly Route[]): boolean {
  return routes.some((route) => route.path === "/health" && !route.file.includes("_layout"));
}
