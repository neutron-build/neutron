/**
 * The client entry point, and the thing that decides how much runtime a page
 * actually downloads.
 *
 * Every Neutron app's entry is the same four lines:
 *
 *     registerRoutes(routes);
 *     void init();
 *
 * so this module is where the tier decision belongs. It holds the route table
 * — cheap, a few fields per route — and imports the router **dynamically**.
 * Rollup therefore splits the router into its own chunk, and a page whose
 * routes are all `mode: "static"` never requests it. No build configuration and
 * no change to any app is needed to get that: the code split falls out of the
 * `import()` below.
 *
 * ## Why the router is not simply always loaded
 *
 * It used to be. `init` was the router's own `init`, so importing it from an
 * app entry statically pulled in the navigation machinery, the click
 * interceptor, the CSS loader and the data-fetch path — on a purely static
 * content page that needs none of them. The page then intercepted every link
 * click and turned it into a round-trip for data that did not exist, which is
 * slower than the browser navigation it replaced.
 *
 * ## What a static page gets instead
 *
 * Islands still hydrate, so interactive components work exactly as before.
 * Navigation is browser-native, made instant by the speculation rules the
 * server emits for this tier — the browser prerenders the next document, which
 * is strictly better than a client router fetching data, because the target is
 * already painted when the click lands.
 */

import { initIslands } from "./island-runtime.js";

type Registration = {
  mode?: "static" | "app";
  hasLoader?: boolean;
  isLayout?: boolean;
};

let registered: Record<string, Registration> = {};
let started = false;

/**
 * Record the route table. Kept here rather than in the router so that reading
 * it does not require downloading the router.
 */
export function registerRoutes(routeMap: Record<string, unknown>): void {
  registered = routeMap as Record<string, Registration>;
}

/**
 * Whether any route in the app needs the client router.
 *
 * App-wide rather than per-route on purpose. The router owns history and the
 * click interceptor, which are document-scoped: loading it only once the user
 * reaches an app route would mean the first navigation *into* that route is
 * still a full page load, and the state the router exists to preserve is gone
 * by the time it arrives. An app that has any `mode: "app"` route is an app.
 *
 * Anything unknown counts as needing it. A route table from an older build
 * carries no `mode`, and guessing "static" there would silently disable
 * client-side navigation for an app that depends on it.
 */
export function appNeedsRouter(
  routeMap: Record<string, Registration> = registered
): boolean {
  const entries = Object.values(routeMap);
  if (entries.length === 0) return true;
  return entries.some((route) => route.mode !== "static");
}

/**
 * Start the client runtime at the tier this app needs.
 *
 * Returns once the page is interactive, so an app that awaits it keeps the
 * previous ordering guarantees.
 */
export async function init(): Promise<void> {
  if (started) return;
  started = true;

  if (appNeedsRouter()) {
    // Dynamic, so this chunk is only requested by apps that have an app route.
    const router = await import("./hydrate.js");
    router.registerRoutes(registered as never);
    await router.init();
    return;
  }

  // Static tier: no router, no click interception, no per-navigation fetch.
  // Islands are the only interactivity a static page can declare, and they
  // hydrate from their own manifest, each lazily importing just its component.
  initIslands();
}
