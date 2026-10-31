/**
 * Client router options from neutron.config.ts `client`, applied by the
 * generated `virtual:neutron/routes` module before the router starts.
 */
export interface NeutronRouterOptions {
  /**
   * After a client navigation has rendered, unlink the route stylesheets the
   * new route does not use. Off by default (Next's App Router keeps them):
   * a stylesheet loaded for one page then stays in the document, and rules
   * that conflict across pages (two sections redefining the same theme
   * tokens on `:root`) resolve by load order instead of by page. On, a page
   * shows the same cascade after navigation as on a fresh load, at the cost
   * of re-fetching (usually from the HTTP cache) a sheet when you navigate
   * back. Only route stylesheets are touched — sheets the build's route-CSS
   * table names, linked by the server, the router or Vite's preload helper —
   * and never one the new route can bring in through a dynamic import. The
   * client entry's and layouts' are in every route's list and stay; links a
   * `head()` adds are never removed. In dev, CSS that Vite injects as
   * `<style>` for imported modules is not touched.
   */
  unloadRouteStylesheets?: boolean;
}

export const routerOptions: NeutronRouterOptions = {};

export function configureRouter(options: NeutronRouterOptions): void {
  Object.assign(routerOptions, options);
}
