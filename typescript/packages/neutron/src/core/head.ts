// Shared <head> resolution — the single implementation of the route→layout
// head() walk used by BOTH the dev/prod app-route renderer and the static
// (SSG) renderer. Each caller loads its route modules its own way (a preloaded
// map for app routes, a file cache for SSG) and hands over already-resolved
// (route, module) pairs; this walks them outermost-first, merges structured
// SeoMetaInput, collects raw-string fragments, and renders the final head HTML.
// Keeping it in one place is what stops head output from drifting between the
// request-serving and build-time paths.
import type { Route, RouteModule, AppContext } from "./types.js";
import { mergeSeoMetaInput, renderDocumentHead, type SeoMetaInput } from "./seo.js";

/** A route paired with its already-loaded module (undefined if not loaded). */
export interface HeadRouteEntry {
  route: Route;
  module: RouteModule | undefined;
}

export interface ResolveHeadHtmlOptions {
  request: Request;
  params: Record<string, string>;
  context: AppContext;
  pathname: string;
  /**
   * Loader data keyed by route id. Each route's head() receives the full map as
   * `loaderData` plus its own slice as `data`, matching what the component gets.
   */
  loaderData: Record<string, unknown>;
  actionData?: unknown;
  /**
   * CSP nonce applied to head-emitted scripts (JSON-LD, inline headScripts).
   * Undefined for SSG — no nonce middleware runs at build time.
   */
  nonce?: string;
}

/**
 * Walk the ordered route chain (outermost layout first, page route last),
 * invoke each module's head() export, merge structured SeoMetaInput and collect
 * raw-string fragments, then render the final <head> HTML.
 */
export async function resolveHeadHtml(
  entries: HeadRouteEntry[],
  options: ResolveHeadHtmlOptions
): Promise<string> {
  let mergedSeo: SeoMetaInput | null = null;
  const headFragments: string[] = [];

  for (const { route, module } of entries) {
    if (!module?.head) {
      continue;
    }

    const resolved = await module.head({
      request: options.request,
      params: options.params,
      context: options.context,
      loaderData: options.loaderData,
      data: options.loaderData[route.id],
      actionData: options.actionData,
      pathname: options.pathname,
    });
    if (!resolved) {
      continue;
    }

    // A raw string returned from head() is developer-authored markup (the
    // explicit escape hatch), emitted faithfully. Data-driven head content
    // should use the structured SeoMetaInput return value, which is escaped.
    if (typeof resolved === "string") {
      headFragments.push(resolved);
      continue;
    }

    mergedSeo = mergeSeoMetaInput(mergedSeo, resolved);
  }

  return renderDocumentHead(options.pathname, mergedSeo, headFragments, options.nonce);
}
