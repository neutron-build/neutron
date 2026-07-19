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

/** Resolved document head: rendered <head> HTML plus the merged SEO input. */
export interface ResolvedHeadDocument {
  headHtml: string;
  /**
   * The merged SeoMetaInput from the route chain, or null if no route returned
   * structured head data. Callers that own the document shell need this for
   * htmlAttrs/bodyAttrs — the <html>/<body> open tags live outside headHtml.
   */
  seo: SeoMetaInput | null;
}

/**
 * Walk the ordered route chain (outermost layout first, page route last),
 * invoke each module's head() export, merge structured SeoMetaInput and collect
 * raw-string fragments, then render the final <head> HTML. Returns the merged
 * SEO input alongside the HTML so document-shell owners can render
 * htmlAttrs/bodyAttrs on the <html>/<body> open tags.
 */
export async function resolveHeadDocument(
  entries: HeadRouteEntry[],
  options: ResolveHeadHtmlOptions
): Promise<ResolvedHeadDocument> {
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

  return {
    headHtml: renderDocumentHead(options.pathname, mergedSeo, headFragments, options.nonce),
    seo: mergedSeo,
  };
}

/**
 * Like resolveHeadDocument, but returns only the rendered <head> HTML. Kept
 * for callers that don't own the document shell.
 */
export async function resolveHeadHtml(
  entries: HeadRouteEntry[],
  options: ResolveHeadHtmlOptions
): Promise<string> {
  return (await resolveHeadDocument(entries, options)).headHtml;
}
