/**
 * Browser build of the root entry (`exports["."].browser`).
 *
 * Route modules import `Link`, `useLocation`, … from `@neutron-build/core`,
 * so every client build walks this entry. The full entry also re-exports the
 * server, the adapters, content collections and the Vite plugins: tree-
 * shaking drops their code, but the bundler still has to resolve their
 * imports (magic-string, @babel/parser, …) and keeps their dynamic imports
 * (`vite`, `ws`, `shiki`, `typescript`) as chunks in the client output.
 *
 * This entry has the same export names. Browser-safe modules are re-exported
 * as they are; the node-only ones are stubs that throw if called. The set of
 * names is pinned to index.ts by index.browser.test.ts.
 */
import { Fragment, h, type ComponentChildren } from "preact";
import type { AnyJsxElement } from "./client/jsx-agnostic.js";

function serverOnly(name: string): (...args: unknown[]) => never {
  return () => {
    throw new Error(`${name} from @neutron-build/core is server-only and cannot run in the browser.`);
  };
}

export {
  isResourceRoute,
  dispatchResourceHandler,
  resourceRouteAllowedMethods,
  RESOURCE_ROUTE_METHODS,
} from "./core/resource-routes.js";
export { renderSpeculationRules, renderStaticLinkSpeculationRules } from "./core/speculation-rules.js";
export { assertRenderedFragment } from "./core/fragment-guard.js";
export { redirect, safeRedirect, isSafeRedirect, json, notFound, isResponse } from "./core/response.js";
export {
  ProblemError,
  isProblemError,
  badRequest,
  unauthorized,
  forbidden,
  notFoundError,
  conflict,
  validationError,
  rateLimited,
  internalError,
  validateJsonBody,
  PROBLEM_CONTENT_TYPE,
  PROBLEM_TYPE_BASE,
} from "./core/problem.js";
export { cache, clearCache, clearCacheByPrefix, resetRequestCache, revalidateTag, revalidateTags, getCacheTags, getCacheKeysByTag } from "./core/cache.js";
export { generateFontHTML, validateFontConfig } from "./core/fonts.js";
export { route } from "./core/typed-routes.js";
export { z } from "zod";
export { calculateReadingTime } from "./content/reading-time.js";
export { extractToc } from "./content/toc.js";
export {
  serializeTransportData,
  deserializeTransportData,
  encodeSerializedPayload,
  decodeSerializedPayload,
  encodeSerializedPayloadAsJson,
  serializeForInlineScript,
  escapeJsonForInlineScript,
} from "./core/serialization.js";
export { runMiddlewareChain, composeMiddleware } from "./core/middleware.js";
export {
  compileRouteRules,
  resolveRouteRuleRedirect,
  resolveRouteRuleRewrite,
  resolveRouteRuleHeaders,
} from "./core/route-rules.js";
export {
  defineConfig,
  resolveRuntime,
  resolveRuntimeAliases,
  resolveRuntimeJsx,
  resolveRuntimeNoExternal,
  RUNTIME_RUNNER_CJS_EXTERNALS,
} from "./config.js";
export { parseCookieHeader, getCookie, serializeCookie } from "./core/cookies.js";
export { installTransportPeer, transportPeer } from "./server/peer.js";
export { createMemoryAppCacheStore, createMemoryLoaderCacheStore } from "./server/cache-store.js";
export {
  runModifyConfigHooks,
  runBuildStartHooks,
  runBuildCompleteHooks,
  runBuildErrorHooks,
} from "./adapters/build-hooks.js";

export {
  useLoaderData,
  useRouteLoaderData,
  useActionData,
  useNavigation,
  useNavigate,
  useSubmit,
  useParams,
  useLocation,
  useSearchParams,
  useRevalidator,
  useMatches,
  useBeforeUnload,
  useBlocker,
} from "./client/hooks.js";
export { useFetcher, useFetchers } from "./client/fetcher.js";
export { Form, Link, NavLink, prefetch } from "./client/components.js";
export { withRouterProviders } from "./core/router-providers.js";
export { Island } from "./client/island.js";
export { Image, defaultImageLoader } from "./client/image.js";
export { Picture } from "./client/picture.js";
export { ViewTransitions } from "./client/view-transitions.js";
export { ScrollReveal } from "./client/scroll-reveal.js";
export { Await } from "./client/await.js";
export { Show, For, Switch, Match, Index } from "./client/control-flow.js";
export { incrementalPrefetch, getCachedPage, clearPrefetchCache, clearPrefetchCacheForUrl, setupIncrementalPrefetch, cleanupIncrementalPrefetch } from "./client/incremental-prefetch.js";
export { navigate, go } from "./client/navigate.js";
export { pushState, replaceState, getState } from "./client/shallow-routing.js";
export { signal, computed, effect, batch, untrack, createMemo, createEffect, createRoot } from "./client/signals.js";
export {
  buildMetaTags,
  renderMetaTags,
  mergeSeoMetaInput,
  inferPageTitle,
  renderDocumentHead,
  buildHtmlOpenTag,
  buildBodyOpenTag,
  renderAttrs,
  buildSitemapXml,
  buildRobotsTxt,
  breadcrumbListSchema,
  faqPageSchema,
  articleSchema,
  organizationSchema,
  websiteSchema,
} from "./core/seo.js";
export { resolveLocalePath, withLocalePath, stripLocalePrefix, createI18nMiddleware } from "./core/i18n.js";
export { buildRssFeed } from "./core/rss.js";

/** The client half of server/server-islands.tsx: in the browser it renders its children. */
export function ServerIsland({ children }: { children: ComponentChildren }): AnyJsxElement | null {
  return h(Fragment, null, children) as AnyJsxElement;
}

// Node-only: filesystem, node:crypto, Hono/node-server, Vite, Babel, MDX.
export const createRouter = serverOnly("createRouter");
export const discoverRoutes = serverOnly("discoverRoutes");
export const parseRouteConfig = serverOnly("parseRouteConfig");
export const renderStatic = serverOnly("renderStatic");
export const renderToString = serverOnly("renderToString");
export const prepareRouteTypes = serverOnly("prepareRouteTypes");
export const generateRouteTypesDeclaration = serverOnly("generateRouteTypesDeclaration");
export const defineCollection = serverOnly("defineCollection");
export const getCollection = serverOnly("getCollection");
export const getEntry = serverOnly("getEntry");
export const renderEntry = serverOnly("renderEntry");
export const prepareContentCollections = serverOnly("prepareContentCollections");
export const setActiveMarkdownConfig = serverOnly("setActiveMarkdownConfig");
export const getActiveMarkdownConfig = serverOnly("getActiveMarkdownConfig");
export const createServer = serverOnly("createServer");
export const startServer = serverOnly("startServer");
export const resolvePreactSsr = serverOnly("resolvePreactSsr");
export const vitePreactAliases = serverOnly("vitePreactAliases");
export const mergePreactAliases = serverOnly("mergePreactAliases");
export const importPreactSsr = serverOnly("importPreactSsr");
export const cspPlugin = serverOnly("cspPlugin");
export const defaultCspConfig = undefined;
export const adapterNode = serverOnly("adapterNode");
export const adapterStatic = serverOnly("adapterStatic");
export const adapterCloudflare = serverOnly("adapterCloudflare");
export const adapterVercel = serverOnly("adapterVercel");
export const adapterNetlify = serverOnly("adapterNetlify");
export const adapterDocker = serverOnly("adapterDocker");
export const createMemorySessionStorage = serverOnly("createMemorySessionStorage");
export const sessionMiddleware = serverOnly("sessionMiddleware");
export const getSessionFromContext = serverOnly("getSessionFromContext");
export const createSQLSessionStorage = serverOnly("createSQLSessionStorage");
export const csrfMiddleware = serverOnly("csrfMiddleware");
export const rateLimitMiddleware = serverOnly("rateLimitMiddleware");
export const apiRateLimit = serverOnly("apiRateLimit");
export const imageRateLimit = serverOnly("imageRateLimit");
export const getIslandComponent = serverOnly("getIslandComponent");
export const clearIslandRegistry = serverOnly("clearIslandRegistry");
export const isRegisteredIsland = serverOnly("isRegisteredIsland");
export const handleIslandRequest = serverOnly("handleIslandRequest");
