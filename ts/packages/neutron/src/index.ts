export { createRouter } from "./core/router.js";
export { discoverRoutes, parseRouteConfig } from "./core/manifest.js";
export { renderStatic, renderToString } from "./core/render-static.js";
export { redirect, safeRedirect, isSafeRedirect, json, notFound, isResponse, defer, isDeferredData, DeferredData } from "./core/response.js";
export { cache, clearCache, clearCacheByPrefix, resetRequestCache, revalidateTag, revalidateTags, getCacheTags, getCacheKeysByTag, type CacheOptions } from "./core/cache.js";
export { generateFontHTML, validateFontConfig, type FontConfig, type FontSource } from "./core/fonts.js";
export { route, type RoutePath, type RouteHref, type NeutronGeneratedRouteMap } from "./core/typed-routes.js";
export {
  prepareRouteTypes,
  generateRouteTypesDeclaration,
  type PrepareRouteTypesOptions,
} from "./core/route-typegen.js";
export {
  z,
  defineCollection,
  getCollection,
  getEntry,
  prepareContentCollections,
  type CollectionDefinition,
  type DefineCollectionOptions,
  type CollectionEntry,
  type ContentCollectionMap,
  type PrepareContentCollectionsOptions,
} from "./content/index.js";
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
  type CompiledRouteRules,
  type RouteRuleRedirectResult,
  type RouteRuleRewriteResult,
  type RouteRuleHeadersResult,
} from "./core/route-rules.js";
export {
  createServer,
  startServer,
  type NeutronServerOptions,
  type NeutronServerHooks,
  type NeutronRequestStartEvent,
  type NeutronRequestEndEvent,
  type NeutronLoaderStartEvent,
  type NeutronLoaderEndEvent,
  type NeutronActionStartEvent,
  type NeutronActionEndEvent,
  type NeutronErrorEvent,
} from "./server/index.js";
export {
  defineConfig,
  resolveRuntime,
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
  type NeutronConfig,
  type NeutronRedirectRule,
  type NeutronRewriteRule,
  type NeutronHeaderRule,
  type NeutronRoutesConfig,
  type NeutronWorkerConfig,
  type NeutronRuntime,
} from "./config.js";
export { cspPlugin, defaultCspConfig, type CspConfig, type CspDirectives } from "./vite/csp-plugin.js";
export {
  adapterNode,
  type NodeAdapterOptions,
} from "./adapters/node.js";
export {
  adapterStatic,
  type StaticAdapterOptions,
} from "./adapters/static.js";
export {
  adapterCloudflare,
  type CloudflareAdapterOptions,
} from "./adapters/cloudflare.js";
export {
  adapterVercel,
  type VercelAdapterOptions,
} from "./adapters/vercel.js";
export {
  adapterDocker,
  type DockerAdapterOptions,
} from "./adapters/docker.js";
export type {
  NeutronAdapter,
  AdapterBuildContext,
  AdapterRoutesSummary,
  AdapterRuntimeBundle,
} from "./adapters/adapter.js";
export {
  parseCookieHeader,
  getCookie,
  serializeCookie,
  type CookieSerializeOptions,
} from "./core/cookies.js";
export {
  createMemorySessionStorage,
  sessionMiddleware,
  getSessionFromContext,
  type Session,
  type SessionData,
  type SessionStorage,
  type SessionRecord,
  type SessionMiddlewareOptions,
  type SessionCookieOptions,
  type MemorySessionStorageOptions,
} from "./server/session.js";
export {
  csrfMiddleware,
  type CsrfOptions,
} from "./server/csrf.js";
export {
  rateLimitMiddleware,
  apiRateLimit,
  imageRateLimit,
  type RateLimitOptions,
} from "./server/rate-limit.js";
export {
  createMemoryAppCacheStore,
  createMemoryLoaderCacheStore,
  type NeutronAppCacheStore,
  type NeutronLoaderCacheStore,
  type NeutronCacheStores,
  type NeutronAppResponseCacheEntry,
  type NeutronLoaderDataCacheEntry,
  type MemoryAppCacheStoreOptions,
  type MemoryLoaderCacheStoreOptions,
} from "./server/cache-store.js";
export {
  type CorsOptions,
} from "./server/http-headers.js";
export {
  defaultHandle,
  defaultHandleError,
  defaultHandleFetch,
  type HandleHook,
  type HandleErrorHook,
  type HandleFetchHook,
  type GlobalServerHooks,
  type HookEvent,
  type ResolveOptions,
} from "./server/hooks.js";
export {
  ServerIsland,
  getIslandComponent,
  clearIslandRegistry,
  isRegisteredIsland,
  handleIslandRequest,
  type ServerIslandProps,
} from "./server/server-islands.js";
export {
  runModifyConfigHooks,
  runBuildStartHooks,
  runBuildCompleteHooks,
  runBuildErrorHooks,
  type BuildContext,
  type NeutronAdapterWithHooks,
} from "./adapters/build-hooks.js";

// Client hooks
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
export type { LoaderData, NavigationState, SubmitOptions, UIMatch, BlockerState } from "./client/hooks.js";

// Client fetcher
export { useFetcher, useFetchers } from "./client/fetcher.js";
export type { Fetcher, FetcherState, FetcherSubmitOptions } from "./client/fetcher.js";

// Client components
export { Form, Link, NavLink, prefetch } from "./client/components.js";
export type { FormProps, LinkProps, NavLinkProps } from "./client/components.js";
export { Island } from "./client/island.js";
export { Image, defaultImageLoader } from "./client/image.js";
export type { ImageProps, ImageLoader, ImageLoaderArgs } from "./client/image.js";
export type { ErrorBoundaryFallbackProps } from "./client/error-boundary.js";
export { ViewTransitions } from "./client/view-transitions.js";
export { ScrollReveal } from "./client/scroll-reveal.js";
export type { ScrollRevealProps } from "./client/scroll-reveal.js";
export { Await } from "./client/await.js";
export type { AwaitProps } from "./client/await.js";
export { Show, For, Switch, Match, Index, type ShowProps, type ForProps, type SwitchProps, type MatchProps, type IndexProps } from "./client/control-flow.js";
export { incrementalPrefetch, getCachedPage, clearPrefetchCache, clearPrefetchCacheForUrl, setupIncrementalPrefetch, cleanupIncrementalPrefetch } from "./client/incremental-prefetch.js";

// Client navigation
export { navigate, go } from "./client/navigate.js";
export { pushState, replaceState, getState, type ShallowRouteState, type PushStateOptions } from "./client/shallow-routing.js";

// Client signals
export { signal, computed, effect, batch, untrack, createMemo, createEffect, createRoot, type Signal, type SignalValue } from "./client/signals.js";

// Core types
export type {
  Route,
  RouteMatch,
  RouteConfig,
  RouteCacheConfig,
  RouteModule,
  LoaderArgs,
  ActionArgs,
  HeadersArgs,
  HeadArgs,
  AppContext,
  MiddlewareFn,
  ErrorBoundaryProps,
  GetStaticPathsResult,
} from "./core/types.js";

export {
  buildMetaTags,
  renderMetaTags,
  mergeSeoMetaInput,
  inferPageTitle,
  renderDocumentHead,
  buildSitemapXml,
  buildRobotsTxt,
  breadcrumbListSchema,
  faqPageSchema,
  articleSchema,
  organizationSchema,
  websiteSchema,
  type SeoMetaInput,
  type SeoTag,
  type SitemapEntry,
  type SitemapOptions,
  type RobotsRule,
  type RobotsOptions,
  type ArticleSchemaInput,
  type OrganizationSchemaInput,
} from "./core/seo.js";

export {
  resolveLocalePath,
  withLocalePath,
  stripLocalePrefix,
  createI18nMiddleware,
  type I18nOptions,
  type ResolvedLocalePath,
} from "./core/i18n.js";
