import * as fs from "node:fs";
import { runWithRequestCache } from "./request-cache.js";
import {
  renderAppRoute,
  emitHook,
  isJsonRequest,
  isMutationMethod,
  toError,
  type RouteRenderer,
} from "../core/render-app-route.js";
import * as path from "node:path";
import { serve, type ServerType } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import type { WebSocketServer } from "ws";
import { compress } from "hono/compress";
import { Hono } from "hono";
import { h } from "preact";
import { renderToString } from "preact-render-to-string";
import { discoverRoutes } from "../core/manifest.js";
import { runMiddlewareChain } from "../core/middleware.js";
import { findGlobalMiddlewareFile, loadGlobalMiddleware } from "../core/global-middleware.js";
import { isResponse, notFound, acceptsHtml, withMutableHeaders } from "../core/response.js";
import { createRouter } from "../core/router.js";
import { dispatchResourceHandler, isResourceRoute, type ResourceRouteMethod } from "../core/resource-routes.js";
import { installTransportPeer } from "./peer.js";
import { appDefinesHealthRoute, DEFAULT_HEALTH_VERSION, healthBody } from "./health.js";
import { onShutdownSignal } from "./shutdown.js";
import {
  compileRouteRules,
  resolveRouteRuleHeaders,
  resolveRouteRuleRedirect,
  resolveRouteRuleRewrite,
} from "../core/route-rules.js";
import {
  applyCorsHeaders,
  applySecurityHeaders,
  createCorsPreflightResponse,
  resolveCorsOptions,
  resolveSecurityHeadersConfig,
  type CorsOptions,
} from "./http-headers.js";
import {
  assertAtomicCacheStore,
  createMemoryAppCacheStore,
  createMemoryLoaderCacheStore,
  type NeutronAppCacheStore,
  type NeutronLoaderCacheStore,
  type NeutronCacheStores,
} from "./cache-store.js";
import { createEntityTag, requestHasMatchingEtag } from "./cache-utils.js";
import { escapeHtml } from "../core/escape.js";
import { internalError, isProblemError, notFoundError } from "../core/problem.js";
import {
  appDefinesSpecRoute,
  serverOpenApiSpec,
  swaggerDocsHtml,
  type NeutronOpenApiOptions,
} from "./openapi.js";
import { assertRenderedFragment, decodeChunkStart } from "../core/fragment-guard.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";
import { neutronPlugin } from "../vite/plugin.js";
import {
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
  RUNTIME_RUNNER_CJS_EXTERNALS,
  type NeutronImageConfig,
  type NeutronRoutesConfig,
  type NeutronRuntime,
} from "../config.js";
import {
  mergeSeoMetaInput,
  renderDocumentHead,
  type SeoMetaInput,
} from "../core/seo.js";
import {
  encodeSerializedPayloadAsJson,
  serializeForInlineScript,
} from "../core/serialization.js";
import { handleImageRequest } from "./image-optimizer.js";
import { capRequestBody, RequestBodyTooLargeError } from "./input-limits.js";
import { handleIslandRequest } from "./server-islands.js";
import { loadServerBundle, serverBundleKey, type LoadedServerBundle } from "./server-bundle.js";
import {
  ROUTE_CSS_MANIFEST,
  parseRouteCssManifest,
  routeStylesheetHrefs,
  type RouteCssManifest,
} from "../core/route-css.js";
import type {
  ActionArgs,
  AppContext,
  ErrorBoundaryProps,
  HeadArgs,
  HeadersArgs,
  LoaderArgs,
  MiddlewareFn,
  ResourceRouteHandler,
  Route,
  RouteMatch,
  RouteModule,
} from "../core/types.js";

export {
  createMemoryAppCacheStore,
  createMemoryLoaderCacheStore,
} from "./cache-store.js";
export { getCookie, serializeCookie, parseCookieHeader } from "../core/cookies.js";
export type { CookieSerializeOptions } from "../core/cookies.js";
export type {
  NeutronAppCacheStore,
  NeutronLoaderCacheStore,
  NeutronCacheStores,
  NeutronAppResponseCacheEntry,
  NeutronLoaderDataCacheEntry,
  MemoryAppCacheStoreOptions,
  MemoryLoaderCacheStoreOptions,
} from "./cache-store.js";
export { csrfMiddleware } from "./csrf.js";
export { findGlobalMiddlewareFile } from "../core/global-middleware.js";
export {
  ROUTE_CSS_MANIFEST,
  ROUTE_CSS_MANIFEST_FORMAT,
  collectChunkCss,
  routeStylesheetHrefs,
  type RouteCssManifest,
  type ViteManifest,
} from "../core/route-css.js";
export {
  SERVER_BUNDLE_MANIFEST,
  SERVER_BUNDLE_FORMAT,
  generateServerBundleEntry,
  type ServerBundleManifest,
} from "./server-bundle.js";
export {
  onShutdownSignal,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
  type ShutdownSignalOptions,
} from "./shutdown.js";
export type { CsrfOptions } from "./csrf.js";
export { rateLimitMiddleware, apiRateLimit, imageRateLimit } from "./rate-limit.js";
export type { RateLimitOptions } from "./rate-limit.js";
export type { NeutronOpenApiOptions } from "./openapi.js";
export { inputLimitsMiddleware, capRequestBody, RequestBodyTooLargeError } from "./input-limits.js";
export type { InputLimitsOptions } from "./input-limits.js";
export {
  tenantIsolation,
  requireOrganization,
  getOrganization,
  auditLogging,
  createMemoryAuditLogger,
  requirePermissions,
  hasPermission,
  hasAnyPermission,
  hasAllPermissions,
  resolvePermissions,
  sessionEnrichment,
} from "./enterprise-auth.js";
export type {
  OrganizationContext,
  EnterpriseAuthContext,
  AuditLogEntry,
  AuditLogger,
  AuditLogQuery,
  Permission,
  Role,
  TenantIsolationOptions,
  AuditLoggingOptions,
  PermissionCheckOptions,
} from "./enterprise-auth.js";

export interface NeutronServerOptions {
  port?: number;
  host?: string;
  rootDir?: string;
  distDir?: string;
  routesDir?: string;
  compress?: boolean;
  runtime?: NeutronRuntime;
  /** Version reported by GET /health (FRAMEWORK_CONTRACT.md). Defaults to "0.1.0". */
  version?: string;
  /**
   * CORS — **opt-in** (default off). Absent or `false`: no `Access-Control-*`
   * headers are sent and preflights are never short-circuited, in every
   * {@link mode}. Provide a config object to enable CORS: `origin` is a single
   * string or an allowlist of origins (use an explicit list, not `"*"`, for
   * credentialed apps — `credentials: true` with `"*"` is refused at
   * startup). Also `methods`, `allowedHeaders`, `exposedHeaders`,
   * `credentials`, `maxAge` (default 600). Enabled responses echo the allowed
   * origin exactly and carry `Vary: Origin`.
   */
  cors?: false | CorsOptions;
  /**
   * Security headers — **on by default** (opt-out). Every response gets
   * `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`,
   * `Referrer-Policy: strict-origin-when-cross-origin`,
   * `Cross-Origin-Opener-Policy: same-origin` and
   * `Cross-Origin-Resource-Policy: same-origin` unless the response already
   * set them. Pass `{ headers }` to override/extend individual values, or
   * `false` to disable entirely. Unchanged by the CORS default-off flip.
   */
  securityHeaders?: false | { headers?: Record<string, string> };
  /**
   * Serve an OpenAPI 3.1 document (FRAMEWORK_CONTRACT.md §4) generated from
   * the discovered app routes: GET per loader, POST per action, path params
   * documented from the route pattern, and every operation referencing the
   * shared RFC 7807 ProblemDetail schema for errors. Absent = the spec
   * surface is not mounted. Per-operation enrichment (request bodies,
   * response schemas) is merged in via `paths`/`components` — TS handlers
   * carry no runtime type information to infer them from.
   */
  openapi?: NeutronOpenApiOptions;
  /**
   * Allow-list of Host header values this server will serve. When set, requests
   * with any other Host get a 400 — preventing Host-header injection into
   * absolute URLs, links, and cache keys. Unset = accept any Host (unchanged).
   */
  trustedHosts?: string[];
  /**
   * Image optimization settings. `images.remotePatterns` is the allowlist
   * that permits `/_neutron/image` to fetch and optimize remote http(s)
   * images: an absolute-URL `src` whose origin is not listed is refused
   * with a 400 that names this config. Absent = only local public-dir
   * images are optimized.
   */
  images?: NeutronImageConfig;
  cache?: NeutronCacheStores;
  routes?: NeutronRoutesConfig;
  hooks?: NeutronServerHooks;
  /**
   * Rendering mode (default `"ssr"`). Controls the HTTP-response machinery:
   * - `"ssr"` — full Neutron: route discovery, Vite SSR runtime, asset/image/island
   *   routes, and the HTML catch-all. The canonical, unchanged behavior.
   * - `"api"` — JSON backend. No route discovery, no SSR, no asset serving. Keeps the
   *   shared batteries (request-id, /health, default security headers, opt-in
   *   CORS, compression) and answers a clean JSON 404 for unmatched paths.
   *   Mount your own routes on the returned `app`.
   * - `"raw"` — bare Hono with only the shared batteries. No catch-all; unmatched paths
   *   get Hono's default 404. Full control.
   *
   * Orthogonal to {@link websocket}: any mode can also carry a WebSocket server.
   */
  mode?: NeutronServerMode;
  /**
   * Attach a WebSocket server to the returned HTTP server (default off). Works with any
   * {@link mode} — e.g. an `"ssr"` dashboard that also streams logs, or an `"api"`/`"raw"`
   * relay. `true` accepts WS upgrades on every path; pass `{ path }` to restrict to one.
   * The returned object gains a `wss` ({@link https://github.com/websockets/ws | ws}
   * `WebSocketServer`); attach `.on("connection", ...)` to handle sockets.
   */
  websocket?: boolean | NeutronWebSocketOptions;
  /**
   * Adapter-level ACTUAL-BYTE cap on request bodies (TS-22). While set, the
   * server wraps every body-bearing request's stream so that reading past
   * `maxRequestBodyBytes` bytes fails the read with a 413 — covering chunked
   * bodies with no Content-Length and requests whose declared length lies.
   * The middleware's Content-Length check stays as the early rejection for
   * honestly-declared sizes (cheaper: no body read at all). Unset = no
   * adapter cap; the input-limits middleware alone applies.
   */
  maxRequestBodyBytes?: number;
  /**
   * Mount Hono sub-apps at path prefixes, so an SSR (or api/raw) server can
   * serve an in-process API alongside its pages — one server instead of two
   * (tRPC's single mountable handler tree is the canonical shape:
   * `mount: { "/api/trpc": trpcApp }`). Each sub-app is registered via
   * `app.route(prefix, subApp)` AFTER the framework batteries (/health,
   * /openapi.json, image/island/static routes) and BEFORE the HTML catch-all,
   * so mounts can neither be shadowed by the catch-all nor shadow the
   * batteries. Works in every {@link mode}; orthogonal to {@link websocket}.
   *
   * Prefixes are literal path prefixes (`/api`, `/api/trpc`) and are
   * validated at startup: a prefix that duplicates/nests another mount's, or
   * that covers a reserved framework path (/health, /openapi.json, /docs,
   * /_neutron/image, /__neutron_island, and /assets//public in ssr mode),
   * is a configuration error. A request under a prefix whose sub-app has no
   * matching route falls through to the rest of the app (in ssr mode, the
   * HTML catch-all) — mounting does not claim the prefix, it registers
   * routes under it.
   *
   * In ssr mode the app's global middleware (`src/middleware.ts`) runs before
   * every mounted sub-app, outermost, with the real request
   * (FRAMEWORK_CONTRACT §5: authentication precedes the route handler, and a
   * mount is a route handler). A response it returns without calling `next()`
   * answers instead of the sub-app. A sub-app that gates itself and must not
   * see the global chain opts out per mount:
   * `mount: { "/api/trpc": { app: trpcApp, globalMiddleware: false } }`.
   */
  mount?: Record<string, NeutronMountApp | NeutronMount>;
  /**
   * Load route modules, layouts and global middleware from the prebuilt
   * server bundle `neutron-ts build` writes to dist (default `true`, used
   * when the dist carries a current one). `false` always transforms them
   * from source through a Vite SSR runtime — for a development server built
   * on `createServer`, where a bundle left in dist by an earlier build would
   * otherwise serve that build's code.
   */
  serverBundle?: boolean;
}

/** The object form of a {@link NeutronServerOptions.mount} entry. */
export interface NeutronMount {
  app: NeutronMountApp;
  /**
   * Run the app's global middleware (`src/middleware.ts`) before this
   * sub-app. Default `true`. Set `false` only when the sub-app enforces its
   * own authentication: the global chain (auth gates included) then does not
   * see requests the sub-app answers.
   */
  globalMiddleware?: boolean;
}

/**
 * A Hono sub-app accepted by {@link NeutronServerOptions.mount}. Any Hono
 * instance works (`new Hono()` with whatever bindings/variables it needs) —
 * `any` generics keep user sub-apps assignable regardless of their own types.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type NeutronMountApp = Hono<any, any, any>;

/** Rendering mode for {@link createServer}. The transport axis is {@link NeutronServerOptions.websocket}. */
export type NeutronServerMode = "ssr" | "api" | "raw";

export interface NeutronWebSocketOptions {
  /**
   * Only accept WebSocket upgrades whose request pathname equals this value; other
   * upgrade attempts get the socket destroyed. Unset = accept upgrades on any path
   * (route inside your `wss.on("connection", (ws, req) => ...)` via `req.url`).
   */
  path?: string;
  /**
   * Fail-closed pre-upgrade authorization (TS-28). Raw upgrades bypass HTTP
   * middleware entirely, so a handshake would otherwise be subject to no
   * auth/rate-limit/origin check at all. Return `true` to complete the
   * upgrade; anything else (false, throw, timeout) destroys the socket.
   * Bounded at 5s — a stuck verifier must not pin the upgrade forever.
   */
  authorize?: (request: import("node:http").IncomingMessage) => boolean | Promise<boolean>;
}

/** Resolved handle returned by {@link createServer}. */
export interface NeutronServer {
  app: Hono<{ Variables: { requestId: string } }>;
  server: ServerType;
  /** Present only when {@link NeutronServerOptions.websocket} is set. */
  wss?: WebSocketServer;
  close: () => Promise<void>;
  url: string;
  /**
   * Where route modules, layouts and global middleware are loaded from:
   * `"server-bundle"` (the dist's prebuilt bundle, no Vite at runtime),
   * `"vite"` (transformed through a Vite SSR runtime — dev, or a dist without
   * a current bundle), or `null` (nothing to load: static-only or api/raw).
   */
  moduleSource: "server-bundle" | "vite" | null;
}

export interface NeutronRequestStartEvent {
  requestId: string;
  method: string;
  url: string;
  pathname: string;
  startedAt: number;
}

export interface NeutronRequestEndEvent {
  requestId: string;
  method: string;
  url: string;
  pathname: string;
  startedAt: number;
  endedAt: number;
  durationMs: number;
  status: number;
  routeId?: string;
  routePath?: string;
  routeMode?: "static" | "app";
  cacheState?: string;
}

export interface NeutronLoaderStartEvent {
  requestId: string;
  method: string;
  pathname: string;
  routeId: string;
  routePath: string;
  startedAt: number;
}

export interface NeutronLoaderEndEvent {
  requestId: string;
  method: string;
  pathname: string;
  routeId: string;
  routePath: string;
  startedAt: number;
  endedAt: number;
  durationMs: number;
  outcome: "success" | "response" | "error";
  cacheStatus?: "hit" | "miss" | "bypass";
  responseStatus?: number;
}

export interface NeutronActionStartEvent {
  requestId: string;
  method: string;
  pathname: string;
  routeId: string;
  routePath: string;
  startedAt: number;
}

export interface NeutronActionEndEvent {
  requestId: string;
  method: string;
  pathname: string;
  routeId: string;
  routePath: string;
  startedAt: number;
  endedAt: number;
  durationMs: number;
  outcome: "success" | "response" | "error";
  responseStatus?: number;
}

export interface NeutronErrorEvent {
  requestId: string;
  method: string;
  pathname: string;
  source: "request" | "action" | "loader" | "render";
  routeId?: string;
  routePath?: string;
  error: Error;
}

export interface NeutronServerHooks {
  onRequestStart?: (event: NeutronRequestStartEvent) => void | Promise<void>;
  onRequestEnd?: (event: NeutronRequestEndEvent) => void | Promise<void>;
  onLoaderStart?: (event: NeutronLoaderStartEvent) => void | Promise<void>;
  onLoaderEnd?: (event: NeutronLoaderEndEvent) => void | Promise<void>;
  onActionStart?: (event: NeutronActionStartEvent) => void | Promise<void>;
  onActionEnd?: (event: NeutronActionEndEvent) => void | Promise<void>;
  onError?: (event: NeutronErrorEvent) => void | Promise<void>;
}

interface SsrServer {
  ssrLoadModule: (id: string) => Promise<unknown>;
  /**
   * The renderer pair resolved through the SAME module graph as the route
   * modules (see RouteRenderer in core/render-app-route.ts). Every render
   * leg — streaming, buffered, error boundary, provider mounting — runs on
   * this pair, so app hooks and the renderer always share one Preact
   * `options` object.
   */
  renderer: RouteRenderer;
  /**
   * Server-island endpoint from the same graph as the routes: `<ServerIsland>`
   * registers into its own module's registry and renders with its own Preact,
   * so the endpoint must be that module's. Absent = core's own.
   */
  handleIslandRequest?: typeof handleIslandRequest;
  source: "server-bundle" | "vite";
  close: () => Promise<void>;
}

interface RequestTraceContext {
  requestId: string;
  method: string;
  url: string;
  pathname: string;
  startedAt: number;
}

interface StaticHtmlEntry {
  body: string;
  headers: Record<string, string>;
}

type StreamRenderFn = (element: preact.VNode) => ReadableStream<Uint8Array> & {
  allReady?: Promise<void>;
};

let cachedStreamRenderFn: StreamRenderFn | null | undefined;

export async function createServer(
  options: NeutronServerOptions = {}
): Promise<NeutronServer> {
  const {
    port = 3000,
    host = "0.0.0.0",
    rootDir = process.cwd(),
    distDir = "dist",
    routesDir = "src/routes",
    compress: enableCompress = true,
    runtime = "preact",
    mode = "ssr",
    websocket,
    mount,
    cors,
    securityHeaders,
    openapi,
    trustedHosts,
    images: imageConfig,
    cache,
    routes: routeRules,
    hooks,
    version: serverVersion = DEFAULT_HEALTH_VERSION,
    serverBundle: useServerBundle = true,
  } = options;

  const resolvedRootDir = path.resolve(rootDir);
  const resolvedDistDir = path.resolve(resolvedRootDir, distDir);
  const resolvedRoutesDir = path.resolve(resolvedRootDir, routesDir);
  const clientEntryScriptSrc = getClientEntryScriptSrc(resolvedDistDir);
  const allStylesheetHrefs = getClientStylesheetHrefs(resolvedDistDir);
  const routeCss = loadRouteCssManifest(resolvedDistDir);
  const stylesheetsFor = (routeId: string) =>
    routeStylesheetHrefs(routeCss, routeId, allStylesheetHrefs);
  const staticRouteHeaders = loadStaticRouteHeaders(resolvedDistDir);
  const staticHtmlCache = buildStaticHtmlCache(resolvedDistDir);
  const corsOptions = resolveCorsOptions(cors);
  const securityHeadersConfig = resolveSecurityHeadersConfig(securityHeaders);
  const compiledRouteRules = compileRouteRules(routeRules);

  // Rendering mode gates the SSR machinery. Only "ssr" walks the routes dir, spins up
  // the Vite SSR runtime, serves assets, and registers the HTML catch-all. "api"/"raw"
  // skip all of it (no fs walk, no hard-fail on a missing routes dir).
  const isSsr = mode === "ssr";
  if (cache?.app) assertAtomicCacheStore(cache.app);
  if (cache?.loader) assertAtomicCacheStore(cache.loader);

  const routes = isSsr ? discoverRoutes({ routesDir: resolvedRoutesDir }) : [];
  const router = createRouter();
  for (const route of routes) {
    router.insert(route);
  }

  const hasAppRoutes = routes.some(
    (route) => !route.file.includes("_layout") && route.config.mode === "app"
  );
  // Resource routes load their handler modules through the SSR runtime like
  // app routes do — an app whose routes are all endpoints still needs one.
  const hasResourceRoutes = routes.some(
    (route) => !route.file.includes("_layout") && isResourceRoute(route)
  );
  // Global middleware is loaded THROUGH the SSR runtime, so the runtime has to
  // exist for it to run at all. Deciding to start one on `hasAppRoutes` alone
  // meant an app whose routes are all `mode: "static"` silently never loaded
  // its src/middleware.ts — the file was not imported, did not run, and did not
  // warn, and adding one app route made it start working. A-022.
  //
  // The file is found with a plain fs check, which needs no runtime, so its
  // presence can be part of the decision to start one.
  const globalMiddlewareFile = isSsr ? findGlobalMiddlewareFile(resolvedRootDir) : null;
  const needsSsrRuntime =
    hasAppRoutes || hasResourceRoutes || globalMiddlewareFile !== null;
  // Fail closed (TS-01): when the SSR runtime is required — app routes exist
  // or a global middleware file was found — a Vite initialization failure
  // must reject createServer, not silently downgrade the app to a static
  // server whose auth/tenant/CSRF middleware never runs. Static-only
  // serving is an explicitly selected mode, not an error fallback.
  //
  // `neutron-ts build` compiles everything this runtime would load into a
  // server bundle; a dist that carries a current one is served from it with
  // no Vite server at all (F-P2-1: transforming the app graph at every boot
  // cost real apps tens of seconds of CPU and gigabytes of RSS).
  const serverBundle = needsSsrRuntime && useServerBundle
    ? await loadServerBundle({
        rootDir: resolvedRootDir,
        distDir: resolvedDistDir,
        runtime,
        requiredFiles: [
          ...routes.map((route) => route.file),
          ...(globalMiddlewareFile ? [globalMiddlewareFile] : []),
        ],
      })
    : null;
  const ssrServer = !needsSsrRuntime
    ? null
    : serverBundle
      ? await createBundledSsrServer(resolvedRootDir, serverBundle)
      : await createSsrServer(resolvedRootDir, resolvedRoutesDir, runtime);
  // An optional src/middleware.ts (default export = a MiddlewareFn) runs
  // OUTERMOST, before any per-route middleware. Loaded once at startup through
  // the same SSR runtime as routes so it shares the module graph (hooks,
  // aliases). Absent file = none. A PRESENT file that fails to import or
  // exports something invalid is a startup error (TS-01): losing the auth
  // gate to a typo must not look like a working server.
  const appGlobalMiddleware: MiddlewareFn[] =
    globalMiddlewareFile && ssrServer
      ? await loadGlobalMiddleware(ssrServer.ssrLoadModule, globalMiddlewareFile)
      : [];

  if (globalMiddlewareFile && !ssrServer) {
    // Unreachable when needsSsrRuntime threw above, kept as a belt-and-braces
    // guard for future callers that construct the pieces by hand.
    throw new Error(
      `Global middleware ${path.relative(resolvedRootDir, globalMiddlewareFile)} was found but ` +
        "the SSR runtime could not be started, so it cannot run. Fix the SSR runtime failure " +
        "or remove the middleware file; serving requests without it would bypass its gate."
    );
  }
  const routeModuleCache = new Map<string, Promise<RouteModule>>();
  const appResponseCacheStore =
    cache?.app || createMemoryAppCacheStore();
  const loaderDataCacheStore =
    cache?.loader || createMemoryLoaderCacheStore();
  // Background cache fills in progress, by app-cache key. A reader that
  // misses must join the fill rather than race it — otherwise a request
  // arriving right behind the first one re-renders (and reports MISS) while
  // the entry is a few microseconds from landing.
  const appPendingStores = new Map<string, Promise<void>>();
  // Monotonic invalidation epoch (TS-07): captured when a cacheable request
  // starts, checked immediately before its fill is committed. A mutation that
  // invalidated the path in between advances the epoch and the stale fill is
  // dropped instead of published after the invalidation.
  let appCacheEpoch = 0;
  // Advance the epoch, then evict the path from both caches (the deletes also
  // bump the stores' generations, fencing remote fills). Given the mutation's
  // response, also evict what its `x-neutron-invalidate` header names — the
  // same directive for page actions and resource-route handlers.
  const invalidateMutatedPath = async (pathname: string, response?: Response): Promise<void> => {
    appCacheEpoch++;
    await appResponseCacheStore.deleteByPath(encodeCacheInvalidationPath(pathname));
    await loaderDataCacheStore.deleteByPath(encodeCacheInvalidationPath(pathname));
    if (response) {
      await applyMutationInvalidationFromResponse(appResponseCacheStore, pathname, response);
      await applyMutationInvalidationToLoaderDataCache(loaderDataCacheStore, pathname, response);
    }
  };

  if (hasAppRoutes && !ssrServer) {
    // Unreachable for isSsr apps (needsSsrRuntime throws first) — kept for
    // hand-assembled configurations.
    throw new Error(
      "App routes detected but the SSR runtime could not be started; refusing to " +
        "degrade to static-only serving. Fix the SSR runtime failure."
    );
  }

  const app = new Hono<{ Variables: { requestId: string } }>();
  app.use("*", (_c, next) => runWithRequestCache(next));

  // FRAMEWORK_CONTRACT.md §2: errors are RFC 7807 problem+json. A ProblemError
  // thrown from a route mounted directly on the Hono app (api/raw mode, or
  // app.get(...) calls on the returned handle) is converted here; errors
  // thrown inside the SSR catch-all are converted there, where the request
  // path is at hand. Anything else propagates to Hono's default 500.
  app.onError((error, c) => {
    // The adapter's capped body stream failing a read surfaces here first:
    // answer 413 like the Content-Length early check, not as a generic 500
    // (TS-22 actual-byte half).
    if (error instanceof RequestBodyTooLargeError) {
      return new Response("Request body too large", {
        status: 413,
        headers: { "Content-Type": "text/plain" },
      });
    }
    if (isProblemError(error)) {
      return error.toResponse(c.req.path);
    }
    throw error;
  });

  // FRAMEWORK_CONTRACT.md §5: Request ID is middleware step 1 (outermost). Reuse an
  // inbound x-request-id for trace propagation, otherwise generate one. It is shared
  // with the per-request trace context and surfaced as the x-request-id response
  // header on every response, including /health.
  app.use("*", async (c, next) => {
    // Install transport peer identity FIRST (TS-32): the node adapter's
    // socket address is the only trustworthy nearest-hop identity. Consumers
    // (session Secure-cookie proxy trust, rate-limit keying) read it via
    // transportPeer() and never from forwarding headers.
    const incoming = (
      c.env as { incoming?: { socket?: { remoteAddress?: string } } } | undefined
    )?.incoming;
    const remoteAddress = incoming?.socket?.remoteAddress;
    if (typeof remoteAddress === "string" && remoteAddress.length > 0) {
      installTransportPeer(c.req.raw, remoteAddress);
    }
    const requestIdHeader = c.req.header("x-request-id");
    const requestId =
      requestIdHeader && requestIdHeader.length > 0 ? requestIdHeader : createRequestId();
    c.set("requestId", requestId);
    await next();
    c.res.headers.set("x-request-id", requestId);
  });

  // Reject untrusted Host headers first, before any other processing, so a
  // spoofed Host can't reach URL/link construction or cache keying.
  if (trustedHosts && trustedHosts.length > 0) {
    const allowedHosts = new Set(trustedHosts.map((value) => value.toLowerCase()));
    app.use("*", async (c, next) => {
      const host = (c.req.raw.headers.get("host") || "").toLowerCase();
      if (!allowedHosts.has(host)) {
        return new Response("Invalid Host", { status: 400 });
      }
      return next();
    });
  }

  // FRAMEWORK_CONTRACT.md middleware order: CORS precedes Compression. (CORS
  // preflight short-circuits before the body is ever compressed.)
  if (corsOptions || securityHeadersConfig) {
    app.use("*", async (c, next) => {
      if (corsOptions) {
        const preflightResponse = createCorsPreflightResponse(c.req.raw, corsOptions);
        if (preflightResponse) {
          return preflightResponse;
        }
      }

      await next();

      if (corsOptions) {
        applyCorsHeaders(c.req.raw, c.res, corsOptions);
      }

      if (securityHeadersConfig) {
        applySecurityHeaders(c.res, securityHeadersConfig);
      }
    });
  }

  if (enableCompress) {
    // `Vary: Accept-Encoding` is ours to add — hono's compress middleware sets
    // Content-Encoding and weakens the ETag, and never touches Vary.
    //
    // Without it a shared cache can store the gzipped body and hand it to a
    // client that did not ask for gzip and cannot decode it. RFC 9110 §12.5.5
    // requires Vary whenever a response is content-negotiated, and the other
    // four SDKs in the conformance matrix all send it.
    //
    // Set on EVERY response this middleware could compress, not only the ones
    // it did: the header describes how the resource varies, so a cache that
    // stored the uncompressed representation needs it just as much.
    app.use("*", async (c, next) => {
      await next();
      const existing = c.res.headers.get("Vary");
      if (!existing) {
        c.res.headers.set("Vary", "Accept-Encoding");
      } else if (!/\baccept-encoding\b/i.test(existing)) {
        c.res.headers.set("Vary", `${existing}, Accept-Encoding`);
      }
    });
    app.use("*", compress());
  }

  // Innermost of the framework's response post-processors (request id,
  // security headers, CORS, Vary, compression all set headers after next()).
  // A handler or mounted sub-app may answer with an immutable-header Response
  // — `Response.redirect()`, a fetched response — which Hono passes through
  // as-is, so the first of those header writes threw and the response became
  // a bare 500.
  app.use("*", async (c, next) => {
    await next();
    const mutable = withMutableHeaders(c.res);
    if (mutable !== c.res) {
      c.res = mutable;
    }
  });

  // GET /health — contract shape { status, nucleus, version }. Registered before
  // the static/SSR routes so it always answers. The SSR server holds no Nucleus
  // pool (loaders connect per-request), so nucleus is "unconfigured" here.
  //
  // Suppressed when the app defines its own /health route: the user route must
  // win so it can report dependency-aware health (e.g. 503 when a backing store
  // is down) instead of the built-in returning a false 200. The catch-all below
  // then serves it like any other route.
  if (!appDefinesHealthRoute(routes)) {
    app.get("/health", (c) => c.json(healthBody(serverVersion)));
  }

  // FRAMEWORK_CONTRACT.md §4: /openapi.json + /docs. Like /health, suppressed
  // when the app defines its own route at the same path — the user route wins.
  if (openapi && !appDefinesSpecRoute(routes)) {
    const spec = serverOpenApiSpec(routes, openapi, serverVersion);
    app.get("/openapi.json", (c) => c.json(spec));
    app.get("/docs", (c) => c.html(swaggerDocsHtml(openapi.title)));
  }

  // Caller-mounted sub-apps (in-process API routes: tRPC, REST, SSE). They are
  // registered AFTER the framework batteries above and BEFORE the HTML
  // catch-all below, so neither shadowing direction can happen:
  //   - a mount cannot shadow /health & co. (registered earlier, and the
  //     prefixes are validated to not cover reserved paths at all);
  //   - the catch-all cannot shadow a mount (REALTIME_PLAN §5 records the
  //     class: an "*" route swallows everything a later caller adds — the
  //     mount is intentionally on the other side of it).
  // A request under the prefix that the sub-app does not match falls through
  // to whatever comes next (in ssr mode, the catch-all): mounting registers
  // routes under a prefix, it does not claim the namespace.
  // Requests whose global middleware already ran ahead of a mount. A request
  // under a prefix the sub-app does not match falls through to the catch-all,
  // which must not run the chain a second time.
  const globallyGatedRequests = new WeakSet<Request>();
  if (mount) {
    validateMountConfig(mount, isSsr);
    for (const [prefix, entry] of Object.entries(mount)) {
      const { app: subApp, globalMiddleware: runGlobal = true } = normalizeMountEntry(entry);
      if (runGlobal && appGlobalMiddleware.length > 0) {
        app.use("*", async (c, next) => {
          if (!mountPrefixCovers(prefix, c.req.path)) {
            return next();
          }
          globallyGatedRequests.add(c.req.raw);
          let result: Response;
          try {
            result = await runMiddlewareChain(appGlobalMiddleware, c.req.raw, {}, async () => {
              await next();
              return c.res;
            });
          } catch (error) {
            // The catch-all's treatment of middleware throws: a thrown
            // Response is a short-circuit, a ProblemError answers as 7807.
            if (isResponse(error)) {
              result = error;
            } else if (isProblemError(error)) {
              result = error.toResponse(c.req.path);
            } else {
              throw error;
            }
          }
          if (result !== c.res) {
            // Replace outright. Assigning over a finalized response would
            // merge the sub-app's headers into the middleware's own answer.
            c.res = undefined;
            c.res = result;
          }
        });
      }
      app.route(prefix, subApp);
    }
  }

  // SSR-only routes: static assets, image optimization, server islands, and the HTML
  // catch-all. "api"/"raw" modes skip these entirely.
  if (isSsr) {
  app.use(
    "/assets/*",
    serveStatic({
      root: resolvedDistDir,
      rewriteRequestPath: (p) => p,
    })
  );
  app.use("/assets/*", async (c, next) => {
    await next();
    if (
      c.res.status >= 200 &&
      c.res.status < 300 &&
      !c.res.headers.has("Cache-Control")
    ) {
      c.res.headers.set("Cache-Control", "public, max-age=31536000, immutable");
    }
  });

  app.use(
    "/public/*",
    serveStatic({
      root: resolvedDistDir,
      rewriteRequestPath: (p) => p,
    })
  );

  // Root-URL public assets (dev/prod parity): Vite copies public/ into the
  // dist ROOT (dist/favicon.svg), and `neutron-ts dev` serves those files at
  // their root URLs — but production only served /assets/* and /public/*
  // (which the build never writes), so an app's own /favicon.svg 404'd in
  // production while working in dev. Only files that came from public/ are
  // served: the dist root also holds the Vite manifest, build metadata and
  // (node/docker presets) a server bundle under server/. Route rules still
  // win, and anything else falls through to the route pipeline below, which
  // owns loaders, route rules, gating and the app cache.
  const rootAssetStatic = serveStatic({
    root: resolvedDistDir,
    rewriteRequestPath: (p) => p,
  });
  const isRootPublicAsset = createRootPublicAssetFilter(resolvedDistDir);
  app.use("*", async (c, next) => {
    const pathname = normalizePathname(c.req.path);
    if (pathname === null || !isRootPublicAsset(pathname)) {
      return next();
    }
    // Redirect rules own the URL space even over physical files.
    if (
      resolveRouteRuleRedirect(compiledRouteRules, pathname, new URL(c.req.url).search)
    ) {
      return next();
    }
    return rootAssetStatic(c, next);
  });

  app.get("/_neutron/image", async (c) => {
    const response = await handleImageRequest(c.req.raw, {
      publicDirs: [
        path.join(resolvedRootDir, "public"),
        resolvedDistDir,
        path.join(resolvedDistDir, "public"),
      ],
      cacheDir: path.join(resolvedRootDir, ".neutron", "image-cache"),
      remotePatterns: imageConfig?.remotePatterns,
    });
    return response;
  });

  app.get("/__neutron_island/:id", async (c) => {
    const islandId = c.req.param("id");
    const html = await (ssrServer?.handleIslandRequest ?? handleIslandRequest)(
      islandId,
      c.req.query("t")
    );
    if (html === null) {
      return c.text("Not Found", 404);
    }

    return new Response(html, {
      status: 200,
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store",
      },
    });
  });

  app.all("*", async (c) => {
    const globalMiddleware = globallyGatedRequests.has(c.req.raw) ? [] : appGlobalMiddleware;
    const requestTrace: RequestTraceContext = {
      requestId: c.get("requestId") ?? createRequestId(),
      method: c.req.method.toUpperCase(),
      url: c.req.raw.url,
      pathname: c.req.path,
      startedAt: Date.now(),
    };

    emitHook(hooks?.onRequestStart, {
      requestId: requestTrace.requestId,
      method: requestTrace.method,
      url: requestTrace.url,
      pathname: requestTrace.pathname,
      startedAt: requestTrace.startedAt,
    });

    const finalize = (
      response: Response,
      routeMeta?: { routeId?: string; routePath?: string; routeMode?: "static" | "app" }
    ): Response => {
      // Middleware, loaders, actions and resource routes may hand back an
      // immutable-header Response; route-rule headers are set right below.
      response = withMutableHeaders(response);
      const normalizedRequestPath = normalizePathname(requestTrace.pathname) || "/";
      applyRouteRuleHeadersToResponse(
        response,
        resolveRouteRuleHeaders(compiledRouteRules, normalizedRequestPath)
      );

      const endedAt = Date.now();
      emitHook(hooks?.onRequestEnd, {
        requestId: requestTrace.requestId,
        method: requestTrace.method,
        url: requestTrace.url,
        pathname: requestTrace.pathname,
        startedAt: requestTrace.startedAt,
        endedAt,
        durationMs: endedAt - requestTrace.startedAt,
        status: response.status,
        routeId: routeMeta?.routeId,
        routePath: routeMeta?.routePath,
        routeMode: routeMeta?.routeMode,
        cacheState: response.headers.get("x-neutron-cache") || undefined,
      });
      return response;
    };

    try {
      const originalPathname = normalizePathname(c.req.path);
      if (originalPathname === null) {
        return finalize(c.text("Bad Request", 400));
      }
      const requestUrl = new URL(c.req.raw.url);

      const method = requestTrace.method;

      const redirect = resolveRouteRuleRedirect(
        compiledRouteRules,
        originalPathname,
        requestUrl.search
      );
      if (redirect) {
        return finalize(
          new Response(null, {
            status: redirect.status,
            headers: {
              Location: redirect.location,
            },
          })
        );
      }

      const rewrite = resolveRouteRuleRewrite(compiledRouteRules, originalPathname);
      const effectivePathname = rewrite?.pathname || originalPathname;

      const match = router.match(effectivePathname);

      // Resource route (method handlers, no component): an HTTP endpoint, not
      // a page. Dispatched BEFORE the static fast path and the static-mode
      // 404 — a prebuilt file at this path must never answer for an endpoint,
      // and the resource route is the route table's authority for its path.
      // Endpoints run per request, so their middleware (global + layout +
      // route, FRAMEWORK_CONTRACT §5 order) runs every time: the A-020
      // prerender/gate contradiction cannot arise because resource routes are
      // never prerendered in the first place.
      if (match && isResourceRoute(match.route)) {
        if (!ssrServer) {
          return finalize(
            internalError("Resource route SSR runtime is unavailable").toResponse(
              requestTrace.pathname
            ),
            { routeId: match.route.id, routePath: match.route.path }
          );
        }
        const response = await handleResourceRouteRequest(
          c.req.raw,
          match,
          ssrServer,
          routeModuleCache,
          globalMiddleware,
          (error) =>
            emitHook(hooks?.onError, {
              requestId: requestTrace.requestId,
              method: requestTrace.method,
              pathname: requestTrace.pathname,
              source: "request",
              routeId: match.route.id,
              routePath: match.route.path,
              error: toError(error),
            })
        );
        // A mutating endpoint can change what app pages show: fence fills
        // that were in flight while it ran (TS-07 completion fence), and
        // evict the pages its `x-neutron-invalidate` header names. No
        // pre-handler call: a resource route's own path holds no cache
        // entries, and this bump already fences GETs that began earlier.
        if (isMutationMethod(requestTrace.method)) {
          await invalidateMutatedPath(effectivePathname, response);
        }
        return finalize(response, {
          routeId: match.route.id,
          routePath: match.route.path,
          routeMode: "app",
        });
      }

      // Static dist HTML may only answer when no non-static app route claims
      // the path. The pre-seeded cache (buildStaticHtmlCache walks dist/ at
      // boot, including index.html -> "/") used to short-circuit BEFORE the
      // router, so an app route at "/" — e.g. an auth-gated home — was
      // silently shadowed by the built shell and its loader never ran.
      // A gated route may never answer from a prebuilt file: that path returns
      // before renderAppRoute, the only place its middleware runs. Falling
      // through re-renders it, which is slower and correct. A-020.
      const gated = match ? routeChainIsGated(match) : false;
      const staticAllowed = !match || (isStaticRoute(match) && !gated);

      // Global middleware is a different case and does not disqualify the fast
      // path, because it can be run right here. It is never registered as
      // app-level middleware — it is only passed into renderAppRoute — so
      // before this it did not run for static hits at all. Now it runs, and a
      // response it returns (a redirect, a 403) wins over the prebuilt file.
      // The framework's own 404s below run through it the same way.
      const withGlobalMiddleware = async (respond: () => Response): Promise<Response> => {
        if (globalMiddleware.length === 0) {
          return respond();
        }
        return runMiddlewareChain(globalMiddleware, c.req.raw, {}, async () => respond());
      };

      // A warm static-HTML entry must never answer a loader-data request:
      // the same `!isJsonRequest` eligibility the disk path applies (TS-09).
      // Without it, warming the HTML cache changed the response to a later
      // X-Neutron-Data request from JSON into HTML.
      if (
        (method === "GET" || method === "HEAD") &&
        !isJsonRequest(c.req.raw) &&
        staticAllowed
      ) {
        const cached = staticHtmlCache.get(effectivePathname);
        if (cached) {
          const response = await withGlobalMiddleware(() =>
            createStaticHtmlResponse(
              cached,
              c.req.raw,
              method,
              staticRouteHeaders.get(effectivePathname)
            )
          );
          return finalize(response, {
            routePath: effectivePathname,
            routeMode: "static",
          });
        }
      }

      if ((method === "GET" || method === "HEAD") && !isJsonRequest(c.req.raw) && staticAllowed) {
        const html = tryReadStaticHtml(resolvedDistDir, effectivePathname);
        if (html !== null) {
          const entry = createStaticHtmlEntry(html);
          staticHtmlCache.set(effectivePathname, entry);
          const response = await withGlobalMiddleware(() =>
            createStaticHtmlResponse(
              entry,
              c.req.raw,
              method,
              staticRouteHeaders.get(effectivePathname)
            )
          );
          return finalize(response, {
            routePath: effectivePathname,
            routeMode: "static",
          });
        }
      }

      if (!match) {
        // Render the app's own not-found page through its layout chain, so a
        // 404 looks like the rest of the application rather than two words of
        // plain text. Falls back to the bare response when the app has no
        // `not-found.tsx`.
        const notFoundMatch = ssrServer ? router.matchNotFound(effectivePathname) : null;
        if (notFoundMatch) {
          try {
            const rendered = await handleAppRouteRequest(
              c.req.raw,
              notFoundMatch,
              ssrServer!,
              clientEntryScriptSrc,
              stylesheetsFor(notFoundMatch.route.id),
              routeModuleCache,
              loaderDataCacheStore,
              requestTrace,
              hooks,
              globalMiddleware
            );
            // The page renders as a normal route, which makes it a 200. The
            // status is the part that matters to crawlers and monitoring — but
            // only a 200 is rewritten: a response the not-found route (or its
            // middleware) produced deliberately — a redirect, a 403, a
            // ProblemError — is the real outcome and must pass through
            // (TS-27).
            const finalResponse =
              rendered.status === 200
                ? new Response(rendered.body, { status: 404, headers: rendered.headers })
                : rendered;
            return finalize(finalResponse, {
                routeId: notFoundMatch.route.id,
                routePath: notFoundMatch.route.path,
                routeMode: notFoundMatch.route.config.mode,
              }
            );
          } catch (error) {
            // A broken not-found page must not turn a 404 into a 500 — that
            // converts "page missing" into "site down" for every bad URL.
            emitHook(hooks?.onError, {
              requestId: requestTrace.requestId,
              method: requestTrace.method,
              pathname: requestTrace.pathname,
              source: "render",
              routeId: notFoundMatch.route.id,
              routePath: notFoundMatch.route.path,
              error: toError(error),
            });
          }
          // The not-found render already ran the global middleware chain.
          return finalize(defaultNotFoundResponse(c.req.raw, originalPathname));
        }
        // FRAMEWORK_CONTRACT §5: global middleware (auth gates) runs for every
        // request, matched or not — it used to run here only when the app had
        // a not-found.tsx to render.
        return finalize(
          await withGlobalMiddleware(() => defaultNotFoundResponse(c.req.raw, originalPathname))
        );
      }

      // A non-app route that reached here was not answered from dist, and
      // normally that is a 404 — a static route is meant to come off disk.
      // A GATED one is the exception: it got here precisely because it must not
      // be served from disk, and 404ing it would turn "this page is protected"
      // into "this page does not exist". Render it through the app path so its
      // middleware actually runs and decides. `neutron build`
      // (neutron-cli/src/commands/build.ts) and core's renderStatic both
      // refuse to prerender this combination, so in practice this is reached
      // only with a dist built before the gate was added — or by files placed
      // in dist by anything other than those two. A-020.
      if (
        match.route.file.includes("_layout") ||
        (match.route.config.mode !== "app" && !isJsonRequest(c.req.raw) && !gated)
      ) {
        // Same content-negotiated default as an unmatched path, behind the
        // same global middleware: a static route whose file is missing from
        // dist is still a 404 a human or a machine will read.
        return finalize(
          await withGlobalMiddleware(() => defaultNotFoundResponse(c.req.raw, originalPathname)),
          {
            routeId: match.route.id,
            routePath: match.route.path,
            routeMode: match.route.config.mode,
          }
        );
      }

      if (!ssrServer) {
        return finalize(c.text("App route SSR runtime is unavailable", 500), {
          routeId: match.route.id,
          routePath: match.route.path,
          routeMode: "app",
        });
      }

      if (isMutationMethod(method)) {
        // Advance the invalidation epoch BEFORE deleting (TS-07): an
        // in-flight cacheable GET that started before this mutation must not
        // publish its (now stale) fill afterwards.
        await invalidateMutatedPath(effectivePathname);
      }

      const appCacheMaxAge = match.route.config.cache?.maxAge ?? 0;
      // SECURITY: the app-response cache is keyed on the full representation
      // identity (variant + path + query + origin + vary-able headers), so it
      // is shared across users. Never read or store a response for a request
      // that carries credentials (Cookie/Authorization), since it may be
      // authenticated/personalized and would otherwise leak one user's
      // rendered page to others. Conditional/no-cache requests without
      // credentials still revalidate normally.
      const appCacheKey =
        appCacheMaxAge > 0 && !requestCarriesCredentials(c.req.raw)
          ? buildAppCacheKey(c.req.raw, effectivePathname)
          : null;
      const cacheReadsPermitted =
        appCacheKey !== null && (method === "GET" || method === "HEAD");
      const requestEpoch = appCacheEpoch;
      const appGeneration = await appResponseCacheStore.getGeneration!();
      const loaderGeneration = await loaderDataCacheStore.getGeneration!();

      // The shared-cache boundary is applied INSIDE the route middleware
      // chain (see renderAppRoute): a cache hit still executes every request
      // middleware — auth gates, rate limits, audit hooks (TS-02). There is
      // deliberately NO response-level single-flight sharing anymore (TS-03):
      // joining a pending Response shared its Set-Cookie headers across
      // concurrent cookieless requests, minting duplicate session cookies.
      const responseCacheBoundary = cacheReadsPermitted
        ? {
            enabled: true,
            read: async (): Promise<Response | null> => {
              const pendingStore = appPendingStores.get(appCacheKey!);
              if (pendingStore) {
                await pendingStore;
              }
              const hit = await readCachedAppResponse(
                appResponseCacheStore,
                appCacheKey!,
                c.req.raw,
                method
              );
              return hit;
            },
            store: (response: Response) => {
              if (method !== "GET" || !appCacheKey) {
                return;
              }
              // maybeStoreAppResponse applies eligibility (status, cookies,
              // cache-control, Vary, byte budget) and re-checks the epoch
              // immediately before committing the entry.
              const store = maybeStoreAppResponse(
                appResponseCacheStore,
                appCacheKey,
                response,
                appCacheMaxAge,
                c.req.raw.headers.get("cache-control"),
                () => appCacheEpoch === requestEpoch,
                appGeneration
              ).catch(() => {});
              appPendingStores.set(appCacheKey, store);
              void store.then(() => {
                if (appPendingStores.get(appCacheKey!) === store) {
                  appPendingStores.delete(appCacheKey!);
                }
              });
            },
          }
        : undefined;

      const response = await handleAppRouteRequest(
        c.req.raw,
        match,
        ssrServer,
        clientEntryScriptSrc,
        stylesheetsFor(match.route.id),
        routeModuleCache,
        loaderDataCacheStore,
        requestTrace,
        hooks,
        globalMiddleware,
        responseCacheBoundary,
        // Loader fills share the app-cache generation fence (TS-07): the
        // epoch is captured per request and re-checked immediately before a
        // loader result is committed, so a GET that began before a mutation
        // completed cannot republish the pre-mutation loader data.
        { stillValid: () => appCacheEpoch === requestEpoch, generation: loaderGeneration }
      );

      if (isMutationMethod(method)) {
        // Advance the epoch AGAIN before completion invalidation (TS-07
        // round 2): a GET that STARTED during the mutation captured the
        // post-pre-invalidation epoch, and its fill must not publish after
        // this final delete. The pre-mutation bump alone fenced only GETs
        // that began before the mutation.
        // GETs may have published old data after the start invalidation while
        // the action was running. Evict those entries and fence remote fills
        // at completion even when the action emits no invalidation header.
        await invalidateMutatedPath(effectivePathname, response);
      }

      return finalize(response, {
        routeId: match.route.id,
        routePath: match.route.path,
        routeMode: "app",
      });
    } catch (error) {
      emitHook(hooks?.onError, {
        requestId: requestTrace.requestId,
        method: requestTrace.method,
        pathname: requestTrace.pathname,
        source: "request",
        error: toError(error),
      });
      // A ProblemError that escaped the render pipeline — e.g. thrown from
      // route or global middleware, which runMiddlewareChain does not convert
      // — answers as RFC 7807 (FRAMEWORK_CONTRACT.md §2), not a bare 500.
      if (isProblemError(error)) {
        return finalize(error.toResponse(requestTrace.pathname));
      }
      // A thrown Response is a documented middleware short-circuit — and the
      // denial path of the framework's own requireOrganization()/
      // requirePermissions() (enterprise-auth). Loaders and actions get
      // `isResponse` treatment in render-app-route; middleware
      // throws land here and must answer as themselves, not as a 500.
      if (isResponse(error)) {
        return finalize(error);
      }
      return finalize(new Response("Internal Server Error", { status: 500 }));
    }
  });
  } // end if (isSsr)

  // "api" mode answers a clean 404 for anything the user didn't mount. Use
  // app.notFound (not an app.all("*") route) so it fires only when nothing matched —
  // an "*" route would shadow routes the caller mounts later on the returned `app`.
  // "raw" mode intentionally adds nothing — unmatched paths get Hono's default 404.
  if (mode === "api") {
    app.notFound((c) =>
      notFoundError(`No route matches ${c.req.path}`).toResponse(c.req.path)
    );
  }

  // TS-22: the actual-byte cap lives where the adapter hands the Request to
  // the app — the middleware cannot substitute the body, but the adapter
  // can. Content-Length stays as the early check (both in the middleware
  // and here, so the cap holds even for requests that never see it).
  let fetchFn: (request: Request) => Response | Promise<Response> = app.fetch;
  if (options.maxRequestBodyBytes !== undefined && options.maxRequestBodyBytes > 0) {
    const cap = options.maxRequestBodyBytes;
    fetchFn = (request: Request): Response | Promise<Response> => {
      const declared = request.headers.get("content-length");
      if (declared !== null && Number(declared) > cap) {
        return new Response("Request body too large", {
          status: 413,
          headers: { "Content-Type": "text/plain" },
        });
      }
      return app.fetch(capRequestBody(request, cap));
    };
  }

  const server = serve({
    fetch: fetchFn,
    port,
    hostname: host,
    // Left on, @hono/node-server replaces globalThis.Response and Request with
    // its own classes for the process lifetime. A Response subclass defined
    // before this call (a Next-compat NextResponse evaluated at import), and
    // every Response.json()/redirect() result, then fail `instanceof Response`
    // in app code. The lightweight classes only save allocations on simple
    // bodies, which Neutron's header-writing pipeline mostly materializes
    // anyway.
    overrideGlobalObjects: false,
  });
  // `serve()` returns before the socket is bound. Resolve once it is, so the
  // caller can close() at once (instead of ERR_SERVER_NOT_RUNNING), `url`
  // names the bound port (also for port 0), and a bind failure (EADDRINUSE)
  // rejects here rather than surfacing as an unhandled 'error' event.
  await new Promise<void>((resolve, reject) => {
    if ((server as { listening?: boolean }).listening) return resolve();
    const onError = (error: Error) => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
  });

  // Transport axis: attach a WebSocket server to the live HTTP server, independent of
  // rendering mode. Raw `ws` in noServer mode so the framework owns the upgrade handshake
  // and the caller just attaches `wss.on("connection", ...)`.
  let wss: WebSocketServer | undefined;
  if (websocket) {
    const { WebSocketServer: WSServer } = await import("ws");
    const wsOptions = websocket === true ? {} : websocket;
    wss = new WSServer({ noServer: true });
    const httpServer = server as unknown as import("node:http").Server;
    httpServer.on("upgrade", (req, socket, head) => {
      // The upgrade runs the configured path filter AND the pre-upgrade
      // authorizer with a hard deadline (TS-28): the handshake is not part
      // of the HTTP middleware chain, so without an explicit hook it is
      // subject to no authorization at all. Fail closed on any outcome
      // other than an explicit `true`.
      const reject = () => socket.destroy();
      if (wsOptions.path) {
        try {
          const pathname = new URL(req.url ?? "/", "http://localhost").pathname;
          if (pathname !== wsOptions.path) {
            reject();
            return;
          }
        } catch {
          reject();
          return;
        }
      }
      if (!wsOptions.authorize) {
        wss!.handleUpgrade(req, socket, head, (ws) => {
          wss!.emit("connection", ws, req);
        });
        return;
      }
      const timer = setTimeout(() => reject(), 5_000);
      void (async () => {
        try {
          const allowed = await wsOptions.authorize!(req);
          if (!allowed || socket.destroyed) {
            reject();
            return;
          }
          wss!.handleUpgrade(req, socket, head, (ws) => {
            wss!.emit("connection", ws, req);
          });
        } catch {
          reject();
        } finally {
          clearTimeout(timer);
        }
      })();
    });
  }

  // One shutdown per server (TS-29): repeated close() calls join the same
  // teardown instead of racing a second one through still-draining stages.
  let closingPromise: Promise<void> | undefined;
  return {
    app,
    server,
    wss,
    close: () => {
      // Shutdown order (TS-29): stop accepting and DRAIN in-flight requests
      // first — they may still need the SSR runtime — then tear down the
      // runtime. The previous order closed Vite first, failing every render
      // still in flight. Stages run SEQUENTIALLY (round 2): the recorded
      // order was constructed correctly but then executed via
      // `Promise.allSettled(teardown.map(...))`, which starts every stage
      // at once — Vite closed while renders were still draining. Each stage
      // still runs even when an earlier one fails; errors are combined so a
      // failure anywhere is observable.
      closingPromise ??= (async () => {
        const teardown: Array<() => Promise<void>> = [
          // Tear down WebSockets before draining HTTP. An upgraded WS socket is
          // NOT an idle HTTP keep-alive, so server.close()/closeIdleConnections()
          // won't reap it — a live client would otherwise hold the drain open
          // until the caller's shutdown timeout. Forcibly terminate each live
          // socket, then await the WS server's own close.
          () =>
            wss
              ? (async () => {
                  for (const client of wss.clients) {
                    client.terminate();
                  }
                  await new Promise<void>((resolve) => wss!.close(() => resolve()));
                })()
              : Promise.resolve(),
          // Await server.close's callback so in-flight requests actually drain
          // (a fire-and-forget resolved before any draining). Close idle
          // keep-alive sockets so they don't hold the drain open indefinitely.
          () =>
            new Promise<void>((resolve, reject) => {
              server.close((err) => (err ? reject(err) : resolve()));
              (server as { closeIdleConnections?: () => void }).closeIdleConnections?.();
            }),
          // Last: the SSR runtime. Nothing still rendering depends on it now.
          () => ssrServer?.close() ?? Promise.resolve(),
        ];
        const errors: unknown[] = [];
        for (const stage of teardown) {
          try {
            await stage();
          } catch (error) {
            errors.push(error);
          }
        }
        if (errors.length > 0) {
          throw new AggregateError(errors, "Neutron server shutdown failed");
        }
      })();
      return closingPromise;
    },
    url: (() => {
      const address = server.address?.();
      if (address && typeof address === "object" && "port" in address) {
        return `http://${host}:${address.port}`;
      }
      return `http://${host}:${port}`;
    })(),
    moduleSource: ssrServer?.source ?? null,
  };
}

async function handleAppRouteRequest(
  request: Request,
  match: RouteMatch,
  ssrServer: SsrServer,
  clientEntryScriptSrc: string | null,
  stylesheetHrefs: string[],
  moduleCache: Map<string, Promise<RouteModule>>,
  loaderDataCache: NeutronLoaderCacheStore,
  requestTrace: RequestTraceContext,
  hooks?: NeutronServerHooks,
  globalMiddleware?: MiddlewareFn[],
  responseCache?: {
    enabled: boolean;
    read: () => Promise<Response | null>;
    store: (response: Response) => void;
  },
  loaderCacheFence?: {
    generation?: string;
    stillValid: () => boolean;
  }
): Promise<Response> {
  // Dev module-loading adapter: load every route/layout module through the
  // Vite SSR runtime, then hand the loaded map to the shared render core —
  // together with the runtime's renderer pair, resolved through the SAME
  // graph so hooks and renderer share one Preact instance.
  const allRoutes = [...match.layouts, match.route];
  const routeModules = new Map<string, RouteModule>();
  await Promise.all(
    allRoutes.map(async (route) => {
      routeModules.set(route.id, await loadRouteModule(ssrServer, route.file, moduleCache));
    })
  );
  return renderAppRoute(request, match, routeModules, {
    clientEntryScriptSrc,
    stylesheetHrefs,
    loaderDataCache,
    requestTrace,
    hooks,
    globalMiddleware,
    responseCache,
    loaderCacheFence,
    renderer: ssrServer.renderer,
  });
}

/**
 * Serve one resource-route request: load the module chain through the SSR
 * runtime, run middleware (global outermost, then layouts, then the route —
 * the exact chain renderAppRoute assembles, so a gate on a parent layout
 * covers endpoints in its subtree like it covers pages), and dispatch the
 * method handler. Loaders never run for resource routes.
 */
async function handleResourceRouteRequest(
  request: Request,
  match: RouteMatch,
  ssrServer: SsrServer,
  moduleCache: Map<string, Promise<RouteModule>>,
  globalMiddleware: MiddlewareFn[] | undefined,
  onError: (error: unknown) => void
): Promise<Response> {
  const allRoutes = [...match.layouts, match.route];
  const routeModules = new Map<string, RouteModule>();
  await Promise.all(
    allRoutes.map(async (route) => {
      routeModules.set(route.id, await loadRouteModule(ssrServer, route.file, moduleCache));
    })
  );

  const middlewares: MiddlewareFn[] = [];
  if (globalMiddleware && globalMiddleware.length > 0) {
    middlewares.push(...globalMiddleware);
  }
  for (const route of allRoutes) {
    const module = routeModules.get(route.id);
    if (module?.middleware) {
      middlewares.push(module.middleware);
    }
  }

  const context: AppContext = {};
  const module = routeModules.get(match.route.id);
  return runMiddlewareChain(middlewares, request, context, async () =>
    dispatchResourceHandler(
      module as Partial<Record<ResourceRouteMethod, ResourceRouteHandler>>,
      request,
      match.params,
      context,
      { onError }
    )
  );
}









function isStaticRoute(match: RouteMatch): boolean {
  if (match.route.file.includes("_layout")) {
    return true;
  }
  return match.route.config.mode === "static";
}

/**
 * Whether anything in the matched chain gates the request with `middleware`.
 *
 * Serving a prebuilt file returns before `renderAppRoute`, which is the only
 * place route and layout middleware run — so a `mode: "static"` route that
 * exports `middleware` was prerendered by SSG and then served from disk with
 * its gate never running. No error, no warning; the page was simply public.
 * A-020.
 *
 * Layouts count: `renderAppRoute` collects middleware from every route in
 * `[...match.layouts, match.route]`, so an auth gate on a parent layout covers
 * a static child. `undefined` counts as gated — see `Route.hasMiddleware`.
 */
function routeChainIsGated(match: RouteMatch): boolean {
  return [...match.layouts, match.route].some((route) => route.hasMiddleware !== false);
}

function loadRouteModule(
  ssrServer: SsrServer,
  routeFile: string,
  moduleCache: Map<string, Promise<RouteModule>>
): Promise<RouteModule> {
  let pending = moduleCache.get(routeFile);
  if (!pending) {
    pending = ssrServer.ssrLoadModule(routeFile).then((loaded) => loaded as RouteModule);
    moduleCache.set(routeFile, pending);
    // A rejected import must not poison the route for the process lifetime
    // (TS-29): delete the cached promise on failure (only if it is still the
    // same one) so a transient load error can be retried on the next request.
    void pending.catch(() => {
      if (moduleCache.get(routeFile) === pending) {
        moduleCache.delete(routeFile);
      }
    });
  }
  return pending;
}

/** Normalize a request pathname for matching/caching. Exported for tests. */
export function normalizePathname(pathname: string): string | null {
  let decoded: string;
  try {
    decoded = decodeURIComponent(pathname || "/");
  } catch {
    return null;
  }

  // Traversal is a whole segment equal to "..", not a substring: `/a..b`
  // and `/v1.2..3` are legal paths, `/a/../b` is not.
  if (!decoded.startsWith("/") || decoded.split("/").includes("..")) {
    return null;
  }

  if (decoded.length > 1 && decoded.endsWith("/")) {
    return decoded.slice(0, -1);
  }

  return decoded;
}

// Framework paths a mount prefix must never take over. Batteries registered
// before the mounts would keep answering anyway (Hono resolves in registration
// order), so a mount that covers one would silently never see those requests —
// refuse it up front instead. /assets and /public are ssr-mode static trees.
const RESERVED_MOUNT_PATHS: Array<{ path: string; ssrOnly: boolean; what: string }> = [
  { path: "/health", ssrOnly: false, what: "GET /health (FRAMEWORK_CONTRACT.md §7)" },
  { path: "/openapi.json", ssrOnly: false, what: "GET /openapi.json (FRAMEWORK_CONTRACT.md §4)" },
  { path: "/docs", ssrOnly: false, what: "GET /docs (FRAMEWORK_CONTRACT.md §4)" },
  { path: "/_neutron/image", ssrOnly: true, what: "the image optimizer route" },
  { path: "/__neutron_island", ssrOnly: true, what: "the server-islands route" },
  { path: "/assets", ssrOnly: true, what: "the built-assets static route" },
  { path: "/public", ssrOnly: true, what: "the public-dir static route" },
];

/** `"/api"` covers `"/api/anything"` — segment-boundary prefix, not substring. */
function mountPrefixCovers(prefix: string, path: string): boolean {
  if (prefix === path) return true;
  return path.startsWith(prefix.endsWith("/") ? prefix : prefix + "/");
}

/**
 * Fail fast on mount configurations whose behavior would be surprising.
 * Every rule here encodes a shadowing/conflict case that would otherwise be
 * resolved silently by registration order or Hono fall-through.
 */
function isHonoApp(value: unknown): value is NeutronMountApp {
  return (
    !!value &&
    typeof (value as { fetch?: unknown }).fetch === "function" &&
    typeof (value as { route?: unknown }).route === "function"
  );
}

function normalizeMountEntry(entry: NeutronMountApp | NeutronMount): NeutronMount {
  return isHonoApp(entry) ? { app: entry } : entry;
}

function validateMountConfig(
  mount: Record<string, NeutronMountApp | NeutronMount>,
  isSsr: boolean
): void {
  const prefixes = Object.keys(mount);

  for (const prefix of prefixes) {
    const entry = mount[prefix];
    const subApp = isHonoApp(entry) ? entry : (entry as NeutronMount | undefined)?.app;
    if (typeof prefix !== "string" || prefix.length === 0 || !prefix.startsWith("/")) {
      throw new Error(
        `Neutron: mount prefix ${JSON.stringify(prefix)} is invalid — prefixes are absolute ` +
          `path prefixes like "/api". For the site root use mode: "raw".`
      );
    }
    if (prefix !== "/" && prefix.endsWith("/")) {
      throw new Error(
        `Neutron: mount prefix "${prefix}" must not end with "/" — write "${prefix.slice(0, -1)}".`
      );
    }
    if (prefix.split("/").some((segment) => segment.includes("*") || segment.includes(":"))) {
      throw new Error(
        `Neutron: mount prefix "${prefix}" must be literal — dynamic segments are not ` +
          `supported in mount prefixes (match inside the sub-app instead).`
      );
    }
    if (!isHonoApp(subApp)) {
      throw new Error(
        `Neutron: mount["${prefix}"] is not a Hono instance — pass the app itself ` +
          `(e.g. new Hono()) or { app, globalMiddleware }, not a handler or a fetch function.`
      );
    }

    for (const reserved of RESERVED_MOUNT_PATHS) {
      if (reserved.ssrOnly && !isSsr) continue;
      if (mountPrefixCovers(prefix, reserved.path)) {
        const hint =
          prefix === "/"
            ? ' Mounting at "/" is not supported — for a fully caller-owned router use mode: "raw".'
            : " Choose a different prefix.";
        throw new Error(
          `Neutron: mount prefix "${prefix}" would take over ${reserved.what} — ` +
            `reserved framework paths cannot be mounted over.${hint}`
        );
      }
    }
  }

  // Pairwise: exact duplicates and nested prefixes. A sub-app that answers a
  // path the outer prefix also reaches makes the winner an accident of object
  // key order; say it once at startup instead.
  for (let i = 0; i < prefixes.length; i++) {
    for (let j = i + 1; j < prefixes.length; j++) {
      const a = prefixes[i];
      const b = prefixes[j];
      if (mountPrefixCovers(a, b) || mountPrefixCovers(b, a)) {
        throw new Error(
          `Neutron: mount prefixes "${a}" and "${b}" overlap — nested/duplicate mount ` +
            `prefixes are not supported (registration order would silently decide ` +
            `which sub-app answers). Mount one sub-app and route the sub-prefix inside it.`
        );
      }
    }
  }
}

function applyRouteRuleHeadersToResponse(
  response: Response,
  ruleHeaders: Array<{ headers: Record<string, string> }>
): void {
  for (const rule of ruleHeaders) {
    for (const [name, value] of Object.entries(rule.headers)) {
      try {
        if (!response.headers.has(name)) {
          response.headers.set(name, value);
        }
      } catch {
        // Some Response instances can expose immutable headers (e.g. redirects).
      }
    }
  }
}

function buildStaticHtmlCache(distDir: string): Map<string, StaticHtmlEntry> {
  const cache = new Map<string, StaticHtmlEntry>();
  if (!fs.existsSync(distDir)) {
    return cache;
  }

  const pending = [distDir];
  while (pending.length > 0) {
    const currentDir = pending.pop();
    if (!currentDir) {
      continue;
    }

    const entries = fs.readdirSync(currentDir, { withFileTypes: true });
    for (const entry of entries) {
      const absolutePath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        pending.push(absolutePath);
        continue;
      }

      if (!entry.isFile() || !entry.name.endsWith(".html")) {
        continue;
      }

      const relativePath = path.relative(distDir, absolutePath);
      const routePath = toRoutePath(relativePath);
      if (!routePath || cache.has(routePath)) {
        continue;
      }

      try {
        const body = fs.readFileSync(absolutePath, "utf-8");
        cache.set(routePath, createStaticHtmlEntry(body));
      } catch (err) {
        console.error(`[neutron] Failed to read static file ${absolutePath}:`, err);
        // Skip this file and continue with others
      }
    }
  }

  return cache;
}

function toRoutePath(relativeHtmlPath: string): string | null {
  const normalized = relativeHtmlPath.split(path.sep).join("/");
  if (!normalized.endsWith(".html")) {
    return null;
  }

  if (normalized === "index.html") {
    return "/";
  }

  if (normalized.endsWith("/index.html")) {
    return `/${normalized.slice(0, -"/index.html".length)}`;
  }

  return `/${normalized.slice(0, -".html".length)}`;
}

function createStaticHtmlEntry(body: string): StaticHtmlEntry {
  const etag = createEntityTag(body);
  return {
    body,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "public, max-age=0, must-revalidate",
      ETag: etag,
    },
  };
}

function createStaticHtmlResponse(
  entry: StaticHtmlEntry,
  request: Request,
  method: string,
  routeHeaders?: Record<string, string>
): Response {
  const headers = new Headers(entry.headers);
  if (routeHeaders) {
    for (const [name, value] of Object.entries(routeHeaders)) {
      if (name.toLowerCase() === "content-length") {
        continue;
      }
      headers.set(name, value);
    }
  }

  const etag = headers.get("ETag");
  if (etag && requestHasMatchingEtag(request, etag)) {
    headers.delete("Content-Length");
    headers.set("x-neutron-cache", "REVALIDATED");
    return new Response(null, {
      status: 304,
      headers,
    });
  }

  if (method === "HEAD") {
    return new Response(null, {
      headers,
    });
  }

  return new Response(entry.body, {
    headers,
  });
}

function loadStaticRouteHeaders(distDir: string): Map<string, Record<string, string>> {
  const headersByRoute = new Map<string, Record<string, string>>();
  const headersPath = path.join(distDir, ".neutron-static-headers.json");
  if (!fs.existsSync(headersPath)) {
    return headersByRoute;
  }

  try {
    const raw = fs.readFileSync(headersPath, "utf-8");
    const parsed = JSON.parse(raw) as Record<string, Record<string, string>>;
    for (const [routePath, routeHeaders] of Object.entries(parsed)) {
      const normalized = normalizePathname(routePath);
      if (!normalized || typeof routeHeaders !== "object" || routeHeaders === null) {
        continue;
      }

      const normalizedHeaders: Record<string, string> = {};
      for (const [name, value] of Object.entries(routeHeaders)) {
        normalizedHeaders[name] = String(value);
      }

      headersByRoute.set(normalized, normalizedHeaders);
    }
  } catch (error) {
    console.warn("Failed to parse static route headers metadata:", error);
  }

  return headersByRoute;
}

function buildAppCacheKey(request: Request, pathname: string): string {
  const url = new URL(request.url);
  const variant = isJsonRequest(request) ? "json" : "html";
  // The key must carry every representation dimension the response can vary
  // on (TS-04): origin (multi-host apps), Accept-Language (loader
  // personalization), and the partial-data selection headers the render core
  // honors (X-Neutron-Data / X-Neutron-Routes). Without them one variant's
  // body can answer another variant's request.
  const acceptLanguage = request.headers.get("accept-language") ?? "";
  const dataHeader = request.headers.get("x-neutron-data") ?? "";
  const routesHeader = request.headers.get("x-neutron-routes") ?? "";
  return [
    variant,
    url.origin,
    pathname,
    url.search,
    acceptLanguage,
    dataHeader,
    routesHeader,
  ].join("\n");
}

/**
 * A request carries credentials when it has a Cookie or Authorization header.
 * Such requests may be authenticated/personalized and must never participate in
 * the shared, path-keyed app-response cache (read, store, or single-flight).
 */
function requestCarriesCredentials(request: Request): boolean {
  return request.headers.has("Authorization") || request.headers.has("Cookie");
}






/** Stores accept URL-encoded paths and decode exactly once. The router and
 * explicit invalidation parsing already produced decoded canonical paths;
 * re-encode their segments to preserve literal percent and reserved characters.
 */
function encodeCacheInvalidationPath(pathname: string): string {
  return pathname.split('/').map(segment => encodeURIComponent(segment)).join('/');
}

async function applyMutationInvalidationFromResponse(
  cache: NeutronAppCacheStore,
  pathname: string,
  response: Response
): Promise<void> {
  const directive = response.headers.get("x-neutron-invalidate");
  if (!directive) {
    return;
  }

  const tokens = directive
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean);

  if (tokens.length === 0) {
    return;
  }

  for (const token of tokens) {
    if (token === "*") {
      await cache.clear();
      return;
    }

    if (token === "self") {
      await cache.deleteByPath(encodeCacheInvalidationPath(pathname));
      continue;
    }

    const normalized = normalizePathname(token);
    if (normalized) {
      await cache.deleteByPath(encodeCacheInvalidationPath(normalized));
    }
  }
}

async function applyMutationInvalidationToLoaderDataCache(
  cache: NeutronLoaderCacheStore,
  pathname: string,
  response: Response
): Promise<void> {
  const directive = response.headers.get("x-neutron-invalidate");
  if (!directive) {
    return;
  }

  const tokens = directive
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean);

  if (tokens.length === 0) {
    return;
  }

  for (const token of tokens) {
    if (token === "*") {
      await cache.clear();
      return;
    }

    if (token === "self") {
      await cache.deleteByPath(encodeCacheInvalidationPath(pathname));
      continue;
    }

    const normalized = normalizePathname(token);
    if (normalized) {
      await cache.deleteByPath(encodeCacheInvalidationPath(normalized));
    }
  }
}

async function readCachedAppResponse(
  cache: NeutronAppCacheStore,
  key: string,
  request: Request,
  method: string
): Promise<Response | null> {
  const entry = await cache.get(key);
  if (!entry) {
    return null;
  }

  const headers = new Headers(entry.headers);
  headers.set("x-neutron-cache", "HIT");
  const etag = headers.get("ETag");
  if (etag && requestHasMatchingEtag(request, etag)) {
    headers.delete("Content-Length");
    return new Response(null, {
      status: 304,
      headers,
    });
  }

  if (method === "HEAD") {
    return new Response(null, {
      status: entry.status,
      statusText: entry.statusText,
      headers,
    });
  }

  // Byte-exact restore: hand the Response the exact stored octets. The
  // copy (slice) guards the stored entry against any downstream mutation of
  // the view; the buffer is a plain ArrayBuffer (entries are built from
  // arrayBuffer()).
  // A store may still hold an entry written before `body` became bytes
  // (core <= 0.2.2 stored a string, and a Redis entry outlives an upgrade):
  // encode it rather than serve an empty body.
  const stored: Uint8Array | string = entry.body;
  const body = typeof stored === "string" ? new TextEncoder().encode(stored) : stored.slice();
  return new Response(body.buffer as ArrayBuffer, {
    status: entry.status,
    statusText: entry.statusText,
    headers,
  });
}

/** Parse a Cache-Control header into lowercase directive names → values. */
function cacheControlDirectives(value: string | null): Map<string, string> {
  return new Map(
    (value ?? "")
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => {
        const [name, ...rest] = part.split("=");
        return [name.toLowerCase(), rest.join("=").replace(/^"|"$/g, "")];
      })
  );
}

/** Per-entry byte budget for a cached response body (TS-06). */
const APP_CACHE_MAX_BODY_BYTES = 2 * 1024 * 1024;

async function maybeStoreAppResponse(
  cache: NeutronAppCacheStore,
  key: string,
  response: Response,
  maxAgeSec: number,
  requestCacheControl: string | null,
  stillValid: () => boolean,
  generation: string
): Promise<void> {
  if (maxAgeSec <= 0 || response.status !== 200) {
    return;
  }

  if (response.headers.has("Set-Cookie")) {
    return;
  }

  // Full Cache-Control policy, parsed case-insensitively (TS-05): the old
  // substring test missed `NO-STORE`, `no-cache`, and request-side
  // directives entirely.
  const requestDirectives = cacheControlDirectives(requestCacheControl);
  const responseDirectives = cacheControlDirectives(
    response.headers.get("Cache-Control")
  );
  if (
    requestDirectives.has("no-store") ||
    requestDirectives.has("no-cache") ||
    responseDirectives.has("private") ||
    responseDirectives.has("no-store") ||
    responseDirectives.has("no-cache")
  ) {
    return;
  }

  // A response that reflects a per-request Origin (CORS) must not be shared —
  // storing it would replay one origin's Access-Control-Allow-Origin to
  // another. (The Accept-based JSON/HTML split is already part of the cache
  // key, and Cookie/Authorization requests are excluded before we get here.)
  if (response.headers.has("Access-Control-Allow-Origin")) {
    return;
  }

  // Vary: the stored entry is keyed on a fixed representation set (variant +
  // origin + accept-language + neutron data headers). Any other Vary field —
  // or `Vary: *` — means this response cannot be safely reused for that key.
  const varyFields = (response.headers.get("Vary") ?? "")
    .split(",")
    .map((field) => field.trim().toLowerCase())
    .filter(Boolean);
  const keyableVary = new Set(["accept", "accept-language", "x-neutron-data", "x-neutron-routes"]);
  if (varyFields.some((field) => field === "*" || !keyableVary.has(field))) {
    return;
  }

  // Cap the stored freshness by the response's own explicit lifetime, when
  // it declares one (TS-05).
  let effectiveMaxAge = maxAgeSec;
  const declared =
    responseDirectives.get("s-maxage") ?? responseDirectives.get("max-age");
  if (declared !== undefined) {
    if (!/^\d+$/.test(declared)) {
      return;
    }
    const declaredSec = Number(declared);
    if (!Number.isSafeInteger(declaredSec) || declaredSec <= 0) {
      return;
    }
    effectiveMaxAge = Math.min(effectiveMaxAge, declaredSec);
  }

  // Byte-exact body capture (TS-06): arrayBuffer() copies the response's
  // octets verbatim — no text transcoding — and the byte budget bounds a
  // single entry's memory. Content-Length is validated against the actual
  // bytes so the stored headers always describe the stored body.
  const declaredLength = Number(response.headers.get("Content-Length") || "0");
  if (Number.isFinite(declaredLength) && declaredLength > APP_CACHE_MAX_BODY_BYTES) {
    return;
  }
  const body = new Uint8Array(await response.clone().arrayBuffer());
  if (body.byteLength > APP_CACHE_MAX_BODY_BYTES) {
    return;
  }

  // Generation fence (TS-07): checked immediately before the (synchronous
  // for the memory store) insertion. A mutation that invalidated this path
  // while the body drained must win.
  if (!stillValid()) {
    return;
  }

  const headers = new Headers(response.headers);
  if (!headers.has("Cache-Control")) {
    headers.set("Cache-Control", `public, max-age=${effectiveMaxAge}`);
  }
  if (!headers.has("ETag")) {
    headers.set("ETag", createEntityTag(body));
  }
  headers.set("x-neutron-cache", "MISS");
  headers.set("Content-Length", String(body.byteLength));
  const headerPairs: [string, string][] = [];
  headers.forEach((value, name) => {
    headerPairs.push([name, value]);
  });

  await cache.setIfGeneration!(key, {
    status: response.status,
    statusText: response.statusText,
    headers: headerPairs,
    body,
    expiresAt: Date.now() + effectiveMaxAge * 1000,
  }, generation);
}

/**
 * Decide which root URLs the SSR server may answer from the dist root. `neutron
 * build` records the URL of every file it copied from public/ in
 * `.neutron-public-files.json`; only those are public. A dist without that
 * metadata (built before it existed, or by other tooling) falls back to: the
 * last segment carries an extension, no segment starts with "." (the Vite
 * manifest, build metadata) and the path is not under server/ (preset server
 * bundles).
 */
function createRootPublicAssetFilter(distDir: string): (pathname: string) => boolean {
  const metadataPath = path.join(distDir, PUBLIC_FILES_METADATA);
  if (fs.existsSync(metadataPath)) {
    let files: unknown = [];
    try {
      files = (JSON.parse(fs.readFileSync(metadataPath, "utf-8")) as { files?: unknown }).files;
    } catch {
      // Malformed metadata serves nothing rather than everything.
    }
    const allowed = new Set(
      Array.isArray(files) ? files.filter((f): f is string => typeof f === "string") : []
    );
    return (pathname) => allowed.has(pathname);
  }
  return (pathname) => {
    const segments = pathname.split("/").filter(Boolean);
    if (segments.length === 0 || segments[0] === "server") {
      return false;
    }
    if (segments.some((segment) => segment.startsWith("."))) {
      return false;
    }
    return segments[segments.length - 1].includes(".");
  };
}

const PUBLIC_FILES_METADATA = ".neutron-public-files.json";

function tryReadStaticHtml(distDir: string, pathname: string): string | null {
  if (pathname === "/") {
    const rootHtml = path.join(distDir, "index.html");
    if (fs.existsSync(rootHtml)) {
      return fs.readFileSync(rootHtml, "utf-8");
    }
    return null;
  }

  const relativePath = pathname.startsWith("/") ? pathname.slice(1) : pathname;
  const indexHtml = path.join(distDir, relativePath, "index.html");
  if (fs.existsSync(indexHtml)) {
    return fs.readFileSync(indexHtml, "utf-8");
  }

  return null;
}

/**
 * Deps that must stay EXTERNAL under the react-compat blanket
 * `noExternal: true` — see {@link RUNTIME_RUNNER_CJS_EXTERNALS} in
 * config.ts (single source; dev composes the same list).
 */
const RUNNER_CJS_EXTERNALS: readonly string[] = RUNTIME_RUNNER_CJS_EXTERNALS;

async function createSsrServer(
  rootDir: string,
  routesDir: string,
  runtime: NeutronRuntime
): Promise<SsrServer> {
  try {
    const vite = await import("vite");
    const loadedConfig = await vite.loadConfigFromFile(
      { command: "serve", mode: "production" },
      undefined,
      rootDir
    );

    const userConfig = loadedConfig?.config || {};
    const runtimeAliases = resolveRuntimeAliases(runtime);
    const runtimeNoExternal = resolveRuntimeNoExternal(runtime);
    // One physical Preact graph for the SSR runner, app copy first: the
    // aliases make preact-render-to-string (and its /stream subpath — a bare
    // root alias would prefix-match it onto a non-file) resolvable even when
    // the app declares only `preact`, and pin react-family ids onto absolute
    // preact/compat files for react-compat. Same composition dev/build use.
    const preactSsr = resolvePreactSsr(rootDir);
    const preactAliases = vitePreactAliases(preactSsr, runtimeAliases);
    const viteServer = await vite.createServer(
      vite.mergeConfig(userConfig, {
        root: rootDir,
        // Fixtures and apps without their own package.json otherwise inherit
        // an ancestor's node_modules/.vite directory. Different roots can
        // then clear one another's optimizer cache during concurrent startup.
        cacheDir: userConfig.cacheDir ?? path.join(rootDir, ".neutron", "vite-ssr"),
        plugins: [neutronPlugin({ routesDir })],
        resolve: { alias: preactAliases },
        // react-compat: process the whole APP dependency graph (see
        // resolveRuntimeNoExternal) — externalized deps natively resolve
        // REAL React next to preact-render-to-string and crash at render.
        // @neutron-build/core is no longer the exception it once was: core
        // now runs IN the SSR graph with only its CJS-only deps (see
        // RUNNER_CJS_EXTERNALS) kept native. Two reasons, same root cause —
        // one Preact `options` object for the whole request render:
        //   1. core's hooks (useLocation, useLoaderData, …) must come from
        //      the app's graph, or they crash/see defaults under the
        //      app-graph renderer (react hooks crash the other way around).
        //   2. the renderer itself is loaded through the same graph below.
        // `ssr.external` in vite.config.ts still opts individual deps back
        // out (Vite checks external before noExternal).
        ...(runtimeNoExternal === true
          ? {
              ssr: {
                noExternal: true,
                external: [...RUNNER_CJS_EXTERNALS],
              },
            }
          : {}),
        server: {
          middlewareMode: true,
          // Production only loads modules through this instance. Without
          // `ws: false`, Vite opens its own HMR WebSocket server on a second
          // port (all interfaces) even in middleware mode — an unexpected
          // listener on a production host. `hmr: false` stops file-change
          // pushes; the user config cannot re-enable either.
          hmr: false,
          ws: false,
        },
        appType: "custom",
        logLevel: "error",
      })
    );

    const ssrLoadModule = (id: string) => viteServer.ssrLoadModule(id);
    // Resolve the renderer THROUGH the runner, exactly the way route modules
    // are loaded: same graph, same Preact instance. Under react-compat the
    // preact family is runner-evaluated (blanket noExternal), so the renderer
    // must be too; under preact the same legs return the externalized native
    // modules the routes already share. A native fallback would bind a
    // second options object and crash hooks with "reading '__H'" — the
    // A-011 / Gap 5 split-graph bug class.
    const renderer = await loadRunnerRenderer(ssrLoadModule);

    return {
      ssrLoadModule,
      renderer,
      handleIslandRequest: await loadGraphIslandHandler(ssrLoadModule),
      source: "vite",
      close: () => viteServer.close(),
    };
  } catch (error) {
    // Propagate (TS-01): callers only start an SSR runtime when routes or a
    // global middleware require it, so a failure here means the app cannot
    // serve correctly. Swallowing it produced a static-only server whose
    // gates never ran.
    throw new Error(`Failed to initialize Vite SSR runtime: ${String(error)}`);
  }
}

/**
 * SSR runtime backed by the dist's prebuilt server bundle. Module ids are the
 * ones the Vite runtime receives — absolute source paths for route modules
 * and global middleware, bare ids for the renderer — so everything above it
 * (loadRouteModule, loadGlobalMiddleware, loadRunnerRenderer) is shared.
 */
async function createBundledSsrServer(
  rootDir: string,
  bundle: LoadedServerBundle
): Promise<SsrServer> {
  const ssrLoadModule = async (id: string): Promise<unknown> => {
    if (Object.prototype.hasOwnProperty.call(bundle.renderer, id)) {
      return bundle.renderer[id];
    }
    const load = bundle.modules[serverBundleKey(rootDir, id)];
    if (!load) {
      throw new Error(
        `${path.relative(rootDir, id) || id} is not in the server bundle ` +
          `${path.relative(rootDir, bundle.entryPath)}; rebuild with \`neutron-ts build\`.`
      );
    }
    return load();
  };
  return {
    ssrLoadModule,
    renderer: await loadRunnerRenderer(ssrLoadModule),
    handleIslandRequest: await loadGraphIslandHandler(ssrLoadModule),
    source: "server-bundle",
    close: async () => {},
  };
}

async function loadGraphIslandHandler(
  ssrLoadModule: (id: string) => Promise<unknown>
): Promise<typeof handleIslandRequest | undefined> {
  try {
    const core = (await ssrLoadModule("@neutron-build/core")) as {
      handleIslandRequest?: typeof handleIslandRequest;
    };
    return typeof core.handleIslandRequest === "function" ? core.handleIslandRequest : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Renderer pair from the Vite SSR graph: the same modules route components
 * import, evaluated by the same runner. Includes the router provider mount
 * (`withRouterProviders`) so providers and their consumers are created from
 * one module instance — a Provider/consumer pair from two instances silently
 * returns the context DEFAULT (A-011's wrong-branch failure mode).
 */
async function loadRunnerRenderer(
  ssrLoadModule: (id: string) => Promise<unknown>
): Promise<RouteRenderer> {
  const preact = (await ssrLoadModule("preact")) as {
    h?: RouteRenderer["h"];
  };
  const rts = (await ssrLoadModule("preact-render-to-string")) as {
    renderToString?: RouteRenderer["renderToString"];
    default?: { renderToString?: RouteRenderer["renderToString"] };
  };
  const renderToString = rts.renderToString ?? rts.default?.renderToString;
  if (!preact.h || !renderToString) {
    throw new Error(
      "[Neutron] preact / preact-render-to-string not resolvable through the SSR graph. " +
        "Ensure the start server applies resolvePreactSsr aliases."
    );
  }

  // Streaming renderer — optional: an older preact-render-to-string without
  // the subpath falls back to buffered rendering.
  let renderToReadableStream: RouteRenderer["renderToReadableStream"];
  try {
    const streamModule = (await ssrLoadModule("preact-render-to-string/stream")) as {
      renderToReadableStream?: RouteRenderer["renderToReadableStream"];
      default?: { renderToReadableStream?: RouteRenderer["renderToReadableStream"] };
    };
    renderToReadableStream =
      streamModule.renderToReadableStream ??
      streamModule.default?.renderToReadableStream;
  } catch {
    renderToReadableStream = undefined;
  }

  // Provider mount from the app's graph (contexts match consumers by module
  // instance, not by shape). Loaded via the ROOT package id: import-only
  // subpaths in package "exports" fail the runner's external resolution
  // (require conditions), and the root id loads in every runtime shape —
  // native/external under preact, in-graph under react-compat.
  let providers: RouteRenderer["withRouterProviders"];
  try {
    const client = (await ssrLoadModule("@neutron-build/core")) as {
      withRouterProviders?: RouteRenderer["withRouterProviders"];
    };
    providers = client.withRouterProviders;
  } catch {
    providers = undefined;
  }
  if (!providers) {
    console.warn(
      "[Neutron] withRouterProviders not resolvable through the SSR graph; " +
        "falling back to core's own provider mount. Context hooks " +
        "(useLocation, useLoaderData, …) may read default values when core " +
        "and the app do not share one module graph."
    );
  }

  return {
    h: preact.h,
    renderToString,
    renderToReadableStream,
    ...(providers ? { withRouterProviders: providers } : {}),
  };
}





/**
 * Render a CSP `nonce` attribute. Only emitted for a syntactically safe nonce
 * (base64/base64url charset), so it can never inject extra attributes.
 */



function getClientEntryScriptSrc(distDir: string): string | null {
  // The build's own metadata is authoritative: it names the exact chunk that
  // hydrates. The filename scan below is a last-resort heuristic, and sorting
  // `index-*.js` lexicographically picks index-9 over index-10 — so it must
  // not run when the metadata exists.
  const metadataPath = path.join(distDir, ".neutron-client-entry.json");
  if (fs.existsSync(metadataPath)) {
    try {
      const metadata = JSON.parse(fs.readFileSync(metadataPath, "utf-8")) as {
        src?: string;
      };
      if (metadata.src) {
        return metadata.src;
      }
    } catch {
      // Ignore malformed metadata and fall through to the scan.
    }
  }

  const assetsDir = path.join(distDir, "assets");
  if (fs.existsSync(assetsDir)) {
    const entryCandidates = fs
      .readdirSync(assetsDir)
      .filter((name) => name.startsWith("index-") && name.endsWith(".js"))
      .sort();

    if (entryCandidates.length > 0) {
      return `/assets/${entryCandidates[entryCandidates.length - 1]}`;
    }
  }

  const indexHtmlPath = path.join(distDir, "index.html");
  if (!fs.existsSync(indexHtmlPath)) {
    return null;
  }

  const indexHtml = fs.readFileSync(indexHtmlPath, "utf-8");
  const match = indexHtml.match(
    /<script[^>]*type="module"[^>]*src="([^"]+)"[^>]*><\/script>/i
  );

  return match?.[1] || null;
}

function loadRouteCssManifest(distDir: string): RouteCssManifest | null {
  try {
    return parseRouteCssManifest(fs.readFileSync(path.join(distDir, ROUTE_CSS_MANIFEST), "utf-8"));
  } catch {
    return null;
  }
}

function getClientStylesheetHrefs(distDir: string): string[] {
  const assetsDir = path.join(distDir, "assets");
  if (!fs.existsSync(assetsDir)) {
    return [];
  }
  return fs
    .readdirSync(assetsDir)
    .filter((name) => name.endsWith(".css"))
    .sort()
    .map((name) => `/assets/${name}`);
}


let requestCounter = 0;

function createRequestId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  requestCounter += 1;
  return `req-${Date.now()}-${requestCounter}`;
}

/**
 * The SSR catch-all's default 404 — what an unmatched path gets when the app
 * ships no `not-found.tsx` (or that page failed to render). Content-negotiated:
 *
 * - HTML-preferring clients (`Accept: text/html`, i.e. browsers) get the same
 *   minimal HTML shell `notFound()` has always served — a human-facing SSR app
 *   answering "Not Found" as bare text/plain is not a page.
 * - Everything else — JSON clients (`X-Neutron-Data`, `Accept:
 *   application/json`), no preference at all, a wildcard-only Accept — gets
 *   the RFC 7807 problem document in the exact shape api mode serves, so
 *   machines see the contract error and the client router never receives
 *   HTML it must parse.
 *
 * A user `not-found.tsx` still wins for every client: this runs only when
 * there is nothing of the app's to render. FRAMEWORK_CONTRACT.md §2.
 */
function defaultNotFoundResponse(request: Request, pathname: string): Response {
  if (!isJsonRequest(request) && acceptsHtml(request)) {
    return notFound();
  }
  return notFoundError(`No route matches ${pathname}`).toResponse(pathname);
}



export async function startServer(
  options: NeutronServerOptions = {}
): Promise<NeutronServer> {
  const running = await createServer(options);
  const { url, close } = running;

  console.log(`\n  Neutron production server running:\n`);
  console.log(`  Local:   ${url}`);
  if (running.moduleSource) {
    console.log(
      `  Modules: ${
        running.moduleSource === "server-bundle"
          ? "prebuilt server bundle"
          : "transformed through Vite (no current server bundle in dist)"
      }`
    );
  }
  console.log(`\n  Press Ctrl+C to stop\n`);

  // Graceful shutdown with a bounded drain (FRAMEWORK_CONTRACT.md §8: 30s).
  onShutdownSignal(close);

  // Return the handle so realtime callers can attach `wss.on("connection", ...)` while
  // still getting startServer's signal-driven graceful shutdown.
  return running;
}
