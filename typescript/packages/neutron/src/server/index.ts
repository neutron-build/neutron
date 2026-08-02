import * as fs from "node:fs";
import {
  renderAppRoute,
  emitHook,
  isJsonRequest,
  isMutationMethod,
  toError,
} from "../core/render-app-route.js";
import * as net from "node:net";
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
import { createRouter } from "../core/router.js";
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
  createMemoryAppCacheStore,
  createMemoryLoaderCacheStore,
  type NeutronAppCacheStore,
  type NeutronLoaderCacheStore,
  type NeutronCacheStores,
} from "./cache-store.js";
import { createEntityTag, requestHasMatchingEtag } from "./cache-utils.js";
import { escapeHtml } from "../core/escape.js";
import { assertRenderedFragment, decodeChunkStart } from "../core/fragment-guard.js";
import { neutronPlugin } from "../vite/plugin.js";
import {
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
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
import { handleIslandRequest } from "./server-islands.js";
import type {
  ActionArgs,
  AppContext,
  ErrorBoundaryProps,
  HeadArgs,
  HeadersArgs,
  LoaderArgs,
  MiddlewareFn,
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
export type { CsrfOptions } from "./csrf.js";
export { rateLimitMiddleware, apiRateLimit, imageRateLimit } from "./rate-limit.js";
export type { RateLimitOptions } from "./rate-limit.js";
export { inputLimitsMiddleware } from "./input-limits.js";
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
  cors?: false | CorsOptions;
  securityHeaders?: false | { headers?: Record<string, string> };
  /**
   * Allow-list of Host header values this server will serve. When set, requests
   * with any other Host get a 400 — preventing Host-header injection into
   * absolute URLs, links, and cache keys. Unset = accept any Host (unchanged).
   */
  trustedHosts?: string[];
  cache?: NeutronCacheStores;
  routes?: NeutronRoutesConfig;
  hooks?: NeutronServerHooks;
  /**
   * Rendering mode (default `"ssr"`). Controls the HTTP-response machinery:
   * - `"ssr"` — full Neutron: route discovery, Vite SSR runtime, asset/image/island
   *   routes, and the HTML catch-all. The canonical, unchanged behavior.
   * - `"api"` — JSON backend. No route discovery, no SSR, no asset serving. Keeps the
   *   shared batteries (request-id, /health, CORS/security, compression) and answers a
   *   clean JSON 404 for unmatched paths. Mount your own routes on the returned `app`.
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
}

/** Rendering mode for {@link createServer}. The transport axis is {@link NeutronServerOptions.websocket}. */
export type NeutronServerMode = "ssr" | "api" | "raw";

export interface NeutronWebSocketOptions {
  /**
   * Only accept WebSocket upgrades whose request pathname equals this value; other
   * upgrade attempts get the socket destroyed. Unset = accept upgrades on any path
   * (route inside your `wss.on("connection", (ws, req) => ...)` via `req.url`).
   */
  path?: string;
}

/** Resolved handle returned by {@link createServer}. */
export interface NeutronServer {
  app: Hono<{ Variables: { requestId: string } }>;
  server: ServerType;
  /** Present only when {@link NeutronServerOptions.websocket} is set. */
  wss?: WebSocketServer;
  close: () => Promise<void>;
  url: string;
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
    cors,
    securityHeaders,
    trustedHosts,
    cache,
    routes: routeRules,
    hooks,
    version: serverVersion = "0.1.0",
  } = options;

  const resolvedRootDir = path.resolve(rootDir);
  const resolvedDistDir = path.resolve(resolvedRootDir, distDir);
  const resolvedRoutesDir = path.resolve(resolvedRootDir, routesDir);
  const clientEntryScriptSrc = getClientEntryScriptSrc(resolvedDistDir);
  const stylesheetHrefs = getClientStylesheetHrefs(resolvedDistDir);
  const staticRouteHeaders = loadStaticRouteHeaders(resolvedDistDir);
  const staticHtmlCache = buildStaticHtmlCache(resolvedDistDir);
  const corsOptions = resolveCorsOptions(cors);
  const securityHeadersConfig = resolveSecurityHeadersConfig(securityHeaders);
  const compiledRouteRules = compileRouteRules(routeRules);

  // Rendering mode gates the SSR machinery. Only "ssr" walks the routes dir, spins up
  // the Vite SSR runtime, serves assets, and registers the HTML catch-all. "api"/"raw"
  // skip all of it (no fs walk, no hard-fail on a missing routes dir).
  const isSsr = mode === "ssr";

  const routes = isSsr ? discoverRoutes({ routesDir: resolvedRoutesDir }) : [];
  const router = createRouter();
  for (const route of routes) {
    router.insert(route);
  }

  const hasAppRoutes = routes.some(
    (route) => !route.file.includes("_layout") && route.config.mode === "app"
  );
  const ssrServer = hasAppRoutes
    ? await createSsrServer(resolvedRootDir, resolvedRoutesDir, runtime)
    : null;
  // Global middleware: an optional src/middleware.ts (default export = a
  // MiddlewareFn) runs OUTERMOST, before any per-route middleware, on every
  // app-route request. Loaded once at startup through the same SSR runtime as
  // routes so it shares the module graph (hooks, aliases). Absent file = none.
  const globalMiddleware: MiddlewareFn[] =
    hasAppRoutes && ssrServer
      ? await loadGlobalMiddleware(ssrServer, resolvedRootDir)
      : [];
  const routeModuleCache = new Map<string, Promise<RouteModule>>();
  const appResponseCacheStore =
    cache?.app || createMemoryAppCacheStore();
  const loaderDataCacheStore =
    cache?.loader || createMemoryLoaderCacheStore();
  const appInFlightRequests = new Map<string, Promise<Response>>();

  if (hasAppRoutes && !ssrServer) {
    console.warn(
      "App routes detected but SSR runtime could not be started. Falling back to static-only behavior."
    );
  }

  const app = new Hono<{ Variables: { requestId: string } }>();

  // FRAMEWORK_CONTRACT.md §5: Request ID is middleware step 1 (outermost). Reuse an
  // inbound x-request-id for trace propagation, otherwise generate one. It is shared
  // with the per-request trace context and surfaced as the x-request-id response
  // header on every response, including /health.
  app.use("*", async (c, next) => {
    const incoming = c.req.header("x-request-id");
    const requestId = incoming && incoming.length > 0 ? incoming : createRequestId();
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
    app.use("*", compress());
  }

  // GET /health — contract shape { status, nucleus, version }. Registered before
  // the static/SSR routes so it always answers. The SSR server holds no Nucleus
  // pool (loaders connect per-request), so nucleus is "unconfigured" here.
  //
  // Suppressed when the app defines its own /health route: the user route must
  // win so it can report dependency-aware health (e.g. 503 when a backing store
  // is down) instead of the built-in returning a false 200. The catch-all below
  // then serves it like any other route.
  const userDefinesHealthRoute = routes.some(
    (route) => route.path === "/health" && !route.file.includes("_layout")
  );
  if (!userDefinesHealthRoute) {
    app.get("/health", (c) =>
      c.json({
        status: "ok",
        nucleus: "unconfigured",
        version: serverVersion,
      }),
    );
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

  app.get("/_neutron/image", async (c) => {
    const response = await handleImageRequest(c.req.raw, {
      publicDirs: [
        path.join(resolvedRootDir, "public"),
        resolvedDistDir,
        path.join(resolvedDistDir, "public"),
      ],
      cacheDir: path.join(resolvedRootDir, ".neutron", "image-cache"),
    });
    return response;
  });

  app.get("/__neutron_island/:id", async (c) => {
    const islandId = c.req.param("id");
    const html = await handleIslandRequest(islandId);
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

      // Static dist HTML may only answer when no non-static app route claims
      // the path. The pre-seeded cache (buildStaticHtmlCache walks dist/ at
      // boot, including index.html -> "/") used to short-circuit BEFORE the
      // router, so an app route at "/" — e.g. an auth-gated home — was
      // silently shadowed by the built shell and its loader never ran.
      const staticAllowed = !match || isStaticRoute(match);

      if ((method === "GET" || method === "HEAD") && staticAllowed) {
        const cached = staticHtmlCache.get(effectivePathname);
        if (cached) {
          const response = createStaticHtmlResponse(
            cached,
            c.req.raw,
            method,
            staticRouteHeaders.get(effectivePathname)
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
          const response = createStaticHtmlResponse(
            entry,
            c.req.raw,
            method,
            staticRouteHeaders.get(effectivePathname)
          );
          return finalize(response, {
            routePath: effectivePathname,
            routeMode: "static",
          });
        }
      }

      if (!match) {
        return finalize(c.text("Not Found", 404));
      }

      if (match.route.file.includes("_layout") || (match.route.config.mode !== "app" && !isJsonRequest(c.req.raw))) {
        return finalize(c.text("Not Found", 404), {
          routeId: match.route.id,
          routePath: match.route.path,
          routeMode: match.route.config.mode,
        });
      }

      if (!ssrServer) {
        return finalize(c.text("App route SSR runtime is unavailable", 500), {
          routeId: match.route.id,
          routePath: match.route.path,
          routeMode: "app",
        });
      }

      if (isMutationMethod(method)) {
        await appResponseCacheStore.deleteByPath(effectivePathname);
        await loaderDataCacheStore.deleteByPath(effectivePathname);
      }

      const appCacheMaxAge = match.route.config.cache?.maxAge ?? 0;
      // SECURITY: the app-response cache is keyed only on path+query, so it is
      // shared across users. Never read, single-flight-share, or store a
      // response for a request that carries credentials (Cookie/Authorization),
      // since it may be authenticated/personalized and would otherwise leak one
      // user's rendered page to others. Conditional/no-cache requests without
      // credentials still revalidate normally.
      const appCacheKey =
        appCacheMaxAge > 0 && !requestCarriesCredentials(c.req.raw)
          ? buildAppCacheKey(c.req.raw, effectivePathname)
          : null;

      if (appCacheKey && (method === "GET" || method === "HEAD")) {
        const hit = await readCachedAppResponse(
          appResponseCacheStore,
          appCacheKey,
          c.req.raw,
          method
        );
        if (hit) {
          return finalize(hit, {
            routeId: match.route.id,
            routePath: match.route.path,
            routeMode: "app",
          });
        }
      }

      if (appCacheKey && method === "GET") {
        const pending = appInFlightRequests.get(appCacheKey);
        if (pending) {
          const shared = await pending;
          return finalize(shared.clone(), {
            routeId: match.route.id,
            routePath: match.route.path,
            routeMode: "app",
          });
        }

        const next = (async () => {
          const response = await handleAppRouteRequest(
            c.req.raw,
            match,
            ssrServer,
            clientEntryScriptSrc,
            stylesheetHrefs,
            routeModuleCache,
            loaderDataCacheStore,
            requestTrace,
            hooks,
            globalMiddleware
          );
          await maybeStoreAppResponse(
            appResponseCacheStore,
            appCacheKey,
            response,
            appCacheMaxAge
          );
          return response;
        })();

        appInFlightRequests.set(appCacheKey, next);
        try {
          const response = await next;
          return finalize(response.clone(), {
            routeId: match.route.id,
            routePath: match.route.path,
            routeMode: "app",
          });
        } finally {
          appInFlightRequests.delete(appCacheKey);
        }
      }

      const response = await handleAppRouteRequest(
        c.req.raw,
        match,
        ssrServer,
        clientEntryScriptSrc,
        stylesheetHrefs,
        routeModuleCache,
        loaderDataCacheStore,
        requestTrace,
        hooks,
        globalMiddleware
      );

      if (isMutationMethod(method)) {
        await applyMutationInvalidationFromResponse(
          appResponseCacheStore,
          effectivePathname,
          response
        );
        await applyMutationInvalidationToLoaderDataCache(
          loaderDataCacheStore,
          effectivePathname,
          response
        );
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
      return finalize(new Response("Internal Server Error", { status: 500 }));
    }
  });
  } // end if (isSsr)

  // "api" mode answers a clean JSON 404 for anything the user didn't mount. Use
  // app.notFound (not an app.all("*") route) so it fires only when nothing matched —
  // an "*" route would shadow routes the caller mounts later on the returned `app`.
  // "raw" mode intentionally adds nothing — unmatched paths get Hono's default 404.
  if (mode === "api") {
    app.notFound((c) => c.json({ error: "Not Found", path: c.req.path }, 404));
  }

  const server = serve({
    fetch: app.fetch,
    port,
    hostname: host,
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
      if (wsOptions.path) {
        const pathname = new URL(req.url ?? "/", "http://localhost").pathname;
        if (pathname !== wsOptions.path) {
          socket.destroy();
          return;
        }
      }
      wss!.handleUpgrade(req, socket, head, (ws) => {
        wss!.emit("connection", ws, req);
      });
    });
  }

  return {
    app,
    server,
    wss,
    close: async () => {
      await ssrServer?.close();
      // Tear down WebSockets before draining HTTP. An upgraded WS socket is NOT an idle
      // HTTP keep-alive, so server.close()/closeIdleConnections() won't reap it — a live
      // client would otherwise hold the drain open until the caller's shutdown timeout.
      // Forcibly terminate each live socket, then await the WS server's own close.
      if (wss) {
        for (const client of wss.clients) {
          client.terminate();
        }
        await new Promise<void>((resolve) => wss!.close(() => resolve()));
      }
      // Await server.close's callback so in-flight requests actually drain
      // (the previous fire-and-forget resolved before any draining). Close idle
      // keep-alive sockets so they don't hold the drain open indefinitely.
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
        (server as { closeIdleConnections?: () => void }).closeIdleConnections?.();
      });
    },
    url: `http://${host}:${port}`,
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
  globalMiddleware?: MiddlewareFn[]
): Promise<Response> {
  // Dev module-loading adapter: load every route/layout module through the
  // Vite SSR runtime, then hand the loaded map to the shared render core.
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
  });
}









function isStaticRoute(match: RouteMatch): boolean {
  if (match.route.file.includes("_layout")) {
    return true;
  }
  return match.route.config.mode === "static";
}

// Load an optional global middleware from <rootDir>/src/middleware.ts (or
// .js/.tsx). Returns its default export if it is a function, else null. A
// present-but-malformed file warns and is ignored rather than crashing boot.
async function loadGlobalMiddleware(
  ssrServer: SsrServer,
  rootDir: string
): Promise<MiddlewareFn[]> {
  const candidates = [
    "src/middleware.ts",
    "src/middleware.tsx",
    "src/middleware.js",
    "src/middleware.mjs",
  ];
  for (const rel of candidates) {
    const abs = path.join(rootDir, rel);
    if (!fs.existsSync(abs)) continue;
    try {
      const mod = (await ssrServer.ssrLoadModule(abs)) as {
        default?: unknown;
        middleware?: unknown;
      };
      // Documented form: `export const middleware: MiddlewareFn[]`. Also
      // accept a single function (default or named) for ergonomics.
      const exported = mod.middleware ?? mod.default;
      const list = normalizeMiddlewareExport(exported);
      if (list.length === 0 && exported !== undefined) {
        console.warn(
          `Global middleware ${rel}: export is neither a function nor an array of functions — ignoring.`
        );
      }
      return list;
    } catch (error) {
      console.warn(`Failed to load global middleware ${rel}: ${String(error)}`);
      return [];
    }
  }
  return [];
}

function normalizeMiddlewareExport(exported: unknown): MiddlewareFn[] {
  if (typeof exported === "function") return [exported as MiddlewareFn];
  if (Array.isArray(exported)) {
    return exported.filter((f): f is MiddlewareFn => typeof f === "function");
  }
  return [];
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
  }
  return pending;
}

function normalizePathname(pathname: string): string | null {
  let decoded: string;
  try {
    decoded = decodeURIComponent(pathname || "/");
  } catch {
    return null;
  }

  if (!decoded.startsWith("/") || decoded.includes("..")) {
    return null;
  }

  if (decoded.length > 1 && decoded.endsWith("/")) {
    return decoded.slice(0, -1);
  }

  return decoded;
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
  return `${variant}:${pathname}${url.search}`;
}

/**
 * A request carries credentials when it has a Cookie or Authorization header.
 * Such requests may be authenticated/personalized and must never participate in
 * the shared, path-keyed app-response cache (read, store, or single-flight).
 */
function requestCarriesCredentials(request: Request): boolean {
  return request.headers.has("Authorization") || request.headers.has("Cookie");
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
      await cache.deleteByPath(pathname);
      continue;
    }

    const normalized = normalizePathname(token);
    if (normalized) {
      await cache.deleteByPath(normalized);
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
      await cache.deleteByPath(pathname);
      continue;
    }

    const normalized = normalizePathname(token);
    if (normalized) {
      await cache.deleteByPath(normalized);
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

  return new Response(entry.body, {
    status: entry.status,
    statusText: entry.statusText,
    headers,
  });
}

async function maybeStoreAppResponse(
  cache: NeutronAppCacheStore,
  key: string,
  response: Response,
  maxAgeSec: number
): Promise<void> {
  if (maxAgeSec <= 0 || response.status !== 200) {
    return;
  }

  if (response.headers.has("Set-Cookie")) {
    return;
  }

  const cacheControl = response.headers.get("Cache-Control") || "";
  if (cacheControl.includes("no-store") || cacheControl.includes("private")) {
    return;
  }

  // A response that reflects a per-request Origin (CORS) must not be shared —
  // storing it would replay one origin's Access-Control-Allow-Origin to
  // another. (The Accept-based JSON/HTML split is already part of the cache
  // key, and Cookie/Authorization requests are excluded before we get here.)
  if (response.headers.has("Access-Control-Allow-Origin")) {
    return;
  }

  const body = await response.clone().text();
  const headers = new Headers(response.headers);
  if (!headers.has("Cache-Control")) {
    headers.set("Cache-Control", `public, max-age=${maxAgeSec}`);
  }
  if (!headers.has("ETag")) {
    headers.set("ETag", createEntityTag(body));
  }
  headers.set("x-neutron-cache", "MISS");
  const headerPairs: [string, string][] = [];
  headers.forEach((value, name) => {
    headerPairs.push([name, value]);
  });

  await cache.set(key, {
    status: response.status,
    statusText: response.statusText,
    headers: headerPairs,
    body,
    expiresAt: Date.now() + maxAgeSec * 1000,
  });
}

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

async function createSsrServer(
  rootDir: string,
  routesDir: string,
  runtime: NeutronRuntime
): Promise<SsrServer | null> {
  try {
    const vite = await import("vite");
    const hmrPort = await getFreePort();
    const loadedConfig = await vite.loadConfigFromFile(
      { command: "serve", mode: "production" },
      undefined,
      rootDir
    );

    const userConfig = loadedConfig?.config || {};
    const runtimeAliases = resolveRuntimeAliases(runtime);
    const runtimeNoExternal = resolveRuntimeNoExternal(runtime);
    const viteServer = await vite.createServer(
      vite.mergeConfig(userConfig, {
        root: rootDir,
        plugins: [neutronPlugin({ routesDir })],
        ...(runtimeAliases ? { resolve: { alias: runtimeAliases } } : {}),
        ...(runtimeNoExternal.length > 0
          ? { ssr: { noExternal: runtimeNoExternal } }
          : {}),
        server: {
          middlewareMode: true,
          // Use a random HMR socket in SSR middleware mode to avoid
          // fixed-port collisions when multiple servers spin up in tests.
          hmr: { port: hmrPort },
        },
        appType: "custom",
        logLevel: "error",
      })
    );

    return {
      ssrLoadModule: (id: string) => viteServer.ssrLoadModule(id),
      close: () => viteServer.close(),
    };
  } catch (error) {
    console.warn("Failed to initialize Vite SSR runtime:", error);
    return null;
  }
}









/**
 * Render a CSP `nonce` attribute. Only emitted for a syntactically safe nonce
 * (base64/base64url charset), so it can never inject extra attributes.
 */



function getClientEntryScriptSrc(distDir: string): string | null {
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
      // Ignore malformed metadata and fall back to index.html parsing.
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



async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") {
        reject(new Error("Failed to resolve free port"));
        return;
      }
      const { port } = address;
      socket.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
    socket.on("error", reject);
  });
}

export async function startServer(
  options: NeutronServerOptions = {}
): Promise<NeutronServer> {
  const running = await createServer(options);
  const { url, close } = running;

  console.log(`\n  Neutron production server running:\n`);
  console.log(`  Local:   ${url}\n`);
  console.log(`  Press Ctrl+C to stop\n`);

  // Graceful shutdown with a bounded drain (FRAMEWORK_CONTRACT.md: 30s).
  const SHUTDOWN_TIMEOUT_MS = 30_000;
  let shuttingDown = false;
  const shutdown = (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;

    console.log(
      `\nReceived ${signal}, draining in-flight requests (up to ${
        SHUTDOWN_TIMEOUT_MS / 1000
      }s)...`,
    );
    const forceExit = setTimeout(() => {
      console.error("Drain timed out; forcing exit.");
      process.exit(1);
    }, SHUTDOWN_TIMEOUT_MS);

    void close().then(
      () => {
        clearTimeout(forceExit);
        console.log("Drained cleanly.");
        process.exit(0);
      },
      (err) => {
        clearTimeout(forceExit);
        console.error("Shutdown error:", err);
        process.exit(1);
      },
    );
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  // Return the handle so realtime callers can attach `wss.on("connection", ...)` while
  // still getting startServer's signal-driven graceful shutdown.
  return running;
}
