import * as fs from "node:fs";
import * as net from "node:net";
import * as path from "node:path";
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
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
  cors?: false | CorsOptions;
  securityHeaders?: false | { headers?: Record<string, string> };
  cache?: NeutronCacheStores;
  routes?: NeutronRoutesConfig;
  hooks?: NeutronServerHooks;
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
const TEXT_ENCODER = new TextEncoder();

type StreamRenderFn = (element: preact.VNode) => ReadableStream<Uint8Array> & {
  allReady?: Promise<void>;
};

let cachedStreamRenderFn: StreamRenderFn | null | undefined;

export async function createServer(options: NeutronServerOptions = {}) {
  const {
    port = 3000,
    host = "0.0.0.0",
    rootDir = process.cwd(),
    distDir = "dist",
    routesDir = "src/routes",
    compress: enableCompress = true,
    runtime = "preact",
    cors,
    securityHeaders,
    cache,
    routes: routeRules,
    hooks,
  } = options;

  const resolvedRootDir = path.resolve(rootDir);
  const resolvedDistDir = path.resolve(resolvedRootDir, distDir);
  const resolvedRoutesDir = path.resolve(resolvedRootDir, routesDir);
  const clientEntryScriptSrc = getClientEntryScriptSrc(resolvedDistDir);
  const staticRouteHeaders = loadStaticRouteHeaders(resolvedDistDir);
  const staticHtmlCache = buildStaticHtmlCache(resolvedDistDir);
  const corsOptions = resolveCorsOptions(cors);
  const securityHeadersConfig = resolveSecurityHeadersConfig(securityHeaders);
  const compiledRouteRules = compileRouteRules(routeRules);

  const routes = discoverRoutes({ routesDir: resolvedRoutesDir });
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

  const app = new Hono();

  if (enableCompress) {
    app.use("*", compress());
  }

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
      requestId: createRequestId(),
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

      if (method === "GET" || method === "HEAD") {
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

      const match = router.match(effectivePathname);

      if ((method === "GET" || method === "HEAD") && !isJsonRequest(c.req.raw) && (!match || isStaticRoute(match))) {
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
      const appCacheKey =
        appCacheMaxAge > 0 ? buildAppCacheKey(c.req.raw, effectivePathname) : null;

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
            routeModuleCache,
            loaderDataCacheStore,
            requestTrace,
            hooks
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
        routeModuleCache,
        loaderDataCacheStore,
        requestTrace,
        hooks
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

  const server = serve({
    fetch: app.fetch,
    port,
    hostname: host,
  });

  return {
    app,
    server,
    close: async () => {
      await ssrServer?.close();
      server.close();
    },
    url: `http://${host}:${port}`,
  };
}

async function handleAppRouteRequest(
  request: Request,
  match: RouteMatch,
  ssrServer: SsrServer,
  clientEntryScriptSrc: string | null,
  moduleCache: Map<string, Promise<RouteModule>>,
  loaderDataCache: NeutronLoaderCacheStore,
  requestTrace: RequestTraceContext,
  hooks?: NeutronServerHooks
): Promise<Response> {
  const allRoutes = [...match.layouts, match.route];
  const includeClientRuntime = allRoutes.every((route) => route.config.hydrate !== false);
  const routeModules = new Map<string, RouteModule>();

  await Promise.all(
    allRoutes.map(async (route) => {
      const loaded = await loadRouteModule(ssrServer, route.file, moduleCache);
      routeModules.set(route.id, loaded);
    })
  );

  const middlewares: MiddlewareFn[] = [];
  for (const route of allRoutes) {
    const module = routeModules.get(route.id);
    if (module?.middleware) {
      middlewares.push(module.middleware);
    }
  }

  const context: AppContext = {};

  return runMiddlewareChain(middlewares, request, context, async () => {
    let actionData: unknown = undefined;
    const pageModule = routeModules.get(match.route.id);
    const requestedRouteIds = resolveRequestedDataRouteIds(
      request,
      allRoutes,
      isMutationMethod(request.method)
    );

    if (!pageModule?.default) {
      return new Response("Not Found", { status: 404 });
    }

    if (isMutationMethod(request.method) && pageModule.action) {
      const actionArgs: ActionArgs = {
        request,
        params: match.params,
        context,
      };
      const actionStartedAt = Date.now();
      emitHook(hooks?.onActionStart, {
        requestId: requestTrace.requestId,
        method: requestTrace.method,
        pathname: requestTrace.pathname,
        routeId: match.route.id,
        routePath: match.route.path,
        startedAt: actionStartedAt,
      });

      try {
        const result = await pageModule.action(actionArgs);
        if (result instanceof Response) {
          const actionEndedAt = Date.now();
          emitHook(hooks?.onActionEnd, {
            requestId: requestTrace.requestId,
            method: requestTrace.method,
            pathname: requestTrace.pathname,
            routeId: match.route.id,
            routePath: match.route.path,
            startedAt: actionStartedAt,
            endedAt: actionEndedAt,
            durationMs: actionEndedAt - actionStartedAt,
            outcome: "response",
            responseStatus: result.status,
          });
          return result;
        }
        actionData = result;
        const actionEndedAt = Date.now();
        emitHook(hooks?.onActionEnd, {
          requestId: requestTrace.requestId,
          method: requestTrace.method,
          pathname: requestTrace.pathname,
          routeId: match.route.id,
          routePath: match.route.path,
          startedAt: actionStartedAt,
          endedAt: actionEndedAt,
          durationMs: actionEndedAt - actionStartedAt,
          outcome: "success",
        });
      } catch (error) {
        if (error instanceof Response) {
          const actionEndedAt = Date.now();
          emitHook(hooks?.onActionEnd, {
            requestId: requestTrace.requestId,
            method: requestTrace.method,
            pathname: requestTrace.pathname,
            routeId: match.route.id,
            routePath: match.route.path,
            startedAt: actionStartedAt,
            endedAt: actionEndedAt,
            durationMs: actionEndedAt - actionStartedAt,
            outcome: "response",
            responseStatus: error.status,
          });
          return error;
        }
        const actionEndedAt = Date.now();
        emitHook(hooks?.onActionEnd, {
          requestId: requestTrace.requestId,
          method: requestTrace.method,
          pathname: requestTrace.pathname,
          routeId: match.route.id,
          routePath: match.route.path,
          startedAt: actionStartedAt,
          endedAt: actionEndedAt,
          durationMs: actionEndedAt - actionStartedAt,
          outcome: "error",
        });
        emitHook(hooks?.onError, {
          requestId: requestTrace.requestId,
          method: requestTrace.method,
          pathname: requestTrace.pathname,
          source: "action",
          routeId: match.route.id,
          routePath: match.route.path,
          error: toError(error),
        });
        return renderErrorResponse(
          allRoutes,
          routeModules,
          match.route,
          toError(error),
          clientEntryScriptSrc,
          includeClientRuntime
        );
      }
    }

    // PARALLEL LOADER EXECUTION
    // All loaders run simultaneously with Promise.all, not sequentially
    // This is critical: 3x faster on pages with multiple loaders
    const loaderPromises = allRoutes.map(async (route) => {
      const module = routeModules.get(route.id);
      if (!module?.loader) {
        return { routeId: route.id, data: undefined };
      }

      if (requestedRouteIds && !requestedRouteIds.has(route.id)) {
        return { routeId: route.id, data: undefined };
      }
      const loaderStartedAt = Date.now();
      emitHook(hooks?.onLoaderStart, {
        requestId: requestTrace.requestId,
        method: requestTrace.method,
        pathname: requestTrace.pathname,
        routeId: route.id,
        routePath: route.path,
        startedAt: loaderStartedAt,
      });

      const routeParams = route.id === match.route.id ? match.params : {};
      const loaderCacheMaxAge = route.config.cache?.loaderMaxAge ?? 0;
      const canCacheLoaderData =
        loaderCacheMaxAge > 0 && isLoaderDataCacheableRequest(request);
      const canReadLoaderCache =
        canCacheLoaderData && isLoaderDataCacheReadableMethod(request.method);
      const loaderCacheKey = canCacheLoaderData
        ? buildLoaderDataCacheKey(request, route.id, routeParams)
        : null;
      if (loaderCacheKey && canReadLoaderCache) {
        const cachedLoaderData = await readCachedLoaderData(loaderDataCache, loaderCacheKey);
        if (cachedLoaderData !== null) {
          const loaderEndedAt = Date.now();
          emitHook(hooks?.onLoaderEnd, {
            requestId: requestTrace.requestId,
            method: requestTrace.method,
            pathname: requestTrace.pathname,
            routeId: route.id,
            routePath: route.path,
            startedAt: loaderStartedAt,
            endedAt: loaderEndedAt,
            durationMs: loaderEndedAt - loaderStartedAt,
            outcome: "success",
            cacheStatus: "hit",
          });
          return { routeId: route.id, data: cachedLoaderData };
        }
      }

      const loaderArgs: LoaderArgs = {
        request,
        params: routeParams,
        context,
      };

      try {
        const data = await module.loader(loaderArgs);
        if (loaderCacheKey) {
          await storeLoaderDataCache(
            loaderDataCache,
            loaderCacheKey,
            data,
            loaderCacheMaxAge
          );
        }
        const loaderEndedAt = Date.now();
        emitHook(hooks?.onLoaderEnd, {
          requestId: requestTrace.requestId,
          method: requestTrace.method,
          pathname: requestTrace.pathname,
          routeId: route.id,
          routePath: route.path,
          startedAt: loaderStartedAt,
          endedAt: loaderEndedAt,
          durationMs: loaderEndedAt - loaderStartedAt,
          outcome: "success",
          cacheStatus: loaderCacheKey ? "miss" : "bypass",
        });
        return { routeId: route.id, data };
      } catch (error) {
        const loaderEndedAt = Date.now();
        if (error instanceof Response) {
          emitHook(hooks?.onLoaderEnd, {
            requestId: requestTrace.requestId,
            method: requestTrace.method,
            pathname: requestTrace.pathname,
            routeId: route.id,
            routePath: route.path,
            startedAt: loaderStartedAt,
            endedAt: loaderEndedAt,
            durationMs: loaderEndedAt - loaderStartedAt,
            outcome: "response",
            responseStatus: error.status,
            cacheStatus: loaderCacheKey ? "miss" : "bypass",
          });
        } else {
          emitHook(hooks?.onLoaderEnd, {
            requestId: requestTrace.requestId,
            method: requestTrace.method,
            pathname: requestTrace.pathname,
            routeId: route.id,
            routePath: route.path,
            startedAt: loaderStartedAt,
            endedAt: loaderEndedAt,
            durationMs: loaderEndedAt - loaderStartedAt,
            outcome: "error",
            cacheStatus: loaderCacheKey ? "miss" : "bypass",
          });
          emitHook(hooks?.onError, {
            requestId: requestTrace.requestId,
            method: requestTrace.method,
            pathname: requestTrace.pathname,
            source: "loader",
            routeId: route.id,
            routePath: route.path,
            error: toError(error),
          });
        }
        return { routeId: route.id, data: null, error };
      }
    });

    // Wait for ALL loaders to complete in parallel
    const loaderResults = await Promise.all(loaderPromises);

    // Check for errors and build data map
    const loaderData: Record<string, unknown> = {};
    for (const result of loaderResults) {
      if (result.error) {
        if (result.error instanceof Response) {
          return result.error;
        }
        const errorRoute = allRoutes.find(r => r.id === result.routeId);
        return renderErrorResponse(
          allRoutes,
          routeModules,
          errorRoute!,
          toError(result.error),
          clientEntryScriptSrc,
          includeClientRuntime
        );
      }
      if (result.data !== undefined) {
        loaderData[result.routeId] = result.data;
      }
    }

    const routeHeaders = await resolveRouteHeaders(allRoutes, routeModules, {
      request,
      params: match.params,
      context,
      loaderData,
      actionData,
    });

    const pathname = new URL(request.url).pathname;
    const headHtml = await resolveRouteHeadHtml(allRoutes, routeModules, {
      request,
      params: match.params,
      context,
      loaderData,
      actionData,
      pathname,
    });

    if (isJsonRequest(request)) {
      const payload: Record<string, unknown> = { ...loaderData };
      if (actionData !== undefined) {
        payload.__action__ = actionData;
      }
      payload.__head__ = headHtml;
      routeHeaders.set("Content-Type", "application/json");
      return new Response(encodeSerializedPayloadAsJson(payload), {
        headers: routeHeaders,
      });
    }

    try {
      let element: any = h(pageModule.default as any, {
        data: loaderData[match.route.id],
        params: match.params,
        actionData,
      });

      for (let i = allRoutes.length - 2; i >= 0; i--) {
        const layoutRoute = allRoutes[i];
        const layoutModule = routeModules.get(layoutRoute.id);
        if (layoutModule?.default) {
          element = h(
            layoutModule.default as any,
            { data: loaderData[layoutRoute.id] },
            element
          );
        }
      }
      return renderAppRouteHtmlResponse({
        request,
        element,
        pathname,
        loaderData,
        actionData,
        headHtml,
        clientEntryScriptSrc,
        includeClientRuntime,
        headers: routeHeaders,
      });
    } catch (error) {
      emitHook(hooks?.onError, {
        requestId: requestTrace.requestId,
        method: requestTrace.method,
        pathname: requestTrace.pathname,
        source: "render",
        routeId: match.route.id,
        routePath: match.route.path,
        error: toError(error),
      });
      return renderErrorResponse(
        allRoutes,
        routeModules,
        match.route,
        toError(error),
        clientEntryScriptSrc,
        includeClientRuntime
      );
    }
  });
}

async function getStreamRenderFn(): Promise<StreamRenderFn | null> {
  if (cachedStreamRenderFn === undefined) {
    try {
      const streamModule = await import("preact-render-to-string/stream");
      cachedStreamRenderFn = streamModule.renderToReadableStream as StreamRenderFn;
    } catch {
      cachedStreamRenderFn = null;
    }
  }
  return cachedStreamRenderFn;
}

interface RenderAppRouteHtmlResponseArgs {
  request: Request;
  element: preact.VNode;
  pathname: string;
  headHtml: string;
  loaderData: Record<string, unknown>;
  actionData?: unknown;
  clientEntryScriptSrc: string | null;
  includeClientRuntime: boolean;
  headers: Headers;
}

async function renderAppRouteHtmlResponse(
  args: RenderAppRouteHtmlResponseArgs
): Promise<Response> {
  const headers = withDefaultContentType(args.headers, "text/html; charset=utf-8");
  if (args.request.method.toUpperCase() === "HEAD") {
    return new Response(null, { headers });
  }

  const streamRenderFn = await getStreamRenderFn();
  if (!streamRenderFn) {
    const html = renderToString(args.element);
    const fullHtml = wrapHtml(
      html,
      args.pathname,
      args.headHtml,
      args.loaderData,
      args.actionData,
      args.clientEntryScriptSrc,
      args.includeClientRuntime
    );
    return new Response(fullHtml, { headers });
  }

  const shellPrefix = buildHtmlPrefix(args.pathname, args.headHtml);
  const shellSuffix = buildHtmlSuffix(
    args.loaderData,
    args.actionData,
    args.clientEntryScriptSrc,
    args.includeClientRuntime
  );

  try {
    const body = streamHtmlDocument(streamRenderFn(args.element), shellPrefix, shellSuffix);
    return new Response(body, { headers });
  } catch {
    const html = renderToString(args.element);
    const fullHtml = wrapHtml(
      html,
      args.pathname,
      args.headHtml,
      args.loaderData,
      args.actionData,
      args.clientEntryScriptSrc,
      args.includeClientRuntime
    );
    return new Response(fullHtml, { headers });
  }
}

function streamHtmlDocument(
  contentStream: ReadableStream<Uint8Array>,
  prefix: string,
  suffix: string
): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      controller.enqueue(TEXT_ENCODER.encode(prefix));
      const reader = contentStream.getReader();
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          if (value) {
            controller.enqueue(value);
          }
        }
        controller.enqueue(TEXT_ENCODER.encode(suffix));
        controller.close();
      } catch (error) {
        controller.error(error);
      } finally {
        reader.releaseLock();
      }
    },
  });
}

async function resolveRouteHeaders(
  allRoutes: Route[],
  modules: Map<string, RouteModule>,
  args: HeadersArgs
): Promise<Headers> {
  const headers = new Headers();

  for (const route of allRoutes) {
    const mod = modules.get(route.id);
    if (!mod?.headers) {
      continue;
    }

    const resolved = await mod.headers(args);
    const next = toHeaders(resolved);
    next.forEach((value, name) => {
      headers.set(name, value);
    });
  }

  return headers;
}

async function resolveRouteHeadHtml(
  allRoutes: Route[],
  modules: Map<string, RouteModule>,
  args: HeadArgs
): Promise<string> {
  let mergedSeo: SeoMetaInput | null = null;
  const headFragments: string[] = [];

  for (const route of allRoutes) {
    const mod = modules.get(route.id);
    if (!mod?.head) {
      continue;
    }

    const resolved = await mod.head({ ...args, data: args.loaderData[route.id] });
    if (!resolved) {
      continue;
    }

    if (typeof resolved === "string") {
      headFragments.push(sanitizeHeadHtml(resolved));
      continue;
    }

    mergedSeo = mergeSeoMetaInput(mergedSeo, resolved);
  }

  return renderDocumentHead(args.pathname, mergedSeo, headFragments);
}

function toHeaders(
  value: Headers | Record<string, string> | null | undefined
): Headers {
  if (!value) {
    return new Headers();
  }
  if (value instanceof Headers) {
    return new Headers(value);
  }

  const headers = new Headers();
  for (const [name, val] of Object.entries(value)) {
    headers.set(name, String(val));
  }
  return headers;
}

function withDefaultContentType(headers: Headers, fallback: string): Headers {
  if (!headers.has("Content-Type")) {
    headers.set("Content-Type", fallback);
  }
  return headers;
}

function isStaticRoute(match: RouteMatch): boolean {
  if (match.route.file.includes("_layout")) {
    return true;
  }
  return match.route.config.mode === "static";
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

function isLoaderDataCacheableRequest(request: Request): boolean {
  const cacheControl = request.headers.get("Cache-Control") || "";
  if (cacheControl.includes("no-cache") || cacheControl.includes("no-store")) {
    return false;
  }

  // Conservative default: avoid caching request-scoped/private data.
  if (request.headers.has("Authorization") || request.headers.has("Cookie")) {
    return false;
  }

  return true;
}

function isLoaderDataCacheReadableMethod(method: string): boolean {
  const normalized = method.toUpperCase();
  return normalized === "GET" || normalized === "HEAD";
}

function buildLoaderDataCacheKey(
  request: Request,
  routeId: string,
  params: Record<string, string>
): string {
  const url = new URL(request.url);
  const encodedParams = stableEncodeParams(params);
  return `${url.pathname}::${url.search}::${routeId}::${encodedParams}`;
}

function stableEncodeParams(params: Record<string, string>): string {
  const sortedEntries = Object.entries(params).sort(([left], [right]) =>
    left.localeCompare(right)
  );
  return JSON.stringify(sortedEntries);
}

async function readCachedLoaderData(
  cache: NeutronLoaderCacheStore,
  key: string
): Promise<unknown | null> {
  const entry = await cache.get(key);
  return entry ? entry.data : null;
}

async function storeLoaderDataCache(
  cache: NeutronLoaderCacheStore,
  key: string,
  data: unknown,
  maxAgeSec: number
): Promise<void> {
  if (maxAgeSec <= 0) {
    return;
  }

  await cache.set(key, {
    data,
    expiresAt: Date.now() + maxAgeSec * 1000,
  });
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

function isMutationMethod(method: string): boolean {
  const normalized = method.toUpperCase();
  return (
    normalized === "POST" ||
    normalized === "PUT" ||
    normalized === "PATCH" ||
    normalized === "DELETE"
  );
}

function isJsonRequest(request: Request): boolean {
  if (isNeutronDataRequest(request)) {
    return true;
  }
  const accept = request.headers.get("Accept") || "";
  return accept.includes("application/json");
}

function isNeutronDataRequest(request: Request): boolean {
  return request.headers.get("X-Neutron-Data") === "true";
}

function resolveRequestedDataRouteIds(
  request: Request,
  routes: Route[],
  isMutation: boolean
): Set<string> | null {
  if (!isNeutronDataRequest(request) || isMutation) {
    return null;
  }

  const rawRouteIds = request.headers.get("X-Neutron-Routes");
  if (!rawRouteIds) {
    return null;
  }

  const requested = rawRouteIds
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean);

  if (requested.length === 0) {
    return null;
  }

  const allowed = new Set(routes.map((route) => route.id));
  const filtered = requested.filter((routeId) => allowed.has(routeId));
  if (filtered.length === 0) {
    return null;
  }

  return new Set(filtered);
}

function renderErrorResponse(
  allRoutes: Route[],
  modules: Map<string, RouteModule>,
  route: Route,
  error: Error,
  clientEntryScriptSrc: string | null,
  includeClientRuntime: boolean
): Response {
  const boundary = findNearestErrorBoundary(allRoutes, modules, route);

  if (!boundary) {
    return new Response(renderDefaultError(error), {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const boundaryElement = h(boundary as any, {
    error,
  } as ErrorBoundaryProps);
  const boundaryHtml = renderToString(boundaryElement);
  const fullHtml = wrapHtml(
    boundaryHtml,
    route.path,
    renderDocumentHead(route.path, null),
    {},
    undefined,
    clientEntryScriptSrc,
    includeClientRuntime
  );

  return new Response(fullHtml, {
    status: 500,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

function findNearestErrorBoundary(
  allRoutes: Route[],
  modules: Map<string, RouteModule>,
  route: Route
): RouteModule["ErrorBoundary"] | undefined {
  const pageModule = modules.get(route.id);
  if (pageModule?.ErrorBoundary) {
    return pageModule.ErrorBoundary;
  }

  for (let i = allRoutes.length - 2; i >= 0; i--) {
    const layoutModule = modules.get(allRoutes[i].id);
    if (layoutModule?.ErrorBoundary) {
      return layoutModule.ErrorBoundary;
    }
  }

  return undefined;
}

function renderDefaultError(error: Error): string {
  const isProd = typeof process !== 'undefined' && process.env.NODE_ENV === 'production';
  const displayMessage = isProd ? 'An unexpected error occurred' : escapeHtml(error.message);
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Error - Neutron</title>
  <style>
    body {
      font-family: system-ui, sans-serif;
      background: #0A0A0A;
      color: #EDEDED;
      padding: 2rem;
      margin: 0;
    }
    .error-container {
      max-width: 800px;
      margin: 0 auto;
    }
    h1 { color: #FF4444; margin-top: 0; }
    pre {
      background: #141414;
      padding: 1rem;
      border-radius: 8px;
      overflow-x: auto;
      border: 1px solid #333;
    }
    .message { font-size: 1.25rem; margin-bottom: 1rem; }
    .stack { font-size: 0.875rem; color: #888; }
  </style>
</head>
<body>
  <div class="error-container">
    <h1>Application Error</h1>
    <p class="message">${displayMessage}</p>
    <p style="margin-top: 2rem; color: #666;">
      Add an <code>ErrorBoundary</code> export to customize this page.
    </p>
  </div>
</body>
</html>`;
}

function wrapHtml(
  content: string,
  pathname: string,
  headHtml: string,
  loaderData: Record<string, unknown>,
  actionData?: unknown,
  clientEntryScriptSrc: string | null = null,
  includeClientRuntime: boolean = true
): string {
  return `${buildHtmlPrefix(pathname, headHtml)}${content}${buildHtmlSuffix(
    loaderData,
    actionData,
    clientEntryScriptSrc,
    includeClientRuntime
  )}`;
}

function buildHtmlPrefix(pathname: string, headHtml: string = ""): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
${headHtml || renderDocumentHead(pathname, null)}
</head>
<body>
<div id="app">`;
}

function buildHtmlSuffix(
  loaderData: Record<string, unknown>,
  actionData?: unknown,
  clientEntryScriptSrc: string | null = null,
  includeClientRuntime: boolean = true
): string {
  if (!includeClientRuntime) {
    return `</div>
</body>
</html>`;
  }

  const allData: Record<string, unknown> = { ...loaderData };
  if (actionData !== undefined) {
    allData.__action__ = actionData;
  }

  const dataScript =
    Object.keys(allData).length > 0
      ? `<script>window.__NEUTRON_DATA_SERIALIZED__=${serializeForInlineScript(allData)};</script>`
      : "";
  const clientScript = clientEntryScriptSrc
    ? `<script type="module" src="${escapeHtml(clientEntryScriptSrc)}"></script>`
    : "";

  return `</div>
${dataScript}
${clientScript}
</body>
</html>`;
}

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

/** Strip <script> tags, event handler attributes, and javascript: URLs from head HTML fragments. */
function sanitizeHeadHtml(html: string): string {
  // Strip script tags and their contents
  html = html.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '');
  // Strip event handler attributes
  html = html.replace(/\s+on\w+\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]*)/gi, '');
  // Strip javascript: URLs
  html = html.replace(/(?:href|src|action)\s*=\s*(?:"javascript:[^"]*"|'javascript:[^']*')/gi, '');
  return html;
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;"); // SECURITY: Escape single quotes for use in single-quoted HTML attributes
}

let requestCounter = 0;

function createRequestId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  requestCounter += 1;
  return `req-${Date.now()}-${requestCounter}`;
}

function emitHook<TEvent>(
  hook: ((event: TEvent) => void | Promise<void>) | undefined,
  event: TEvent
): void {
  if (!hook) {
    return;
  }

  try {
    const result = hook(event);
    if (result && typeof (result as Promise<void>).then === "function") {
      void (result as Promise<void>).catch((error) => {
        console.warn("Neutron hook failed:", error);
      });
    }
  } catch (error) {
    console.warn("Neutron hook failed:", error);
  }
}

function toError(value: unknown): Error {
  if (value instanceof Error) {
    return value;
  }

  if (typeof value === "string") {
    return new Error(value);
  }

  return new Error("Unknown error");
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

export async function startServer(options: NeutronServerOptions = {}) {
  const { url, close } = await createServer(options);

  console.log(`\n  Neutron production server running:\n`);
  console.log(`  Local:   ${url}\n`);
  console.log(`  Press Ctrl+C to stop\n`);

  let shuttingDown = false;
  const shutdown = () => {
    if (shuttingDown) return;
    shuttingDown = true;

    console.log("\nShutting down...");
    void close().finally(() => {
      process.exit(0);
    });
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}
