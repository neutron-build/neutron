// Shared render core — the single implementation of the app-route render
// pipeline (loader/action/middleware loop, head/headers resolution, HTML
// wrapping, streaming, error rendering). Runtime-agnostic: it operates on an
// ALREADY-LOADED route-module map, so both request-serving entry points (the
// dev Vite server and the prod codegen entry) load modules their own way and
// then call renderAppRoute. This replaced two drifting copies of the same
// logic. The static (SSG) renderer keeps its own lean, mutation-free pipeline
// (see render-static.ts) but shares head resolution via core/head.ts, so head
// output can never drift between the request-serving and build-time paths.
import { h } from "preact";
import type * as preact from "preact";

import { escapeHtml } from "./escape.js";
import { encodeSerializedPayloadAsJson, serializeForInlineScript } from "./serialization.js";
import { assertRenderedFragment, decodeChunkStart } from "./fragment-guard.js";
import {
  renderDocumentHead,
  buildHtmlOpenTag,
  buildBodyOpenTag,
  type SeoMetaInput,
} from "./seo.js";
import { resolveHeadDocument } from "./head.js";
import { runMiddlewareChain } from "./middleware.js";
import { renderToString } from "preact-render-to-string";
import type {
  Route,
  RouteMatch,
  RouteModule,
  MiddlewareFn,
  AppContext,
  LoaderArgs,
  ActionArgs,
  HeadersArgs,
  ErrorBoundaryProps,
} from "./types.js";
import type { NeutronLoaderCacheStore } from "../server/cache-store.js";

const TEXT_ENCODER = new TextEncoder();
type StreamRenderFn = (element: preact.VNode) => ReadableStream<Uint8Array> & {
  allReady?: Promise<void>;
};
let cachedStreamRenderFn: StreamRenderFn | null | undefined = undefined;

// Structural hook/trace types kept local to avoid a server import cycle.
export interface RenderRequestTrace {
  requestId: string;
  method: string;
  pathname: string;
}
// The render core forwards observability events to whatever host supplies
// them (dev server, prod entry, SSG) without inspecting their shape, so the
// event parameter is intentionally host-defined. Using `any` here keeps the
// core structurally compatible with the host's specifically-typed hook maps
// (e.g. NeutronServerHooks) without a core->server type dependency.
export interface RenderHooks {
  onLoaderStart?: (e: any) => void | Promise<void>;
  onLoaderEnd?: (e: any) => void | Promise<void>;
  onActionStart?: (e: any) => void | Promise<void>;
  onActionEnd?: (e: any) => void | Promise<void>;
  onError?: (e: any) => void | Promise<void>;
}
export interface RenderAppRouteOptions {
  clientEntryScriptSrc: string | null;
  loaderDataCache: NeutronLoaderCacheStore;
  requestTrace: RenderRequestTrace;
  hooks?: RenderHooks;
  globalMiddleware?: MiddlewareFn[];
}

export function toError(value: unknown): Error {
  if (value instanceof Error) {
    return value;
  }

  if (typeof value === "string") {
    return new Error(value);
  }

  return new Error("Unknown error");
}

export function emitHook<TEvent>(
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

export function isMutationMethod(method: string): boolean {
  const normalized = method.toUpperCase();
  return (
    normalized === "POST" ||
    normalized === "PUT" ||
    normalized === "PATCH" ||
    normalized === "DELETE"
  );
}

export function isJsonRequest(request: Request): boolean {
  if (isNeutronDataRequest(request)) {
    return true;
  }
  const accept = request.headers.get("Accept") || "";
  return accept.includes("application/json");
}

function isNeutronDataRequest(request: Request): boolean {
  return request.headers.get("X-Neutron-Data") === "true";
}

function withDefaultContentType(headers: Headers, fallback: string): Headers {
  if (!headers.has("Content-Type")) {
    headers.set("Content-Type", fallback);
  }
  return headers;
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
  <title>Error</title>
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

function buildHtmlPrefix(
  pathname: string,
  headHtml: string = "",
  seo: SeoMetaInput | null = null
): string {
  return `<!DOCTYPE html>
${buildHtmlOpenTag(seo?.htmlAttrs)}
<head>
${headHtml || renderDocumentHead(pathname, null)}
</head>
${buildBodyOpenTag(seo?.bodyAttrs)}
<div id="app">`;
}

function buildHtmlSuffix(
  loaderData: Record<string, unknown>,
  actionData?: unknown,
  clientEntryScriptSrc: string | null = null,
  includeClientRuntime: boolean = true,
  nonce?: string
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

  // Carry the CSP nonce onto the framework's own inline/module scripts so a
  // nonce-based Content-Security-Policy admits them (without it, the data
  // script is blocked and hydration breaks).
  const na = nonceAttr(nonce);
  const dataScript =
    Object.keys(allData).length > 0
      ? `<script${na}>window.__NEUTRON_DATA_SERIALIZED__=${serializeForInlineScript(allData)};</script>`
      : "";
  const clientScript = clientEntryScriptSrc
    ? `<script type="module"${na} src="${escapeHtml(clientEntryScriptSrc)}"></script>`
    : "";

  return `</div>
${dataScript}
${clientScript}
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
  includeClientRuntime: boolean = true,
  nonce?: string,
  seo: SeoMetaInput | null = null
): string {
  return `${buildHtmlPrefix(pathname, headHtml, seo)}${content}${buildHtmlSuffix(
    loaderData,
    actionData,
    clientEntryScriptSrc,
    includeClientRuntime,
    nonce
  )}`;
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

function streamHtmlDocument(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  firstChunk: ReadableStreamReadResult<Uint8Array>,
  prefix: string,
  suffix: string
): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      controller.enqueue(TEXT_ENCODER.encode(prefix));
      try {
        if (!firstChunk.done) {
          if (firstChunk.value) {
            controller.enqueue(firstChunk.value);
          }
          while (true) {
            const { done, value } = await reader.read();
            if (done) {
              break;
            }
            if (value) {
              controller.enqueue(value);
            }
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

interface RenderAppRouteHtmlResponseArgs {
  request: Request;
  element: preact.VNode;
  pathname: string;
  headHtml: string;
  /** Merged SeoMetaInput from the route chain — source of htmlAttrs/bodyAttrs. */
  seo?: SeoMetaInput | null;
  loaderData: Record<string, unknown>;
  actionData?: unknown;
  clientEntryScriptSrc: string | null;
  includeClientRuntime: boolean;
  headers: Headers;
  nonce?: string;
  /** Outermost layout/route file, named in the full-document guard error. */
  sourceFile?: string;
}

async function renderAppRouteHtmlResponse(
  args: RenderAppRouteHtmlResponseArgs
): Promise<Response> {
  const headers = withDefaultContentType(args.headers, "text/html; charset=utf-8");
  if (args.request.method.toUpperCase() === "HEAD") {
    return new Response(null, { headers });
  }

  // Non-streaming compose: render, guard against a full-document render, then
  // wrap the fragment in the shell. Also the fallback when streaming is
  // unavailable or fails to initialize.
  const composeFullDocument = (): string => {
    const html = renderToString(args.element);
    assertRenderedFragment(html, args.sourceFile);
    return wrapHtml(
      html,
      args.pathname,
      args.headHtml,
      args.loaderData,
      args.actionData,
      args.clientEntryScriptSrc,
      args.includeClientRuntime,
      args.nonce,
      args.seo ?? null
    );
  };

  const streamRenderFn = await getStreamRenderFn();
  if (!streamRenderFn) {
    return new Response(composeFullDocument(), { headers });
  }

  const shellPrefix = buildHtmlPrefix(args.pathname, args.headHtml, args.seo ?? null);
  const shellSuffix = buildHtmlSuffix(
    args.loaderData,
    args.actionData,
    args.clientEntryScriptSrc,
    args.includeClientRuntime,
    args.nonce
  );

  let reader: ReadableStreamDefaultReader<Uint8Array>;
  let firstChunk: ReadableStreamReadResult<Uint8Array>;
  try {
    reader = streamRenderFn(args.element).getReader();
    firstChunk = await reader.read();
  } catch {
    return new Response(composeFullDocument(), { headers });
  }

  // Guard the first streamed bytes before the shell prefix is emitted, so a
  // full-document render (nested <html>/<body> inside #app) fails as a clean
  // error response rather than shipping malformed, doubly-hydrated markup.
  if (!firstChunk.done && firstChunk.value) {
    assertRenderedFragment(decodeChunkStart(firstChunk.value), args.sourceFile);
  }

  const body = streamHtmlDocument(reader, firstChunk, shellPrefix, shellSuffix);
  return new Response(body, { headers });
}

function stableEncodeParams(params: Record<string, string>): string {
  const sortedEntries = Object.entries(params).sort(([left], [right]) =>
    left.localeCompare(right)
  );
  return JSON.stringify(sortedEntries);
}

function nonceAttr(nonce?: string): string {
  if (!nonce || !/^[A-Za-z0-9+/_=-]+$/.test(nonce)) {
    return "";
  }
  return ` nonce="${nonce}"`;
}

export async function renderAppRoute(
  request: Request,
  match: RouteMatch,
  routeModules: Map<string, RouteModule>,
  opts: RenderAppRouteOptions
): Promise<Response> {
  const { clientEntryScriptSrc, loaderDataCache, requestTrace, hooks, globalMiddleware } = opts;
  const allRoutes = [...match.layouts, match.route];
  const includeClientRuntime = allRoutes.every((route) => route.config.hydrate !== false);
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

  return runMiddlewareChain(middlewares, request, context, async () => {
    let actionData: unknown = undefined;
    const pageModule = routeModules.get(match.route.id);
    const requestedRouteIds = resolveRequestedDataRouteIds(
      request,
      allRoutes,
      isMutationMethod(request.method)
    );

    if (!pageModule?.default) {
      // Resource route: a module with no component can still serve — its
      // loader (GET) or action (mutations) must produce a raw Response
      // (returned or thrown). Anything else has nothing to render → 404.
      const handler = isMutationMethod(request.method) ? pageModule?.action : pageModule?.loader;
      if (handler) {
        try {
          const result = await handler({ request, params: match.params, context });
          if (result instanceof Response) return result;
        } catch (error) {
          if (error instanceof Response) return error;
          throw error;
        }
      }
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
        // A loader may RETURN a raw Response (redirect, custom status,
        // streamed body) — not only throw one. Serve it directly instead of
        // treating it as component data. Matches resource-route + action
        // semantics; a plain object is still normal loader data.
        if (data instanceof Response) {
          return { routeId: route.id, data: undefined, response: data };
        }
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
      if ((result as { response?: Response }).response) {
        return (result as { response: Response }).response;
      }
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
    // Carry the CSP nonce (set by createCspNonceMiddleware) onto head-emitted
    // scripts (JSON-LD, inline headScripts) so a nonce-based CSP admits them.
    const cspNonce =
      typeof (context as { cspNonce?: unknown }).cspNonce === "string"
        ? ((context as { cspNonce?: unknown }).cspNonce as string)
        : undefined;
    const { headHtml, seo } = await resolveHeadDocument(
      allRoutes.map((route) => ({ route, module: routeModules.get(route.id) })),
      {
        request,
        params: match.params,
        context,
        pathname,
        loaderData,
        actionData,
        nonce: cspNonce,
      }
    );

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
      // Awaited so a synchronous compose failure (e.g. the full-document guard)
      // is caught here and routed through the render error boundary, rather than
      // escaping to the generic request-level handler. Streaming body errors
      // still surface via the stream controller, unaffected by this await.
      return await renderAppRouteHtmlResponse({
        request,
        element,
        pathname,
        loaderData,
        actionData,
        headHtml,
        seo,
        clientEntryScriptSrc,
        includeClientRuntime,
        headers: routeHeaders,
        // Outermost layout/route — the most likely author of a stray <html>.
        sourceFile: allRoutes[0]?.file,
        // Set by createCspNonceMiddleware (if used) before next(); carried onto
        // the framework's inline scripts so a nonce-based CSP admits them.
        nonce:
          typeof (context as { cspNonce?: unknown }).cspNonce === "string"
            ? ((context as { cspNonce?: unknown }).cspNonce as string)
            : undefined,
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
