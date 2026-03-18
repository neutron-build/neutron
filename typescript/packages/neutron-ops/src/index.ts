import type {
  AppContext,
  MiddlewareFn,
  NeutronErrorEvent,
  NeutronRequestEndEvent,
  NeutronServerHooks,
} from "neutron";

export interface RequestContextMiddlewareOptions {
  requestIdHeader?: string;
  responseHeader?: string;
  requestIdContextKey?: string;
  traceIdContextKey?: string;
}

export interface HealthcheckMiddlewareOptions {
  healthPath?: string;
  readyPath?: string;
  ready?: () => Promise<boolean> | boolean;
  service?: string;
}

export interface JsonLoggingHooksOptions {
  logger?: {
    info: (line: string) => void;
    error: (line: string) => void;
  };
  baseFields?: Record<string, unknown>;
}

const DEFAULT_REQUEST_ID_CONTEXT_KEY = "requestId";
const DEFAULT_TRACE_ID_CONTEXT_KEY = "traceId";

export function createRequestContextMiddleware(
  options: RequestContextMiddlewareOptions = {}
): MiddlewareFn {
  const requestIdHeader = (options.requestIdHeader || "x-request-id").toLowerCase();
  const responseHeader = options.responseHeader || "x-request-id";
  const requestIdContextKey =
    options.requestIdContextKey || DEFAULT_REQUEST_ID_CONTEXT_KEY;
  const traceIdContextKey = options.traceIdContextKey || DEFAULT_TRACE_ID_CONTEXT_KEY;

  return async (request, context, next) => {
    const existingRequestId = request.headers.get(requestIdHeader);
    const requestId = existingRequestId || createRequestId();
    const traceId = extractTraceId(request);

    context[requestIdContextKey] = requestId;
    if (traceId) {
      context[traceIdContextKey] = traceId;
    }

    const response = await next();
    if (!response.headers.has(responseHeader)) {
      response.headers.set(responseHeader, requestId);
    }
    return response;
  };
}

export function createHealthcheckMiddleware(
  options: HealthcheckMiddlewareOptions = {}
): MiddlewareFn {
  const healthPath = normalizePath(options.healthPath || "/healthz");
  const readyPath = normalizePath(options.readyPath || "/readyz");
  const service = options.service || "neutron-app";

  return async (request, _context, next) => {
    const pathname = normalizePath(new URL(request.url).pathname);
    if (pathname === healthPath) {
      return jsonResponse(200, {
        ok: true,
        status: "healthy",
        service,
      });
    }

    if (pathname === readyPath) {
      const isReady = options.ready ? await options.ready() : true;
      if (!isReady) {
        return jsonResponse(503, {
          ok: false,
          status: "not-ready",
          service,
        });
      }
      return jsonResponse(200, {
        ok: true,
        status: "ready",
        service,
      });
    }

    return await next();
  };
}

export function createJsonLoggingHooks(
  options: JsonLoggingHooksOptions = {}
): NeutronServerHooks {
  const logger = options.logger || {
    info: (line: string) => console.log(line),
    error: (line: string) => console.error(line),
  };
  const base = options.baseFields || {};

  return {
    onRequestEnd(event) {
      logger.info(
        JSON.stringify({
          level: "info",
          event: "request.end",
          ...base,
          ...toRequestFields(event),
          timestamp: new Date(event.endedAt).toISOString(),
        })
      );
    },
    onError(event) {
      logger.error(
        JSON.stringify({
          level: "error",
          event: "request.error",
          ...base,
          requestId: event.requestId,
          method: event.method,
          pathname: event.pathname,
          source: event.source,
          routeId: event.routeId,
          routePath: event.routePath,
          error: event.error.message,
          stack: event.error.stack,
          timestamp: new Date().toISOString(),
        })
      );
    },
  };
}

export function getRequestIdFromContext(
  context: AppContext,
  contextKey: string = DEFAULT_REQUEST_ID_CONTEXT_KEY
): string | null {
  const value = context[contextKey];
  return typeof value === "string" ? value : null;
}

export function getTraceIdFromContext(
  context: AppContext,
  contextKey: string = DEFAULT_TRACE_ID_CONTEXT_KEY
): string | null {
  const value = context[contextKey];
  return typeof value === "string" ? value : null;
}

export function mergeNeutronHooks(
  base: NeutronServerHooks | undefined,
  extra: NeutronServerHooks | undefined
): NeutronServerHooks | undefined {
  if (!base && !extra) {
    return undefined;
  }
  if (!base) {
    return extra;
  }
  if (!extra) {
    return base;
  }

  return {
    onRequestStart: chain(base.onRequestStart, extra.onRequestStart),
    onRequestEnd: chain(base.onRequestEnd, extra.onRequestEnd),
    onLoaderStart: chain(base.onLoaderStart, extra.onLoaderStart),
    onLoaderEnd: chain(base.onLoaderEnd, extra.onLoaderEnd),
    onActionStart: chain(base.onActionStart, extra.onActionStart),
    onActionEnd: chain(base.onActionEnd, extra.onActionEnd),
    onError: chain(base.onError, extra.onError),
  };
}

function chain<TEvent>(
  left: ((event: TEvent) => void | Promise<void>) | undefined,
  right: ((event: TEvent) => void | Promise<void>) | undefined
): ((event: TEvent) => Promise<void>) | undefined {
  if (!left && !right) {
    return undefined;
  }
  if (!left) {
    return async (event) => {
      await right?.(event);
    };
  }
  if (!right) {
    return async (event) => {
      await left(event);
    };
  }

  return async (event) => {
    await left(event);
    await right(event);
  };
}

function jsonResponse(status: number, payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function toRequestFields(event: NeutronRequestEndEvent): Record<string, unknown> {
  return {
    requestId: event.requestId,
    method: event.method,
    url: event.url,
    pathname: event.pathname,
    status: event.status,
    durationMs: event.durationMs,
    routeId: event.routeId,
    routePath: event.routePath,
    routeMode: event.routeMode,
    cacheState: event.cacheState,
  };
}

function extractTraceId(request: Request): string | null {
  const traceparent = request.headers.get("traceparent");
  if (!traceparent) {
    return null;
  }

  const parts = traceparent.split("-");
  if (parts.length < 4) {
    return null;
  }
  const traceId = parts[1];
  return /^[0-9a-f]{32}$/i.test(traceId) ? traceId : null;
}

function normalizePath(pathname: string): string {
  if (!pathname.startsWith("/")) {
    return `/${pathname}`;
  }
  if (pathname.length > 1 && pathname.endsWith("/")) {
    return pathname.slice(0, -1);
  }
  return pathname;
}

function createRequestId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `req-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
}
