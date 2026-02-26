import type {
  NeutronActionEndEvent,
  NeutronActionStartEvent,
  NeutronErrorEvent,
  NeutronLoaderEndEvent,
  NeutronLoaderStartEvent,
  NeutronRequestEndEvent,
  NeutronRequestStartEvent,
  NeutronServerHooks,
} from "neutron";

interface OTelSpan {
  setAttribute(key: string, value: string | number | boolean): void;
  recordException(error: Error): void;
  setStatus(status: { code: number; message?: string }): void;
  end(endTime?: number): void;
}

interface OTelTracer {
  startSpan(name: string, options?: Record<string, unknown>, context?: unknown): OTelSpan;
}

interface OTelContextApi {
  active(): unknown;
}

interface OTelTraceApi {
  getTracer(name: string, version?: string): OTelTracer;
  setSpan(context: unknown, span: OTelSpan): unknown;
}

interface OTelPropagationApi {
  extract(context: unknown, carrier: unknown): unknown;
}

interface OTelApi {
  context: OTelContextApi;
  trace: OTelTraceApi;
  propagation?: OTelPropagationApi;
  SpanStatusCode?: {
    ERROR: number;
  };
}

export interface OpenTelemetryHooksOptions {
  api?: OTelApi;
  serviceName?: string;
  serviceVersion?: string;
  tracerName?: string;
  defaultAttributes?: Record<string, string | number | boolean>;
  logger?: Pick<Console, "warn">;
}

export async function createOpenTelemetryHooks(
  options: OpenTelemetryHooksOptions = {}
): Promise<NeutronServerHooks> {
  const logger = options.logger || console;
  const api =
    options.api ||
    (await loadOpenTelemetryApi(logger));

  if (!api) {
    return {};
  }

  const tracer = api.trace.getTracer(
    options.tracerName || options.serviceName || "neutron",
    options.serviceVersion
  );

  const requestSpans = new Map<string, OTelSpan>();
  const loaderSpans = new Map<string, OTelSpan>();
  const actionSpans = new Map<string, OTelSpan>();
  const defaultAttributes = {
    "neutron.runtime": "node",
    ...(options.serviceName ? { "service.name": options.serviceName } : {}),
    ...(options.serviceVersion ? { "service.version": options.serviceVersion } : {}),
    ...(options.defaultAttributes || {}),
  };

  const hooks: NeutronServerHooks = {
    onRequestStart(event) {
      const span = tracer.startSpan(
        `${event.method} ${event.pathname}`,
        {
          attributes: {
            ...defaultAttributes,
            "http.method": event.method,
            "http.route": event.pathname,
            "url.path": event.pathname,
            "neutron.request_id": event.requestId,
          },
          startTime: event.startedAt,
        },
        api.context.active()
      );
      requestSpans.set(event.requestId, span);
    },
    onRequestEnd(event) {
      const span = requestSpans.get(event.requestId);
      if (!span) {
        return;
      }

      setCommonHttpResultAttributes(span, event);
      if (event.routeId) {
        span.setAttribute("neutron.route_id", event.routeId);
      }
      if (event.routeMode) {
        span.setAttribute("neutron.route_mode", event.routeMode);
      }
      if (event.cacheState) {
        span.setAttribute("neutron.cache_state", event.cacheState);
      }
      span.end(event.endedAt);
      requestSpans.delete(event.requestId);
    },
    onLoaderStart(event) {
      const requestSpan = requestSpans.get(event.requestId);
      const parentCtx = requestSpan
        ? api.trace.setSpan(api.context.active(), requestSpan)
        : api.context.active();
      const span = tracer.startSpan(
        `loader ${event.routePath}`,
        {
          attributes: {
            ...defaultAttributes,
            "neutron.request_id": event.requestId,
            "neutron.route_id": event.routeId,
            "neutron.route_path": event.routePath,
            "neutron.phase": "loader",
          },
          startTime: event.startedAt,
        },
        parentCtx
      );
      loaderSpans.set(loaderSpanKey(event), span);
    },
    onLoaderEnd(event) {
      const key = loaderSpanKey(event);
      const span = loaderSpans.get(key);
      if (!span) {
        return;
      }

      span.setAttribute("neutron.loader.outcome", event.outcome);
      if (event.cacheStatus) {
        span.setAttribute("neutron.loader.cache_status", event.cacheStatus);
      }
      if (typeof event.responseStatus === "number") {
        span.setAttribute("http.status_code", event.responseStatus);
      }
      span.end(event.endedAt);
      loaderSpans.delete(key);
    },
    onActionStart(event) {
      const requestSpan = requestSpans.get(event.requestId);
      const parentCtx = requestSpan
        ? api.trace.setSpan(api.context.active(), requestSpan)
        : api.context.active();
      const span = tracer.startSpan(
        `action ${event.routePath}`,
        {
          attributes: {
            ...defaultAttributes,
            "neutron.request_id": event.requestId,
            "neutron.route_id": event.routeId,
            "neutron.route_path": event.routePath,
            "neutron.phase": "action",
          },
          startTime: event.startedAt,
        },
        parentCtx
      );
      actionSpans.set(actionSpanKey(event), span);
    },
    onActionEnd(event) {
      const key = actionSpanKey(event);
      const span = actionSpans.get(key);
      if (!span) {
        return;
      }

      span.setAttribute("neutron.action.outcome", event.outcome);
      if (typeof event.responseStatus === "number") {
        span.setAttribute("http.status_code", event.responseStatus);
      }
      span.end(event.endedAt);
      actionSpans.delete(key);
    },
    onError(event) {
      const requestSpan = requestSpans.get(event.requestId);
      if (!requestSpan) {
        return;
      }

      requestSpan.recordException(event.error);
      requestSpan.setAttribute("neutron.error.source", event.source);
      if (event.routeId) {
        requestSpan.setAttribute("neutron.route_id", event.routeId);
      }
      if (api.SpanStatusCode) {
        requestSpan.setStatus({
          code: api.SpanStatusCode.ERROR,
          message: event.error.message,
        });
      }
    },
  };

  return hooks;
}

function loaderSpanKey(
  event: NeutronLoaderStartEvent | NeutronLoaderEndEvent
): string {
  return `${event.requestId}:${event.routeId}`;
}

function actionSpanKey(
  event: NeutronActionStartEvent | NeutronActionEndEvent
): string {
  return `${event.requestId}:${event.routeId}`;
}

function setCommonHttpResultAttributes(
  span: OTelSpan,
  event: NeutronRequestEndEvent
): void {
  span.setAttribute("http.method", event.method);
  span.setAttribute("http.status_code", event.status);
  span.setAttribute("http.route", event.pathname);
  span.setAttribute("http.server_duration_ms", event.durationMs);
}

async function loadOpenTelemetryApi(
  logger: Pick<Console, "warn">
): Promise<OTelApi | null> {
  try {
    const imported = await dynamicImport("@opentelemetry/api");
    return imported as OTelApi;
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    logger.warn(
      `[@neutron/otel] Optional dependency @opentelemetry/api is unavailable. OTel hooks disabled. ${reason}`
    );
    return null;
  }
}

type DynamicImporter = (specifier: string) => Promise<unknown>;

const dynamicImport = new Function(
  "specifier",
  "return import(specifier);"
) as DynamicImporter;

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
