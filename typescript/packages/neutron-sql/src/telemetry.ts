import { AsyncLocalStorage } from "node:async_hooks";
import type { Logger, SqlEvent } from "./logger.js";

export interface QueryMetrics {
  readonly started: number;
  readonly succeeded: number;
  readonly failed: number;
  readonly canceled: number;
  readonly durationMs: number;
}
interface MutableMetrics { started: number; succeeded: number; failed: number; canceled: number; durationMs: number }
/** Structural subset of an OpenTelemetry Span; no SDK/exporter is installed here. */
export interface SqlTelemetrySpan {
  addEvent(name: string, attributes?: Record<string, string | number | boolean>): unknown;
}

/** One application-owned observer; run() creates an isolated request counter.
 * The application supplies its active OTel span from its configured context manager.
 * SQL text, parameters, error messages, connection details and transaction IDs
 * are never forwarded, including when diagnostic SQL logging is enabled. */
export function createQueryTelemetry(activeSpan?: () => SqlTelemetrySpan | undefined) {
  const storage = new AsyncLocalStorage<MutableMetrics>();
  const logger: Logger = (event: SqlEvent) => {
    const metrics = storage.getStore();
    if (metrics) {
      if (event.kind === "query-begin") metrics.started++;
      if (event.kind === "query-end") metrics.succeeded++;
      if (event.kind === "query-error") metrics.failed++;
      if (event.kind === "cancel") metrics.canceled++;
      if ((event.kind === "query-end" || event.kind === "query-error") && typeof event.durationMs === "number" && Number.isFinite(event.durationMs) && event.durationMs >= 0) metrics.durationMs += event.durationMs;
    }
    try {
      const span = activeSpan?.();
      if (!span) return;
      const attributes: Record<string, string | number | boolean> = {};
      if (/^[0-9a-f]{16}$/.test(event.statementId)) attributes["neutron.statement.id"] = event.statementId;
      if (typeof event.durationMs === "number" && Number.isFinite(event.durationMs) && event.durationMs >= 0) attributes["neutron.duration.ms"] = event.durationMs;
      if (event.error?.sqlstate && /^[0-9A-Z]{5}$/.test(event.error.sqlstate)) attributes["neutron.sqlstate"] = event.error.sqlstate;
      const result = span.addEvent(`neutron.${event.kind}`, attributes);
      if (result !== null && (typeof result === "object" || typeof result === "function") && typeof (result as { then?: unknown }).then === "function") void Promise.resolve(result).catch(() => {});
    } catch { /* exporter failures cannot change SQL outcomes */ }
  };
  return {
    logger,
    async run<T>(work: () => Promise<T>): Promise<{ value: T; metrics: QueryMetrics }> {
      const metrics: MutableMetrics = { started: 0, succeeded: 0, failed: 0, canceled: 0, durationMs: 0 };
      return storage.run(metrics, async () => {
        const value = await work();
        return { value, metrics: Object.freeze({ ...metrics }) };
      });
    },
    /** Detached snapshot for middleware that reports metrics even on failure. */
    current(): QueryMetrics | undefined {
      const metrics = storage.getStore();
      return metrics ? Object.freeze({ ...metrics }) : undefined;
    },
  };
}
