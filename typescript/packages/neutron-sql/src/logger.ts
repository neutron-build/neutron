// ---------------------------------------------------------------------------
// @neutron-build/sql — structured SQL observability (I02)
// ---------------------------------------------------------------------------
// Events are REDACTED BY DEFAULT: parameter values and connection strings
// never appear. The `params` field is populated only when the process sets
// NEUTRON_SQL_LOG_PARAMS=1 (an explicit, redaction-free mode — the emitted
// values are then visible in whatever sink receives the events; the docs
// warn against enabling it where logs are shared). Event kinds: query-begin,
// query-end, query-error, tx-begin, tx-commit, tx-rollback, savepoint,
// cancel. The default logger (`logger: true`) prints one JSON line per
// event. Error entries carry {name, message, sqlstate} — the server's own
// wording is omitted because native messages can contain bound values.

import { createHash } from "node:crypto";

export type SqlEventKind =
  | "query-begin"
  | "query-end"
  | "query-error"
  | "tx-begin"
  | "tx-commit"
  | "tx-rollback"
  | "savepoint"
  | "cancel";

export type IsolationLevel = "read-committed" | "repeatable-read" | "serializable";

/** One structured observability event. Resolved sinks omit `sql` and `params`
 * unless NEUTRON_SQL_LOG_PARAMS=1 explicitly exposes diagnostic values. */
export interface SqlEvent {
  readonly kind: SqlEventKind;
  /** Deterministic id of the statement (sha256 of its SQL text, 16 hex
   * chars); transaction-control events carry the id of their control
   * statement. */
  readonly statementId: string;
  sql?: string;
  durationMs?: number;
  /** Error summary: {name, message, sqlstate?}. Never the raw Error object
   * (its properties are unbounded); never parameter values. */
  error?: { name: string; message: string; sqlstate?: string };
  /** Transaction id shared by every event of one transaction attempt. */
  txId?: string;
  savepointName?: string;
  savepointAction?: "create" | "release" | "rollback-to";
  isolation?: IsolationLevel;
  readOnly?: boolean;
  deferrable?: boolean;
  cancelReason?: "deadline" | "signal";
  attempt?: number;
  params?: readonly unknown[];
}

export type Logger = (event: SqlEvent) => void;

export type LoggerOption = boolean | Logger;

/** Observe without changing an operation's outcome, including async sinks. */
export function observeSafely(observer: Logger | undefined, event: SqlEvent): void {
  try {
    const result: unknown = observer?.(event);
    if (result !== null && (typeof result === "object" || typeof result === "function") && typeof (result as { then?: unknown }).then === "function") {
      void Promise.resolve(result).catch(() => {});
    }
  } catch { /* best-effort observation */ }
}

/** Deprecated pre-I02 name of SqlEvent. The shape changed with structured
 * events: `params` is now optional (populated only under
 * NEUTRON_SQL_LOG_PARAMS=1) and `error` is a redacted summary object. */
export type LogEvent = SqlEvent;

/** True when NEUTRON_SQL_LOG_PARAMS=1 opts into redaction-free parameter
 * logging. Read lazily so tests can toggle it per process. */
export function paramsLoggingEnabled(): boolean {
  return process.env.NEUTRON_SQL_LOG_PARAMS === "1";
}

/** Deterministic statement id for events: 16 hex chars of sha256(sql). */
export function statementIdOf(sqlText: string): string {
  return createHash("sha256").update(sqlText, "utf8").digest("hex").slice(0, 16);
}

/** Redacted error summary for events. */
export function errorSummary(err: unknown): { name: string; message: string; sqlstate?: string } {
  const candidate =
    typeof (err as { sqlstate?: unknown })?.sqlstate === "string"
      ? (err as { sqlstate: string }).sqlstate
      : typeof (err as { code?: unknown })?.code === "string"
        ? (err as { code: string }).code
        : undefined;
  const sqlstate = candidate !== undefined && /^[0-9A-Z]{5}$/.test(candidate) ? candidate : undefined;
  const name = err instanceof Error ? err.constructor.name : typeof err;
  const safeNames = ["Error", "TypeError", "RangeError", "NeutronSqlError", "ServerSqlError", "QueryCanceledError", "ConnectionFailedError", "MissingDriverError", "CommitAmbiguityError"];
  return {
    name: err instanceof Error ? (safeNames.includes(name) ? name : "Error") : typeof err,
    message: sqlstate ? `SQL request failed (${sqlstate})` : "SQL request failed",
    sqlstate,
  };
}

export function resolveLogger(option: LoggerOption | undefined): Logger | null {
  if (option === undefined || option === false) return null;
  const sink: Logger = option === true
    ? (event) => console.log(`[neutron-sql] ${JSON.stringify(event)}`)
    : option;
  return (event) => {
    // Raw SQL can contain literal secrets even when every bound parameter is
    // omitted. Both sinks receive identifiers/counts/timing by default.
    const { params, sql, ...rest } = event;
    const payload = paramsLoggingEnabled() ? { ...rest, sql, params } : rest;
    // Telemetry cannot turn an acknowledged write into an application failure.
    observeSafely(sink, payload);
  };
}
