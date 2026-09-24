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
// wording is preserved for diagnostics; parameters are never attached.

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

/** One structured, redacted observability event. `sql` is the compiled
 * statement text (placeholders, never bound values) on query events; `params`
 * exists ONLY under NEUTRON_SQL_LOG_PARAMS=1. */
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
  return {
    name: err instanceof Error ? err.constructor.name : typeof err,
    message: err instanceof Error ? err.message : String(err),
    sqlstate,
  };
}

export function resolveLogger(option: LoggerOption | undefined): Logger | null {
  if (option === undefined || option === false) return null;
  if (option === true) {
    return (event) => {
      // Defense in depth: even if an event somehow carries params, the
      // default sink never prints them unless the process opted in.
      const { params, ...rest } = event;
      const payload = paramsLoggingEnabled() ? { ...rest, params } : rest;
      console.log(`[neutron-sql] ${JSON.stringify(payload)}`);
    };
  }
  return option;
}
