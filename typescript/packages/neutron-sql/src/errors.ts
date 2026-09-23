// ---------------------------------------------------------------------------
// @neutron-build/sql — driver error taxonomy
// ---------------------------------------------------------------------------
// Stable error classes wrapping both drivers (pg 8.x, postgres.js 3.x) so
// callers can classify failures without matching message text. The original
// driver error is always preserved as `cause`, and SQLSTATE codes survive
// every wrapper (see ServerSqlError.sqlstate / getSqlState).
//
// Classification is by error SOURCE, never by message wording:
//   MissingDriverError   — the driver module itself is not importable
//   ConnectionFailedError— transport/connection-lifecycle failure (the server
//                          was never reached, the socket died, the pool was
//                          ended, the URL could not be parsed)
//   ServerSqlError       — the server answered with an SQL error (SQLSTATE
//                          retained; includes auth failures such as 28P01,
//                          which are server responses, not transport errors)

/** Base class of every error this package throws deliberately. */
export class NeutronSqlError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options as ErrorOptions);
    this.name = this.constructor.name;
  }
}

/** The requested driver package ("pg" / "postgres") is not installed or not
 *  importable in this process. Never thrown for connection failures. */
export class MissingDriverError extends NeutronSqlError {
  /** Driver module name that could not be loaded. */
  readonly driver: string;
  constructor(driver: string, message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.driver = driver;
  }
}

/** Transport-level failure: server unreachable (ECONNREFUSED, ETIMEDOUT,
 *  ENOTFOUND, ...), connect timeout, socket closed mid-query, connection
 *  already ended, or the connection URL could not be parsed. The server never
 *  produced an SQL error for this request. */
export class ConnectionFailedError extends NeutronSqlError {
  /** Raw driver error code when present ("ECONNREFUSED", "CONNECT_TIMEOUT",
   *  "CONNECTION_ENDED", ...). */
  readonly code: string | undefined;
  readonly address: string | undefined;
  readonly port: number | undefined;
  constructor(
    message: string,
    fields: { code?: string; address?: string; port?: number; cause?: unknown } = {},
  ) {
    super(message, { cause: fields.cause });
    this.code = fields.code;
    this.address = fields.address;
    this.port = fields.port;
  }
}

/** The server rejected the statement or request with an SQL error. The
 *  SQLSTATE code is retained verbatim on `sqlstate` (and stays on the
 *  original `cause`, whose own `code` field the drivers also populate). */
export class ServerSqlError extends NeutronSqlError {
  /** SQLSTATE, e.g. "23505", "42P01", "42601". Always 5 chars [0-9A-Z]. */
  readonly sqlstate: string;
  readonly severity: string | undefined;
  readonly detail: string | undefined;
  readonly hint: string | undefined;
  readonly position: string | undefined;
  constructor(
    message: string,
    fields: {
      sqlstate: string;
      severity?: string;
      detail?: string;
      hint?: string;
      position?: string;
      cause?: unknown;
    },
  ) {
    super(message, { cause: fields.cause });
    this.sqlstate = fields.sqlstate;
    this.severity = fields.severity;
    this.detail = fields.detail;
    this.hint = fields.hint;
    this.position = fields.position;
  }
}

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

const SQLSTATE_SHAPE = /^[0-9A-Z]{5}$/;

/** postgres.js synthetic connection codes (src/errors.js CONNECTION_* family).
 *  These carry no server response. */
const POSTGRES_JS_CONNECTION_CODES = new Set([
  "CONNECTION_DESTROYED",
  "CONNECT_TIMEOUT",
  "CONNECTION_CLOSED",
  "CONNECTION_ENDED",
]);

function field(err: unknown, key: string): unknown {
  if (typeof err !== "object" || err === null) return undefined;
  return (err as Record<string, unknown>)[key];
}

function isNodeSystemError(err: unknown): boolean {
  // Node transport errors set a symbolic `code` plus a numeric `errno` and/or
  // a `syscall` name. Neither driver's SQL errors ever has those.
  const code = field(err, "code");
  return (
    typeof code === "string" &&
    (typeof field(err, "errno") === "number" ||
      typeof field(err, "errno") === "string" ||
      typeof field(err, "syscall") === "string")
  );
}

function isPgPoolEndedError(err: unknown): boolean {
  // pg 8.22.0 / pg-pool 3.14.0: "Cannot use a pool after calling end on the
  // pool" (pg-pool index.js connect()). Older lines phrased it "...on it" —
  // both shapes classify as connection-lifecycle failures.
  return typeof field(err, "message") === "string" && /Cannot use (?:a )?(?:pool|client) after calling end/.test(field(err, "message") as string);
}

/** Best-effort classification of one driver-thrown error into the taxonomy.
 *  Already-classified errors pass through unchanged (no double wrapping). */
export function classifyDriverError(err: unknown, driverKind: string): NeutronSqlError {
  if (err instanceof NeutronSqlError) return err;
  if (isNodeSystemError(err) || isPgPoolEndedError(err) || POSTGRES_JS_CONNECTION_CODES.has(field(err, "code") as string)) {
    const code = field(err, "code");
    const address = field(err, "address");
    const port = field(err, "port");
    return new ConnectionFailedError(
      `${driverKind}: ${err instanceof Error ? err.message : String(err)}`,
      {
        code: typeof code === "string" ? code : undefined,
        address: typeof address === "string" ? address : undefined,
        port: typeof port === "number" ? port : undefined,
        cause: err,
      },
    );
  }
  const code = field(err, "code");
  if (typeof code === "string" && SQLSTATE_SHAPE.test(code)) {
    return new ServerSqlError(`${driverKind}: ${err instanceof Error ? err.message : String(err)}`, {
      sqlstate: code,
      severity: typeof field(err, "severity") === "string" ? (field(err, "severity") as string) : undefined,
      detail: typeof field(err, "detail") === "string" ? (field(err, "detail") as string) : undefined,
      hint: typeof field(err, "hint") === "string" ? (field(err, "hint") as string) : undefined,
      position: typeof field(err, "position") === "string" ? (field(err, "position") as string) : undefined,
      cause: err,
    });
  }
  // Unknown shape (driver-internal TypeError etc.): keep the cause, claim no
  // source class.
  return new NeutronSqlError(`${driverKind}: ${err instanceof Error ? err.message : String(err)}`, { cause: err });
}

/** A failure while CONSTRUCTING the adapter (unparseable URL, rejected
 *  options): the connection can never be established, but this is not a
 *  missing driver — auto-selection must not fall back on it. */
export function connectionConstructionError(driverKind: string, err: unknown): ConnectionFailedError {
  return new ConnectionFailedError(`${driverKind}: connection could not be established: ${err instanceof Error ? err.message : String(err)}`, { cause: err });
}

/** Walk a wrapper chain and return the first SQLSTATE found (our wrappers,
 *  raw pg DatabaseError, raw postgres.js PostgresError). */
export function getSqlState(err: unknown): string | undefined {
  let current: unknown = err;
  for (let depth = 0; current !== null && typeof current === "object" && depth < 10; depth++) {
    const record = current as Record<string, unknown>;
    if (current instanceof ServerSqlError) return current.sqlstate;
    const code = record.code;
    if (typeof code === "string" && SQLSTATE_SHAPE.test(code)) return code;
    current = record.cause;
  }
  return undefined;
}

/** True when the error is (or wraps) a connection/transport failure. */
export function isConnectionError(err: unknown): boolean {
  return err instanceof ConnectionFailedError;
}

/** True when the error means the driver package itself is unavailable. */
export function isMissingDriverError(err: unknown): boolean {
  return err instanceof MissingDriverError;
}

/** True when an error thrown by `import()` means the module is not installed
 *  (versus, say, a constructor failure inside a loaded module). */
export function isModuleNotFoundError(err: unknown): boolean {
  const code = field(err, "code");
  return code === "ERR_MODULE_NOT_FOUND" || code === "MODULE_NOT_FOUND" || code === "ERR_PACKAGE_PATH_NOT_EXPORTED" || code === "ERR_UNSUPPORTED_DIR_IMPORT";
}
