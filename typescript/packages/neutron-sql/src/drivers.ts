// ---------------------------------------------------------------------------
// @neutron-build/sql — drivers (zero runtime deps; dynamic imports)
// ---------------------------------------------------------------------------
// Adapter ownership contract (I01): adapters this package CREATES from a URL
// are owned and closed on terminate; injected pools/clients are BORROWED by
// default — terminating a borrowed driver never touches the owner's resource
// (V14: closing a borrowed client leaves its owner functional). terminate is
// idempotent; a second call never re-runs the underlying end().
//
// Errors crossing the adapter boundary are classified into the stable
// taxonomy (errors.ts): connection failures are never mislabeled as
// missing-driver, and "auto" driver selection falls back ONLY when the
// preferred module is genuinely not installed.
//
// Cancellation (I02): deadlines and AbortSignals reach the DATABASE, not just
// the caller's promise. pg has no per-query cancel API on a pooled client, so
// the pg leg cancels through a side channel: pg_cancel_backend(pid) executed
// on a SECOND pooled connection (the backend pid is read from the connection
// running the query). postgres.js exposes native Query.cancel() (a dedicated
// cancel connection managed by the driver) — used directly. In both cases
// the canceled statement fails with SQLSTATE 57014 and the connection
// returns to a clean state; a subsequent query on the same pool works.
//
// Transactions (I02) run on a pinned connection (pool checkout on pg;
// sql.reserve() on postgres.js) through the ONE shared runner in
// transactions.ts: modes, savepoints, rollback and commit-ambiguity
// classification behave identically on both drivers.

import { createHash } from "node:crypto";
import {
  MissingDriverError,
  NeutronSqlError,
  QueryCanceledError,
  ServerSqlError,
  classifyDriverError,
  connectionConstructionError,
  isFatalConnectionLoss,
  isModuleNotFoundError,
} from "./errors.js";
import {
  runTransaction,
  type PinnedExecutor,
  type QueryExecutionOptions,
  type TransactionModes,
  type TransactionScope,
} from "./transactions.js";

/** A connection-scoped prepared statement (Q04). `query`/`execute` bind
 *  parameters exactly like Driver.query/Driver.execute; the server-side
 *  parse/plan is reused per connection across calls. */
export interface PreparedStatement {
  /** The SQL text this statement was created from. */
  readonly sql: string;
  /** The deterministic server-side statement name when the adapter names
 *  statements itself (pg: `nsqp_` + sha256(sql) hex — stable across
 *  processes, unique per SQL text; postgres.js owns per-connection
 *  auto-naming keyed by its SQL+types signature — undefined here). */
  readonly name: string | undefined;
  /** Run the statement; resolve to rows as plain objects keyed by column
 *  label. */
  query<T = Record<string, unknown>>(params?: unknown[]): Promise<T[]>;
  /** Run the statement; resolve to the affected row count. */
  execute(params?: unknown[]): Promise<number>;
}

export interface Driver {
  /** Run a query; resolve to rows as plain objects keyed by column label.
   *  `options.deadlineMs`/`options.signal` cancel the query AT THE SERVER
   *  (see the I02 notes above). */
  query<T = Record<string, unknown>>(sqlText: string, params?: unknown[], options?: QueryExecutionOptions): Promise<T[]>;
  /** Run DML; resolve to affected row count. Same cancellation contract as
   *  query(). */
  execute(sqlText: string, params?: unknown[], options?: QueryExecutionOptions): Promise<number>;
  /** Run `fn` inside a transaction with a scoped driver. Modes (isolation,
   *  read-only, deferrable) render into the BEGIN statement; nesting inside
   *  `fn` uses real savepoints. Commit-transport failures throw
   *  CommitAmbiguityError. */
  begin<T>(fn: (tx: Driver) => Promise<T>, options?: TransactionModes): Promise<T>;
  /** Alias of `lifecycle.terminate()`: owned adapters close exactly once;
   *  borrowed adapters resolve WITHOUT closing the owner's resource. */
  close(): Promise<void>;
  /** Explicit, typed lifecycle for this adapter. */
  readonly lifecycle: DriverLifecycle;
  /** Connection-scoped prepared execution (Q04). OPTIONAL BY DESIGN — this
   *  is the adapter capability gate: present exactly where the adapter's
   *  semantics support safe prepared statements (both bundled drivers do;
   *  custom adapters may not). Use preparedStatement() for a fail-closed
   *  accessor. Statement identity is derived from the SQL text, never shared
   *  across different SQL; see README "Prepared execution". */
  prepare?(sqlText: string): PreparedStatement;
  /** Reserve a dedicated connection for the shared transaction runner
   *  (I02). Present on both bundled drivers. Custom adapters without it
   *  keep working for plain queries and default-mode transactions, but
   *  transaction options/retry/savepoints require it. */
  pin?(): Promise<PinnedExecutor>;
}

/** Fail-closed accessor for adapter prepared execution: drivers without
 *  `prepare` (custom adapters) error clearly instead of silently falling
 *  back to unnamed execution. */
export function preparedStatement(driver: Driver, sqlText: string): PreparedStatement {
  if (typeof driver.prepare !== "function") {
    throw new Error(
      "prepared statements: this adapter does not advertise support (Driver.prepare is absent) — wrap a pg pool/client or a postgres.js client via wrapPgPool/wrapPostgresJs, or extend the custom adapter",
    );
  }
  return driver.prepare(sqlText);
}

/** Deterministic, collision-free server-side statement name for the pg
 *  adapter: `nsqp_` + 58 hex chars of sha256(sql) (232 bits, within PG's
 *  63-byte NAMEDATALEN limit). Same SQL text -> same name on every machine;
 *  different SQL can never share a name (pg rejects a name reused with
 *  different text on one connection). */
export function pgStatementName(sqlText: string): string {
  return `nsqp_${createHash("sha256").update(sqlText, "utf8").digest("hex").slice(0, 58)}`;
}

export type DriverKind = "postgres" | "pg" | "auto";

export interface LoadDriverOptions {
  driver?: DriverKind;
  max?: number;
  idleTimeout?: number;
  connectTimeout?: number;
}

// ---------------------------------------------------------------------------
// Cancellation plumbing (shared by both drivers)
// ---------------------------------------------------------------------------

/** Validate execution options; return the immediate-cancel reason when the
 *  query must be rejected without a server round trip (pre-aborted signal).
 *  Throws on an invalid deadlineMs. */
function preCancelCheck(options: QueryExecutionOptions | undefined): "signal" | undefined {
  if (!options) return undefined;
  if (options.deadlineMs !== undefined && (!Number.isFinite(options.deadlineMs) || options.deadlineMs <= 0)) {
    throw new NeutronSqlError(`deadlineMs must be a positive finite number of milliseconds (got ${String(options.deadlineMs)})`);
  }
  if (options.signal?.aborted) return "signal";
  return undefined;
}

/** Arm deadline + signal; `fire` runs at most once with the reason that
 *  triggered it. Returns the disarm function (idempotent). */
function armCancellation(options: QueryExecutionOptions | undefined, fire: (reason: "deadline" | "signal") => void): () => void {
  if (!options) return () => {};
  let fired = false;
  const trigger = (reason: "deadline" | "signal"): void => {
    if (fired) return;
    fired = true;
    fire(reason);
  };
  let timer: ReturnType<typeof setTimeout> | undefined;
  const onAbort = (): void => trigger("signal");
  if (options.signal) options.signal.addEventListener("abort", onAbort, { once: true });
  if (options.deadlineMs !== undefined) {
    timer = setTimeout(() => trigger("deadline"), options.deadlineMs);
    timer.unref?.();
  }
  return () => {
    fired = true;
    if (timer !== undefined) clearTimeout(timer);
    options.signal?.removeEventListener("abort", onAbort);
  };
}

/** Wrap a 57014 ServerSqlError with the cancel reason when OUR cancel was
 *  dispatched; a 57014 we did not request (statement_timeout, an
 *  administrator cancel) passes through unwrapped. */
function wrapIfCanceled(err: unknown, dispatched: boolean, reason: "deadline" | "signal" | undefined, driverKind: string): unknown {
  if (dispatched && reason !== undefined && err instanceof ServerSqlError && err.sqlstate === "57014") {
    return new QueryCanceledError(`${driverKind}: statement canceled at the server (${reason}) — SQLSTATE 57014`, { reason, dispatched: true, cause: err });
  }
  return err;
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

export type AdapterOwnership = "owned" | "borrowed";

export interface DriverLifecycle {
  /** Who owns the underlying resource. "owned": this package (or an explicit
   *  `{ ownership: "owned" }` wrap) owns it and terminate() closes it.
   *  "borrowed": the caller owns it; terminate() never closes it. */
  readonly ownership: AdapterOwnership;
  /** True once an owned terminate have settled. */
  readonly terminated: boolean;
  /** Idempotent. Owned: closes the underlying adapter exactly once and never
   *  twice. Borrowed: resolves immediately without touching the resource. */
  terminate(): Promise<void>;
}

export function makeLifecycle(ownership: AdapterOwnership, closeOnce: () => Promise<void>): DriverLifecycle {
  let terminated = false;
  let pending: Promise<void> | null = null;
  const lifecycle: DriverLifecycle = {
    ownership,
    get terminated() {
      return terminated;
    },
    terminate(): Promise<void> {
      if (ownership === "borrowed") return Promise.resolve();
      if (pending) return pending;
      pending = Promise.resolve()
        .then(closeOnce)
        .then(() => {
          terminated = true;
        })
        .catch((err: unknown) => {
          pending = null;
          throw err;
        });
      return pending;
    },
  };
  return lifecycle;
}

/** Options for wrapping an externally created pool/client. */
export interface WrapAdapterOptions {
  /** Ownership of the injected resource. Default "borrowed": the wrapper
   *  never closes it. "owned" transfers disposal to the wrapper (terminate
   *  closes it; the injector must not). */
  ownership?: AdapterOwnership;
}

// ---------------------------------------------------------------------------
// Loader
// ---------------------------------------------------------------------------

/** Runtime-support guard (I03): the bundled adapters drive `pg` and
 *  `postgres` through Node transports and no edge/browser adapter exists
 *  (adding one requires a real transport fixture, per the runtime-support
 *  contract). Detection is POSITIVE — is a Node process present? — never
 *  `typeof window` inference, so a runtime that genuinely provides Node
 *  compatibility is not blocked. A runtime without it fails here with one
 *  precise error instead of crashing inside a driver import. */
export function assertNodeRuntime(operation: string): void {
  const versions =
    typeof process === "undefined"
      ? undefined
      : (process as { versions?: { node?: string } }).versions;
  if (!versions?.node) {
    throw new NeutronSqlError(
      `${operation} requires a Node.js runtime (no process.versions.node was found). ` +
        `@neutron-build/sql's pg/postgres.js adapters use Node sockets and have no edge/browser ` +
        `transport adapter — run in Node.js (see "Runtime support" in the README).`,
    );
  }
}

export async function loadDriver(url: string, options: LoadDriverOptions = {}): Promise<Driver> {
  assertNodeRuntime("loadDriver");
  const kind = options.driver ?? "auto";
  if (kind === "pg") return loadNodePostgres(url, options);
  if (kind === "postgres") return loadPostgresJs(url, options);
  // "auto": prefer postgres.js, fall back to pg ONLY when the postgres.js
  // module is not installed. Connection/URL failures from a loaded driver
  // propagate — they must never masquerade as a missing-driver fallback.
  try {
    return await loadPostgresJs(url, options);
  } catch (err) {
    if (err instanceof MissingDriverError) return loadNodePostgres(url, options);
    throw err;
  }
}

// --- postgres.js -----------------------------------------------------------

interface PostgresJsResult extends Array<Record<string, unknown>> {
  count: number;
}

/** postgres.js's Query objects are thenable AND cancelable (src/query.js:
 * `class Query extends Promise` with `cancel()`). unsafe() returns them
 * directly. */
export interface CancelablePromise<T> extends Promise<T> {
  /** Request cancellation: postgres.js opens a dedicated connection, sends
   *  the protocol cancel request, and the query rejects with 57014. Returns
   *  null when the cancel slot was already consumed (postgres.js nulls its
   *  canceller after the first call). */
  cancel(): Promise<unknown> | null;
}

/** The query surface shared by a postgres.js client and a reserved
 *  connection. */
export interface PostgresJsExecutor {
  /** postgres.js `unsafe`. Options: `{ prepare: true }` switches the text
   *  from the unsafe default (prepare off) to postgres.js's per-connection
   *  automatic named-statement cache (signature-keyed: SQL text + parameter
   *  types; names are generated per connection). */
  unsafe(sqlText: string, params?: unknown[], options?: { prepare?: boolean; simple?: boolean }): CancelablePromise<PostgresJsResult>;
}

export interface PostgresJsClient extends PostgresJsExecutor {
  begin<T>(fn: (tx: PostgresJsClient) => Promise<T>): Promise<T>;
  end(opts?: { timeout?: number }): Promise<void>;
  /** Reserve a dedicated connection (postgres.js `sql.reserve()`): the
   *  returned executor is pinned to one backend until release(). Used to
   *  drive transactions with explicit BEGIN/COMMIT control. */
  reserve(): Promise<PostgresJsReserved>;
}

export interface PostgresJsReserved extends PostgresJsExecutor {
  /** Return the connection to the client's pool. */
  release(): void;
}

async function loadPostgresJs(url: string, options: LoadDriverOptions): Promise<Driver> {
  const specifier = "postgres";
  let mod: { default: (url: string, opts?: Record<string, unknown>) => PostgresJsClient };
  try {
    mod = (await import(specifier)) as unknown as typeof mod;
  } catch (err) {
    if (isModuleNotFoundError(err)) {
      throw new MissingDriverError("postgres", `driver package "postgres" (postgres.js) is not installed — install it or pass driverOptions: { driver: "pg" }`, { cause: err });
    }
    throw err;
  }
  if (typeof mod.default !== "function") {
    throw new MissingDriverError("postgres", `package "postgres" loaded but exports no default client function (broken install?)`);
  }
  let client: PostgresJsClient;
  try {
    client = mod.default(url, {
      max: options.max ?? 10,
      idle_timeout: options.idleTimeout ?? 20,
      connect_timeout: options.connectTimeout ?? 10,
    });
  } catch (err) {
    // Constructor failures (unparseable URL, bad options) are connection
    // failures — never a reason to fall back to another driver.
    throw connectionConstructionError("postgres", err);
  }
  return wrapPostgresJs(client, { ownership: "owned" });
}

/** Execute one statement on a postgres.js executor with server-side
 *  cancellation armed. */
async function execPostgresJs(
  owner: PostgresJsExecutor,
  driverKind: string,
  kind: "query" | "execute",
  sqlText: string,
  params: unknown[] | undefined,
  options: QueryExecutionOptions | undefined,
): Promise<unknown> {
  const preReason = preCancelCheck(options);
  if (preReason !== undefined) {
    throw new QueryCanceledError(`${driverKind}: query canceled (${preReason}) before submission — no server round trip was made`, { reason: preReason, dispatched: false });
  }
  let cancelReason: "deadline" | "signal" | undefined;
  let dispatched = false;
  const query = owner.unsafe(sqlText, (params ?? []) as unknown[]);
  const disarm = armCancellation(options, (reason) => {
    cancelReason = reason;
    dispatched = true;
    query.cancel()?.catch(() => {
      // cancel dispatch failure: the query itself will settle with its own
      // error (or complete normally) — nothing further to do
    });
  });
  try {
    const res = await query;
    return kind === "query" ? res : (res.count ?? 0);
  } catch (err) {
    throw wrapIfCanceled(classifyDriverError(err, driverKind), dispatched, cancelReason, driverKind);
  } finally {
    disarm();
  }
}

/** Wrap an injected postgres.js client. Borrowed by default: `close()` will
 *  not end the client you still own. */
export function wrapPostgresJs(client: PostgresJsClient, options: WrapAdapterOptions = {}): Driver {
  const ownership = options.ownership ?? "borrowed";
  const lifecycle = makeLifecycle(ownership, () => client.end({ timeout: 5 }));

  if (typeof client.reserve !== "function") {
    throw new NeutronSqlError("wrapPostgresJs: the injected client has no reserve() — transactions on injected adapters require a real postgres.js client (3.x)");
  }

  // Prepared execution rides postgres.js's own per-connection statement
  // cache: `{ prepare: true }` (plus simple:false so even zero-parameter
  // text takes the extended protocol) keys statements by SQL text + inferred
  // parameter types PER CONNECTION with auto-generated names. The wrapper
  // supplies no name of its own — there is no safe cross-connection name to
  // give (see README "Prepared execution").
  const postgresJsPrepared = (owner: PostgresJsExecutor, sqlText: string): PreparedStatement => ({
    sql: sqlText,
    name: undefined,
    async query<T>(params: unknown[] = []): Promise<T[]> {
      try {
        const res = await owner.unsafe(sqlText, params as unknown[], { prepare: true, simple: false });
        return res as unknown as T[];
      } catch (err) {
        throw classifyDriverError(err, "postgres");
      }
    },
    async execute(params: unknown[] = []): Promise<number> {
      try {
        const res = await owner.unsafe(sqlText, params as unknown[], { prepare: true, simple: false });
        return res.count ?? 0;
      } catch (err) {
        throw classifyDriverError(err, "postgres");
      }
    },
  });

  const postgresJsPin = async (): Promise<PinnedExecutor> => {
    const reserved = await client.reserve();
    // postgres.js 3.4.8 bug dodge: reserved.release() unconditionally calls
    // onopen(c), which moves a connection back into the open pool — if the
    // socket died while reserved, that reopens a DEAD connection and the
    // next pooled query crashes writing to its nulled socket (reproduced on
    // raw postgres.js; see I02 evidence). When we observed a fatal loss on
    // this reservation we skip release(): postgres.js's own onclose already
    // cleared the reservation bookkeeping and moved the connection to its
    // closed queue, where it is properly reconnected on demand.
    let dead = false;
    const markFatal = (err: unknown): void => {
      if (isFatalConnectionLoss(err)) dead = true;
    };
    const run = async (kind: "query" | "execute", sqlText: string, params?: unknown[], execOptions?: QueryExecutionOptions): Promise<unknown> => {
      try {
        return await execPostgresJs(reserved, "postgres", kind, sqlText, params, execOptions);
      } catch (err) {
        markFatal(err);
        throw err;
      }
    };
    return {
      async query<T = Record<string, unknown>>(sqlText: string, params?: unknown[], execOptions?: QueryExecutionOptions): Promise<T[]> {
        return (await run("query", sqlText, params, execOptions)) as T[];
      },
      async execute(sqlText: string, params?: unknown[], execOptions?: QueryExecutionOptions): Promise<number> {
        return (await run("execute", sqlText, params, execOptions)) as number;
      },
      release(err?: unknown): void {
        if (err !== undefined) dead = true;
        if (dead) return;
        reserved.release();
      },
      prepare: (sqlText: string): PreparedStatement => postgresJsPrepared(reserved, sqlText),
    };
  };

  const driver: Driver = {
    async query<T>(sqlText: string, params: unknown[] = [], execOptions?: QueryExecutionOptions): Promise<T[]> {
      return execPostgresJs(client, "postgres", "query", sqlText, params, execOptions) as Promise<T[]>;
    },
    async execute(sqlText: string, params: unknown[] = [], execOptions?: QueryExecutionOptions): Promise<number> {
      return (await execPostgresJs(client, "postgres", "execute", sqlText, params, execOptions)) as number;
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>, modes?: TransactionModes): Promise<T> {
      const pin = await postgresJsPin();
      return runTransaction(pin, (scope: TransactionScope) => fn(scope), modes);
    },
    close: () => driver.lifecycle.terminate(),
    lifecycle,
    prepare: (sqlText: string): PreparedStatement => postgresJsPrepared(client, sqlText),
    pin: postgresJsPin,
  };
  return driver;
}

// --- node-postgres ---------------------------------------------------------

/** pg's query-config form. `name` requests a NAMED prepared statement —
 *  session-scoped on the server; pg tracks parsed names per connection and
 *  re-Parses the same name on connections that have not seen it yet. */
export interface PgQueryConfig {
  name?: string;
  text: string;
  values?: unknown[];
}

export interface PgPoolClientLike {
  query(sqlText: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
  query(config: PgQueryConfig): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
  /** Return the client to its pool. Pass an error to REMOVE the client
   *  (pg-pool semantics: a truthy error never returns to the idle set). */
  release(err?: unknown): void;
  /** pg clients are EventEmitters; the pin attaches an error listener for
   *  its lifetime so a dying socket cannot surface as an unhandled
   *  'error' event while the pool's own listener is detached (checked-out
   *  clients have none). Optional for duck-typed custom clients. */
  once?(eventName: "error", listener: (err: Error) => void): unknown;
  removeListener?(eventName: "error", listener: (err: Error) => void): unknown;
}

export interface PgPoolLike {
  query(sqlText: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
  query(config: PgQueryConfig): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
  connect(): Promise<PgPoolClientLike>;
  end(): Promise<void>;
}

async function loadNodePostgres(url: string, options: LoadDriverOptions): Promise<Driver> {
  const specifier = "pg";
  let mod: { Pool: new (opts: Record<string, unknown>) => PgPoolLike };
  try {
    mod = (await import(specifier)) as unknown as typeof mod;
  } catch (err) {
    if (isModuleNotFoundError(err)) {
      throw new MissingDriverError("pg", `driver package "pg" (node-postgres) is not installed — install it or pass driverOptions: { driver: "postgres" }`, { cause: err });
    }
    throw err;
  }
  if (typeof mod.Pool !== "function") {
    throw new MissingDriverError("pg", `package "pg" loaded but exports no Pool constructor (broken install?)`);
  }
  let pool: PgPoolLike;
  try {
    pool = new mod.Pool({
      connectionString: url,
      max: options.max ?? 10,
      idleTimeoutMillis: (options.idleTimeout ?? 20) * 1000,
      connectionTimeoutMillis: (options.connectTimeout ?? 10) * 1000,
    });
  } catch (err) {
    throw connectionConstructionError("pg", err);
  }
  return wrapPgPool(pool, { ownership: "owned" });
}

// pg named prepared statements are SESSION-scoped. Sending every execution
// as `{ name, text }` is pool-safe: pg re-Parses the name on each pooled
// connection that has not parsed it (its per-connection parsedStatements
// map then caches the parse), and its submit() guard rejects a name reused
// with DIFFERENT text on one connection. The name is sha256-derived from
// the SQL text (pgStatementName), so that guard can never fire through
// this wrapper and names are never shared across different SQL.
// Verified live on PG 17 (live.prepared suite): statements survive
// transaction ROLLBACK and ABORT — the session is their only lifetime.
// (postgres.js additionally retries 26000 "statement does not exist" for
// environments where statements DO vanish, e.g. transaction poolers.)
const pgPrepared = (
  owner: { query(config: PgQueryConfig): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }> },
  sqlText: string,
): PreparedStatement => {
  const name = pgStatementName(sqlText);
  return {
    sql: sqlText,
    name,
    async query<T>(params: unknown[] = []): Promise<T[]> {
      const res = await owner.query({ name, text: sqlText, values: params });
      return res.rows as T[];
    },
    async execute(params: unknown[] = []): Promise<number> {
      const res = await owner.query({ name, text: sqlText, values: params });
      return res.rowCount ?? 0;
    },
  };
};

/** Execute one statement on a checked-out pg client with server-side
 *  cancellation armed (pg_cancel_backend on a second pooled connection). */
async function execPg(
  pool: PgPoolLike,
  client: PgPoolClientLike,
  backendPid: () => Promise<number>,
  kind: "query" | "execute",
  sqlText: string,
  params: unknown[] | undefined,
  options: QueryExecutionOptions | undefined,
): Promise<unknown> {
  const preReason = preCancelCheck(options);
  if (preReason !== undefined) {
    throw new QueryCanceledError(`pg: query canceled (${preReason}) before submission — no server round trip was made`, { reason: preReason, dispatched: false });
  }
  if (options !== undefined) {
    // Eagerly read the backend pid BEFORE submitting the statement: the
    // cancel side channel needs it while this client is busy running the
    // query (a lazy read would queue behind the very statement we want to
    // cancel). One extra round trip, only on cancellation-armed queries.
    await backendPid();
  }
  let cancelReason: "deadline" | "signal" | undefined;
  let dispatched = false;
  const disarm = armCancellation(options, (reason) => {
    cancelReason = reason;
    dispatched = true;
    void (async () => {
      try {
        const pid = await backendPid();
        // Side channel: a SECOND pooled connection asks the server to cancel
        // the backend running our query. Requires spare pool capacity (a
        // max:1 pool cannot service the side channel while our client is
        // checked out — the query then simply runs to completion).
        await pool.query("select pg_cancel_backend($1) as canceled", [pid]);
      } catch {
        // side-channel failure: the query settles with its own error (or
        // completes normally if the connection is fine)
      }
    })();
  });
  try {
    const res = await client.query(params !== undefined && params.length > 0 ? { text: sqlText, values: params } : { text: sqlText });
    return kind === "query" ? res.rows : (res.rowCount ?? 0);
  } catch (err) {
    throw wrapIfCanceled(classifyDriverError(err, "pg"), dispatched, cancelReason, "pg");
  } finally {
    disarm();
  }
}

/** Pin one pg pool client for the shared transaction runner (or a
 *  cancellation-armed single query). Attaches an error listener for the
 *  pin lifetime: pg-pool detaches its own while the client is checked out,
 *  and a dying socket otherwise surfaces as an unhandled 'error' event. */
async function pgPin(pool: PgPoolLike): Promise<PinnedExecutor> {
  let client: PgPoolClientLike;
  try {
    client = await pool.connect();
  } catch (err) {
    throw classifyDriverError(err, "pg");
  }
  let broken = false;
  const onClientError = (): void => {
    broken = true;
  };
  client.once?.("error", onClientError);
  let pidPromise: Promise<number> | null = null;
  const backendPid = (): Promise<number> => {
    if (!pidPromise) {
      pidPromise = client.query("select pg_backend_pid() as pid", []).then(
        (res) => {
          const pid = Number((res.rows[0] as { pid?: unknown } | undefined)?.pid);
          if (!Number.isInteger(pid) || pid <= 0) throw new NeutronSqlError(`pg: pg_backend_pid() returned ${String(pid)}`);
          return pid;
        },
        (err: unknown) => {
          pidPromise = null;
          throw err;
        },
      );
    }
    return pidPromise;
  };
  const run = async (kind: "query" | "execute", sqlText: string, params?: unknown[], execOptions?: QueryExecutionOptions): Promise<unknown> => {
    try {
      return await execPg(pool, client, backendPid, kind, sqlText, params, execOptions);
    } catch (err) {
      // a fatal connection loss (transport failure or FATAL 57P0x) means
      // this connection must never return to the idle set; SQL errors —
      // including 57014 cancellations — leave it perfectly usable
      if (isFatalConnectionLoss(err)) broken = true;
      throw err;
    }
  };
  return {
    async query<T = Record<string, unknown>>(sqlText: string, params?: unknown[], execOptions?: QueryExecutionOptions): Promise<T[]> {
      return (await run("query", sqlText, params, execOptions)) as T[];
    },
    async execute(sqlText: string, params?: unknown[], execOptions?: QueryExecutionOptions): Promise<number> {
      return (await run("execute", sqlText, params, execOptions)) as number;
    },
    release(err?: unknown): void {
      client.removeListener?.("error", onClientError);
      if (err !== undefined || broken) {
        // a truthy error removes the client from the pool (pg-pool release
        // semantics) — suspect connections are never returned to the idle set
        client.release(err ?? new NeutronSqlError("pg: connection suspect after a failure; removed from the pool"));
      } else {
        client.release();
      }
    },
    prepare: (sqlText: string): PreparedStatement => pgPrepared(client, sqlText),
  };
}

/** Wrap an injected node-postgres pool. Borrowed by default: `close()` will
 *  not end the pool you still own. */
export function wrapPgPool(pool: PgPoolLike, options: WrapAdapterOptions = {}): Driver {
  const ownership = options.ownership ?? "borrowed";
  const lifecycle = makeLifecycle(ownership, async () => {
    try {
      await pool.end();
    } catch (err) {
      throw classifyDriverError(err, "pg");
    }
  });

  const driver: Driver = {
    async query<T>(sqlText: string, params: unknown[] = [], execOptions?: QueryExecutionOptions): Promise<T[]> {
      if (execOptions === undefined) {
        try {
          const res = await pool.query(sqlText, params);
          return res.rows as T[];
        } catch (err) {
          throw classifyDriverError(err, "pg");
        }
      }
      const pin = await pgPin(pool);
      try {
        return await pin.query<T>(sqlText, params, execOptions);
      } finally {
        pin.release();
      }
    },
    async execute(sqlText: string, params: unknown[] = [], execOptions?: QueryExecutionOptions): Promise<number> {
      if (execOptions === undefined) {
        try {
          const res = await pool.query(sqlText, params);
          return res.rowCount ?? 0;
        } catch (err) {
          throw classifyDriverError(err, "pg");
        }
      }
      const pin = await pgPin(pool);
      try {
        return await pin.execute(sqlText, params, execOptions);
      } finally {
        pin.release();
      }
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>, modes?: TransactionModes): Promise<T> {
      const pin = await pgPin(pool);
      return runTransaction(pin, (scope: TransactionScope) => fn(scope), modes);
    },
    close: () => driver.lifecycle.terminate(),
    lifecycle,
    prepare: (sqlText: string): PreparedStatement => pgPrepared(pool, sqlText),
    pin: () => pgPin(pool),
  };
  return driver;
}
