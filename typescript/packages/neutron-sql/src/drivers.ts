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

import { createHash } from "node:crypto";
import {
  MissingDriverError,
  classifyDriverError,
  connectionConstructionError,
  isModuleNotFoundError,
} from "./errors.js";

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
  /** Run a query; resolve to rows as plain objects keyed by column label. */
  query<T = Record<string, unknown>>(sqlText: string, params?: unknown[]): Promise<T[]>;
  /** Run DML; resolve to affected row count. */
  execute(sqlText: string, params?: unknown[]): Promise<number>;
  /** Run `fn` inside a transaction with a scoped driver. */
  begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T>;
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
// Lifecycle
// ---------------------------------------------------------------------------

export type AdapterOwnership = "owned" | "borrowed";

export interface DriverLifecycle {
  /** Who owns the underlying resource. "owned": this package (or an explicit
   *  `{ ownership: "owned" }` wrap) owns it and terminate() closes it.
   *  "borrowed": the caller owns it; terminate() never closes it. */
  readonly ownership: AdapterOwnership;
  /** True once an owned terminate has settled. */
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

export async function loadDriver(url: string, options: LoadDriverOptions = {}): Promise<Driver> {
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

export interface PostgresJsClient {
  /** postgres.js `unsafe`. Options: `{ prepare: true }` switches the text
   *  from the unsafe default (prepare off) to postgres.js's per-connection
   *  automatic named-statement cache (signature-keyed: SQL text + parameter
   *  types; names are generated per connection). */
  unsafe(sqlText: string, params?: unknown[], options?: { prepare?: boolean; simple?: boolean }): Promise<PostgresJsResult>;
  begin<T>(fn: (tx: PostgresJsClient) => Promise<T>): Promise<T>;
  end(opts?: { timeout?: number }): Promise<void>;
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

/** Wrap an injected postgres.js client. Borrowed by default: `close()` will
 *  not end the client you still own. */
export function wrapPostgresJs(client: PostgresJsClient, options: WrapAdapterOptions = {}): Driver {
  const ownership = options.ownership ?? "borrowed";
  const lifecycle = makeLifecycle(ownership, () => client.end({ timeout: 5 }));

  // Prepared execution rides postgres.js's own per-connection statement
  // cache: `{ prepare: true }` (plus simple:false so even zero-parameter
  // text takes the extended protocol) keys statements by SQL text + inferred
  // parameter types PER CONNECTION with auto-generated names. The wrapper
  // supplies no name of its own — there is no safe cross-connection name to
  // give (see README "Prepared execution").
  const postgresJsPrepared = (owner: PostgresJsClient, sqlText: string): PreparedStatement => ({
    sql: sqlText,
    name: undefined,
    async query<T>(params: unknown[] = []): Promise<T[]> {
      try {
        return (await owner.unsafe(sqlText, params as unknown[], { prepare: true, simple: false })) as unknown as T[];
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

  const scoped = (tx: PostgresJsClient): Driver => {
    const scope: Driver = {
      async query<T>(sqlText: string, params: unknown[] = []): Promise<T[]> {
        try {
          return (await tx.unsafe(sqlText, params as unknown[])) as unknown as T[];
        } catch (err) {
          throw classifyDriverError(err, "postgres");
        }
      },
      async execute(sqlText: string, params: unknown[] = []): Promise<number> {
        try {
          const res = await tx.unsafe(sqlText, params as unknown[]);
          return res.count ?? 0;
        } catch (err) {
          throw classifyDriverError(err, "postgres");
        }
      },
      begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
        return tx.begin(async (inner) => fn(scoped(inner)));
      },
      close: () => scope.lifecycle.terminate(),
      lifecycle: makeLifecycle("borrowed", () => Promise.resolve()),
      prepare: (sqlText: string): PreparedStatement => postgresJsPrepared(tx, sqlText),
    };
    return scope;
  };

  const driver: Driver = {
    async query<T>(sqlText: string, params: unknown[] = []): Promise<T[]> {
      try {
        return (await client.unsafe(sqlText, params as unknown[])) as unknown as T[];
      } catch (err) {
        throw classifyDriverError(err, "postgres");
      }
    },
    async execute(sqlText: string, params: unknown[] = []): Promise<number> {
      try {
        const res = await client.unsafe(sqlText, params as unknown[]);
        return res.count ?? 0;
      } catch (err) {
        throw classifyDriverError(err, "postgres");
      }
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      try {
        return await client.begin(async (tx) => fn(scoped(tx)));
      } catch (err) {
        // Errors thrown by the user callback are not driver errors; pass
        // them through untouched. postgres.js surfaces callback rejections
        // as-is (the transaction SQL itself is driven by client.begin).
        throw err instanceof Error && isDriverBoundaryError(err) ? classifyDriverError(err, "postgres") : err;
      }
    },
    close: () => driver.lifecycle.terminate(),
    lifecycle,
    prepare: (sqlText: string): PreparedStatement => postgresJsPrepared(client, sqlText),
  };
  return driver;
}

function isDriverBoundaryError(err: Error): boolean {
  // postgres.js wraps user-callback rejections verbatim; distinguish driver
  // SQL failures by their known shapes (PostgresError fields or connection
  // codes). Anything else is assumed to originate in user code.
  const record = err as unknown as Record<string, unknown>;
  return (
    (typeof record.code === "string" && /^[0-9A-Z]{5}$/.test(record.code)) ||
    typeof record.errno !== "undefined" ||
    typeof record.syscall === "string"
  );
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
  release(): void;
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
  const pgPrepared = (owner: { query(config: PgQueryConfig): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }> }, sqlText: string): PreparedStatement => {
    const name = pgStatementName(sqlText);
    return {
      sql: sqlText,
      name,
      async query<T>(params: unknown[] = []): Promise<T[]> {
        try {
          const res = await owner.query({ name, text: sqlText, values: params });
          return res.rows as T[];
        } catch (err) {
          throw classifyDriverError(err, "pg");
        }
      },
      async execute(params: unknown[] = []): Promise<number> {
        try {
          const res = await owner.query({ name, text: sqlText, values: params });
          return res.rowCount ?? 0;
        } catch (err) {
          throw classifyDriverError(err, "pg");
        }
      },
    };
  };

  const fromClient = (client: PgPoolClientLike): Driver => {
    const scope: Driver = {
      async query<T>(sqlText: string, params: unknown[] = []): Promise<T[]> {
        try {
          const res = await client.query(sqlText, params);
          return res.rows as T[];
        } catch (err) {
          throw classifyDriverError(err, "pg");
        }
      },
      async execute(sqlText: string, params: unknown[] = []): Promise<number> {
        try {
          const res = await client.query(sqlText, params);
          return res.rowCount ?? 0;
        } catch (err) {
          throw classifyDriverError(err, "pg");
        }
      },
      begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
        throw new Error("nested transactions are not supported by the pg driver");
      },
      close: () => scope.lifecycle.terminate(),
      lifecycle: makeLifecycle("borrowed", () => Promise.resolve()),
      prepare: (sqlText: string): PreparedStatement => pgPrepared(client, sqlText),
    };
    return scope;
  };

  const driver: Driver = {
    async query<T>(sqlText: string, params: unknown[] = []): Promise<T[]> {
      try {
        const res = await pool.query(sqlText, params);
        return res.rows as T[];
      } catch (err) {
        throw classifyDriverError(err, "pg");
      }
    },
    async execute(sqlText: string, params: unknown[] = []): Promise<number> {
      try {
        const res = await pool.query(sqlText, params);
        return res.rowCount ?? 0;
      } catch (err) {
        throw classifyDriverError(err, "pg");
      }
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      let client: PgPoolClientLike;
      try {
        client = await pool.connect();
      } catch (err) {
        throw classifyDriverError(err, "pg");
      }
      try {
        await fromClient(client).query("begin");
        const result = await fn(fromClient(client));
        await fromClient(client).query("commit");
        return result;
      } catch (err) {
        try {
          await client.query("rollback");
        } catch {
          // connection-level failure: the server dying takes the tx with it
        }
        throw err;
      } finally {
        client.release();
      }
    },
    close: () => driver.lifecycle.terminate(),
    lifecycle,
    prepare: (sqlText: string): PreparedStatement => pgPrepared(pool, sqlText),
  };
  return driver;
}
