// ---------------------------------------------------------------------------
// @neutron-build/sql — drivers (zero runtime deps; dynamic imports)
// ---------------------------------------------------------------------------

export interface Driver {
  /** Run a query; resolve to rows as plain objects keyed by column label. */
  query<T = Record<string, unknown>>(sqlText: string, params?: unknown[]): Promise<T[]>;
  /** Run DML; resolve to affected row count. */
  execute(sqlText: string, params?: unknown[]): Promise<number>;
  /** Run `fn` inside a transaction with a scoped driver. */
  begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

export type DriverKind = "postgres" | "pg" | "auto";

export interface LoadDriverOptions {
  driver?: DriverKind;
  max?: number;
  idleTimeout?: number;
  connectTimeout?: number;
}

export async function loadDriver(url: string, options: LoadDriverOptions = {}): Promise<Driver> {
  const kind = options.driver ?? "auto";
  if (kind === "pg") return loadNodePostgres(url, options);
  if (kind === "postgres") return loadPostgresJs(url, options);
  try {
    return await loadPostgresJs(url, options);
  } catch {
    return loadNodePostgres(url, options);
  }
}

// --- postgres.js -----------------------------------------------------------

interface PostgresJsResult extends Array<Record<string, unknown>> {
  count: number;
}

interface PostgresJsClient {
  unsafe(sqlText: string, params?: unknown[]): Promise<PostgresJsResult>;
  begin<T>(fn: (tx: PostgresJsClient) => Promise<T>): Promise<T>;
  end(opts?: { timeout?: number }): Promise<void>;
}

async function loadPostgresJs(url: string, options: LoadDriverOptions): Promise<Driver> {
  const specifier = "postgres";
  const mod = (await import(specifier)) as unknown as { default: (url: string, opts?: Record<string, unknown>) => PostgresJsClient };
  if (typeof mod.default !== "function") {
    throw new Error("postgres.js loaded but no default export found");
  }
  const client = mod.default(url, {
    max: options.max ?? 10,
    idle_timeout: options.idleTimeout ?? 20,
    connect_timeout: options.connectTimeout ?? 10,
  });
  return {
    async query<T>(sqlText: string, params: unknown[] = []): Promise<T[]> {
      const rows = await client.unsafe(sqlText, params as unknown[]);
      return rows as unknown as T[];
    },
    async execute(sqlText: string, params: unknown[] = []): Promise<number> {
      const res = await client.unsafe(sqlText, params as unknown[]);
      return res.count ?? 0;
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      return client.begin(async (tx) => fn(wrapPostgresJs(tx)));
    },
    close() {
      return client.end({ timeout: 5 });
    },
  };
}

function wrapPostgresJs(client: PostgresJsClient): Driver {
  return {
    async query<T>(sqlText: string, params: unknown[] = []): Promise<T[]> {
      return (await client.unsafe(sqlText, params as unknown[])) as unknown as T[];
    },
    async execute(sqlText: string, params: unknown[] = []): Promise<number> {
      const res = await client.unsafe(sqlText, params as unknown[]);
      return res.count ?? 0;
    },
    begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      return client.begin(async (tx) => fn(wrapPostgresJs(tx)));
    },
    close: () => Promise.resolve(),
  };
}

// --- node-postgres ---------------------------------------------------------

interface PgPoolClientLike {
  query(sqlText: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
  release(): void;
}

interface PgPoolLike {
  query(sqlText: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
  connect(): Promise<PgPoolClientLike>;
  end(): Promise<void>;
}

async function loadNodePostgres(url: string, options: LoadDriverOptions): Promise<Driver> {
  const specifier = "pg";
  const mod = (await import(specifier)) as unknown as { Pool: new (opts: Record<string, unknown>) => PgPoolLike };
  if (typeof mod.Pool !== "function") {
    throw new Error("pg loaded but Pool export not found");
  }
  const pool = new mod.Pool({
    connectionString: url,
    max: options.max ?? 10,
    idleTimeoutMillis: (options.idleTimeout ?? 20) * 1000,
    connectionTimeoutMillis: (options.connectTimeout ?? 10) * 1000,
  });

  const fromClient = (client: PgPoolClientLike): Driver => ({
    async query<T>(sqlText: string, params: unknown[] = []): Promise<T[]> {
      const res = await client.query(sqlText, params);
      return res.rows as T[];
    },
    async execute(sqlText: string, params: unknown[] = []): Promise<number> {
      const res = await client.query(sqlText, params);
      return res.rowCount ?? 0;
    },
    begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      throw new Error("nested transactions are not supported by the pg driver");
    },
    close: () => Promise.resolve(),
  });

  return {
    async query<T>(sqlText: string, params: unknown[] = []): Promise<T[]> {
      const res = await pool.query(sqlText, params);
      return res.rows as T[];
    },
    async execute(sqlText: string, params: unknown[] = []): Promise<number> {
      const res = await pool.query(sqlText, params);
      return res.rowCount ?? 0;
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      const client = await pool.connect();
      try {
        await client.query("begin");
        const result = await fn(fromClient(client));
        await client.query("commit");
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
    close() {
      return pool.end();
    },
  };
}
