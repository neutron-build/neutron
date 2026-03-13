// ---------------------------------------------------------------------------
// Nucleus client — shared types
// ---------------------------------------------------------------------------

/** Transport interface for communicating with a Nucleus / PostgreSQL server. */
export interface Transport {
  /** Execute a SQL query and return typed rows. */
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;

  /** Execute a SQL statement (INSERT / UPDATE / DELETE) and return the affected row count. */
  execute(sql: string, params?: unknown[]): Promise<number>;

  /** Execute a query and return the first column of the first row, or null. */
  fetchval<T = unknown>(sql: string, params?: unknown[]): Promise<T | null>;

  /** Begin a transaction, returning a transport scoped to that transaction. */
  beginTransaction(isolationLevel?: IsolationLevel): Promise<TransactionTransport>;

  /** Close the transport and release resources. */
  close(): Promise<void>;

  /** Verify the server is reachable. */
  ping(): Promise<void>;
}

/** A transport that lives inside a transaction and can commit / rollback. */
export interface TransactionTransport extends Transport {
  commit(): Promise<void>;
  rollback(): Promise<void>;
}

/** Rows returned from a query. */
export interface QueryResult<T> {
  rows: T[];
  rowCount: number;
}

/** SQL transaction isolation levels. */
export type IsolationLevel = 'read_committed' | 'repeatable_read' | 'serializable';

/** Feature flags detected from the connected database on connect. */
export interface NucleusFeatures {
  isNucleus: boolean;
  hasKV: boolean;
  hasVector: boolean;
  hasTimeSeries: boolean;
  hasDocument: boolean;
  hasGraph: boolean;
  hasFTS: boolean;
  hasGeo: boolean;
  hasBlob: boolean;
  hasStreams: boolean;
  hasColumnar: boolean;
  hasDatalog: boolean;
  hasCDC: boolean;
  hasPubSub: boolean;
  version: string;
}

/**
 * Plugin interface for the `.use()` composition pattern.
 *
 * `T` is the shape that gets merged into the client when the plugin is used.
 * For example `NucleusPlugin<{ sql: SQLModel }>` adds a `sql` property.
 */
export interface NucleusPlugin<T> {
  /** Human-readable plugin name (used for diagnostics). */
  name: string;
  /** Called once when `.connect()` resolves. Must return the object to merge. */
  init(transport: Transport, features: NucleusFeatures): T;
}
