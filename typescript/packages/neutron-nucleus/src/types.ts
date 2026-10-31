// ---------------------------------------------------------------------------
// Nucleus client — shared types
// ---------------------------------------------------------------------------

/**
 * Identity of a SQL table that a resource (KV namespace, blob bucket, geo
 * layer) can be bound to. A REFERENCE for naming/scoping only — resource
 * bindings never emit DDL, never create SQL columns, and promise nothing
 * about cross-model atomicity.
 */
export interface SqlTableIdentity {
  schema: string;
  table: string;
}

/** Cancellation/deadline options accepted by every transport query method. */
export interface QuerySignalOptions {
  /**
   * Abort the operation. PgTransport dispatches server cancellation through
   * an independent pg_cancel_backend channel. HTTP aborts response consumption
   * without proving server rollback. PgTransactionTransport checks only
   * before dispatch: it does not cancel an already running transaction query.
   * Embedded transport refuses signal-bearing calls. A pre-aborted signal
   * rejects before dispatch. Cancellation failure can leave server work running;
   * draining target/cancel promises is not a bounded server-termination guarantee.
   *
   * MobileTransport: a call carrying a signal bypasses the offline queue
   * entirely — a canceled caller is never queued for later replay; the call
   * goes to the online path and rejects per the signal.
   */
  signal?: AbortSignal;
  /** Caller assertion that this operation has no effects (including scalar SELECT functions).
   * Mobile transport may retry such calls; never set this on mutations. */
  readOnly?: boolean;
  /** Opt this pure read into mobile caching; also requires readOnly and cacheEnabled. */
  cache?: boolean;
}

export type { ConnectionCapabilities } from '@neutron-build/sql/lifecycle';

/** Transport interface for communicating with a Nucleus / PostgreSQL server. */
export interface Transport {
  /** Absent on custom transports means unknown; admitConnection refuses required guarantees. */
  readonly capabilities?: Readonly<import('@neutron-build/sql/lifecycle').ConnectionCapabilities>;
  /** Endpoint plus authentication/config identity, hashed without exposing secrets.
   * Custom transports must explicitly implement this to hand off diagnostic evidence. */
  capabilityEndpoint?(): string | Promise<string>;
  /** Execute a SQL query and return typed rows. */
  query<T = Record<string, unknown>>(sql: string, params?: unknown[], opts?: QuerySignalOptions): Promise<QueryResult<T>>;

  /** Execute a SQL statement (INSERT / UPDATE / DELETE) and return the affected row count. */
  execute(sql: string, params?: unknown[], opts?: QuerySignalOptions): Promise<number>;

  /** Execute a query and return the first column of the first row, or null. */
  fetchval<T = unknown>(sql: string, params?: unknown[], opts?: QuerySignalOptions): Promise<T | null>;

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
