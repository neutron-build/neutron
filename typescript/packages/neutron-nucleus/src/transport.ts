// ---------------------------------------------------------------------------
// Nucleus client — transport implementations
// ---------------------------------------------------------------------------

import { closeResources, resourceLifecycle } from '@neutron-build/sql/lifecycle';

import type { Transport, TransactionTransport, QueryResult, IsolationLevel, QuerySignalOptions } from './types.js';
import {
  NucleusAuthError,
  NucleusError,
  NucleusConflictError,
  NucleusConnectionError,
  NucleusUnknownOutcomeError,
  NucleusNotFoundError,
  NucleusQueryError,
  NucleusTransactionError,
  NucleusNotSupportedError,
} from './errors.js';

// ---------------------------------------------------------------------------
// Transport configuration types
// ---------------------------------------------------------------------------

/** Configuration for the base HTTP transport. */
export interface TransportConfig {
  /** Base URL of the Nucleus server. */
  url: string;
  /** Extra HTTP headers sent with every request. */
  headers?: Record<string, string>;
  /** Request timeout in milliseconds (default 30000). */
  timeout?: number;
}

/** Extended configuration for mobile transport with retry, cache, and offline queue. */
export interface MobileTransportConfig extends TransportConfig {
  /** Maximum number of retry attempts for failed requests (default 3). */
  maxRetries?: number;
  /** Base delay in ms between retries — uses exponential backoff (default 1000). */
  retryDelay?: number;
  /** Enable caching for calls explicitly marked readOnly and cache (default false). */
  cacheEnabled?: boolean;
  /** Time-to-live for cached entries in ms (default 60000). */
  cacheTTL?: number;
  /** Whether to queue undispatched writes when offline (default true). Dispatched writes are never replayed. */
  offlineQueueEnabled?: boolean;
  /** Maximum number of queued offline operations (default 100). */
  maxQueueSize?: number;
}

/**
 * Configuration for the PostgreSQL wire transport. `timeout` is honored via
 * the pool's native statement/query/connection timeouts; `headers` is
 * rejected at construction — see `PgTransport`.
 */
export interface PgTransportConfig {
  /** Explicit PostgreSQL text-protocol scalar read shape for SQL-only clients. */
  valueProfile?: 'lossless-read-v1';
  /** Not supported: the PostgreSQL wire protocol has no HTTP headers. */
  headers?: Record<string, string>;
  /** Request timeout in milliseconds, applied to the pool's native timeouts. */
  timeout?: number;
}

// ---------------------------------------------------------------------------
// URL sanitization
// ---------------------------------------------------------------------------

function sanitizeUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.username || parsed.password) {
      parsed.username = '***';
      parsed.password = '***';
    }
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString();
  } catch {
    return url.replace(/\/\/[^@]+@/, '//***:***@');
  }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

interface ApiResponse<T = unknown> {
  ok: boolean;
  data?: T;
  error?: string;
  rowCount?: number;
  affected?: number;
}

async function request<T>(
  url: string,
  body: unknown,
  headers: Record<string, string>,
  timeout?: number,
  signal?: AbortSignal,
): Promise<ApiResponse<T>> {
  if (signal?.aborted) {
    throw new DOMException('This operation was aborted', 'AbortError');
  }
  const controller = new AbortController();
  const deadline = timeout ?? 30_000;
  const timer = deadline > 0
    ? setTimeout(() => controller.abort(new DOMException('Nucleus request timed out', 'TimeoutError')), deadline)
    : undefined;
  const onAbort = (): void => controller.abort(new DOMException('This operation was aborted', 'AbortError'));
  signal?.addEventListener('abort', onAbort, { once: true });
  // Race the complete response, not just fetch's header promise. This also
  // bounds non-cooperative/custom fetch implementations during body parsing.
  let onInternalAbort: (() => void) | undefined;
  const aborted = new Promise<never>((_, reject) => {
    onInternalAbort = () => reject(controller.signal.reason);
    controller.signal.addEventListener('abort', onInternalAbort, { once: true });
  });
  try {
    return await Promise.race([aborted, (async () => {
      let res: Response;
      try {
        res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...headers },
          body: JSON.stringify(body),
          signal: controller.signal,
          keepalive: true,
        });
      } catch (err) {
        if (controller.signal.aborted) throw controller.signal.reason;
        throw new NucleusConnectionError('Failed to reach Nucleus server', {
          cause: err instanceof Error ? err : undefined,
          meta: { url: sanitizeUrl(url) },
        });
      }
      if (!res.ok) {
        const text = await res.text();
        mapHttpError(res.status, text, url);
      }
      return (await res.json()) as ApiResponse<T>;
    })()]);
  } catch (err) {
    if (err instanceof NucleusError || (err as Error)?.name === 'AbortError' || (err as Error)?.name === 'TimeoutError') throw err;
    throw new NucleusConnectionError('Nucleus response could not be consumed', {
      cause: err instanceof Error ? err : undefined, meta: { url: sanitizeUrl(url) },
    });
  } finally {
    if (timer != null) clearTimeout(timer);
    signal?.removeEventListener('abort', onAbort);
    if (onInternalAbort) controller.signal.removeEventListener('abort', onInternalAbort);
  }
}

function mapHttpError(status: number, body: string, url: string): never {
  const meta = { status, url: sanitizeUrl(url) };
  switch (status) {
    case 401:
    case 403:
      throw new NucleusAuthError(body || 'Authentication failed', { meta });
    case 404:
      throw new NucleusNotFoundError(body || 'Resource not found', { meta });
    case 409:
      throw new NucleusConflictError(body || 'Conflict', { meta });
    default:
      throw new NucleusQueryError(body || `HTTP ${status}`, { meta });
  }
}

// ---------------------------------------------------------------------------
// HttpTransport
// ---------------------------------------------------------------------------

async function endpointDigest(config: unknown): Promise<string> {
  if (!globalThis.crypto?.subtle) throw new NucleusNotSupportedError('Endpoint admission needs WebCrypto SHA-256');
  const digest = await globalThis.crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(config)));
  return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, '0')).join('');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function beginUnknown(url: string, cause: unknown): NucleusUnknownOutcomeError {
  return new NucleusUnknownOutcomeError('BEGIN was dispatched but its outcome or identity is unknown; reconcile before retrying', {
    cause: cause instanceof Error ? cause : undefined,
    meta: { operation: 'BEGIN', url: sanitizeUrl(url) },
  });
}

export class HttpTransport implements Transport {
  readonly capabilities = Object.freeze({ cancellation: 'response-only', mutationOutcome: 'begin-unknown', automaticMutationReplay: false } as const);
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly timeout: number | undefined;
  private closed = false;
  private assertOpen(): void { if (this.closed) throw new NucleusError('CLOSED', 'Transport is closed'); }
  capabilityEndpoint(): Promise<string> { return endpointDigest([this.baseUrl, this.headers]); }

  constructor(url: string, headers: Record<string, string> = {}, timeout?: number) {
    // Strip trailing slash for consistent URL building
    this.baseUrl = url.replace(/\/+$/, '');
    this.headers = headers;
    if (timeout != null && (!Number.isFinite(timeout) || timeout < 0)) throw new RangeError('timeout must be a finite non-negative number');
    this.timeout = timeout ?? 30_000;

    // Warn about insecure connections
    if (this.baseUrl.startsWith('http://') && typeof process !== 'undefined' && process.env.NODE_ENV === 'production') {
      console.warn('[neutron-nucleus] WARNING: Using unencrypted HTTP connection. Use HTTPS in production.');
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<QueryResult<T>> {
    this.assertOpen();
    const res = await request<T[]>(`${this.baseUrl}/api/query`, { sql, params }, this.headers, this.timeout, opts?.signal);
    const rows = (res.data ?? []) as T[];
    return { rows, rowCount: res.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<number> {
    this.assertOpen();
    const res = await request<void>(`${this.baseUrl}/api/execute`, { sql, params }, this.headers, this.timeout, opts?.signal);
    return res.affected ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params, opts);
    if (result.rows.length === 0) return null;
    const first = result.rows[0];
    const keys = Object.keys(first);
    if (keys.length === 0) return null;
    return first[keys[0]] as T;
  }

  async beginTransaction(isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
    this.assertOpen();
    const url = `${this.baseUrl}/api/transaction/begin`;
    let res: unknown;
    try { res = await request<{ txId: string }>(
      `${this.baseUrl}/api/transaction/begin`,
      { isolationLevel },
      this.headers,
      this.timeout,
    );
    } catch (cause) {
      // Only an explicit client/protocol rejection proves BEGIN was rejected.
      const status = cause instanceof NucleusError ? cause.meta?.status : undefined;
      if (typeof status === 'number' && [400, 401, 403, 404, 409, 422].includes(status)) throw cause;
      throw beginUnknown(url, cause);
    }
    if (!isRecord(res) || (res.ok !== undefined && typeof res.ok !== 'boolean') ||
        (res.error !== undefined && typeof res.error !== 'string')) {
      throw beginUnknown(url, new NucleusTransactionError('Malformed BEGIN envelope'));
    }
    if (res.data !== undefined && res.data !== null && !isRecord(res.data))
      throw beginUnknown(url, new NucleusTransactionError('Malformed BEGIN data envelope'));
    if (res.ok === false) {
      if (isRecord(res.data) && res.data.txId !== undefined)
        throw beginUnknown(url, new NucleusTransactionError('Contradictory BEGIN rejection and remote identity'));
      throw new NucleusTransactionError(res.error as string || 'Server rejected BEGIN');
    }
    const txId = isRecord(res.data) ? res.data.txId : undefined;
    // An opaque identity must survive the actual native header encoding exactly.
    let usable = typeof txId === 'string' && txId.trim().length > 0 && !/[\x00-\x1f\x7f-\x9f]/.test(txId);
    let headerCause: Error | undefined;
    try { usable = usable && new Headers({ 'X-Nucleus-TxId': txId as string }).get('X-Nucleus-TxId') === txId; }
    catch (error) { usable = false; headerCause = error instanceof Error ? error : undefined; }
    if (!usable) throw beginUnknown(url, new NucleusTransactionError('Server did not return a transportable transaction ID', { cause: headerCause }));
    return new HttpTransactionTransport(this.baseUrl, this.headers, txId as string, this.timeout);
  }

  async close(): Promise<void> {
    this.closed = true;
  }

  async ping(): Promise<void> {
    this.assertOpen();
    await request<void>(`${this.baseUrl}/api/query`, { sql: 'SELECT 1', params: [] }, this.headers, this.timeout);
  }
}

// ---------------------------------------------------------------------------
// HttpTransactionTransport
// ---------------------------------------------------------------------------

class HttpTransactionTransport implements TransactionTransport {
  readonly capabilities = Object.freeze({ cancellation: 'response-only', mutationOutcome: 'driver-error', automaticMutationReplay: false } as const);
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly txId: string;
  private readonly timeout: number | undefined;
  private finished = false;

  constructor(baseUrl: string, headers: Record<string, string>, txId: string, timeout?: number) {
    this.baseUrl = baseUrl;
    this.headers = { ...headers, 'X-Nucleus-TxId': txId };
    this.txId = txId;
    this.timeout = timeout;
  }

  private assertOpen(): void {
    if (this.finished) {
      throw new NucleusTransactionError('Transaction has already been committed or rolled back');
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<QueryResult<T>> {
    this.assertOpen();
    const res = await request<T[]>(
      `${this.baseUrl}/api/query`,
      { sql, params, txId: this.txId },
      this.headers,
      this.timeout,
      opts?.signal,
    );
    const rows = (res.data ?? []) as T[];
    return { rows, rowCount: res.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<number> {
    this.assertOpen();
    const res = await request<void>(
      `${this.baseUrl}/api/execute`,
      { sql, params, txId: this.txId },
      this.headers,
      this.timeout,
      opts?.signal,
    );
    return res.affected ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params, opts);
    if (result.rows.length === 0) return null;
    const first = result.rows[0];
    const keys = Object.keys(first);
    if (keys.length === 0) return null;
    return first[keys[0]] as T;
  }

  async beginTransaction(_isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
    throw new NucleusTransactionError('Nested transactions are not supported');
  }

  async commit(): Promise<void> {
    this.assertOpen();
    await request<void>(
      `${this.baseUrl}/api/transaction/commit`,
      { txId: this.txId },
      this.headers,
      this.timeout,
    );
    this.finished = true;
  }

  async rollback(): Promise<void> {
    this.assertOpen();
    await request<void>(
      `${this.baseUrl}/api/transaction/rollback`,
      { txId: this.txId },
      this.headers,
      this.timeout,
    );
    this.finished = true;
  }

  async close(): Promise<void> {
    if (!this.finished) {
      await this.rollback();
    }
  }

  async ping(): Promise<void> {
    this.assertOpen();
    await this.query('SELECT 1');
  }
}

// ---------------------------------------------------------------------------
// MobileTransport — retry, caching, offline queue
// ---------------------------------------------------------------------------

interface QueuedWrite {
  resolve: (value: number) => void;
  reject: (reason: unknown) => void;
  sql: string;
  params: unknown[];
}

/**
 * Transport for mobile (React Native) environments.
 *
 * Wraps `HttpTransport` and adds:
 * - Retry only operations explicitly asserted to be pure reads
 * - Optional cache for explicitly opted-in pure reads
 * - Offline write queue that flushes when connectivity is restored
 */
export class MobileTransport implements Transport {
  readonly capabilities = Object.freeze({ cancellation: 'response-only', mutationOutcome: 'unknown-error', automaticMutationReplay: false } as const);
  private readonly http: HttpTransport;
  private readonly cache: Map<string, { data: unknown; timestamp: number }>;
  private readonly cacheTTL: number;
  private readonly cacheEnabled: boolean;
  private readonly maxRetries: number;
  private readonly retryDelay: number;
  private readonly offlineQueueEnabled: boolean;
  private readonly maxQueueSize: number;
  private offlineQueue: QueuedWrite[] = [];
  private isOnline: boolean;
  private cacheGeneration = 0;
  private pendingMutations = 0;
  private activeTransactions = 0;
  private closed = false;
  private assertOpen(): void { if (this.closed) throw new NucleusError('CLOSED', 'Transport is closed'); }
  capabilityEndpoint(): Promise<string> { return this.http.capabilityEndpoint(); }

  constructor(config: MobileTransportConfig) {
    this.http = new HttpTransport(config.url, config.headers, config.timeout);
    this.cache = new Map();
    this.cacheTTL = config.cacheTTL ?? 60_000;
    this.cacheEnabled = config.cacheEnabled === true;
    this.maxRetries = config.maxRetries ?? 3;
    this.retryDelay = config.retryDelay ?? 1_000;
    this.offlineQueueEnabled = config.offlineQueueEnabled !== false;
    this.maxQueueSize = config.maxQueueSize ?? 100;
    this.isOnline = typeof navigator === 'undefined' || navigator.onLine !== false;

    if (typeof window !== 'undefined') {
      window.addEventListener('online', () => {
        if (this.closed) return;
        this.isOnline = true;
        void this.flushQueue().catch(() => {});
      });
      window.addEventListener('offline', () => {
        this.isOnline = false;
      });
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<QueryResult<T>> {
    this.assertOpen();
    if (opts?.signal?.aborted) throw new DOMException('This operation was aborted', 'AbortError');
    const isRead = opts?.readOnly === true;
    const cacheable = isRead && opts?.cache === true && this.cacheEnabled &&
      this.pendingMutations === 0 && this.activeTransactions === 0;
    const cacheKey = JSON.stringify({ sql, params });
    if (cacheable) {
      const cached = this.cache.get(cacheKey);
      if (cached && Date.now() - cached.timestamp < this.cacheTTL) return cached.data as QueryResult<T>;
    }
    // Unknown SQL, including SELECT model functions, is potentially mutating.
    // Fence pending reads before dispatch, even when the write outcome is lost.
    if (!isRead) this.invalidateCache();
    const generation = this.cacheGeneration;
    const result = isRead
      ? await this.withRetry(() => this.http.query<T>(sql, params, opts), opts?.signal)
      : await this.once(() => this.http.query<T>(sql, params, opts));
    if (cacheable && generation === this.cacheGeneration) {
      this.cache.set(cacheKey, { data: result, timestamp: Date.now() });
      if (this.cache.size > 500) this.cache.delete(this.cache.keys().next().value!);
    }
    return result;
  }

  async execute(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<number> {
    this.assertOpen();
    if (opts?.signal?.aborted) {
      return Promise.reject(new DOMException('This operation was aborted', 'AbortError'));
    }
    // Offline queueing is intentionally skipped when a signal is passed: a
    // canceled caller must never be handed a queued/replayed result. Such
    // calls go to the online retry path directly and reject per the signal
    // (documented in QuerySignalOptions).
    if (!this.isOnline && this.offlineQueueEnabled && !opts?.signal) {
      return new Promise<number>((resolve, reject) => {
        if (this.offlineQueue.length >= this.maxQueueSize) {
          reject(new NucleusConnectionError('Offline queue full', { meta: { queueSize: this.maxQueueSize } }));
          return;
        }
        this.offlineQueue.push({ resolve, reject, sql, params });
      });
    }
    this.invalidateCache();
    return this.once(() => this.http.execute(sql, params, opts));
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params, opts);
    if (result.rows.length === 0) return null;
    const first = result.rows[0];
    const keys = Object.keys(first);
    if (keys.length === 0) return null;
    return first[keys[0]] as T;
  }

  async beginTransaction(isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
    this.assertOpen();
    // Transactions go through the underlying HTTP transport directly — no caching or queueing
    this.invalidateCache();
    const tx = await this.once(() => this.http.beginTransaction(isolationLevel));
    this.activeTransactions++;
    let finished = false;
    const finish = (): void => {
      if (!finished) { finished = true; this.activeTransactions--; this.invalidateCache(); }
    };
    return new Proxy(tx, {
      get: (target, property) => {
        const value = Reflect.get(target, property);
        if (typeof value !== 'function') return value;
        if (['commit', 'rollback', 'close'].includes(String(property))) {
          return async (...args: unknown[]) => {
            const result = await this.once(() => value.apply(target, args));
            finish();
            return result;
          };
        }
        if (['query', 'execute', 'fetchval'].includes(String(property))) {
          return (...args: unknown[]) => {
            if ((args[2] as QuerySignalOptions | undefined)?.signal?.aborted) {
              return Promise.reject(new DOMException('This operation was aborted', 'AbortError'));
            }
            return this.once(() => value.apply(target, args));
          };
        }
        return value.bind(target);
      },
    });
  }

  async close(): Promise<void> {
    this.closed = true;
    this.cache.clear();
    for (const item of this.offlineQueue.splice(0)) item.reject(new NucleusError('CLOSED', 'Transport closed before queued write dispatch'));
    await this.http.close();
  }

  async ping(): Promise<void> {
    this.assertOpen();
    await this.withRetry(() => this.http.ping());
  }

  // -- Mobile-specific API --------------------------------------------------

  /** Clear all cached query results, or only those matching `pattern`. */
  invalidateCache(pattern?: string): void {
    this.cacheGeneration++;
    if (!pattern) {
      this.cache.clear();
      return;
    }
    for (const key of this.cache.keys()) {
      if (key.includes(pattern)) this.cache.delete(key);
    }
  }

  /** Number of operations waiting in the offline queue. */
  get queueSize(): number {
    return this.offlineQueue.length;
  }

  // -- Internals ------------------------------------------------------------

  /** A dispatched mutation cannot be safely replayed without server deduplication. */
  private async once<T>(fn: () => Promise<T>): Promise<T> {
    this.invalidateCache();
    this.pendingMutations++;
    try { return await fn(); }
    catch (err) {
      const status = (err as NucleusError)?.meta?.status;
      if (err instanceof NucleusConnectionError ||
          (typeof status === 'number' && status >= 500) ||
          (err as Error)?.name === 'AbortError' || (err as Error)?.name === 'TimeoutError' ||
          err instanceof SyntaxError) {
        throw new NucleusUnknownOutcomeError('The dispatched operation may have completed; reconcile its outcome before retrying', {
          cause: err instanceof Error ? err : undefined,
        });
      }
      throw err;
    } finally {
      this.pendingMutations--;
      this.invalidateCache();
    }
  }

  private async withRetry<T>(fn: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    let lastError: Error | undefined;
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        return await fn();
      } catch (err: unknown) {
        // Aborts are terminal — never retried, never swallowed into backoff.
        if (signal?.aborted || (err as DOMException | undefined)?.name === 'AbortError') throw err;
        lastError = err instanceof Error ? err : new Error(String(err));
        // Don't retry client errors (4xx)
        const status = (err as { meta?: { status?: number } })?.meta?.status;
        if (status != null && status >= 400 && status < 500) throw err;
        if (attempt < this.maxRetries) {
          await new Promise((r) => setTimeout(r, this.retryDelay * Math.pow(2, attempt)));
        }
      }
    }
    throw lastError;
  }

  private async flushQueue(): Promise<void> {
    this.assertOpen();
    const queue = this.offlineQueue;
    this.offlineQueue = [];
    for (const item of queue) {
      try {
        this.invalidateCache();
        const result = await this.once(() => this.http.execute(item.sql, item.params));
        item.resolve(result);
      } catch (err) {
        item.reject(err);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// EmbeddedTransport — Tauri / neutron:// protocol (desktop with embedded Nucleus)
// ---------------------------------------------------------------------------

type InvokeFn = (cmd: string, args: Record<string, unknown>) => Promise<unknown>;

/**
 * Transport for desktop environments where Nucleus is embedded via Tauri.
 *
 * Skips HTTP serialization overhead by calling Tauri's IPC `invoke()` directly.
 * Falls back to the `neutron://` custom protocol when Tauri internals are not
 * available.
 */
export class EmbeddedTransport implements Transport {
  readonly capabilities = Object.freeze({ cancellation: 'unsupported', mutationOutcome: 'driver-error', automaticMutationReplay: false } as const);
  private readonly invoke: InvokeFn;
  private closed = false;
  private assertOpen(): void { if (this.closed) throw new NucleusError('CLOSED', 'Transport is closed'); }
  /** Reject signal usage: the embedded channel has no cancellation path. */
  private rejectIfSignal(opts?: QuerySignalOptions): void {
    this.assertOpen();
    if (opts?.signal) {
      throw new NucleusNotSupportedError(
        'cancellation is not supported by EmbeddedTransport: the Tauri IPC / neutron:// channel has no abort mechanism',
      );
    }
  }


  constructor() {
    // Prefer Tauri's IPC invoke when available
    if (typeof window !== 'undefined' && (window as unknown as Record<string, unknown>).__TAURI_INTERNALS__) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      this.invoke = (window as any).__TAURI_INTERNALS__.invoke as InvokeFn;
    } else {
      // Fallback to neutron:// custom protocol
      this.invoke = async (cmd: string, args: Record<string, unknown>) => {
        const res = await fetch(`neutron://localhost/api/${cmd}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(args),
        });
        if (!res.ok) {
          const text = await res.text().catch(() => '');
          throw new NucleusQueryError(text || `Embedded call failed: ${res.status}`, {
            meta: { status: res.status, cmd },
          });
        }
        return res.json();
      };
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<QueryResult<T>> {
    this.rejectIfSignal(opts);
    const result = (await this.invoke('nucleus_query', { sql, params })) as {
      rows?: T[];
      rowCount?: number;
      data?: T[];
    };
    const rows = (result.rows ?? result.data ?? []) as T[];
    return { rows, rowCount: result.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<number> {
    this.rejectIfSignal(opts);
    const result = (await this.invoke('nucleus_execute', { sql, params })) as {
      affected?: number;
      rowsAffected?: number;
    };
    return result.affected ?? result.rowsAffected ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params, opts);
    if (result.rows.length === 0) return null;
    const first = result.rows[0];
    const keys = Object.keys(first);
    if (keys.length === 0) return null;
    return first[keys[0]] as T;
  }

  async beginTransaction(isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
    this.assertOpen();
    const result = (await this.invoke('nucleus_transaction_begin', {
      isolationLevel: isolationLevel ?? null,
    })) as { txId?: string };
    const txId = result.txId;
    if (!txId) {
      throw new NucleusTransactionError('Embedded Nucleus did not return a transaction ID');
    }
    return new EmbeddedTransactionTransport(this.invoke, txId);
  }

  async close(): Promise<void> {
    this.closed = true;
  }

  async ping(): Promise<void> {
    this.assertOpen();
    await this.invoke('nucleus_query', { sql: 'SELECT 1', params: [] });
  }
}

// ---------------------------------------------------------------------------
// EmbeddedTransactionTransport
// ---------------------------------------------------------------------------

class EmbeddedTransactionTransport implements TransactionTransport {
  readonly capabilities = Object.freeze({ cancellation: 'unsupported', mutationOutcome: 'driver-error', automaticMutationReplay: false } as const);
  private readonly invoke: InvokeFn;
  private closed = false;
  private readonly txId: string;
  private finished = false;

  constructor(invoke: InvokeFn, txId: string) {
    this.invoke = invoke;
    this.txId = txId;
  }

  /** Reject signal usage: the embedded channel has no cancellation path. */
  private rejectIfSignal(opts?: QuerySignalOptions): void {
    this.assertOpen();
    if (opts?.signal) {
      throw new NucleusNotSupportedError(
        'cancellation is not supported by EmbeddedTransport transactions: the Tauri IPC / neutron:// channel has no abort mechanism',
      );
    }
  }

  private assertOpen(): void {
    if (this.closed) throw new NucleusError('CLOSED', 'Transport is closed');
    if (this.finished) {
      throw new NucleusTransactionError('Transaction has already been committed or rolled back');
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<QueryResult<T>> {
    this.assertOpen();
    this.rejectIfSignal(opts);
    const result = (await this.invoke('nucleus_query', { sql, params, txId: this.txId })) as {
      rows?: T[];
      rowCount?: number;
      data?: T[];
    };
    const rows = (result.rows ?? result.data ?? []) as T[];
    return { rows, rowCount: result.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<number> {
    this.assertOpen();
    this.rejectIfSignal(opts);
    const result = (await this.invoke('nucleus_execute', { sql, params, txId: this.txId })) as {
      affected?: number;
      rowsAffected?: number;
    };
    return result.affected ?? result.rowsAffected ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params, opts);
    if (result.rows.length === 0) return null;
    const first = result.rows[0];
    const keys = Object.keys(first);
    if (keys.length === 0) return null;
    return first[keys[0]] as T;
  }

  async beginTransaction(_isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
    throw new NucleusTransactionError('Nested transactions are not supported');
  }

  async commit(): Promise<void> {
    this.assertOpen();
    await this.invoke('nucleus_transaction_commit', { txId: this.txId });
    this.finished = true;
  }

  async rollback(): Promise<void> {
    this.assertOpen();
    await this.invoke('nucleus_transaction_rollback', { txId: this.txId });
    this.finished = true;
  }

  async close(): Promise<void> {
    if (!this.finished) {
      await this.rollback();
    }
  }

  async ping(): Promise<void> {
    this.assertOpen();
    await this.query('SELECT 1');
  }
}

// ---------------------------------------------------------------------------
// PgTransport — the canonical Nucleus connection path (PostgreSQL wire)
// ---------------------------------------------------------------------------

// `pg` is an OPTIONAL dependency imported lazily, so browser/RN bundles that
// use HttpTransport/MobileTransport never pull it. Typed loosely because it
// is not a compile-time dependency.
type PgModule = {
  Pool: new (cfg: {
    connectionString: string;
    max?: number;
    statement_timeout?: number;
    query_timeout?: number;
    connectionTimeoutMillis?: number;
    types?: { getTypeParser(oid: number, format?: string): (value: string) => unknown };
  }) => PgPool;
  types?: { getTypeParser(oid: number, format?: string): (value: string) => unknown };
};
interface PgPool {
  query(sql: string, params?: unknown[]): Promise<{ rows: unknown[]; rowCount: number | null }>;
  connect(): Promise<PgPoolClient>;
  end(): Promise<void>;
  on(event: 'error', cb: (err: Error) => void): void;
}
interface PgPoolClient {
  query(sql: string, params?: unknown[]): Promise<{ rows: unknown[]; rowCount: number | null }>;
  /** node-postgres: release(err) also removes (destroys) the client. */
  release(err?: Error): void;
  on?(event: 'error', cb: (err: Error) => void): void;
  removeListener?(event: 'error', cb: (err: Error) => void): void;
}

let pgModulePromise: Promise<PgModule> | null = null;
async function loadPg(): Promise<PgModule> {
  if (!pgModulePromise) {
    // Specifier via variable so the optional 'pg' dep isn't a compile-time
    // requirement — resolved at runtime only when a postgres:// URL is used.
    const pgSpecifier = 'pg';
    pgModulePromise = import(pgSpecifier).then(
      (m) => ((m as { default?: PgModule }).default ?? (m as unknown as PgModule)),
      () => {
        throw new NucleusConnectionError(
          "postgres:// connections require the 'pg' package — install it: npm i pg"
        );
      }
    );
  }
  return pgModulePromise;
}

// Decoder policy belongs to this pool. Never mutate pg's process-global types:
// another SQL client in the same process may need native int8 strings.
function poolTypeParsers(pg: PgModule, profile?: 'lossless-read-v1') {
  if (!pg.types) throw new NucleusError('PG_TYPES_UNSUPPORTED', 'PostgreSQL type parser registry is required');
  const native = pg.types;
  const safeInt8 = (value: string) => {
    const n = Number(value);
    if (!Number.isSafeInteger(n)) throw new NucleusError('INT8_PRECISION',
      `int8 value ${value} exceeds Number.MAX_SAFE_INTEGER. Use a separate SQL-only ` +
      `PgTransport with valueProfile: 'lossless-read-v1', or explicitly read ::text.`);
    return n;
  };
  const identity = (value: string) => value;
  const exact = new Map<number, (value: string) => unknown>([
    [20, identity], [1700, identity], [25, identity], [1042, identity], [1043, identity], [2950, identity],
    [21, value => Number(value)], [23, value => Number(value)],
    [16, value => { if (value === 't') return true; if (value === 'f') return false;
      throw new NucleusError('PG_VALUE_FORMAT', 'Unsupported PostgreSQL boolean encoding'); }],
    [17, value => { if (!/^\\x(?:[0-9a-fA-F]{2})*$/.test(value))
      throw new NucleusError('PG_VALUE_FORMAT', 'lossless-read-v1 requires hexadecimal bytea output');
      return Buffer.from(value.slice(2), 'hex'); }],
  ]);
  return { getTypeParser(oid: number, format = 'text'): (value: string) => unknown {
    if (profile === 'lossless-read-v1') {
      if (format !== 'text' || !exact.has(oid)) return () => { throw new NucleusError('PG_VALUE_TYPE_UNSUPPORTED',
        `lossless-read-v1 does not support PostgreSQL OID ${oid} in ${format} format`); };
      return exact.get(oid)!;
    }
    if (format !== 'text') return native.getTypeParser(oid, format);
    return oid === 20 ? safeInt8 : native.getTypeParser(oid, format);
  }};
}

const ISOLATION_SQL: Record<IsolationLevel, string> = {
  read_committed: 'READ COMMITTED',
  repeatable_read: 'REPEATABLE READ',
  serializable: 'SERIALIZABLE',
};

/**
 * Nucleus client over the PostgreSQL wire protocol — the canonical path
 * (`nucleus start`, no gateway between). This is the proven Node transport
 * (mirrors Teploy Ship's adapter). The pool is created lazily on first use
 * so construction stays synchronous and browser-safe.
 */
export class PgTransport implements Transport {
  readonly capabilities = Object.freeze({ cancellation: 'server-attempt', mutationOutcome: 'driver-error', automaticMutationReplay: false } as const);
  readonly valueProfile: 'lossless-read-v1' | undefined;
  private readonly url: string;
  private readonly timeout: number | undefined;
  // The memoized promise is assigned synchronously, before any await: the
  // old check-then-`await`-then-assign let two concurrent first queries each
  // construct a Pool, and the loser's connections were never `.end()`ed.
  private poolPromise: Promise<PgPool> | null = null;
  private cancellationPoolPromise: Promise<PgPool> | null = null;
  private readonly lifecycle = resourceLifecycle('owned', async () => {
    const pools = [this.poolPromise, this.cancellationPoolPromise];
    const failures = await closeResources(pools.filter((p): p is Promise<PgPool> => p !== null).map(pending => async () => (await pending).end()));
    if (failures.length) throw new AggregateError(failures, 'Nucleus PG pools cleanup failed', { cause: failures[0] });
  });
  capabilityEndpoint(): Promise<string> { return endpointDigest(this.url); }

  constructor(url: string, config: PgTransportConfig = {}) {
    // Honors-or-rejects-at-construction: silently dropping config the caller
    // believes is applied is the bug this prevents. The wire protocol has no
    // headers to carry them on — auth belongs in the connection URL.
    if (config.headers != null) {
      throw new NucleusError(
        'PG_HEADERS_UNSUPPORTED',
        'PgTransport does not accept headers: the PostgreSQL wire protocol ' +
          'has no HTTP headers. Pass auth via the connection URL instead.',
      );
    }
    if (config.valueProfile != null && config.valueProfile !== 'lossless-read-v1') {
      throw new NucleusError('PG_VALUE_PROFILE_UNSUPPORTED', 'Unknown PostgreSQL value profile');
    }
    this.valueProfile = config.valueProfile;
    this.url = url;
    this.timeout = config.timeout;
  }

  private getPool(): Promise<PgPool> {
    this.lifecycle.assertOpen();
    if (!this.poolPromise) {
      const creation = loadPg().then((pg) => {
        const types = poolTypeParsers(pg, this.valueProfile);
        // `timeout` maps onto pg's native knobs: statement_timeout and
        // query_timeout per client, connectionTimeoutMillis for pool checkout.
        const pool = new pg.Pool({
          connectionString: this.url,
          types,
          max: 8,
          ...(this.timeout != null && {
            statement_timeout: this.timeout,
            query_timeout: this.timeout,
            connectionTimeoutMillis: this.timeout,
          }),
        });
        // An idle pooled connection dying (engine restart / accessory upgrade)
        // emits 'error' on the pool; unhandled, that CRASHES the process. This
        // handler absorbs the idle death so the pool can mint fresh connections;
        // in-flight queries still reject through their own promises.
        pool.on('error', () => {});
        return pool;
      });
      // A failed creation must not be cached — the next call retries.
      creation.catch(() => {
        if (this.poolPromise === creation) this.poolPromise = null;
      });
      this.poolPromise = creation;
    }
    return this.poolPromise;
  }

  private getCancellationPool(): Promise<PgPool> {
    this.lifecycle.assertOpen();
    if (!this.cancellationPoolPromise) {
      const creation = loadPg().then((pg) => {
        const pool = new pg.Pool({ connectionString: this.url, max: 8,
          statement_timeout: this.timeout ?? 30_000,
          query_timeout: this.timeout ?? 30_000,
          connectionTimeoutMillis: this.timeout ?? 30_000 });
        pool.on('error', () => {});
        return pool;
      });
      creation.catch(() => {
        if (this.cancellationPoolPromise === creation) this.cancellationPoolPromise = null;
      });
      this.cancellationPoolPromise = creation;
    }
    return this.cancellationPoolPromise;
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<QueryResult<T>> {
    if (!opts?.signal) {
      const pool = await this.getPool();
    this.lifecycle.assertOpen();
      const res = await pool.query(sql, params);
      return { rows: res.rows as T[], rowCount: res.rowCount ?? 0 };
    }
    const res = await this.queryCancelable(sql, params, opts.signal);
    return { rows: res.rows as T[], rowCount: res.rowCount ?? 0 };
  }

  async execute(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<number> {
    if (!opts?.signal) {
      const pool = await this.getPool();
    this.lifecycle.assertOpen();
      const res = await pool.query(sql, params);
      return res.rowCount ?? 0;
    }
    const res = await this.queryCancelable(sql, params, opts.signal);
    return res.rowCount ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params, opts);
    const row = result.rows[0];
    if (row === undefined) return null;
    const value = Object.values(row)[0];
    return (value ?? null) as T | null;
  }

  /** Reserve an independent cancel connection before submitting user SQL.
   * Cancel failures are observed immediately and drained before either client
   * is released, even when the target statement itself fails. */
  private async queryCancelable(
    sql: string,
    params: unknown[],
    signal: AbortSignal,
  ): Promise<{ rows: unknown[]; rowCount: number | null }> {
    const preflight = (): void => {
      this.lifecycle.assertOpen();
      if (signal.aborted) throw new DOMException('This operation was aborted', 'AbortError');
    };
    preflight();
    const pool = await this.getPool();
    this.lifecycle.assertOpen();
    preflight();
    const client = await pool.connect();
    let cancelClient: PgPoolClient | undefined;
    let pid: number | null = null;
    let cancelAttempt: Promise<void> | undefined;
    let cancelError: Error | undefined;
    let targetError: Error | undefined;
    let pidProbeError: unknown;
    let confirmed = false;
    const onTargetError = (err: Error): void => { targetError = err; };
    const onCancelError = (err: Error): void => { cancelError = err; };
    client.on?.('error', onTargetError);
    const abortListener = (): void => {
      if (pid == null || !cancelClient) return;
      // The rejection handler is installed when the promise is created.
      cancelAttempt = Promise.resolve().then(() => cancelClient!.query(
        'SELECT pg_cancel_backend($1) AS canceled', [pid],
      )).then((res) => {
        confirmed = (res.rows[0] as { canceled?: boolean } | undefined)?.canceled === true;
      }, (err: unknown) => { cancelError = err instanceof Error ? err : new Error(String(err)); });
    };
    try {
      preflight();
      cancelClient = await (await this.getCancellationPool()).connect();
      cancelClient.on?.('error', onCancelError);
      preflight();
      const pidRow = await client.query('SELECT pg_backend_pid() AS pid').catch((err: unknown) => {
        pidProbeError = err;
        return null;
      });
      const value = pidRow ? (pidRow.rows[0] as { pid?: number | string } | undefined)?.pid : undefined;
      const candidate = Number(value);
      pid = value != null && Number.isSafeInteger(candidate) && candidate > 0 ? candidate : null;
      preflight();
      signal.addEventListener('abort', abortListener, { once: true });
      let result: { rows: unknown[]; rowCount: number | null } | undefined;
      let failed = false;
      let failure: unknown;
      try { result = await client.query(sql, params); }
      catch (err) { failed = true; failure = err; }
      signal.removeEventListener('abort', abortListener);
      await cancelAttempt;
      if (signal.aborted && (cancelError || !confirmed)) {
        const code = (cancelError as Error & { code?: string } | undefined)?.code;
        const unsupported = code === '0A000' || code === '42883';
        const options = {
          cause: cancelError ?? (pidProbeError instanceof Error ? pidProbeError : undefined),
          meta: { cancellation: unsupported ? 'unsupported' : cancelAttempt ? 'failed' : 'not-dispatched',
            targetFailed: failed },
        };
        if (unsupported || !cancelAttempt) {
          throw new NucleusNotSupportedError('statement cancellation could not be confirmed; the statement may have completed', options);
        }
        throw new NucleusError('CANCELLATION_FAILED', 'statement cancellation failed; the statement may have completed', options);
      }
      if (failed) throw failure;
      if (signal.aborted) throw new DOMException('This operation was aborted', 'AbortError');
      return result!;
    } finally {
      signal.removeEventListener('abort', abortListener);
      await cancelAttempt;
      cancelClient?.removeListener?.('error', onCancelError);
      cancelClient?.release(cancelError);
      client.removeListener?.('error', onTargetError);
      client.release(targetError ?? (signal.aborted ? new Error('canceled by AbortSignal') : undefined));
    }
  }

  async beginTransaction(isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
    const pool = await this.getPool();
    this.lifecycle.assertOpen();
    const client = await pool.connect();
    try {
      this.lifecycle.assertOpen();
      const level = isolationLevel ? ` ISOLATION LEVEL ${ISOLATION_SQL[isolationLevel]}` : '';
      await client.query(`BEGIN${level}`);
    } catch (err) {
      client.release();
      throw err;
    }
    return new PgTransactionTransport(client);
  }

  async ping(): Promise<void> {
    const pool = await this.getPool();
    this.lifecycle.assertOpen();
    await pool.query('SELECT 1');
  }

  async close(): Promise<void> {
    return this.lifecycle.terminate();
  }
}

/** A transaction bound to one checked-out pooled connection. */
export class PgTransactionTransport implements TransactionTransport {
  readonly capabilities = Object.freeze({ cancellation: 'pre-dispatch', mutationOutcome: 'driver-error', automaticMutationReplay: false } as const);
  private readonly client: PgPoolClient;
  private finished = false;

  constructor(client: PgPoolClient) {
    this.client = client;
  }

  private assertOpen(): void {
    if (this.finished) throw new NucleusTransactionError('transaction already finished');
  }

  // Signal support on the transaction client is pre-flight only: an
  // already-aborted signal rejects before anything is sent. Mid-statement
  // cancellation would need the pool for a pg_cancel_backend side channel,
  // and this transport deliberately does not own the pool.
  private rejectIfAborted(opts?: QuerySignalOptions): void {
    if (opts?.signal?.aborted) {
      throw new DOMException('This operation was aborted', 'AbortError');
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<QueryResult<T>> {
    this.assertOpen();
    this.rejectIfAborted(opts);
    const res = await this.client.query(sql, params);
    return { rows: res.rows as T[], rowCount: res.rowCount ?? 0 };
  }

  async execute(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<number> {
    this.assertOpen();
    this.rejectIfAborted(opts);
    const res = await this.client.query(sql, params);
    return res.rowCount ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: QuerySignalOptions): Promise<T | null> {
    this.assertOpen();
    this.rejectIfAborted(opts);
    const res = await this.client.query(sql, params);
    const row = res.rows[0] as Record<string, unknown> | undefined;
    if (row === undefined) return null;
    const value = Object.values(row)[0];
    return (value ?? null) as T | null;
  }

  // Nested transactions are not supported — a savepoint model would go here.
  async beginTransaction(): Promise<TransactionTransport> {
    throw new NucleusTransactionError('nested transactions are not supported');
  }

  async commit(): Promise<void> {
    this.assertOpen();
    try {
      await this.client.query('COMMIT');
    } catch (err) {
      // The connection's transaction state is unknown after a failed COMMIT:
      // hand it back with the error so the pool destroys it instead of
      // returning a poisoned client (or, worse, leaking it — the pool is
      // max: 8, so eight leaked clients deadlock the app).
      this.finished = true;
      this.client.release(err as Error);
      throw err;
    }
    this.finished = true;
    this.client.release();
  }

  async rollback(): Promise<void> {
    if (this.finished) return;
    try {
      await this.client.query('ROLLBACK');
    } catch (err) {
      this.finished = true;
      this.client.release(err as Error);
      throw err;
    }
    this.finished = true;
    this.client.release();
  }

  async ping(): Promise<void> {
    this.assertOpen();
    await this.client.query('SELECT 1');
  }

  async close(): Promise<void> {
    if (!this.finished) await this.rollback();
  }
}

// ---------------------------------------------------------------------------
// Auto-detection factory
// ---------------------------------------------------------------------------

/**
 * Create the best transport for the current platform.
 *
 * Detection order:
 * 1. Desktop with Tauri internals -> `EmbeddedTransport` (IPC, zero serialization overhead)
 * 2. React Native -> `MobileTransport` (retries, caching, offline queue)
 * 3. Everything else -> `HttpTransport` (standard fetch)
 */
export function createTransport(config: MobileTransportConfig): Transport {
  // Desktop with Tauri — use embedded transport (skips HTTP entirely)
  if (typeof window !== 'undefined' && (window as unknown as Record<string, unknown>).__TAURI_INTERNALS__) {
    return new EmbeddedTransport();
  }

  // React Native — use mobile transport with retries, caching, and offline queue
  if (typeof navigator !== 'undefined' && navigator.product === 'ReactNative') {
    return new MobileTransport(config);
  }

  // Node with a postgres:// URL — the canonical Nucleus path (pgwire).
  // This is the proven connection story; HttpTransport targets an HTTP
  // query gateway that `nucleus start` does not serve, so it only applies
  // to a deployment that fronts Nucleus with such a gateway.
  const isNode = typeof process !== 'undefined' && !!process.versions?.node;
  if (isNode && /^postgres(ql)?:\/\//i.test(config.url)) {
    return new PgTransport(config.url, { headers: config.headers, timeout: config.timeout });
  }

  // Web / http(s) gateway — standard HTTP.
  return new HttpTransport(config.url, config.headers, config.timeout);
}
