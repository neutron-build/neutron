// ---------------------------------------------------------------------------
// Nucleus client — transport implementations
// ---------------------------------------------------------------------------

import type { Transport, TransactionTransport, QueryResult, IsolationLevel } from './types.js';
import {
  NucleusAuthError,
  NucleusConflictError,
  NucleusConnectionError,
  NucleusNotFoundError,
  NucleusQueryError,
  NucleusTransactionError,
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
  /** Whether to cache SELECT query results (default true). */
  cacheEnabled?: boolean;
  /** Time-to-live for cached entries in ms (default 60000). */
  cacheTTL?: number;
  /** Whether to queue writes when the device is offline (default true). */
  offlineQueueEnabled?: boolean;
  /** Maximum number of queued offline operations (default 100). */
  maxQueueSize?: number;
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
): Promise<ApiResponse<T>> {
  let res: Response;
  const controller = timeout != null ? new AbortController() : undefined;
  const timer = controller ? setTimeout(() => controller.abort(), timeout) : undefined;

  try {
    res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: JSON.stringify(body),
      signal: controller?.signal,
      keepalive: true,
    });
  } catch (err) {
    throw new NucleusConnectionError('Failed to reach Nucleus server', {
      cause: err instanceof Error ? err : undefined,
      meta: { url: sanitizeUrl(url) },
    });
  } finally {
    if (timer != null) clearTimeout(timer);
  }

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    mapHttpError(res.status, text, url);
  }

  return (await res.json()) as ApiResponse<T>;
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

export class HttpTransport implements Transport {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly timeout: number | undefined;

  constructor(url: string, headers: Record<string, string> = {}, timeout?: number) {
    // Strip trailing slash for consistent URL building
    this.baseUrl = url.replace(/\/+$/, '');
    this.headers = headers;
    this.timeout = timeout;

    // Warn about insecure connections
    if (this.baseUrl.startsWith('http://') && typeof process !== 'undefined' && process.env.NODE_ENV === 'production') {
      console.warn('[neutron-nucleus] WARNING: Using unencrypted HTTP connection. Use HTTPS in production.');
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    const res = await request<T[]>(`${this.baseUrl}/api/query`, { sql, params }, this.headers, this.timeout);
    const rows = (res.data ?? []) as T[];
    return { rows, rowCount: res.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    const res = await request<void>(`${this.baseUrl}/api/execute`, { sql, params }, this.headers, this.timeout);
    return res.affected ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params);
    if (result.rows.length === 0) return null;
    const first = result.rows[0];
    const keys = Object.keys(first);
    if (keys.length === 0) return null;
    return first[keys[0]] as T;
  }

  async beginTransaction(isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
    const res = await request<{ txId: string }>(
      `${this.baseUrl}/api/transaction/begin`,
      { isolationLevel },
      this.headers,
      this.timeout,
    );
    const txId = res.data?.txId;
    if (!txId) {
      throw new NucleusTransactionError('Server did not return a transaction ID');
    }
    return new HttpTransactionTransport(this.baseUrl, this.headers, txId, this.timeout);
  }

  async close(): Promise<void> {
    // HTTP is stateless — nothing to close.
  }

  async ping(): Promise<void> {
    await request<void>(`${this.baseUrl}/api/query`, { sql: 'SELECT 1', params: [] }, this.headers, this.timeout);
  }
}

// ---------------------------------------------------------------------------
// HttpTransactionTransport
// ---------------------------------------------------------------------------

class HttpTransactionTransport implements TransactionTransport {
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

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    this.assertOpen();
    const res = await request<T[]>(
      `${this.baseUrl}/api/query`,
      { sql, params, txId: this.txId },
      this.headers,
      this.timeout,
    );
    const rows = (res.data ?? []) as T[];
    return { rows, rowCount: res.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    this.assertOpen();
    const res = await request<void>(
      `${this.baseUrl}/api/execute`,
      { sql, params, txId: this.txId },
      this.headers,
      this.timeout,
    );
    return res.affected ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params);
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
 * - Automatic retry with exponential backoff for transient failures
 * - In-memory cache for SELECT queries
 * - Offline write queue that flushes when connectivity is restored
 */
export class MobileTransport implements Transport {
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

  constructor(config: MobileTransportConfig) {
    this.http = new HttpTransport(config.url, config.headers, config.timeout);
    this.cache = new Map();
    this.cacheTTL = config.cacheTTL ?? 60_000;
    this.cacheEnabled = config.cacheEnabled !== false;
    this.maxRetries = config.maxRetries ?? 3;
    this.retryDelay = config.retryDelay ?? 1_000;
    this.offlineQueueEnabled = config.offlineQueueEnabled !== false;
    this.maxQueueSize = config.maxQueueSize ?? 100;
    this.isOnline = typeof navigator !== 'undefined' ? navigator.onLine : true;

    if (typeof window !== 'undefined') {
      window.addEventListener('online', () => {
        this.isOnline = true;
        void this.flushQueue();
      });
      window.addEventListener('offline', () => {
        this.isOnline = false;
      });
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    const isRead = sql.trimStart().toUpperCase().startsWith('SELECT');

    // Check cache for read queries
    if (isRead && this.cacheEnabled) {
      const cacheKey = JSON.stringify({ sql, params });
      const cached = this.cache.get(cacheKey);
      if (cached && Date.now() - cached.timestamp < this.cacheTTL) {
        return cached.data as QueryResult<T>;
      }
    }

    const result = await this.withRetry(() => this.http.query<T>(sql, params));

    // Cache read results
    if (isRead && this.cacheEnabled) {
      const cacheKey = JSON.stringify({ sql, params });
      this.cache.set(cacheKey, { data: result, timestamp: Date.now() });
      // Evict oldest entries when cache grows too large
      if (this.cache.size > 500) {
        const entries = [...this.cache.entries()].sort((a, b) => a[1].timestamp - b[1].timestamp);
        for (let i = 0; i < 100; i++) this.cache.delete(entries[i][0]);
      }
    }

    return result;
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    if (!this.isOnline && this.offlineQueueEnabled) {
      return new Promise<number>((resolve, reject) => {
        if (this.offlineQueue.length >= this.maxQueueSize) {
          reject(new NucleusConnectionError('Offline queue full', { meta: { queueSize: this.maxQueueSize } }));
          return;
        }
        this.offlineQueue.push({ resolve, reject, sql, params });
      });
    }
    return this.withRetry(() => this.http.execute(sql, params));
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params);
    if (result.rows.length === 0) return null;
    const first = result.rows[0];
    const keys = Object.keys(first);
    if (keys.length === 0) return null;
    return first[keys[0]] as T;
  }

  async beginTransaction(isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
    // Transactions go through the underlying HTTP transport directly — no caching or queueing
    return this.withRetry(() => this.http.beginTransaction(isolationLevel));
  }

  async close(): Promise<void> {
    this.cache.clear();
    await this.http.close();
  }

  async ping(): Promise<void> {
    await this.withRetry(() => this.http.ping());
  }

  // -- Mobile-specific API --------------------------------------------------

  /** Clear all cached query results, or only those matching `pattern`. */
  invalidateCache(pattern?: string): void {
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

  private async withRetry<T>(fn: () => Promise<T>): Promise<T> {
    let lastError: Error | undefined;
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        return await fn();
      } catch (err: unknown) {
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
    const queue = this.offlineQueue;
    this.offlineQueue = [];
    for (const item of queue) {
      try {
        const result = await this.http.execute(item.sql, item.params);
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
  private readonly invoke: InvokeFn;

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

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    const result = (await this.invoke('nucleus_query', { sql, params })) as {
      rows?: T[];
      rowCount?: number;
      data?: T[];
    };
    const rows = (result.rows ?? result.data ?? []) as T[];
    return { rows, rowCount: result.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    const result = (await this.invoke('nucleus_execute', { sql, params })) as {
      affected?: number;
      rowsAffected?: number;
    };
    return result.affected ?? result.rowsAffected ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params);
    if (result.rows.length === 0) return null;
    const first = result.rows[0];
    const keys = Object.keys(first);
    if (keys.length === 0) return null;
    return first[keys[0]] as T;
  }

  async beginTransaction(isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
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
    // Embedded — nothing to close from the client side.
  }

  async ping(): Promise<void> {
    await this.invoke('nucleus_query', { sql: 'SELECT 1', params: [] });
  }
}

// ---------------------------------------------------------------------------
// EmbeddedTransactionTransport
// ---------------------------------------------------------------------------

class EmbeddedTransactionTransport implements TransactionTransport {
  private readonly invoke: InvokeFn;
  private readonly txId: string;
  private finished = false;

  constructor(invoke: InvokeFn, txId: string) {
    this.invoke = invoke;
    this.txId = txId;
  }

  private assertOpen(): void {
    if (this.finished) {
      throw new NucleusTransactionError('Transaction has already been committed or rolled back');
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    this.assertOpen();
    const result = (await this.invoke('nucleus_query', { sql, params, txId: this.txId })) as {
      rows?: T[];
      rowCount?: number;
      data?: T[];
    };
    const rows = (result.rows ?? result.data ?? []) as T[];
    return { rows, rowCount: result.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    this.assertOpen();
    const result = (await this.invoke('nucleus_execute', { sql, params, txId: this.txId })) as {
      affected?: number;
      rowsAffected?: number;
    };
    return result.affected ?? result.rowsAffected ?? 0;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    const result = await this.query<Record<string, unknown>>(sql, params);
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

  // Web / Node / everything else — standard HTTP
  return new HttpTransport(config.url, config.headers, config.timeout);
}
