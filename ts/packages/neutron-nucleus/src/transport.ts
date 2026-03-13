// ---------------------------------------------------------------------------
// Nucleus client — HTTP transport implementation
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
// HTTP helpers
// ---------------------------------------------------------------------------

interface ApiResponse<T = unknown> {
  ok: boolean;
  data?: T;
  error?: string;
  rowCount?: number;
  affected?: number;
}

async function request<T>(url: string, body: unknown, headers: Record<string, string>): Promise<ApiResponse<T>> {
  let res: Response;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: JSON.stringify(body),
    });
  } catch (err) {
    throw new NucleusConnectionError('Failed to reach Nucleus server', {
      cause: err instanceof Error ? err : undefined,
      meta: { url },
    });
  }

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    mapHttpError(res.status, text, url);
  }

  return (await res.json()) as ApiResponse<T>;
}

function mapHttpError(status: number, body: string, url: string): never {
  const meta = { status, url };
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

  constructor(url: string, headers: Record<string, string> = {}) {
    // Strip trailing slash for consistent URL building
    this.baseUrl = url.replace(/\/+$/, '');
    this.headers = headers;
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    const res = await request<T[]>(`${this.baseUrl}/api/query`, { sql, params }, this.headers);
    const rows = (res.data ?? []) as T[];
    return { rows, rowCount: res.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    const res = await request<void>(`${this.baseUrl}/api/execute`, { sql, params }, this.headers);
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
    );
    const txId = res.data?.txId;
    if (!txId) {
      throw new NucleusTransactionError('Server did not return a transaction ID');
    }
    return new HttpTransactionTransport(this.baseUrl, this.headers, txId);
  }

  async close(): Promise<void> {
    // HTTP is stateless — nothing to close.
  }

  async ping(): Promise<void> {
    await request<void>(`${this.baseUrl}/api/query`, { sql: 'SELECT 1', params: [] }, this.headers);
  }
}

// ---------------------------------------------------------------------------
// HttpTransactionTransport
// ---------------------------------------------------------------------------

class HttpTransactionTransport implements TransactionTransport {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly txId: string;
  private finished = false;

  constructor(baseUrl: string, headers: Record<string, string>, txId: string) {
    this.baseUrl = baseUrl;
    this.headers = { ...headers, 'X-Nucleus-TxId': txId };
    this.txId = txId;
  }

  private assertOpen(): void {
    if (this.finished) {
      throw new NucleusTransactionError('Transaction has already been committed or rolled back');
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    this.assertOpen();
    const res = await request<T[]>(`${this.baseUrl}/api/query`, { sql, params, txId: this.txId }, this.headers);
    const rows = (res.data ?? []) as T[];
    return { rows, rowCount: res.rowCount ?? rows.length };
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    this.assertOpen();
    const res = await request<void>(`${this.baseUrl}/api/execute`, { sql, params, txId: this.txId }, this.headers);
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
    await request<void>(`${this.baseUrl}/api/transaction/commit`, { txId: this.txId }, this.headers);
    this.finished = true;
  }

  async rollback(): Promise<void> {
    this.assertOpen();
    await request<void>(`${this.baseUrl}/api/transaction/rollback`, { txId: this.txId }, this.headers);
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
