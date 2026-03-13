// ---------------------------------------------------------------------------
// @neutron/nucleus/sql — SQL model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures, IsolationLevel } from '../types.js';
import { NucleusNotFoundError } from '../errors.js';

// ---------------------------------------------------------------------------
// SQLModel interface
// ---------------------------------------------------------------------------

export interface SQLModel {
  /** Query rows into typed results. */
  query<T = Record<string, unknown>>(sql: string, ...params: unknown[]): Promise<T[]>;

  /** Query exactly one row. Throws `NucleusNotFoundError` if none. */
  queryOne<T = Record<string, unknown>>(sql: string, ...params: unknown[]): Promise<T>;

  /** Query one row, or `null` if none. */
  queryOneOrNull<T = Record<string, unknown>>(sql: string, ...params: unknown[]): Promise<T | null>;

  /** Execute a DML statement and return the affected row count. */
  execute(sql: string, ...params: unknown[]): Promise<number>;

  /** Execute many statements in batch and return each affected-row count. */
  executeBatch(statements: Array<{ sql: string; params?: unknown[] }>): Promise<number[]>;

  /** Fetch a single scalar value from the first column of the first row. */
  fetchval<T = unknown>(sql: string, ...params: unknown[]): Promise<T | null>;

  /**
   * Run `fn` inside a transaction. Commits on success, rolls back on error.
   * The callback receives a scoped `SQLModel` whose operations all execute
   * within the same transaction.
   */
  transaction<T>(fn: (tx: SQLModel) => Promise<T>, opts?: { isolationLevel?: IsolationLevel }): Promise<T>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class SQLModelImpl implements SQLModel {
  constructor(private readonly transport: Transport) {}

  async query<T = Record<string, unknown>>(sql: string, ...params: unknown[]): Promise<T[]> {
    const result = await this.transport.query<T>(sql, params);
    return result.rows;
  }

  async queryOne<T = Record<string, unknown>>(sql: string, ...params: unknown[]): Promise<T> {
    const rows = await this.query<T>(sql, ...params);
    if (rows.length === 0) {
      throw new NucleusNotFoundError('Expected one row but found none');
    }
    return rows[0];
  }

  async queryOneOrNull<T = Record<string, unknown>>(sql: string, ...params: unknown[]): Promise<T | null> {
    const rows = await this.query<T>(sql, ...params);
    return rows.length > 0 ? rows[0] : null;
  }

  async execute(sql: string, ...params: unknown[]): Promise<number> {
    return this.transport.execute(sql, params);
  }

  async executeBatch(statements: Array<{ sql: string; params?: unknown[] }>): Promise<number[]> {
    const results: number[] = [];
    for (const stmt of statements) {
      results.push(await this.transport.execute(stmt.sql, stmt.params));
    }
    return results;
  }

  async fetchval<T = unknown>(sql: string, ...params: unknown[]): Promise<T | null> {
    return this.transport.fetchval<T>(sql, params);
  }

  async transaction<T>(
    fn: (tx: SQLModel) => Promise<T>,
    opts?: { isolationLevel?: IsolationLevel },
  ): Promise<T> {
    const txTransport = await this.transport.beginTransaction(opts?.isolationLevel);
    const txSql = new SQLModelImpl(txTransport);
    try {
      const result = await fn(txSql);
      await txTransport.commit();
      return result;
    } catch (err) {
      await txTransport.rollback();
      throw err;
    }
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.sql` to the client. */
export const withSQL: NucleusPlugin<{ sql: SQLModel }> = {
  name: 'sql',
  init(transport: Transport, _features: NucleusFeatures) {
    return { sql: new SQLModelImpl(transport) };
  },
};
