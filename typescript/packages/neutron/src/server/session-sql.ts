import { randomUUID } from 'node:crypto';
import type { SessionData, SessionRecord, SessionStorage } from './session.js';

/** Structural node-postgres pool contract; no pg dependency on the core path. */
export interface SessionSQLConnection {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
  release(error?: Error): void;
}
export interface SessionSQLPool { connect(): Promise<SessionSQLConnection> }
export interface SQLSessionStorage extends SessionStorage {
  /** Periodic maintenance: expired rows cannot authenticate even before purge. */
  purgeExpiredSessions(): Promise<number>;
}
export interface SQLSessionOptions {
  /** Default persisted lifetime when middleware supplies none. Default: 24 hours. */
  ttlSeconds?: number;
}

/** Initialize a shared SQL revision store using conditional single-statement DML.
 * The pool must use a trusted database principal and bounded query/connect
 * timeouts. The table is private authentication state, not client-editable data.
 * Existing KV/legacy sessions require fresh authentication after switching. */
export async function createSQLSessionStorage(pool: SessionSQLPool, options: SQLSessionOptions = {}): Promise<SQLSessionStorage> {
  const ttlSeconds = options.ttlSeconds ?? 86400;
  if (!Number.isFinite(ttlSeconds) || ttlSeconds <= 0) throw new Error('session SQL TTL must be positive');
  const expiry = (explicit?: number) => {
    const value = explicit ?? Date.now() + ttlSeconds * 1000;
    if (!Number.isFinite(value)) throw new Error('session SQL expiry must be finite');
    return value;
  };
  async function connection<T>(fn: (client: SessionSQLConnection) => Promise<T>): Promise<T> {
    const client = await pool.connect();
    let failure: Error | undefined;
    try { return await fn(client); }
    catch (error) { failure = error instanceof Error ? error : new Error('session SQL failed'); throw error; }
    finally { client.release(failure); }
  }
  await connection(client => client.query('CREATE TABLE IF NOT EXISTS neutron_auth_sessions (id TEXT PRIMARY KEY, revision TEXT NOT NULL, data TEXT NOT NULL, expires_at BIGINT)'));
  const revision = () => randomUUID();
  return {
    async purgeExpiredSessions() {
      return connection(async client => (await client.query('DELETE FROM neutron_auth_sessions WHERE expires_at<=$1', [Date.now()])).rowCount ?? 0);
    },
    async getSession(id): Promise<SessionRecord | null> {
      return connection(async client => {
        const result = await client.query('SELECT revision,data,expires_at FROM neutron_auth_sessions WHERE id=$1', [id]);
        const row = result.rows[0]; if (!row) return null;
        const expiresAt = row.expires_at == null ? undefined : Number(row.expires_at);
        if (expiresAt !== undefined && (!Number.isFinite(expiresAt) || expiresAt <= Date.now())) return null;
        if (typeof row.revision !== 'string' || !row.revision || typeof row.data !== 'string') throw new Error('invalid session record');
        return { revision: row.revision, data: JSON.parse(row.data) as SessionData, expiresAt };
      });
    },
    // Administrative unconditional operations; request middleware uses CAS.
    async setSession(id, data, expiresAt) {
      await connection(client => client.query('INSERT INTO neutron_auth_sessions(id,revision,data,expires_at) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET revision=EXCLUDED.revision,data=EXCLUDED.data,expires_at=EXCLUDED.expires_at', [id, revision(), JSON.stringify(data), expiry(expiresAt)]));
    },
    async deleteSession(id) { await connection(client => client.query('DELETE FROM neutron_auth_sessions WHERE id=$1', [id])); },
    async commitSession(id, expected, replacement) {
      // One statement fences each mutation. Never INSERT a stale loaded ID.
      const data = replacement ? JSON.stringify(replacement.data) : undefined;
      return connection(async client => {
        try {
          if (expected === null) {
            if (!replacement || replacement.id !== id) return false;
            await client.query('INSERT INTO neutron_auth_sessions(id,revision,data,expires_at) VALUES($1,$2,$3,$4)', [id, revision(), data, expiry(replacement.expiresAt)]);
            return true;
          }
          if (!replacement) {
            const result = await client.query('DELETE FROM neutron_auth_sessions WHERE id=$1 AND revision=$2 AND (expires_at IS NULL OR expires_at>$3)', [id, expected, Date.now()]);
            return result.rowCount === 1;
          }
          const result = await client.query('UPDATE neutron_auth_sessions SET id=$3,revision=$4,data=$5,expires_at=$6 WHERE id=$1 AND revision=$2 AND (expires_at IS NULL OR expires_at>$7)', [id, expected, replacement.id, revision(), data, expiry(replacement.expiresAt), Date.now()]);
          return result.rowCount === 1;
        } catch (error) {
          const code = (error as { code?: string }).code;
          if (code === '23505' || code === '40001') return false;
          throw error;
        }
      });
    },
  };
}
