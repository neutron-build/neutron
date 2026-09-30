import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { createSQLSessionStorage, type SessionSQLPool } from './session-sql.js';

const url = process.env.NEUTRON_AUTH_TEST_DATABASE_URL;
if (!url && process.env.NEUTRON_LIVE_REQUIRED === '1') {
  throw new Error('NEUTRON_AUTH_TEST_DATABASE_URL required for live session SQL tests');
}
// pg is an optional application driver, not a dependency of the core path.
describe.skipIf(!url)('persistent session SQL revision fencing', () => {
  it('preserves collision state and refuses stale/ABA writes across connections', async () => {
    const { Pool } = createRequire(import.meta.url)('pg');
    const pool = new Pool({ connectionString: url, max: 4, connectionTimeoutMillis: 5000, query_timeout: 5000 }) as SessionSQLPool & { end(): Promise<void> };
    const store = await createSQLSessionStorage(pool);
    const prefix = `finish_auth_ts_${randomUUID()}`;
    const ids = [prefix + '_old', prefix + '_new', prefix + '_collision'];
    try {
      await store.setSession(ids[0], { user: 'alice' });
      const old = (await store.getSession(ids[0]))!;
      expect(old.revision).toBeTruthy();
      expect(old.expiresAt).toBeGreaterThan(Date.now());
      await store.setSession(ids[2], { user: 'bob' });
      expect(await store.commitSession!(ids[0], old.revision!, { id: ids[2], data: { user: 'eve' } })).toBe(false);
      expect((await store.getSession(ids[0]))?.revision).toBe(old.revision);
      expect((await store.getSession(ids[2]))?.data.user).toBe('bob');
      expect(await store.commitSession!(ids[0], old.revision!, { id: ids[1], data: old.data })).toBe(true);
      expect(await store.getSession(ids[0])).toBeNull();
      expect(await store.commitSession!(ids[0], old.revision!, { id: ids[0], data: old.data })).toBe(false);
      for (let attempt = 0; attempt < 20; attempt++) {
        const current = (await store.getSession(ids[1]))!;
        const receipts = await Promise.all([0, 1].map(winner => store.commitSession!(ids[1], current.revision!, { id: ids[1], data: { winner } })));
        expect(receipts.sort()).toEqual([false, true]);
      }
      const current = (await store.getSession(ids[1]))!;
      expect(await store.commitSession!(ids[1], current.revision!, null)).toBe(true);
      expect(await store.commitSession!(ids[1], current.revision!, { id: ids[1], data: current.data })).toBe(false);
      await store.setSession(ids[1], { user: 'new-owner' });
      expect(await store.commitSession!(ids[1], current.revision!, null)).toBe(false);
      expect((await store.getSession(ids[1]))?.data.user).toBe('new-owner');
      expect(await store.commitSession!(ids[0], null, { id: ids[0], data: { fresh: true } })).toBe(true);
      expect(await store.commitSession!(ids[0], null, { id: ids[2], data: { wrong: true } })).toBe(false);
    } finally {
      for (const id of ids) await store.deleteSession(id);
      await pool.end();
    }
  }, 15000);
});
