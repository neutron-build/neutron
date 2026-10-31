import assert from 'node:assert/strict';
import { it } from 'node:test';
import { PgTransport } from './transport.js';

const url = process.env.NEUTRON_TEST_DATABASE_URL ?? '';
it('independent cancellation works with all eight application connections busy (TSD-07)', { skip: !url, timeout: 15_000 }, async () => {
  const t = new PgTransport(url, { timeout: 10_000 });
  const oracle = new PgTransport(url);
  const ac = new AbortController();
  // This unique SQL marker lets the independent observer verify actual pool
  // saturation, rather than assuming a timer implies all queries started.
  const marker = `nucleus_cancel_saturation_${Date.now()}`;
  const outcomes = Array.from({ length: 8 }, () => t.query(`SELECT pg_sleep(9) /* ${marker} */`, [], { signal: ac.signal })
    .then(() => ({ failed: false, code: '' }), (err: Error & { code?: string }) => ({ failed: true, code: err.code ?? err.name })));
  try {
    let active = 0;
    for (let i = 0; i < 100 && active < 8; i++) {
      active = Number(await oracle.fetchval("SELECT count(*) FROM pg_stat_activity WHERE query LIKE $1 AND state = 'active' AND pid <> pg_backend_pid()", [`%${marker}%`]));
      if (active < 8) await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(active, 8, 'all application connections must be occupied before abort');
    const began = Date.now();
    ac.abort();
    const results = await Promise.all(outcomes);
    assert.ok(Date.now() - began < 5000, 'cancel must not queue behind the sleeping application pool');
    assert.ok(results.every((result) => result.failed && (result.code === '57014' || result.code === 'AbortError')), JSON.stringify(results));
    assert.equal(await t.fetchval('SELECT 42'), 42, 'transport remains usable after destroying canceled clients');
  } finally {
    ac.abort();
    await Promise.all(outcomes);
    await t.close(); await oracle.close();
  }
});
