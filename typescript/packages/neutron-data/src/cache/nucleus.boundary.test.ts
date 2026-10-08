import assert from 'node:assert/strict';
import test from 'node:test';
import { withKV } from '@neutron-build/nucleus/kv';
import { createNucleusCacheClient, type NucleusKVLike } from './nucleus.js';
import { nextCounter } from './counter.js';

test('bundled KVModel to cache supports native valid counters through one KV_INCR per call', async () => {
  let stored = 4; const sql: string[] = [];
  const kv = withKV.init({ fetchval: async (statement: string) => { sql.push(statement); return ++stored; } } as any,
    { isNucleus: true, hasKV: true } as any).kv;
  const cache = createNucleusCacheClient({ kv });
  assert.deepEqual(cache.counterCapabilities, { plain: 'native', atomicTTL: false });
  assert.equal(await cache.incr('valid'), 5); assert.equal(await cache.incr('valid'), 6);
  assert.deepEqual(sql, ['SELECT KV_INCR($1)', 'SELECT KV_INCR($1)']);
  await assert.rejects(() => cache.incr('valid', 60), /atomic increment-with-expiry/);
  assert.equal(sql.length, 2);
  assert.throws(() => createNucleusCacheClient({ kv, counterMode: 'strict' }), /checked increment/);
});

test('strict checked provider contract refuses malformed/unsafe counters before modeled mutation', async () => {
  let raw: string | null = null; let mutations = 0;
  const provider: NucleusKVLike = {
    get: async () => { throw new Error('racy GET forbidden'); }, set: async () => {}, delete: async () => false,
    incr: async () => { throw new Error('unchecked INCR forbidden'); }, expire: async () => { throw new Error('split EXPIRE forbidden'); },
    incrChecked: async () => { const value = nextCounter(raw ?? '0'); raw = String(value); mutations++; return value; },
  };
  const cache = createNucleusCacheClient({ kv: provider, counterMode: 'strict' });
  assert.equal(cache.counterCapabilities.plain, 'checked');
  for (const value of ['malformed', '01', '1.5', '9007199254740991', '-9007199254740992']) {
    raw = value; await assert.rejects(() => cache.incr('bad'), /counter value/);
    assert.equal(raw, value); assert.equal(mutations, 0);
  }
  raw = '4'; assert.equal(await cache.incr('valid'), 5); assert.equal(mutations, 1);
});

test('native unsafe acknowledgement reports reconciliation without a GET or retry', async () => {
  let sends = 0;
  const provider = { incr: async () => { sends++; return 9007199254740992; } } as unknown as NucleusKVLike;
  const cache = createNucleusCacheClient({ kv: provider });
  await assert.rejects(() => cache.incr('unsafe'), /may have completed.*reconcile/);
  assert.equal(sends, 1);
});
