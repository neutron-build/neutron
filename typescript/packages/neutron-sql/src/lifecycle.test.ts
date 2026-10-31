import assert from 'node:assert/strict';
import test from 'node:test';
import { cleanupAfterFailure, resourceLifecycle, admitConnection } from './lifecycle.js';
import { createDatabase } from './db.js';
import { wrapPgPool, wrapPostgresJs } from './drivers.js';

test('startup cleanup drains delayed owners and retains primary plus every failure', async () => {
  let release!: () => void; const pending = new Promise<void>(resolve => { release = resolve; });
  const primary = new Error('startup'); const a = new Error('a'); const b = new Error('b');
  let settled = false;
  const observed = cleanupAfterFailure(primary, [() => { throw a; }, async () => { await pending; throw b; }]).catch(error => { settled = true; return error; });
  await new Promise(resolve => setImmediate(resolve)); assert.equal(settled, false);
  release(); const error = await observed;
  assert.equal(error.cause, primary); assert.deepEqual(error.errors, [primary, a, b]);
});

test('owned failure is terminal and repeat close observes it; borrowed lifecycle retains owner control', async () => {
  let closes = 0;
  const error = new Error('end'); const lifecycle = resourceLifecycle('owned', () => { closes++; throw error; });
  const first = lifecycle.terminate(); assert.equal(lifecycle.terminate(), first);
  assert.throws(() => lifecycle.assertOpen(), /closed/);
  await assert.rejects(() => first, (failure: unknown) => failure === error);
  assert.equal(lifecycle.terminated, true);
  await assert.rejects(() => lifecycle.terminate()); assert.equal(closes, 1);
  const borrowed = resourceLifecycle('borrowed', () => { closes++; });
  await borrowed.terminate(); borrowed.assertOpen(); assert.equal(closes, 1);
});

test('first-party PG wrapper aggregates native owner failures, refuses post-close dispatch and admits capabilities', async () => {
  let sends = 0;
  const a = new Error('app pool'); const b = new Error('cancel pool');
  const pool = { query: async () => { sends++; return { rows: [], rowCount: 0 }; }, connect: async () => { throw new Error('unused'); }, end: async () => { throw a; } };
  const cancellationPool = { ...pool, end: async () => { throw b; } };
  const driver = wrapPgPool(pool as any, { ownership: 'owned', cancellationPool: cancellationPool as any });
  admitConnection(driver, { cancellation: 'server-attempt' });
  const prepared = driver.prepare!('SELECT 1');
  await assert.rejects(() => driver.close(), (error: any) => { assert.deepEqual(error.errors, [a, b]); return true; });
  await assert.rejects(() => driver.query('SELECT 1'), /closed/); assert.equal(sends, 0);
  await assert.rejects(() => prepared.query(), /closed/); assert.equal(sends, 0);
  const custom = wrapPostgresJs({ unsafe: async () => [], end: async () => {}, reserve: async () => { throw new Error("unused"); } } as any);
  assert.throws(() => admitConnection(custom, { cancellation: 'server-attempt' }), /not admitted/);
});


test('SQL factory capability refusal preserves primary and cleanup without issuing user SQL', async () => {
  const cleanup = new Error('driver disposal'); let closes = 0; let sends = 0;
  const driver = { close: async () => { closes++; throw cleanup; }, query: async () => { sends++; return []; } } as any;
  await assert.rejects(() => createDatabase({ driver, requiredCapabilities: { cancellation: 'server-attempt' } }), (error: any) => {
    assert.match(error.errors[0].message, /not admitted/);
    assert.equal(error.cause, error.errors[0]); assert.equal(error.errors[1], cleanup); return true;
  });
  assert.equal(closes, 1); assert.equal(sends, 0);
});
