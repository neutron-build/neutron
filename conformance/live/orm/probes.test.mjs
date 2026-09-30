import assert from 'node:assert/strict';
import test from 'node:test';
import { PROBES } from './probes.mjs';

const probe = PROBES.find(p => p.id === 'txn.serializable_write_skew');
function context(begin) {
  let index = 0;
  return {
    name: () => 'probe_classification_fixture',
    session: async () => {
      const current = index++;
      return { execute: async () => {}, begin: fn => begin(current, fn) };
    },
  };
}

test('SERIALIZABLE refusal retains its server SQLSTATE for unsupported classification', async () => {
  const refusal = Object.assign(new Error('SERIALIZABLE unavailable on this storage engine'), { sqlstate: '0A000' });
  await assert.rejects(probe.run(context(async () => { throw refusal; })), error => error === refusal);
});

test('other transaction failures are not counted as 40001 serialization failures', async () => {
  const failure = Object.assign(new Error('read failure'), { sqlstate: 'XX000' });
  await assert.rejects(probe.run(context(async () => { throw failure; })), error => error === failure);
});

test('one committed writer and one 40001 still satisfy the original oracle', async () => {
  const serialization = Object.assign(new Error('serialization failure'), { sqlstate: '40001' });
  await probe.run(context(async (index, fn) => fn({
    query: async () => [{ sum: 0 }],
    execute: async () => { if (index === 1) throw serialization; },
  })));
});

test('two 40001 failures still fail the exactly-one oracle', async () => {
  const serialization = Object.assign(new Error('serialization failure'), { sqlstate: '40001' });
  await assert.rejects(probe.run(context(async () => { throw serialization; })), /expected exactly one serialization failure, got 2/);
});
