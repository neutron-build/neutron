import assert from 'node:assert/strict';
import test from 'node:test';
import { HttpTransport, MobileTransport, PgTransport, PgTransactionTransport } from './transport.js';
import { admitConnection } from '@neutron-build/sql/lifecycle';

for (const kind of ['http', 'mobile']) {
  test(`${kind} BEGIN validates complete envelopes/identities and never replays ambiguity`, async () => {
    const original = globalThis.fetch;
    try {
      for (const payload of [null, [], 0, 'bad', { ok: false, data: [] }, { ok: false, data: { txId: 'created-despite-rejection' } }, { ok: 'true', data: { txId: 'tx' } }, { ok: true, data: null },
        { ok: true, data: [] }, { ok: true, data: { txId: 'txĀ' } }, { data: { txId: ' tx ' } },
        { data: { txId: '\ttx' } }, { data: { txId: 'tx\u0000' } }, { data: { txId: '' } }, { data: { txId: '\u0085' } }, { data: { txId: '\u00a0' } }]) {
        let sends = 0;
        globalThis.fetch = (async () => { sends++; return new Response(JSON.stringify(payload)); }) as typeof fetch;
        const transport = kind === 'http' ? new HttpTransport('https://user:secret@local') : new MobileTransport({ url: 'https://user:secret@local', retryDelay: 1 });
        await assert.rejects(() => transport.beginTransaction(), (error: any) => {
          assert.equal(error.code, 'UNKNOWN_OUTCOME');
          assert.equal(error.meta.operation, 'BEGIN');
          assert.ok(error.cause instanceof Error);
          assert.ok(!error.meta.url.includes('secret'));
          return true;
        });
        assert.equal(sends, 1);
        await transport.close();
      }
      for (const status of [408, 500, 502, 503]) {
        let sends = 0;
        globalThis.fetch = (async () => { sends++; return new Response('gateway failed', { status }); }) as typeof fetch;
        const transport = kind === 'http' ? new HttpTransport('https://local') : new MobileTransport({ url: 'https://local', retryDelay: 1 });
        await assert.rejects(() => transport.beginTransaction(), (error: any) => {
          assert.equal(error.code, 'UNKNOWN_OUTCOME'); assert.equal(error.cause.meta.status, status); return true;
        });
        assert.equal(sends, 1);
        await transport.close();
      }
      for (const payload of [{ data: { txId: 'legacy-id' } }, { ok: true, data: { txId: 'txé' } }]) {
        globalThis.fetch = (async () => new Response(JSON.stringify(payload))) as typeof fetch;
        const transport = kind === 'http' ? new HttpTransport('https://local') : new MobileTransport({ url: 'https://local' });
        const tx = await transport.beginTransaction();
        globalThis.fetch = (async (_url, init) => {
          assert.equal(new Headers(init?.headers).get('X-Nucleus-TxId'), payload.data.txId);
          assert.equal(JSON.parse(String(init?.body)).txId, payload.data.txId);
          return new Response(JSON.stringify({ ok: true }));
        }) as typeof fetch;
        await tx.commit(); await transport.close();
      }
    } finally { globalThis.fetch = original; }
  });
}

test('PgTransport drains delayed cleanup after another pool failure, aggregates and stays terminal', async () => {
  let finish!: () => void;
  const barrier = new Promise<void>(resolve => { finish = resolve; });
  const first = new Error('main close'); const second = new Error('cancel close');
  let calls = 0; let settled = false;
  const transport = new PgTransport('postgres://unused');
  (transport as any).poolPromise = Promise.resolve({ end: () => { calls++; throw first; } });
  (transport as any).cancellationPoolPromise = Promise.resolve({ end: async () => { calls++; await barrier; throw second; } });
  const close = transport.close();
  const observed = close.catch(error => { settled = true; return error; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(calls, 2); assert.equal(settled, false);
  finish(); const error = await observed;
  assert.deepEqual(error.errors, [first, second]);
  await assert.rejects(() => transport.close(), (again: unknown) => again === error);
  await assert.rejects(() => transport.ping(), /closed/);
  assert.equal(calls, 2);
});

test('transport capability admission distinguishes cancellation, unknown custom metadata and closed refusal', async () => {
  const http = new HttpTransport('https://unused');
  assert.equal(admitConnection(http, { cancellation: 'response-only' }), http);
  assert.throws(() => admitConnection(http, { cancellation: 'server-attempt' }), /not admitted/);
  assert.throws(() => admitConnection({}, { cancellation: 'pre-dispatch' }), /not admitted/);
  const tx = new PgTransactionTransport({ query: async () => ({ rows: [], rowCount: 0 }), release: () => {} } as any);
  assert.equal(admitConnection(tx, { cancellation: 'pre-dispatch' }), tx);
  await http.close(); await assert.rejects(() => http.query('SELECT 1'), { code: 'CLOSED' });
});
