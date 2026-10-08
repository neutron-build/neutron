import assert from 'node:assert/strict';
import test from 'node:test';
import { createClient } from '../client.js';
import { withTimeSeries, withTimeSeriesProfile } from './index.js';
import type { Transport, NucleusFeatures } from '../types.js';
const features = { isNucleus: true, hasTimeSeries: true, version: 'Nucleus r5' } as NucleusFeatures;
function endpoint(options: { endpoint?: string; writes?: boolean; version?: string; features?: string; bucket?: 'echo' | 'constant' } = {}) {
  let writes = 0; let disposed = false;
  const points: { t: number; v: number }[] = [];
  const transport: Transport = {
    capabilityEndpoint: () => options.endpoint ?? 'same-endpoint-and-auth',
    execute: async (_sql, params = []) => {
      assert.equal(disposed, false); writes++;
      if (!options.writes) throw new Error('production rejects ALL writes');
      points.push({ t: params[1] as number, v: params[2] as number }); return 1;
    },
    fetchval: async <T>(sql: string, params: unknown[] = []): Promise<T | null> => {
      assert.equal(disposed, false);
      let value: unknown;
      if (sql === 'SELECT VERSION()') value = options.version ?? 'Nucleus r5';
      else if (sql === 'SELECT NUCLEUS_FEATURES()') value = options.features ?? '{"timeseries":true,"kv":true}';
      else if (sql.includes('TIME_BUCKET')) value = options.bucket === 'echo' ? params[1] : options.bucket === 'constant' ? 0 : Math.floor(Number(params[1]) / Number(params[0])) * Number(params[0]);
      else if (sql.includes('TS_RANGE_COUNT')) value = points.filter(p => p.t >= Number(params[1]) && p.t < Number(params[2])).length;
      else if (sql.includes('TS_RANGE_AVG')) value = 4;
      else if (sql.includes('TS_COUNT')) value = points.length;
      else if (sql.includes('TS_RANGE')) value = JSON.stringify(options.writes ? points.filter(p => p.t >= Number(params[1]) && p.t < Number(params[2])) : [{ t: 3000, v: 2 }, { t: 6000, v: 6 }]);
      else throw new Error(sql);
      return value as T;
    },
    query: async () => ({ rows: [], rowCount: 0 }),
    ping: async () => {}, close: async () => { disposed = true; },
    beginTransaction: async () => { throw new Error('unsupported'); },
  };
  return { transport, writes: () => writes, disposed: () => disposed, disposeNamespace: async () => { await transport.close(); points.length = 0; }, points: () => points.length };
}

test('pure TIME_BUCKET admission accepts a production transport rejecting all writes; echo/constant refuse', async () => {
  for (const bucket of [undefined, 'echo', 'constant'] as const) {
    const production = endpoint({ bucket });
    const { timeseries } = withTimeSeries.init(production.transport, features);
    await assert.rejects(() => timeseries.timeBucket('second', new Date(12345)), /unverified/);
    const evidence = await timeseries.admitPureCapabilities();
    assert.equal(evidence.bucketFn, bucket === undefined);
    if (!bucket) assert.equal(await timeseries.timeBucket('second', new Date(12345)), 12000);
    else await assert.rejects(() => timeseries.timeBucket('second', new Date(12345)), /disabled/);
    assert.equal(production.writes(), 0);
  }
});

test('qualified disposed diagnostic namespace admits independent read-only production; identity/stale/unverified controls refuse', async (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: 1000 });
  const diagnostic = endpoint({ writes: true });
  const { timeseries } = withTimeSeries.init(diagnostic.transport, features);
  await assert.rejects(() => timeseries.probeCapabilities({} as any), /consent/);
  const evidence = await timeseries.probeCapabilities({ allowPersistentProbeWrites: true });
  assert.equal(evidence.rangeFetch, true); assert.equal(diagnostic.writes(), 4);
  await assert.rejects(() => timeseries.admissionProfile(), /namespace disposal/);
  const profile = await timeseries.admissionProfile({ validForMs: 1000, disposeDiagnosticNamespace: diagnostic.disposeNamespace });
  assert.equal(diagnostic.disposed(), true); assert.equal(diagnostic.points(), 0);
  assert.ok(Object.isFrozen(profile)); assert.ok(Object.isFrozen(profile.identity)); assert.ok(Object.isFrozen(profile.evidence.checks[0]));
  assert.throws(() => withTimeSeriesProfile({ ...profile }), /Unverified/);
  const production = endpoint();
  const client = await createClient({ url: 'https://unused', transport: production.transport }).use(withTimeSeriesProfile(profile)).connect();
  const admitted = client.timeseries;
  const points = await admitted.query('production-series', new Date(2000), new Date(7000));
  assert.deepEqual(points.map(p => p.value), [2, 6]);
  assert.equal(await admitted.timeBucket('second', new Date(12345)), 12000);
  assert.equal(production.writes(), 0);
  for (const wrong of [{ endpoint: 'different-engine' }, { version: 'Nucleus newer' }, { features: '{"timeseries":true,"kv":false}' }]) {
    const control = endpoint(wrong);
    const model = withTimeSeriesProfile(profile).init(control.transport, features).timeseries;
    await assert.rejects(() => model.query('series', new Date(2000), new Date(7000)), /identity/);
    assert.equal(control.writes(), 0);
  }
  t.mock.timers.setTime(2000);
  await assert.rejects(() => admitted.query('series', new Date(2000), new Date(7000)), /stale/);
  const unverified = withTimeSeries.init(production.transport, features).timeseries;
  await assert.rejects(() => unverified.query('series', new Date(2000), new Date(7000)), /unverified/);
  assert.equal(production.writes(), 0);
});


test('failed diagnostic namespace disposal never publishes a profile or retries uncertain disposal', async () => {
  const diagnostic = endpoint({ writes: true });
  const model = withTimeSeries.init(diagnostic.transport, features).timeseries;
  await model.probeCapabilities({ allowPersistentProbeWrites: true });
  let disposals = 0; const cause = new Error('namespace disposal uncertain');
  await assert.rejects(() => model.admissionProfile({ disposeDiagnosticNamespace: async () => { disposals++; throw cause; } }), error => error === cause);
  await assert.rejects(() => model.admissionProfile({ disposeDiagnosticNamespace: diagnostic.disposeNamespace }), error => error === cause);
  assert.equal(disposals, 1);
});
