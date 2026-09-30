import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import * as data from '@neutron-build/data';
const require=createRequire(import.meta.url);
const peers=['postgres','drizzle-orm','@neutron-build/nucleus','@libsql/client','ioredis','bullmq','@aws-sdk/client-s3'];
for(const peer of peers) assert.throws(()=>require.resolve(peer),{code:'MODULE_NOT_FOUND'});
await import('@neutron-build/data/drizzle'); // Runtime import is lazy, unlike its typed declaration.
const cache=new data.MemoryCacheClient();
await cache.set('k','value');assert.equal(await cache.get('k'),'value');
await assert.rejects(()=>data.createDrizzleDatabase({profile:{provider:'postgres',connectionString:'postgres://unused'}}),/Missing optional dependency "postgres"/);
await assert.rejects(()=>data.createDrizzleDatabase({profile:{provider:'nucleus',connectionString:'postgres://unused'}}),/Missing optional dependency "postgres"/);
console.log(JSON.stringify({passed:10,failed:0,scope:'Root and lazy runtime imports with all optional peers absent; memory cache and missing-driver refusals, no database parity claim.'}));
