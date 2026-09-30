import assert from 'node:assert/strict';
import postgres from 'postgres';
import {pgTable,integer,text} from 'drizzle-orm/pg-core';
import {eq} from 'drizzle-orm';
import {createDrizzleDatabase} from '@neutron-build/data/drizzle';
const native=process.env.INSTALLED_NATIVE==='1';
const url=native?process.env.NEUTRON_NUCLEUS_TEST_URL:process.env.NEUTRON_TEST_PG_URL;
assert.ok(url);
const name='installed_data_'+process.pid+'_'+Date.now();
const table=pgTable(name,{id:integer('id').primaryKey(),value:text('value').notNull()});
const database=await createDrizzleDatabase({profile:{provider:native?'nucleus':'postgres',connectionString:url},schema:{table}});
const oracle=postgres(url,{max:1,prepare:false});
const results=[];
async function check(name,fn){try{await fn();results.push({name,passed:true});}catch(error){results.push({name,passed:false,error:String(error),code:error.code});}}
try {
 await check('provider/native optional client',async()=>{assert.equal(database.profile.provider,native?'nucleus':'postgres');if(native)assert.equal(database.nucleus.features.isNucleus,true);else assert.equal(database.nucleus,null);});
 await oracle.unsafe(`CREATE TABLE ${name} (id INT PRIMARY KEY, value TEXT NOT NULL)`);
 await check('Drizzle insert/select versus raw driver',async()=>{await database.db.insert(table).values({id:1,value:'installed 世界'});assert.deepEqual(await database.db.select().from(table),[{id:1,value:'installed 世界'}]);assert.deepEqual([...await oracle.unsafe(`SELECT id,value FROM ${name}`)],[{id:1,value:'installed 世界'}]);});
 await check('Drizzle transaction commit',async()=>{await database.db.transaction(async tx=>{await tx.update(table).set({value:'committed'}).where(eq(table.id,1));});assert.equal((await oracle.unsafe(`SELECT value FROM ${name} WHERE id=1`))[0].value,'committed');});
 await check('Drizzle rollback versus raw driver',async()=>{await assert.rejects(()=>database.db.transaction(async tx=>{await tx.insert(table).values({id:2,value:'rolled back'});throw Error('installed rollback');}),/installed rollback/);assert.equal((await oracle.unsafe(`SELECT id FROM ${name} WHERE id=2`)).length,0);});
 if(native){
  const {createClient,PgTransport,withKV,withBlob,NucleusFeatureError}=await import('@neutron-build/nucleus');
  const {createNucleusCacheClient,createNucleusStorageDriver}=await import('@neutron-build/data');
  const client=await createClient({url,transport:new PgTransport(url,{max:1})}).use(withKV).use(withBlob).connect();
  try {
   await check('native cache adapter roundtrip',async()=>{const cache=createNucleusCacheClient({kv:client.kv,prefix:name+':'});await cache.set('k','packed cache');assert.equal(await cache.get('k'),'packed cache');await cache.del('k');assert.equal(await cache.get('k'),null);});
   await check('native storage adapter bytes/content type',async()=>{const storage=createNucleusStorageDriver({blob:client.blob,bucket:name});const body=Uint8Array.from([0,127,128,255]);await storage.put({key:'bytes',body,contentType:'application/octet-stream'});const got=await storage.get('bytes');assert.deepEqual([...got.body],[...body]);assert.equal(got.contentType,'application/octet-stream');await storage.del('bytes');assert.equal(await storage.get('bytes'),null);});
  } finally {await client.close();}
  const control=await createClient({url:process.env.NEUTRON_TEST_PG_URL,transport:new PgTransport(process.env.NEUTRON_TEST_PG_URL,{max:1})}).use(withKV).use(withBlob).connect();
  try {
   await check('native data cache refuses PostgreSQL capability mismatch',async()=>{const cache=createNucleusCacheClient({kv:control.kv,prefix:name+':'});await assert.rejects(()=>cache.set('k','refused'),NucleusFeatureError);});
   await check('native data storage refuses PostgreSQL capability mismatch',async()=>{const storage=createNucleusStorageDriver({blob:control.blob,bucket:name});await assert.rejects(()=>storage.put({key:'refused',body:Uint8Array.from([1])}),NucleusFeatureError);});
  }finally{await control.close();}
  await check('installed native client connection failure surfaces',async()=>{await assert.rejects(()=>createDrizzleDatabase({profile:{provider:'nucleus',connectionString:'postgres://postgres@127.0.0.1:1/postgres?connect_timeout=1'}}));});
 } else {
  await check('Nucleus provider without optional SDK remains Drizzle-only',async()=>{const optional=await createDrizzleDatabase({profile:{provider:'nucleus',connectionString:url}});try{assert.equal(optional.nucleus,null);assert.equal((await optional.client`SELECT 1 AS value`)[0].value,1);}finally{await optional.close();}});
 }
} finally {try{await oracle.unsafe(`DROP TABLE IF EXISTS ${name}`);}finally{await database.close();await oracle.end();}}
console.log(JSON.stringify({native,results,passed:results.filter(x=>x.passed).length,failed:results.filter(x=>!x.passed).length,scope:'Installed Drizzle SQL commit/rollback with raw-driver observations; bounded native KV/blob adapters when SDK is present. No native vector/FTS or universal PostgreSQL parity claim.'}));
if(results.some(x=>!x.passed))process.exitCode=1;
