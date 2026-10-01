import assert from 'node:assert/strict';
import { it } from 'node:test';
import { PgTransport } from './transport.js';
import { migrate, migrateDown, adoptMigrations, migrationStatus, migrationLockInfo, forceUnlockMigrations } from './migrate.js';
import type { Transport } from './types.js';

const url = process.env.NEUTRON_NAMESPACE_DATABASE_URL ?? '';
const quote = (s: string): string => '"' + s.replaceAll('"', '""') + '"';
async function fixture(run: (t: PgTransport, oracle: PgTransport, a: string, b: string) => Promise<void>): Promise<void> {
 const oracle = new PgTransport(url), t = new PgTransport(url);
 const a = 'v10_namespace_ts_' + Date.now() + '_' + Math.random().toString(16).slice(2,8), b = a + '_other';
 try {
  await oracle.execute(`CREATE SCHEMA ${quote(a)}; CREATE SCHEMA ${quote(b)}`);
  await t.execute(`SET search_path TO ${quote(a)}`);
  await run(t, oracle, a, b);
 } finally {
  await t.close();
  await oracle.execute(`DROP SCHEMA IF EXISTS ${quote(a)} CASCADE; DROP SCHEMA IF EXISTS ${quote(b)} CASCADE`);
  await oracle.close();
 }
}

it('migration namespaces refuse temporary/later/view/empty identity before mutation', { skip: !url }, async () => {
 for (const kind of ['temp-history','temp-claim','later-history','later-claim','view','temp-schema','empty']) await fixture(async(t,o,a,b)=>{
  if(kind==='temp-history')await t.execute('CREATE TEMP TABLE _neutron_migrations(version text)');
  if(kind==='temp-claim')await t.execute('CREATE TEMP TABLE _neutron_migration_lock(id integer)');
  if(kind.startsWith('later')){await o.execute(`CREATE TABLE ${quote(b)}.${kind==='later-history'?'_neutron_migrations':'_neutron_migration_lock'}(id integer)`);await t.execute(`SET search_path TO ${quote(a)},${quote(b)}`);}
  if(kind==='view')await t.execute('CREATE VIEW _neutron_migrations AS SELECT 1 AS version');
  if(kind==='temp-schema')await t.execute('CREATE TEMP TABLE marker(id integer);SET search_path TO pg_temp');
  if(kind==='empty')await t.execute('SET search_path TO missing_namespace');
  const plan=[{version:1,name:'pending',up:`CREATE TABLE ${quote(a)}.effect(id integer)`,down:`DROP TABLE ${quote(a)}.effect`}];
  for(const operation of [()=>migrate(t,plan),()=>migrateDown(t,plan,1),()=>adoptMigrations(t,plan),()=>migrationStatus(t),()=>migrationLockInfo(t),()=>forceUnlockMigrations(t)])await assert.rejects(operation);
  assert.equal(await o.fetchval(`SELECT count(*)::int FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname IN ('effect','_neutron_migration_lock')`,[a]),0);
 });
});

it('migration internal SQL stays qualified after SET LOCAL, including down and claims', { skip: !url }, async()=>fixture(async(t,o,a,b)=>{
 await o.execute(`CREATE TABLE ${quote(b)}._neutron_migrations(version text)`);
 const plan=[{version:1,name:'local',up:`SET LOCAL search_path TO ${quote(b)};CREATE TABLE ${quote(a)}.effect(id integer)`,down:`SET LOCAL search_path TO ${quote(b)};DROP TABLE ${quote(a)}.effect`}];
 assert.deepEqual(await migrate(t,plan),['local']);assert.deepEqual(await migrate(t,plan),[]);
 assert.equal(await o.fetchval(`SELECT count(*)::int FROM ${quote(a)}._neutron_migrations`),1);
 assert.equal((await migrationStatus(t)).length,1);
 await assert.rejects(()=>migrate(t,[{...plan[0],up:plan[0].up+';SELECT 1'}]),/modified/);
 await migrateDown(t,plan,1);
 await o.execute(`INSERT INTO ${quote(a)}._neutron_migration_lock(id,token,owner)VALUES(1,17,'owner')`);
 assert.equal((await migrationLockInfo(t)).owner,'owner');await forceUnlockMigrations(t);
 assert.equal(await o.fetchval(`SELECT count(*)::int FROM ${quote(a)}._neutron_migration_lock`),0);
}));

it('migration invocation freezes a namespace across real transport query settings', { skip: !url }, async()=>fixture(async(t,o,a,b)=>{
 await migrate(t,[{version:1,name:'a',up:'SELECT 1'}]);await o.execute(`CREATE TABLE ${quote(b)}._neutron_migrations(version text)`);
 let calls=0;
 const rotating=new Proxy(t,{get(target,key){if(key==='query')return async(sql:string,params?:unknown[])=>{await target.execute(`SET search_path TO ${quote(++calls%2?a:b)}`);return target.query(sql,params)};const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}}) as Transport;
 const records=await migrationStatus(rotating);assert.equal(records.length,1);assert.equal(records[0].name,'a');assert.ok(calls>=4);
}));

it('quoted metadata namespaces support explicit unverified adoption and canonical refusal', { skip: !url }, async()=>fixture(async(t,o,a)=>{
 const name=a+'_neutron_migrations"select';await o.execute(`CREATE SCHEMA ${quote(name)}`);
 try {
  await t.execute(`SET search_path TO ${quote(name)};CREATE TABLE _neutron_migrations(version integer primary key,name text,applied_at timestamptz default now());INSERT INTO _neutron_migrations(version,name)VALUES(1,'legacy')`);
  assert.deepEqual((await adoptMigrations(t,[])).unverified,[1]);assert.equal((await migrationStatus(t)).length,1);
  await migrate(t,[{version:2,name:'new',up:'SELECT 2'}]);
  await o.execute(`DROP TABLE ${quote(name)}._neutron_migrations;CREATE TABLE ${quote(name)}._neutron_migrations(version text)`);
  await assert.rejects(()=>migrate(t,[{version:3,name:'no',up:'SELECT 3'}]),/text column/);
 } finally {await o.execute(`DROP SCHEMA ${quote(name)} CASCADE`);}
}));

it('migration namespace capture ignores functions shadowing pg_catalog', { skip: !url }, async()=>fixture(async(t,o,a,b)=>{
 await o.execute(`CREATE FUNCTION ${quote(a)}.current_schema() RETURNS name LANGUAGE sql AS $$ SELECT '${b}'::name $$`);
 await t.execute(`SET search_path TO ${quote(a)},pg_catalog`);
 assert.equal(await t.fetchval('SELECT current_schema()::text'),b);
 await migrate(t,[{version:1,name:'original',up:'SELECT 1'}]);assert.equal((await migrationStatus(t))[0].name,'original');
 assert.equal(await o.fetchval(`SELECT count(*)::int FROM ${quote(a)}._neutron_migrations`),1);
 assert.equal(await o.fetchval("SELECT count(*)::int FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname IN ('_neutron_migrations','_neutron_migration_lock')",[b]),0);
}));
