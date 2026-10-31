import assert from 'node:assert/strict';
import {it} from 'node:test';
import {PgTransport} from './transport.js';
import {createClient} from './client.js';
import {withSQL} from './sql/index.js';
import {migrate,migrateDown,adoptMigrations,migrationStatus,migrationLockInfo,forceUnlockMigrations} from './migrate.js';

const url=process.env.NEUTRON_TEST_DATABASE_URL??'';
const scalarSQL=`SELECT 9223372036854775807::int8 AS maximum,
 '-9223372036854775808'::int8 AS minimum,5::int8 AS small,
 1234567890123456789012.123456789012345679::numeric(40,18) AS amount,
 '-32768'::int2 AS short,2147483647::int4 AS integer,true AS flag,
 'value'::text AS text,'abc'::varchar AS varying,'xy'::char(2) AS fixed,
 '00000000-0000-4000-8000-000000000001'::uuid AS id,
 decode('00ff01','hex') AS bytes,NULL::int8 AS nullable`;
const expected={maximum:'9223372036854775807',minimum:'-9223372036854775808',small:'5',
 amount:'1234567890123456789012.123456789012345679',short:-32768,integer:2147483647,flag:true,
 text:'value',varying:'abc',fixed:'xy',id:'00000000-0000-4000-8000-000000000001',bytes:Buffer.from([0,255,1]),nullable:null};

it('default int8 policy stays safe-number and refuses overflow', {skip:!url},async()=>{
 const t=new PgTransport(url);try{
  assert.equal(await t.fetchval('SELECT 5::int8'),5);
  for(const value of ['9223372036854775807','-9223372036854775808','9007199254740993'])await assert.rejects(t.fetchval(`SELECT '${value}'::int8`),{code:'INT8_PRECISION'});
  assert.equal(await t.fetchval('SELECT 1::int4'),1);
 }finally{await t.close();}
});

it('lossless SQL-only profile preserves selected scalars and transaction reads', {skip:!url},async()=>{
 const t=new PgTransport(url,{valueProfile:'lossless-read-v1'});
 const db=await createClient({url,transport:t}).use(withSQL).connect();try{
  assert.deepEqual(await db.sql.queryOne(scalarSQL),expected);
  await db.sql.transaction(async tx=>assert.deepEqual(await tx.queryOne(scalarSQL),expected));
  assert.deepEqual(await db.sql.queryOne("SELECT false AS flag,decode('','hex') AS bytes,''::text AS text"),{flag:false,bytes:Buffer.alloc(0),text:''});
 }finally{await db.close();}
});

it('pool-local parsers leave raw pg and a second default transport unchanged', {skip:!url},async()=>{
 const specifier='pg';const pg=(await import(specifier)).default;
 const parser=pg.types.getTypeParser(20),raw=new pg.Pool({connectionString:url,max:1});
 const normal=new PgTransport(url),lossless=new PgTransport(url,{valueProfile:'lossless-read-v1'});
 try{
  assert.equal(await normal.fetchval('SELECT 7::int8'),7);
  assert.equal(await lossless.fetchval('SELECT 7::int8'),'7');
  assert.equal(pg.types.getTypeParser(20),parser);
  const value=(await raw.query('SELECT 9223372036854775807::int8 AS value')).rows[0].value;
  assert.equal(value,parser('9223372036854775807'));assert.equal(typeof value,'string');
  assert.equal(await normal.fetchval('SELECT 7::int8'),7);
 }finally{await Promise.all([normal.close(),lossless.close(),raw.end()]);}
});

it('unsupported non-null temporal/array/JSON values refuse and pool remains usable', {skip:!url},async()=>{
 const t=new PgTransport(url,{valueProfile:'lossless-read-v1'});try{
  for(const sql of ["SELECT TIMESTAMPTZ '2026-09-30T12:34:56.123456Z'","SELECT ARRAY[1,2]::int4[]","SELECT '{}'::jsonb"]){
   await assert.rejects(t.query(sql),{code:'PG_VALUE_TYPE_UNSUPPORTED'});
   assert.equal(await t.fetchval('SELECT 1::int4'),1);
  }
  const tx=await t.beginTransaction();try{
   await tx.execute("SET LOCAL bytea_output = 'escape'");
   await assert.rejects(tx.query("SELECT decode('00ff01','hex')"),{code:'PG_VALUE_FORMAT'});
  }finally{await tx.rollback();}
  assert.deepEqual(await t.fetchval("SELECT decode('00ff01','hex')"),Buffer.from([0,255,1]));
 }finally{await t.close();}
});

it('unknown profile and non-SQL composition refuse before connecting',async()=>{
 assert.throws(()=>new PgTransport('postgres://localhost:1/no',{valueProfile:'unknown' as 'lossless-read-v1'}),{code:'PG_VALUE_PROFILE_UNSUPPORTED'});
 const t=new PgTransport('postgres://localhost:1/no',{valueProfile:'lossless-read-v1'});
 await assert.rejects(createClient({url:'postgres://localhost:1/no',transport:t}).use({name:'count-model',init:()=>({count:()=>0})}).connect(),/SQL-only read connection/);
 await t.close();
});

it('SDK migration APIs refuse an explicit lossless SQL read transport', {skip:!url},async()=>{
 const t=new PgTransport(url,{valueProfile:'lossless-read-v1'}),oracle=new PgTransport(url);try{
  const query="SELECT count(*)::int FROM pg_catalog.pg_class WHERE relname IN ('_neutron_migrations','_neutron_migration_lock')";
  const before=await oracle.fetchval(query),plan=[{version:1,name:'refused',up:'SELECT 1'}];
  for(const operation of [()=>migrate(t,plan),()=>migrateDown(t,plan,1),()=>adoptMigrations(t,plan),()=>migrationStatus(t),()=>migrationLockInfo(t),()=>forceUnlockMigrations(t)])await assert.rejects(operation,/SQL read profile/);
  assert.equal(await oracle.fetchval(query),before);
 }finally{await Promise.all([t.close(),oracle.close()]);}
});
