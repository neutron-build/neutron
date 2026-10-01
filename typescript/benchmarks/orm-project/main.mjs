import assert from 'node:assert/strict';
import {mkdir,writeFile,appendFile,readFile} from 'node:fs/promises';
import {createHash,randomUUID} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import pg from 'pg';
import {open} from './providers.mjs';
const out=process.env.ORM_OUTPUT_DIR;
if(!out||!process.env.ORM_ADMIN_URL)throw new Error('ORM_OUTPUT_DIR and ORM_ADMIN_URL required');
await mkdir(out); // Refuse reuse so prior failures and samples cannot be mixed.
const log=async(name,row)=>appendFile(`${out}/${name}.jsonl`,JSON.stringify(row)+'\n');
const admin=new pg.Client({connectionString:process.env.ORM_ADMIN_URL});await admin.connect();
const name='v10_orm_'+randomUUID().replaceAll('-','');
const url=new URL(process.env.ORM_ADMIN_URL);url.pathname='/'+name;
let control;
const amount='1234567890123456789012.123456789012345679';
const fixture={tenant:'a',id:1,projectId:1,version:9223372036854775807n,amount,note:null,binary:Buffer.from([0,255,1])};
function norm(row){if(row==null)return null;const result={...row};if('version'in result)result.version=String(result.version);if('amount'in result)result.amount=typeof result.amount?.toFixed==='function'?result.amount.toFixed(18):String(result.amount);if('binary'in result)result.binary=Buffer.from(result.binary).toString('hex');if(result.documents)result.documents=result.documents.map(norm);return result;}
const ddl=`CREATE TABLE projects(tenant TEXT NOT NULL,id INTEGER NOT NULL,title TEXT NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE documents(tenant TEXT NOT NULL,id INTEGER NOT NULL,project_id INTEGER NOT NULL,version BIGINT NOT NULL,amount NUMERIC(40,18) NOT NULL,note TEXT,payload BYTEA NOT NULL,PRIMARY KEY(tenant,id),FOREIGN KEY(tenant,project_id) REFERENCES projects(tenant,id)); CREATE INDEX documents_relation ON documents(tenant,project_id,id)`;
async function seed(){await control.query('TRUNCATE documents,projects');await control.query("INSERT INTO projects VALUES('a',1,'Alpha'),('b',1,'Other'),('a',2,'Empty')");for(const row of [fixture,{...fixture,id:2,version:-9223372036854775808n,note:'',binary:Buffer.alloc(0)},{...fixture,tenant:'b'}])await control.query('INSERT INTO documents VALUES($1,$2,$3,$4,$5,$6,$7)',[row.tenant,row.id,row.projectId,row.version,row.amount,row.note,row.binary]);}
async function oracle(t,id){return norm((await control.query('SELECT tenant,id,project_id AS "projectId",version,amount,note,payload AS binary FROM documents WHERE tenant=$1 AND id=$2',[t,id])).rows[0]);}
try{
 await admin.query(`CREATE DATABASE "${name}"`);control=new pg.Client({connectionString:url.href});await control.connect();await control.query(ddl);
 const server=(await control.query('SELECT version()')).rows[0];
 const runtimePackage=JSON.parse(await readFile(new URL('../package.json',import.meta.url)));
 const sourceHashes={};for(const file of ['main.mjs','providers.mjs','schema.prisma'])sourceHashes[file]=createHash('sha256').update(await readFile(new URL(file,import.meta.url))).digest('hex');
 const catalog=(await control.query("SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position")).rows;
 await writeFile(`${out}/catalog.json`,JSON.stringify(catalog,null,2));
 await writeFile(`${out}/manifest.json`,JSON.stringify({database:name,node:process.version,platform:process.platform,arch:process.arch,server,ddlHash:createHash('sha256').update(ddl).digest('hex'),runtimeLockHash:createHash('sha256').update(await readFile(new URL('../package-lock.json',import.meta.url))).digest('hex'),packages:runtimePackage.dependencies,sourceHashes,poolMax:4,warmups:20,samples:100,profile:'correctness+sequential-smoke',timingClaim:'diagnostic only; not rankings'},null,2));
 let failures=0;let expectedDigest;
 for(const provider of ['neutron','drizzle','prisma','raw-pg']){
  await seed();const audit=[];let db;
  try{
   if(provider==='neutron'){try{const native=await open(provider,url.href,audit,true);try{assert.deepEqual(norm(await native.nativeRelation('a',1)),{tenant:'a',id:1,title:'Alpha',documents:[await oracle('a',1),await oracle('a',2)]});await log('capabilities',{provider,nativeRelation:'PASS'});}finally{await native.close();}}catch(e){await log('capabilities',{provider,nativeRelation:'FAIL',error:e.message});}}
   db=await open(provider,url.href,audit);
   assert.deepEqual(norm(await db.point('a',1)),await oracle('a',1));
   assert.deepEqual(norm(await db.point('a',2)),await oracle('a',2));
   assert.equal(await db.point('a',999),null);assert.deepEqual(norm(await db.point('b',1)),await oracle('b',1));
   assert.deepEqual((await db.list('a',0,1)).map(norm),[await oracle('a',1)]);assert.deepEqual((await db.list('a',1,1)).map(norm),[await oracle('a',2)]);assert.deepEqual(await db.list('a',2,1),[]);
   if(db.nativeRelation){try{assert.deepEqual(norm(await db.nativeRelation('a',1)),{tenant:'a',id:1,title:'Alpha',documents:[await oracle('a',1),await oracle('a',2)]});await log('capabilities',{provider,nativeRelation:'PASS'});}catch(e){await log('capabilities',{provider,nativeRelation:'FAIL',error:e.message});}}
   assert.deepEqual(norm(await db.relation('a',1)),{tenant:'a',id:1,title:'Alpha',documents:[await oracle('a',1),await oracle('a',2)]});
   assert.deepEqual(norm(await db.relation('a',2)),{tenant:'a',id:2,title:'Empty',documents:[]});
   const row={...fixture,id:10,version:9007199254740993n};await db.create(row);assert.deepEqual(await oracle('a',10),norm(row));
   assert.deepEqual((await Promise.all([db.cas('a',10,row.version),db.cas('a',10,row.version)])).sort(),[0,1]);assert.equal((await oracle('a',10)).version,'9007199254740994');
   await assert.rejects(db.rollback({...row,id:11}),/intentional rollback/);assert.equal(await oracle('a',11),null);
   await assert.rejects(async()=>await db.create({...row,id:12,projectId:999}));assert.equal(await oracle('a',12),null);
   assert.equal(await db.remove('a',10),1);assert.equal(await oracle('a',10),null);
   const digest=createHash('sha256').update(JSON.stringify((await control.query('SELECT tenant,id,project_id,version::text,amount::text,note,encode(payload,\'hex\') AS payload FROM documents ORDER BY tenant,id')).rows)).digest('hex');
   if(expectedDigest)assert.equal(digest,expectedDigest);else expectedDigest=digest;
   await log('correctness',{provider,status:'PASS',checks:['point-native-values','null-empty-bytes','tenant-predicate','list-keyset','relations-empty','create','concurrent-cas','rollback','fk-no-mutation','delete'],finalDigest:digest});
   // Fixed diagnostic samples after correctness. No confidence/ranking claims.
   for(let i=0;i<20;i++)await db.point('a',1);
   const expected=await oracle('a',1);const times=[];for(let i=0;i<100;i++){const start=performance.now();let error=null;try{assert.deepEqual(norm(await db.point('a',1)),expected);}catch(e){error={name:e.name,code:e.code,message:e.message};}const ms=performance.now()-start;times.push(ms);await log('operations',{provider,operation:'point+normalization+assertion',sequence:i,ms,error});if(error)throw Error('smoke correctness failed');}
   times.sort((a,b)=>a-b);await log('smoke',{provider,samples:100,p50:times[49],p95:times[94],p99:times[98],errors:0,note:'precomputed independent oracle; sequential fixed order; no speed comparison'});
  }catch(e){failures++;await log('correctness',{provider,status:'FAIL',error:{name:e.name,code:e.code,message:e.message,stack:e.stack}});console.error(provider,e.message);}
  finally{for(const entry of audit)await log('sql-audit',entry);if(db)await db.close();}
 }
 if(failures)process.exitCode=1;
}finally{if(control)await control.end();await admin.query(`DROP DATABASE IF EXISTS "${name}"`);await admin.end();}
