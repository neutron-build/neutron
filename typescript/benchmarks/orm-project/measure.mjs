import assert from 'node:assert/strict';
import {mkdir,writeFile,readFile} from 'node:fs/promises';
import {createHash,randomUUID} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import os from 'node:os';
import pg from 'pg';
import {open} from './providers.mjs';

const out=process.env.ORM_OUTPUT_DIR;
if(!out||!process.env.ORM_ADMIN_URL)throw Error('ORM_OUTPUT_DIR and ORM_ADMIN_URL required');
function setting(name,fallback,min,max){const text=process.env[name]??String(fallback);if(!/^\d+$/.test(text))throw Error(`Invalid ${name}`);const value=Number(text);if(value<min||value>max)throw Error(`${name} outside ${min}..${max}`);return value;}
const trials=setting('ORM_TRIALS',4,4,20);
if(trials%4)throw Error('ORM_TRIALS must be a multiple of four for balanced provider order');
const samples=setting('ORM_SAMPLES',1000,100,10000),warmups=setting('ORM_WARMUPS',100,20,1000);
const providers=['neutron','drizzle','prisma','raw-pg'];
const amount='1234567890123456789012.123456789012345679';
function norm(row){if(row==null)return null;if(Array.isArray(row))return row.map(norm);const r={...row};if('version'in r)r.version=String(r.version);if('amount'in r)r.amount=typeof r.amount?.toFixed==='function'?r.amount.toFixed(18):String(r.amount);if('binary'in r)r.binary=Buffer.from(r.binary).toString('hex');if(r.documents)r.documents=r.documents.map(norm);return r;}
const row=(id)=>({tenant:'a',id,projectId:Math.ceil(id/20),version:'9007199254740993',amount,note:id%2?'':null,binary:'00ff01'});
const cases={
 point:{invoke:(db,i)=>db.point('a',i%2000+1),expected:i=>row(i%2000+1)},
 list:{invoke:(db,i)=>db.list('a',(i%99)*20,20),expected:i=>Array.from({length:20},(_,j)=>row((i%99)*20+j+1))},
 relation:{invoke:(db,i)=>db.relation('a',i%100+1),expected:i=>({tenant:'a',id:i%100+1,title:`Project ${i%100+1}`,documents:Array.from({length:20},(_,j)=>row((i%100)*20+j+1))})},
};
const ddl=`CREATE TABLE projects(tenant TEXT NOT NULL,id INTEGER NOT NULL,title TEXT NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE documents(tenant TEXT NOT NULL,id INTEGER NOT NULL,project_id INTEGER NOT NULL,version BIGINT NOT NULL,amount NUMERIC(40,18) NOT NULL,note TEXT,payload BYTEA NOT NULL,PRIMARY KEY(tenant,id),FOREIGN KEY(tenant,project_id) REFERENCES projects(tenant,id)); CREATE INDEX documents_relation ON documents(tenant,project_id,id)`;
await mkdir(out); // A fresh directory preserves failures and run identity.
const admin=new pg.Client({connectionString:process.env.ORM_ADMIN_URL});await admin.connect();
const name='v10_orm_'+randomUUID().replaceAll('-',''),url=new URL(process.env.ORM_ADMIN_URL);url.pathname='/'+name;
let control,created=false;const results=[],operations=[];
try{
 await admin.query(`CREATE DATABASE "${name}"`);created=true;
 control=new pg.Client({connectionString:url.href});await control.connect();await control.query(ddl);
 await control.query("INSERT INTO projects SELECT t,g,'Project '||g FROM unnest(ARRAY['a','b']) t CROSS JOIN generate_series(1,100) g");
 await control.query("INSERT INTO documents SELECT t,g,((g-1)/20)+1,9007199254740993,$1::numeric,CASE WHEN g%2=1 THEN '' ELSE NULL END,decode('00ff01','hex') FROM unnest(ARRAY['a','b']) t CROSS JOIN generate_series(1,2000) g",[amount]);
 await control.query('ANALYZE projects;ANALYZE documents');
 const oracle=norm((await control.query('SELECT tenant,id,project_id AS "projectId",version,amount,note,payload AS binary FROM documents WHERE tenant=$1 AND id=$2',['a',1])).rows[0]);assert.deepEqual(oracle,row(1));
 const before=(await control.query("SELECT md5(string_agg(ROW(tenant,id,project_id,version,amount,note,encode(payload,'hex'))::text,',' ORDER BY tenant,id)) AS digest FROM documents")).rows[0].digest;
 const pkg=JSON.parse(await readFile(new URL('../package.json',import.meta.url))),sourceHashes={};
 for(const file of ['measure.mjs','providers.mjs','schema.prisma'])sourceHashes[file]=createHash('sha256').update(await readFile(new URL(file,import.meta.url))).digest('hex');
 const manifest={profile:'readonly-balanced-trials',node:process.version,platform:process.platform,arch:process.arch,cpu:os.cpus()[0]?.model,cpuCount:os.cpus().length,totalMemory:os.totalmem(),server:(await control.query('SELECT version()')).rows[0],serverSettings:(await control.query("SELECT name,setting,unit FROM pg_catalog.pg_settings WHERE name IN ('shared_buffers','work_mem','max_connections','jit','TimeZone','server_encoding') ORDER BY name")).rows,packages:pkg.dependencies,neutronFixture:pkg.neutronFixture??{kind:'published',version:'0.1.0'},sourceHashes,runtimeLockHash:createHash('sha256').update(await readFile(new URL('../package-lock.json',import.meta.url))).digest('hex'),ddlHash:createHash('sha256').update(ddl).digest('hex'),trials,samples,warmups,poolMax:4,concurrency:[1,4],tenants:2,projectsPerTenant:100,documentsPerTenant:2000,rowsPerRelation:20,providerOrder:'cyclic Latin order; each provider occupies each position equally',latency:'public API resolution only; normalization/assertion/file output excluded',throughput:'completed validated calls / wall duration, includes dispatch and validation',limits:'single local machine and PostgreSQL container; warm read-only data; no write or production capacity claim; no universal ranking'};
 await writeFile(`${out}/manifest.json`,JSON.stringify(manifest,null,2));
 // Audited preflight and timings use different instances. No query hooks/logging during timing.
 for(const provider of providers){const audit=[];const db=await open(provider,url.href,audit);try{for(const [operation,test]of Object.entries(cases))assert.deepEqual(norm(await test.invoke(db,0)),test.expected(0));}finally{await db.close();}await writeFile(`${out}/preflight-${provider}.json`,JSON.stringify(audit,null,2));}
 for(let trial=0;trial<trials;trial++){
  const order=providers.map((_,i)=>providers[(i+trial)%4]);
  for(const provider of order){const db=await open(provider,url.href,null);try{
   for(const [operation,test]of Object.entries(cases))for(const concurrency of [1,4]){
    for(let i=0;i<warmups;i++)assert.deepEqual(norm(await test.invoke(db,i)),test.expected(i));
    let next=0,errors=0;const times=[],records=[];const wall=performance.now();
    await Promise.all(Array.from({length:concurrency},async()=>{while(next<samples){const sequence=next++;let value,error=null;const start=performance.now();try{value=await test.invoke(db,sequence);}catch(e){error={name:e.name,code:e.code,message:e.message};}const ms=performance.now()-start;
     if(!error)try{assert.deepEqual(norm(value),test.expected(sequence));}catch(e){error={name:e.name,code:e.code,message:e.message};}
     if(error)errors++;times.push(ms);records.push({trial,provider,operation,concurrency,sequence,ms,error});
    }}));
    const wallMs=performance.now()-wall;times.sort((a,b)=>a-b);const percentile=p=>times[Math.ceil(p*times.length)-1];
    results.push({trial,provider,operation,concurrency,samples,errors,wallMs,callsPerSecond:samples/(wallMs/1000),p50:percentile(.5),p95:percentile(.95),p99:percentile(.99)});operations.push(...records);
    // Write after the measured phase; preserve raw failures before raising.
    await writeFile(`${out}/results.json`,JSON.stringify(results,null,2));
    await writeFile(`${out}/operations.jsonl`,operations.map(r=>JSON.stringify(r)).join('\n')+'\n');
    if(errors)throw Error(`${provider}/${operation}: ${errors} failures`);
   }
  }finally{await db.close();}}
 }
 const after=(await control.query("SELECT md5(string_agg(ROW(tenant,id,project_id,version,amount,note,encode(payload,'hex'))::text,',' ORDER BY tenant,id)) AS digest FROM documents")).rows[0].digest;assert.equal(after,before);
 const summary=[];for(const provider of providers)for(const operation of Object.keys(cases))for(const concurrency of [1,4]){const rows=results.filter(r=>r.provider===provider&&r.operation===operation&&r.concurrency===concurrency);const range=key=>{const values=rows.map(r=>r[key]).sort((a,b)=>a-b);return {min:values[0],median:(values[Math.floor((values.length-1)/2)]+values[Math.floor(values.length/2)])/2,max:values.at(-1)};};summary.push({provider,operation,concurrency,trials:rows.length,errors:rows.reduce((n,r)=>n+r.errors,0),callsPerSecond:range('callsPerSecond'),p50:range('p50'),p95:range('p95'),p99:range('p99')});}
 await writeFile(`${out}/summary.json`,JSON.stringify({status:'PASS',unchangedStateDigest:after,summary,limits:manifest.limits},null,2));
}finally{if(control)await control.end();if(created)await admin.query(`DROP DATABASE "${name}" WITH (FORCE)`);await admin.end();}
