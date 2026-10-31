import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
import path from 'node:path';

let native, db, kind;
try {
  const request=JSON.parse(readFileSync(0,'utf8'));
  if(request.protocol!=='polyglot-performance-v1'||!/^neutron_polyglot_[0-9a-f]{32}$/.test(request.schema_scope)||!['raw','orm'].includes(request.mode)||!['read','transaction'].includes(request.workload)||request.warmup!==1024||request.iterations!==2048) throw Error('frozen workload required');
  const entry=path.resolve(process.argv[process.argv.indexOf('--module')+1]);
  kind=process.argv.includes('--postgres-js')?'postgres':'pg';
  const require=createRequire(entry);
  const orm=await import(pathToFileURL(entry).href);
  let driverDirectory=path.dirname(require.resolve(kind));
  let driverVersion;
  while(driverDirectory!==path.dirname(driverDirectory)) {
    try {const metadata=JSON.parse(readFileSync(path.join(driverDirectory,'package.json'),'utf8'));if(metadata.name===kind){driverVersion=metadata.version;break;}} catch {}
    driverDirectory=path.dirname(driverDirectory);
  }
  if(!driverVersion) throw Error('driver identity missing');
  let count=0;
  if(kind==='pg') {
    const {Pool}=require('pg');
    native=new Pool({connectionString:process.env.NEUTRON_TEST_DATABASE_URL,max:1});
    native.on('connect',client=>{
      const query=client.query.bind(client);
      client.query=(...args)=>{count++;return query(...args);};
    });
  } else {
    const postgres=require('postgres');
    native=postgres(process.env.NEUTRON_TEST_DATABASE_URL,{max:1,prepare:false,debug(){count++;}});
  }
  const driver=kind==='pg'?orm.wrapPgPool(native):orm.wrapPostgresJs(native);
  const table=orm.pgSchema(request.schema_scope).table('perf_fixture',{
    id:orm.integer('id').primaryKey(),big:orm.bigint('big',{mode:'string'}),body:orm.text('body')});
  db=await orm.createDatabase({driver,tables:{fixture:table}});
  const select=`SELECT id,big,body FROM "${request.schema_scope}".perf_fixture WHERE id=$1`;
  const update=`UPDATE "${request.schema_scope}".perf_fixture SET body=$1 WHERE id=$2`;
  async function step(i,phase) {
    const key=i%64+1,marker=phase+':'+i;
    let row;
    if(request.mode==='orm') {
      const body=async executor=>{
        const rows=await executor.select({id:table.id,big:table.big,body:table.body}).from(table).where(orm.eq(table.id,key));
        if(rows.length!==1) throw Error('read cardinality');
        row=rows[0];
        if(request.workload==='transaction') await executor.update(table).set({body:marker}).where(orm.eq(table.id,key));
      };
      if(request.workload==='transaction') await db.transaction(body,{isolation:'read-committed'});
      else await body(db);
    } else {
      const client=kind==='pg'?await native.connect():await native.reserve();
      const query=async(text,values=[])=>kind==='pg'?(await client.query(text,values)).rows:await client.unsafe(text,values);
      try {
        if(request.workload==='transaction') await query('BEGIN ISOLATION LEVEL READ COMMITTED');
        const rows=await query(select,[key]);
        if(rows.length!==1) throw Error('read cardinality');
        row=rows[0];
        if(request.workload==='transaction'){await query(update,[marker,key]);await query('COMMIT');}
      } catch(error) {
        if(request.workload==='transaction') await query('ROLLBACK');
        throw error;
      } finally {client.release();}
    }
    if(row.id!==key||String(row.big)!==String(9007199254740993n+BigInt(key))||typeof row.body!=='string') throw Error('point oracle mismatch');
    return BigInt(row.big);
  }
  for(let i=0;i<request.warmup;i++) await step(i,'warmup');
  count=0;
  const before=process.memoryUsage();
  const start=process.hrtime.bigint();let checksum=0n;
  for(let i=0;i<request.iterations;i++) checksum+=await step(i,'measure');
  const elapsed=process.hrtime.bigint()-start;
  const expected=request.iterations*(request.workload==='transaction'?4:1);
  if(count!==expected) throw Error('unexpected native dispatch count');
  console.log(JSON.stringify({...request,checksum:String(checksum),elapsed_ns:Number(elapsed),query_count:count,
    query_count_scope:kind==='pg'?'native client.query dispatches':'native postgres.js debug dispatches',
    memory:{before,after:process.memoryUsage(),peak_rss_bytes:process.resourceUsage().maxRSS*1024},
    runtime:process.version,driver_version:driverVersion}));
} catch {
  console.log(JSON.stringify({status:'fail',diagnostics:'installed TypeScript performance consumer failed'}));
  process.exitCode=1;
} finally {
  try {if(db) await db.close();if(native) await (kind==='pg'?native.end():native.end({timeout:5}));}
  catch {process.exitCode=1;}
}
