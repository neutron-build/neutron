
import assert from 'node:assert/strict';
import {createClient,PgTransport,withSQL,withKV,withDocument,withGraph,withTimeSeries,withColumnar,withGeo,withBlob,withVector,withFTS,NucleusFeatureError} from '@neutron-build/nucleus';
const subpaths = ['','/sql','/kv','/vector','/document','/graph','/fts','/geo','/blob','/timeseries','/streams','/columnar','/datalog','/cdc','/pubsub'];
const results=[];
for (const sub of subpaths) { const mod=await import('@neutron-build/nucleus'+sub); assert.ok(Object.keys(mod).length>0); results.push({name:'import'+(sub||'/'),status:'passed',exports:Object.keys(mod)}); }
const name='installed_'+process.versions.node.split('.')[0]+'_'+Date.now();
const url=process.env.NEUTRON_NUCLEUS_TEST_URL;
assert.ok(url);
const transport=new PgTransport(url,{max:1});
const db=await createClient({url,transport}).use(withSQL).use(withKV).use(withDocument).use(withGraph).use(withTimeSeries).use(withColumnar).use(withGeo).use(withBlob).connect();
async function check(label, fn) { try { const actual=await fn(); results.push({name:label,status:'passed',actual}); } catch(e) {results.push({name:label,status:'failed',error:String(e),code:e.code});} }
try {
await check('engine identity',async()=>{assert.equal(db.features.isNucleus,true);return db.features;});
await check('SQL bound write/read',async()=>{await db.sql.execute('CREATE TABLE '+name+' (id INT, value TEXT)');assert.equal(await db.sql.execute('INSERT INTO '+name+' VALUES ($1::int,$2)',7,'packed unicode 世界'),1);const rows=await db.sql.query('SELECT id,value FROM '+name);assert.deepEqual(rows,[{id:7,value:'packed unicode 世界'}]);return rows;});
await check('KV typed roundtrip',async()=>{const expected={v:"quotes ' 世界",n:7};await db.kv.setTyped(name,expected);const actual=await db.kv.getTyped(name);assert.deepEqual(actual,expected);return actual;});
await check('document scoped roundtrip',async()=>{const expected={label:name,n:7};const id=await db.document.insert(name,expected);const actual=await db.document.getIn(name,id);assert.deepEqual(actual,expected);return {id,actual};});
await check('graph adjacency',async()=>{const a=await db.graph.addNode(['InstalledConsumer'],{run:name});const b=await db.graph.addNode(['InstalledConsumer'],{run:name});const edge=await db.graph.addEdge(a,b,'INSTALLED_LINK');const rows=await db.graph.neighbors(a,'INSTALLED_LINK');assert.deepEqual(rows,[{neighborId:b,edgeId:edge,edgeType:'INSTALLED_LINK'}]);return {a,b,edge,rows};});
await check('time series count/last',async()=>{const now=Date.now();await db.timeseries.write(name,[{timestamp:new Date(now),value:2},{timestamp:new Date(now+1),value:6}]);const count=await db.timeseries.count(name),last=await db.timeseries.last(name);assert.equal(count,2);assert.equal(last,6);return {count,last};});
await check('columnar typed aggregates',async()=>{assert.equal(await db.columnar.insert(name,{v:2}),true);assert.equal(await db.columnar.insert(name,{v:6}),true);const count=await db.columnar.count(name),sum=await db.columnar.sum(name,'v'),avg=await db.columnar.avg(name,'v');assert.equal(count,2);assert.equal(sum,8);assert.equal(avg,4);return {count,sum,avg};});
await check('geo haversine hand oracle',async()=>{const actual=await db.geo.distance({lat:0,lon:0},{lat:0,lon:1});const expected=6371000*Math.PI/180;assert.ok(Math.abs(actual-expected)<1e-6);return {actual,expected};});
await check('blob byte roundtrip',async()=>{const expected=Uint8Array.from([0,1,127,128,255]);await db.blob.put(name,'bytes',expected,{contentType:'application/octet-stream'});const actual=await db.blob.get(name,'bytes');assert.ok(actual);assert.deepEqual([...actual.data],[...expected]);return {bytes:[...actual.data],meta:actual.meta};});
} finally { try { await db.sql.execute('DROP TABLE IF EXISTS '+name); } finally { await db.close(); } }
// Feature refusal on a real PostgreSQL control is separate from Nucleus
// specialty success; vector and FTS imports do not establish their parity.
const controlUrl=process.env.NEUTRON_TEST_PG_URL;
assert.ok(controlUrl);
const control=await createClient({url:controlUrl,transport:new PgTransport(controlUrl,{max:1})}).use(withKV).use(withVector).use(withFTS).connect();
try {
 await check('PostgreSQL identity',async()=>{assert.equal(control.features.isNucleus,false);return control.features;});
 await check('PostgreSQL native KV refuses',async()=>{await assert.rejects(()=>control.kv.set(name,'v'),NucleusFeatureError);return 'feature refusal';});
 await check('PostgreSQL native vector refuses',async()=>{await assert.rejects(()=>control.vector.createCollection(name,3),NucleusFeatureError);return 'feature refusal';});
 await check('PostgreSQL native FTS refuses',async()=>{await assert.rejects(()=>control.fts.index(1,'text'),NucleusFeatureError);return 'feature refusal';});
} finally {await control.close();}
const failed=results.filter(r=>r.status==='failed');
console.log(JSON.stringify({node:process.version,run:name,results,passed:results.length-failed.length,failed:failed.length,scope:'15 installed subpath imports; eight bounded Nucleus families; PostgreSQL native-feature refusals. Native vector/FTS behavior, concurrent coherence, durability and universal parity are outside this smoke gate.'},null,2));
if(failed.length) process.exitCode=1;
