#!/usr/bin/env node
// Real packed SDK consumers, outside the checkout. Nothing is published.
// node typescript/scripts/installed-sdk-gate.mjs --package nucleus|data
//   [--out report.local.json] [--keep-tarball dir] [--keep-work]
// Required endpoints: NEUTRON_NUCLEUS_TEST_URL and NEUTRON_TEST_PG_URL.
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {cpSync,existsSync,mkdirSync,mkdtempSync,readFileSync,realpathSync,rmSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const HERE=path.dirname(fileURLToPath(import.meta.url));
const REPO=path.resolve(HERE,'../..');
const args=process.argv.slice(2);
const option=(key,fallback)=>{const at=args.indexOf(key);return at<0?fallback:args[at+1];};
const kind=option('--package','');
assert.ok(['nucleus','data'].includes(kind),'--package nucleus|data is required');
assert.ok([22,24].includes(Number(process.versions.node.split('.')[0])),'run this gate on Node 22 or 24');
const out=option('--out',null),keep=option('--keep-tarball',null);
const versions=option('--ts','5.7.2,5.9.3').split(',');
assert.deepEqual(versions,['5.7.2','5.9.3'],'both pinned supported TypeScript versions are required');
const endpoints={nucleus:process.env.NEUTRON_NUCLEUS_TEST_URL,postgres:process.env.NEUTRON_TEST_PG_URL};
assert.ok(endpoints.nucleus && endpoints.postgres,'both live endpoints are required; this gate never silently skips live checks');
const work=mkdtempSync(path.join(os.tmpdir(),'neutron-installed-sdk-'));
assert.ok(!realpathSync(work).startsWith(realpathSync(REPO)+path.sep),'consumer directory must be outside checkout');
const results=[],tarballs={};
const scope=kind==='nucleus'?'Installed exports/declarations plus bounded eight-family Nucleus operations and PostgreSQL feature refusals. Native vector/FTS parity, durability and concurrent coherence are not certified.':'Installed optional-peer isolation, root declarations and genuine Drizzle application typing/SQL transactions plus bounded native KV/blob adapters. Drizzle upstream strict-library declaration limitations are recorded separately.';
function run(command,argv,cwd,timeout=180000){return execFileSync(command,argv,{cwd,encoding:'utf8',timeout,env:process.env,stdio:['ignore','pipe','pipe']});}
function check(name,fn){try{const detail=fn();results.push({name,pass:true,detail});console.error('PASS '+name);return detail;}catch(error){results.push({name,pass:false,error:String(error),diagnostics:String(error.stdout||'')+String(error.stderr||'')});console.error('FAIL '+name+': '+error);throw error;}}
function pack(which){
 const pkg=path.join(REPO,'typescript/packages/neutron-'+which);
 run('pnpm',['run','build'],pkg);
 const meta=JSON.parse(readFileSync(path.join(pkg,'package.json'),'utf8'));
 run('pnpm',['pack','--pack-destination',work],pkg);
 const tgz=path.join(work,meta.name.replace('@','').replace('/','-')+'-'+meta.version+'.tgz');
 const names=run('tar',['-tzf',tgz],work).trim().split('\n');
 const packed=JSON.parse(run('tar',['-xzOf',tgz,'package/package.json'],work));
 assert.equal(packed.engines.node,'>=22');
 assert.ok(names.every(n=>/^package\/(package.json|README.md|LICENSE(?:\.md)?|dist\/)/.test(n)),'unexpected source/harness payload');
 assert.ok(names.every(n=>!/(?:\.test\.|\.spec\.|\/src\/)/.test(n)),'tests or source files shipped');
 assert.ok(!JSON.stringify(packed).includes('workspace:'),'unresolved workspace dependency');
 for(const targets of Object.values(packed.exports))for(const target of Object.values(targets))assert.ok(names.includes('package/'+target.replace(/^\.\//,'')),'missing export target '+target);
 if(which==='data'){
  const nucleusVersion=JSON.parse(readFileSync(path.join(REPO,'typescript/packages/neutron-nucleus/package.json'),'utf8')).version;
  assert.equal(packed.peerDependencies['@neutron-build/nucleus'],'^'+nucleusVersion);
  for(const name of Object.keys(packed.peerDependencies))assert.equal(packed.peerDependenciesMeta[name].optional,true);
  assert.equal(packed.peerDependencies['drizzle-orm'],'^0.44.5');
  assert.equal(packed.peerDependencies.postgres,'^3.4.7');
  const ownDecl=run('tar',['-xzOf',tgz,'package/dist/db/drizzle.d.ts'],work);
  assert.ok(!ownDecl.includes('from "@libsql/client"'),'own optional SQLite driver declaration leak');
 } else assert.equal(packed.optionalDependencies.pg,'^8.11.0');
 const record={name:packed.name,version:packed.version,engines:packed.engines,peers:packed.peerDependencies,optionalDependencies:packed.optionalDependencies,entries:names.length,exports:packed.exports,sha256:createHash('sha256').update(readFileSync(tgz)).digest('hex')};
 if(keep){mkdirSync(keep,{recursive:true});cpSync(tgz,path.join(keep,path.basename(tgz)));}
 tarballs[which]={file:tgz,record};return record;
}
function consumer(label,deps){
 const dir=path.join(work,label);mkdirSync(dir);
 writeFileSync(path.join(dir,'package.json'),JSON.stringify({name:'installed-'+label,private:true,type:'module',dependencies:deps},null,2));
 run('npm',['install','--engine-strict','--omit=optional','--no-audit','--no-fund'],dir,300000);
 return dir;
}
function fixture(dir,name){cpSync(path.join(HERE,'installed-sdk',name),path.join(dir,name));return name;}
function runtime(dir,file,env={}){
 const output=execFileSync(process.execPath,[file],{cwd:dir,encoding:'utf8',timeout:180000,env:{...process.env,...env},stdio:['ignore','pipe','pipe']});
 const parsed=JSON.parse(output);assert.equal(parsed.failed,0);return parsed;
}
function types(dir,file,{skipLibCheck=false,unsupported=false}={}){
 const reports=[];
 for(const version of versions){
  const tool=path.join(work,'ts-'+version);
  if(!existsSync(tool)){mkdirSync(tool);writeFileSync(path.join(tool,'package.json'),'{"private":true}');run('npm',['install','--engine-strict','--no-audit','--no-fund','--save-exact','typescript@'+version],tool,300000);}
  for(const resolution of ['NodeNext','Bundler']){
   const config={compilerOptions:{target:'ES2022',module:resolution==='NodeNext'?'NodeNext':'ESNext',moduleResolution:resolution,strict:true,skipLibCheck,noEmit:true,types:['node'],extendedDiagnostics:true},files:[file]};
   writeFileSync(path.join(dir,'tsconfig.json'),JSON.stringify(config));
   const start=Date.now();let text='',success=true;
   try{text=run(process.execPath,['--max-old-space-size=768',path.join(tool,'node_modules/typescript/bin/tsc'),'-p','tsconfig.json'],dir,120000);}catch(error){success=false;text=String(error.stdout||'')+String(error.stderr||'');if(!unsupported)throw error;}
   if(unsupported){
    assert.equal(success,false,'documented unsupported declaration profile changed; review and promote rather than silently retaining limitation');
    const errors=text.split('\n').filter(line=>line.includes('error TS'));
    assert.ok(errors.length>0 && errors.every(line=>line.startsWith('node_modules/drizzle-orm/')),'unsupported profile contains application or own SDK errors');
   }
   else assert.equal(success,true);
   const instantiations=Number(/Instantiations:\s+(\d+)/.exec(text)?.[1]);
   const memoryKiB=Number(/Memory used:\s+(\d+)K/.exec(text)?.[1]);
   // Unsupported full third-party declaration diagnostics are heavier than
   // the supported application fixture; keep their cost separately bounded.
   const budget=unsupported?{instantiations:5000000,memoryKiB:655360}:{instantiations:1000000,memoryKiB:262144};
   assert.ok(Number.isFinite(instantiations) && instantiations<=budget.instantiations,'instantiation budget exceeded/missing');
   assert.ok(Number.isFinite(memoryKiB) && memoryKiB<=budget.memoryKiB,'memory budget exceeded/missing');
   reports.push({typescript:version,resolution,strict:true,skipLibCheck,supported:!unsupported,classification:unsupported?'documented third-party/optional declaration refusal':'supported',elapsedMs:Date.now()-start,instantiations,memoryKiB,budget,diagnostics:unsupported?text.slice(0,10000):undefined});
  }
 }
 return reports;
}
let fatal;
try{
 check('pack nucleus',()=>pack('nucleus'));
 if(kind==='data')check('pack data',()=>pack('data'));
 if(kind==='nucleus'){
  const dir=check('clean nucleus engine-strict install',()=>consumer('nucleus',{'@neutron-build/nucleus':'file:'+tarballs.nucleus.file,pg:'8.22.0','@types/node':'22.10.7'}));
  check('nucleus all-export strict declarations',()=>types(dir,fixture(dir,'nucleus-consumer.ts')));
  check('nucleus installed imports/live families/control refusals',()=>runtime(dir,fixture(dir,'nucleus-live.mjs')));
 }else{
  const base=check('clean data optional peers absent install',()=>consumer('data-base',{'@neutron-build/data':'file:'+tarballs.data.file,'@types/node':'22.10.7'}));
  check('data root strict declarations without optional peers',()=>types(base,fixture(base,'data-root-consumer.ts')));
  check('data optional peer absence/runtime refusals',()=>runtime(base,fixture(base,'data-optional.mjs')));
  const deps={'@neutron-build/data':'file:'+tarballs.data.file,postgres:'3.4.8','drizzle-orm':'0.44.7','@libsql/client':'0.17.0','@types/node':'22.10.7'};
  const sql=check('clean data SQL installed peer profile',()=>consumer('data-sql',deps));
  cpSync(path.join(REPO,'typescript/packages/neutron-data/types.consumer/consumer-types.ts'),path.join(sql,'consumer.ts'));
  check('data typed Drizzle application declarations',()=>types(sql,'consumer.ts',{skipLibCheck:true}));
  check('data documented strict-library declaration refusal',()=>types(sql,'consumer.ts',{unsupported:true}));
  check('data installed Drizzle PostgreSQL/SDK-absent profile',()=>runtime(sql,fixture(sql,'data-live.mjs')));
  const native=check('clean data native installed peer profile',()=>consumer('data-native',{...deps,'@neutron-build/nucleus':'file:'+tarballs.nucleus.file,pg:'8.22.0'}));
  check('data installed Drizzle Nucleus/cache/blob/failure surfacing',()=>runtime(native,fixture(native,'data-live.mjs'),{INSTALLED_NATIVE:'1'}));
 }
}catch(error){fatal=String(error);}
const safeEndpoint=value=>{const u=new URL(value);u.password='';u.username='';return u.toString();};
const supportedDeclarationCompilations=results.flatMap(x=>Array.isArray(x.detail)?x.detail:[]).filter(x=>x.supported===true).length;
const unsupportedDeclarationObservations=results.flatMap(x=>Array.isArray(x.detail)?x.detail:[]).filter(x=>x.supported===false).length;
const runtimeChecks=results.filter(x=>x.detail && typeof x.detail.passed==='number').reduce((n,x)=>n+x.detail.passed,0);
const report={runtimeChecks,supportedDeclarationCompilations,unsupportedDeclarationObservations,node:process.version,platform:process.platform,arch:process.arch,package:kind,sourceSha:run('git',['rev-parse','HEAD'],REPO).trim(),sourceTrees:Object.fromEntries(['nucleus','data'].map(k=>[k,run('git',['rev-parse','HEAD:typescript/packages/neutron-'+k],REPO).trim()])),work,endpoints:Object.fromEntries(Object.entries(endpoints).map(([k,v])=>[k,safeEndpoint(v)])),tarballs:Object.fromEntries(Object.entries(tarballs).map(([k,v])=>[k,v.record])),results,passed:results.filter(x=>x.pass).length,failed:results.filter(x=>!x.pass).length,fatal,scope};
if(out){mkdirSync(path.dirname(path.resolve(out)),{recursive:true});writeFileSync(out,JSON.stringify(report,null,2)+'\n');}
if(!args.includes('--keep-work'))rmSync(work,{recursive:true,force:true});
console.log(JSON.stringify({package:kind,passed:report.passed,failed:report.failed,fatal,scope},null,2));
if(fatal || report.failed)process.exitCode=1;
