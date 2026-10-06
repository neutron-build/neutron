import {performance} from 'node:perf_hooks';
const start=performance.now(),baselineRss=process.memoryUsage().rss;
const {open}=await import('./extended-providers.mjs');
const db=await open(process.env.ORM_PROVIDER,process.env.ORM_DATABASE_URL,null);
function verify(r){if(!r||r.tenant!=='a'||r.id!==17||r.projectId!==1||String(r.version)!=='9007199254740993'||(typeof r.amount?.toFixed==='function'?r.amount.toFixed(18):String(r.amount))!=='1234567890123456789012.123456789012345679'||r.note!==null||Buffer.from(r.binary).toString('hex')!=='00ff01')throw Error('exact working-read mismatch');}
try {
 verify(await db.point('a',17));
 const insideStartupMs=performance.now()-start,readyRss=process.memoryUsage().rss;
 process.stdout.write(JSON.stringify({kind:'ready',provider:process.env.ORM_PROVIDER,insideStartupMs,baselineRss,readyRss})+'\n');
 for(let i=0;i<1000;i++)verify(await db.point('a',17));
 process.stdout.write(JSON.stringify({kind:'working',provider:process.env.ORM_PROVIDER,insideStartupMs,baselineRss,readyRss,workingReads:1000,workingRss:process.memoryUsage().rss,maxRssKiB:process.resourceUsage().maxRSS})+'\n');
}finally{await db.close();}
