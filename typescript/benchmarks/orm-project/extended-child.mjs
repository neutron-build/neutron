import {performance} from 'node:perf_hooks';
const start=performance.now(),baselineRss=process.memoryUsage().rss;
const {open}=await import('./extended-providers.mjs');
const db=await open(process.env.ORM_PROVIDER,process.env.ORM_DATABASE_URL,null);
try {
 const row=await db.point('a',17);if(!row||row.id!==17||String(row.version)!=='9007199254740993')throw Error('startup first-query mismatch');
 const insideStartupMs=performance.now()-start,readyRss=process.memoryUsage().rss;
 for(let i=0;i<1000;i++){const r=await db.point('a',17);if(!r||r.id!==17||String(r.version)!=='9007199254740993'||(typeof r.amount?.toFixed==='function'?r.amount.toFixed(18):String(r.amount))!=='1234567890123456789012.123456789012345679'||r.note!==null||Buffer.from(r.binary).toString('hex')!=='00ff01')throw Error('memory working-read mismatch');}
 process.stdout.write(JSON.stringify({provider:process.env.ORM_PROVIDER,insideStartupMs,baselineRss,readyRss,workingReads:1000,workingRss:process.memoryUsage().rss,maxRssKiB:process.resourceUsage().maxRSS})+'\n');
}finally{await db.close();}
