import pg from 'pg';
import * as n from '@neutron-build/sql';
import * as d from 'drizzle-orm';
import * as c from 'drizzle-orm/pg-core';
import { drizzle } from 'drizzle-orm/node-postgres';
import { PrismaClient } from '../generated/index.js';
import { PrismaPg } from '@prisma/adapter-pg';

export function schemas(lib, core=lib) {
  const binary=core.bytea??core.customType({dataType:()=> 'bytea',fromDriver:v=>v,toDriver:v=>Buffer.from(v)});
  const projects=core.pgTable('projects',{tenant:core.text('tenant').notNull(),id:core.integer('id').notNull(),title:core.text('title').notNull()},t=>[core.primaryKey({columns:[t.tenant,t.id]})]);
  const documents=core.pgTable('documents',{tenant:core.text('tenant').notNull(),id:core.integer('id').notNull(),projectId:core.integer('project_id').notNull(),version:core.bigint('version',{mode:'bigint'}).notNull(),amount:core.numeric('amount',{precision:40,scale:18}).notNull(),note:core.text('note'),binary:binary('payload').notNull()},t=>[core.primaryKey({columns:[t.tenant,t.id]})]);
  const projectRelations=lib.relations(projects,({many})=>({documents:many(documents)}));
  const documentRelations=lib.relations(documents,({one})=>({project:one(projects,{fields:[documents.tenant,documents.projectId],references:[projects.tenant,projects.id]})}));
  return {projects,documents,projectRelations,documentRelations};
}
export async function open(name,url,audit,native=false) {
  if(name==='prisma') {
    const db=new PrismaClient({adapter:new PrismaPg({connectionString:url,max:4}),log:[{emit:'event',level:'query'}]});
    db.$on('query',e=>audit.push({provider:name,sql:e.query,duration:e.duration}));
    return {close:()=>db.$disconnect(),
      point:(tenant,id)=>db.document.findUnique({where:{tenant_id:{tenant,id}}}),
      list:(tenant,after,limit)=>db.document.findMany({where:{tenant,id:{gt:after}},orderBy:{id:'asc'},take:limit}),
      relation:(tenant,id)=>db.project.findUnique({where:{tenant_id:{tenant,id}},include:{documents:{orderBy:{id:'asc'}}}}),
      create:row=>db.document.create({data:row}),
      cas:async(tenant,id,expected)=> (await db.document.updateMany({where:{tenant,id,version:expected},data:{version:{increment:1}}})).count,
      remove:async(tenant,id)=>(await db.document.deleteMany({where:{tenant,id}})).count,
      rollback:row=>db.$transaction(async tx=>{await tx.document.create({data:row});throw new Error('intentional rollback');})};
  }
  const pool=new pg.Pool({connectionString:url,max:4});
  // Observe both pool and checked-out client commands once, at Client.query.
  pool.on('connect',client=>{const original=client.query;client.query=function(...args){audit.push({provider:name,sql:typeof args[0]==='string'?args[0]:args[0]?.text});return original.apply(this,args);};});
  if(name==='raw-pg') return {
    close:()=>pool.end(), point:async(t,id)=>(await pool.query('SELECT tenant,id,project_id AS "projectId",version,amount,note,payload AS binary FROM documents WHERE tenant=$1 AND id=$2',[t,id])).rows[0]??null,
    list:async(t,after,limit)=>(await pool.query('SELECT tenant,id,project_id AS "projectId",version,amount,note,payload AS binary FROM documents WHERE tenant=$1 AND id>$2 ORDER BY id LIMIT $3',[t,after,limit])).rows,
    relation:async(t,id)=>{const p=(await pool.query('SELECT * FROM projects WHERE tenant=$1 AND id=$2',[t,id])).rows[0];if(!p)return null;return {...p,documents:(await pool.query('SELECT tenant,id,project_id AS "projectId",version,amount,note,payload AS binary FROM documents WHERE tenant=$1 AND project_id=$2 ORDER BY id',[t,id])).rows};},
    create:async r=>{await pool.query('INSERT INTO documents VALUES($1,$2,$3,$4,$5,$6,$7)',[r.tenant,r.id,r.projectId,r.version,r.amount,r.note,r.binary]);},
    cas:async(t,id,v)=>(await pool.query('UPDATE documents SET version=version+1 WHERE tenant=$1 AND id=$2 AND version=$3',[t,id,v])).rowCount,
    remove:async(t,id)=>(await pool.query('DELETE FROM documents WHERE tenant=$1 AND id=$2',[t,id])).rowCount,
    rollback:async r=>{const client=await pool.connect();try{await client.query('BEGIN');await client.query('INSERT INTO documents VALUES($1,$2,$3,$4,$5,$6,$7)',[r.tenant,r.id,r.projectId,r.version,r.amount,r.note,r.binary]);throw new Error('intentional rollback');}finally{await client.query('ROLLBACK');client.release();}}};
  const lib=name==='neutron'?n:d;const s=schemas(lib,name==='neutron'?n:c);
  const db=name==='neutron'?await n.createDatabase({url,driverOptions:{driver:'pg',max:4},tables:{projects:s.projects,documents:s.documents},relations:native?{projects:s.projectRelations,documents:s.documentRelations}:undefined,logger:e=>{if(e.kind==='query-end'||e.kind==='query-error')audit.push({provider:name,...e});}}):drizzle(pool,{schema:s});
  if(name==='neutron')await pool.end(); // Neutron owns its public driver pool.
  const where=(t,id)=>lib.and(lib.eq(s.documents.tenant,t),lib.eq(s.documents.id,id));
  return {close:()=>name==='neutron'?db.close():pool.end(),
    point:async(t,id)=>(await db.select().from(s.documents).where(where(t,id)))[0]??null,
    list:(t,after,limit)=>db.select().from(s.documents).where(lib.and(lib.eq(s.documents.tenant,t),lib.gt(s.documents.id,after))).orderBy(lib.asc(s.documents.id)).limit(limit),
    nativeRelation:name==='neutron'&&!native?undefined:async(t,id)=>(await db.query.projects.findFirst({where:lib.and(lib.eq(s.projects.tenant,t),lib.eq(s.projects.id,id)),with:{documents:{orderBy:[lib.asc(s.documents.id)]}}}))??null,
    relation:async(t,id)=>{const parent=(await db.select().from(s.projects).where(lib.and(lib.eq(s.projects.tenant,t),lib.eq(s.projects.id,id))))[0];if(!parent)return null;return {...parent,documents:await db.select().from(s.documents).where(lib.and(lib.eq(s.documents.tenant,t),lib.eq(s.documents.projectId,id))).orderBy(lib.asc(s.documents.id))};},
    create:r=>db.insert(s.documents).values(r).returning(),
    cas:async(t,id,v)=>(await db.update(s.documents).set({version:lib.sql`${s.documents.version} + 1`}).where(lib.and(where(t,id),lib.eq(s.documents.version,v))).returning()).length,
    remove:async(t,id)=>(await db.delete(s.documents).where(where(t,id)).returning()).length,
    rollback:r=>db.transaction(async tx=>{await tx.insert(s.documents).values(r);throw new Error('intentional rollback');})};
}
