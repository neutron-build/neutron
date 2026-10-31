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
    const {PrismaClient}=await import('../generated/index.js'); const {PrismaPg}=await import('@prisma/adapter-pg');
    const db=new PrismaClient({adapter:new PrismaPg({connectionString:url,max:4}),log:audit?[{emit:'event',level:'query'}]:[]});
    if(audit)db.$on('query',e=>audit.push({provider:name,sql:e.query,duration:e.duration}));
    return {cycle:r=>db.$transaction(async tx=>{await tx.document.create({data:r}); const changed=await tx.document.updateMany({where:{tenant:r.tenant,id:r.id,version:r.version},data:{version:{increment:1}}}); if(changed.count!==1)throw Error('CAS failed'); const result=await tx.document.findUnique({where:{tenant_id:{tenant:r.tenant,id:r.id}}}); await tx.document.delete({where:{tenant_id:{tenant:r.tenant,id:r.id}}});return result;}),close:()=>db.$disconnect(),
      point:(tenant,id)=>db.document.findUnique({where:{tenant_id:{tenant,id}}}),
      list:(tenant,after,limit)=>db.document.findMany({where:{tenant,id:{gt:after}},orderBy:{id:'asc'},take:limit}),
      relation:(tenant,id)=>db.project.findUnique({where:{tenant_id:{tenant,id}},include:{documents:{orderBy:{id:'asc'}}}}),
      create:row=>db.document.create({data:row}),
      cas:async(tenant,id,expected)=> (await db.document.updateMany({where:{tenant,id,version:expected},data:{version:{increment:1}}})).count,
      remove:async(tenant,id)=>(await db.document.deleteMany({where:{tenant,id}})).count,
      rollback:row=>db.$transaction(async tx=>{await tx.document.create({data:row});throw new Error('intentional rollback');})};
  }
  const {default:pg}=await import('pg');
  const pool=new pg.Pool({connectionString:url,max:4});
  // Observe both pool and checked-out client commands once, at Client.query.
  if(audit)pool.on('connect',client=>{const original=client.query;client.query=function(...args){audit.push({provider:name,sql:typeof args[0]==='string'?args[0]:args[0]?.text});return original.apply(this,args);};});
  if(name==='raw-pg') return {
    cycle:async r=>{const client=await pool.connect();try{await client.query('BEGIN');await client.query('INSERT INTO documents VALUES($1,$2,$3,$4,$5,$6,$7)',[r.tenant,r.id,r.projectId,r.version,r.amount,r.note,r.binary]);const changed=await client.query('UPDATE documents SET version=version+1 WHERE tenant=$1 AND id=$2 AND version=$3',[r.tenant,r.id,r.version]);if(changed.rowCount!==1)throw Error('CAS failed');const result=(await client.query('SELECT tenant,id,project_id AS "projectId",version,amount,note,payload AS binary FROM documents WHERE tenant=$1 AND id=$2',[r.tenant,r.id])).rows[0];await client.query('DELETE FROM documents WHERE tenant=$1 AND id=$2',[r.tenant,r.id]);await client.query('COMMIT');return result;}catch(e){await client.query('ROLLBACK');throw e;}finally{client.release();}},
    close:()=>pool.end(), point:async(t,id)=>(await pool.query('SELECT tenant,id,project_id AS "projectId",version,amount,note,payload AS binary FROM documents WHERE tenant=$1 AND id=$2',[t,id])).rows[0]??null,
    list:async(t,after,limit)=>(await pool.query('SELECT tenant,id,project_id AS "projectId",version,amount,note,payload AS binary FROM documents WHERE tenant=$1 AND id>$2 ORDER BY id LIMIT $3',[t,after,limit])).rows,
    relation:async(t,id)=>{const p=(await pool.query('SELECT * FROM projects WHERE tenant=$1 AND id=$2',[t,id])).rows[0];if(!p)return null;return {...p,documents:(await pool.query('SELECT tenant,id,project_id AS "projectId",version,amount,note,payload AS binary FROM documents WHERE tenant=$1 AND project_id=$2 ORDER BY id',[t,id])).rows};},
    create:async r=>{await pool.query('INSERT INTO documents VALUES($1,$2,$3,$4,$5,$6,$7)',[r.tenant,r.id,r.projectId,r.version,r.amount,r.note,r.binary]);},
    cas:async(t,id,v)=>(await pool.query('UPDATE documents SET version=version+1 WHERE tenant=$1 AND id=$2 AND version=$3',[t,id,v])).rowCount,
    remove:async(t,id)=>(await pool.query('DELETE FROM documents WHERE tenant=$1 AND id=$2',[t,id])).rowCount,
    rollback:async r=>{const client=await pool.connect();try{await client.query('BEGIN');await client.query('INSERT INTO documents VALUES($1,$2,$3,$4,$5,$6,$7)',[r.tenant,r.id,r.projectId,r.version,r.amount,r.note,r.binary]);throw new Error('intentional rollback');}finally{await client.query('ROLLBACK');client.release();}}};
  const lib=await import(name==='neutron'?'@neutron-build/sql':'drizzle-orm');const n=lib;const c=name==='neutron'?lib:await import('drizzle-orm/pg-core');const {drizzle}=name==='neutron'?{}:await import('drizzle-orm/node-postgres');const s=schemas(lib,c);
  const db=name==='neutron'?await n.createDatabase({url,driverOptions:{driver:'pg',max:4},tables:{projects:s.projects,documents:s.documents},relations:native?{projects:s.projectRelations,documents:s.documentRelations}:undefined,logger:audit?e=>{if(e.kind==='query-end'||e.kind==='query-error')audit.push({provider:name,...e});}:undefined}):drizzle(pool,{schema:s});
  if(name==='neutron')await pool.end(); // Neutron owns its public driver pool.
  const where=(t,id)=>lib.and(lib.eq(s.documents.tenant,t),lib.eq(s.documents.id,id));
  return {cycle:r=>db.transaction(async tx=>{await tx.insert(s.documents).values(r);const changed=await tx.update(s.documents).set({version:lib.sql`${s.documents.version} + 1`}).where(lib.and(where(r.tenant,r.id),lib.eq(s.documents.version,r.version))).returning();if(changed.length!==1)throw Error('CAS failed');const result=(await tx.select().from(s.documents).where(where(r.tenant,r.id)))[0];await tx.delete(s.documents).where(where(r.tenant,r.id));return result;}),close:()=>name==='neutron'?db.close():pool.end(),
    point:async(t,id)=>(await db.select().from(s.documents).where(where(t,id)))[0]??null,
    list:(t,after,limit)=>db.select().from(s.documents).where(lib.and(lib.eq(s.documents.tenant,t),lib.gt(s.documents.id,after))).orderBy(lib.asc(s.documents.id)).limit(limit),
    nativeRelation:name==='neutron'&&!native?undefined:async(t,id)=>(await db.query.projects.findFirst({where:lib.and(lib.eq(s.projects.tenant,t),lib.eq(s.projects.id,id)),with:{documents:{orderBy:[lib.asc(s.documents.id)]}}}))??null,
    relation:async(t,id)=>{const parent=(await db.select().from(s.projects).where(lib.and(lib.eq(s.projects.tenant,t),lib.eq(s.projects.id,id))))[0];if(!parent)return null;return {...parent,documents:await db.select().from(s.documents).where(lib.and(lib.eq(s.documents.tenant,t),lib.eq(s.documents.projectId,id))).orderBy(lib.asc(s.documents.id))};},
    create:r=>db.insert(s.documents).values(r).returning(),
    cas:async(t,id,v)=>(await db.update(s.documents).set({version:lib.sql`${s.documents.version} + 1`}).where(lib.and(where(t,id),lib.eq(s.documents.version,v))).returning()).length,
    remove:async(t,id)=>(await db.delete(s.documents).where(where(t,id)).returning()).length,
    rollback:r=>db.transaction(async tx=>{await tx.insert(s.documents).values(r);throw new Error('intentional rollback');})};
}
