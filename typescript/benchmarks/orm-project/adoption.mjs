// A small tenant-scoped document service. Its provider boundary is intentionally explicit.
export function documentService(db,tenant) {
 return {create:async row=>{await db.create({...row,tenant});return db.point(tenant,row.id);},
 edit:async(id,expected)=>{if(await db.cas(tenant,id,expected)!==1)return {status:'conflict'};return {status:'updated',document:await db.point(tenant,id)};},
 list:(after=0)=>db.list(tenant,after,20),project:id=>db.relation(tenant,id),
 remove:id=>db.remove(tenant,id),rollback:row=>db.rollback({...row,tenant})};
}
